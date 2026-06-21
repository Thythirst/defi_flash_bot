"""
Multi-block Pre-compute Engine.

Builds pre-encoded calldata for liquidation bundles — both direct
(liquidateCall) and flash-loan-assisted (borrow debt → liquidate → repay).
These are stored in PostgreSQL and consumed by the mempool trigger for
same-block execution.

Bundle types:
  - 'direct_liquidation': liquidateCall() directly (bot has sufficient capital)
  - 'flash_loan_liquidation': flashLoan + liquidateCall + repay (no capital needed)

The calldata is pre-ABI-encoded, ready to submit to the FlashExecutor contract
without any node calls — just sign and broadcast.

Contract addresses:
  - Aave V3 Pool (Arbitrum):    0x794a61358D6845594F94dc1Db02A252b5b4814ad
  - FlashExecutorV3:            0x4CDaDED000000000000000000000000000000000
  - Balancer Vault:             0xBA12222222228d8Ba445958a75a0704d566BF2C8
"""

from __future__ import annotations

import json
import logging
import uuid
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from eth_abi import encode as abi_encode
from eth_utils import keccak

logger = logging.getLogger(__name__)

# ── Contract addresses (Arbitrum One) ────────────────────────────────────

AAVE_V3_POOL = "0x794a61358D6845594F94dc1Db02A252b5b4814ad"
FLASH_EXECUTOR_V3 = "0x83d60B7DE4334Fd34492E18cA95B2b9e47F00D80"
BALANCER_VAULT = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"

# Aave V3 Pool ABI (minimal, for ABI encoding)
LIQUIDATION_CALL_SELECTOR = keccak(text="liquidationCall(address,address,address,uint256,bool)")[:4]

# FlashExecutorV3 entry points
# executeLiquidation: flash-loan path (Balancer V2)
EXECUTE_LIQUIDATION_SELECTOR = keccak(
    text="executeLiquidation(address,address,address,uint256,bool,address,bytes)"
)[:4]
# executeLiquidationDirect: pre-funded path (no flash loan)
EXECUTE_LIQUIDATION_DIRECT_SELECTOR = keccak(
    text="executeLiquidationDirect(address,address,address,uint256,bool,address,bytes)"
)[:4]


class PrecomputeEngine:
    """
    Builds pre-encoded calldata bundles for liquidation execution.
    
    Two paths:
    1. Direct liquidation — bot holds the debt asset already
    2. Flash loan liquidation — borrow debt asset via flash loan, liquidate, repay
    """

    def __init__(self, pg_pool, redis_client=None):
        self.pg = pg_pool
        self.redis = redis_client

    async def build_bundle(
        self,
        user_addr: str,
        debt_asset_addr: str,
        debt_asset_symbol: str,
        coll_asset_addr: str,
        coll_asset_symbol: str,
        debt_to_cover_wei: int,
        trigger_feed: str,
        expected_profit_usd: Decimal,
        priority: int = 1,
        use_flash_loan: bool = True,
    ) -> Optional[str]:
        """
        Build and store a pre-computed liquidation bundle.
        Returns bundle_id if successful.
        """
        if use_flash_loan:
            calldata, contract_addr, value = self._build_flash_loan_bundle(
                user_addr, debt_asset_addr, coll_asset_addr, debt_to_cover_wei
            )
            bundle_type = "flash_loan_liquidation"
        else:
            calldata, contract_addr, value = self._build_direct_liquidation(
                user_addr, debt_asset_addr, coll_asset_addr, debt_to_cover_wei
            )
            bundle_type = "direct_liquidation"

        bundle_id = uuid.uuid4()

        try:
            async with self.pg.acquire() as conn:
                await conn.execute(
                    """INSERT INTO precomputed_bundles
                       (bundle_id, user_addr, trigger_feed, bundle_type,
                        debt_asset, debt_asset_addr, coll_asset, coll_asset_addr,
                        debt_to_cover, contract_addr, calldata, value_wei,
                        expected_profit_usd, priority, flash_loan_asset,
                        flash_loan_amount, flash_loan_pool)
                       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)""",
                    bundle_id,
                    user_addr,
                    trigger_feed,
                    bundle_type,
                    debt_asset_symbol,
                    debt_asset_addr,
                    coll_asset_symbol,
                    coll_asset_addr,
                    debt_to_cover_wei,
                    contract_addr,
                    "0x" + calldata.hex(),
                    value,
                    expected_profit_usd,
                    priority,
                    debt_asset_symbol if use_flash_loan else None,
                    debt_to_cover_wei if use_flash_loan else None,
                    BALANCER_VAULT if use_flash_loan else None,
                )
        except Exception:
            logger.exception("Failed to store bundle for %s", user_addr)
            return None

        # Also cache in Redis for ultra-fast lookup
        if self.redis:
            bundle_data = json.dumps({
                "bundle_id": str(bundle_id),
                "user_addr": user_addr,
                "trigger_feed": trigger_feed,
                "contract_addr": contract_addr,
                "calldata": "0x" + calldata.hex(),
                "value_wei": str(value),
                "expected_profit_usd": str(expected_profit_usd),
                "priority": priority,
                "bundle_type": bundle_type,
            })
            await self.redis.hset(
                f"precompute:bundle:{trigger_feed}", user_addr, bundle_data
            )
            await self.redis.expire(f"precompute:bundle:{trigger_feed}", 120)

        logger.info(
            "Bundle built: %s %s user=%s debt=%s(%d) coll=%s profit=$%.0f",
            bundle_type, str(bundle_id)[:8], user_addr[:10],
            debt_asset_symbol, debt_to_cover_wei, coll_asset_symbol,
            float(expected_profit_usd),
        )
        return str(bundle_id)

    # ── Calldata builders ───────────────────────────────────────────────

    def _build_direct_liquidation(
        self, user: str, debt_asset: str, coll_asset: str, debt_to_cover: int
    ) -> Tuple[bytes, str, int]:
        """
        Build calldata for FlashExecutorV3.executeLiquidationDirect().
        Pre-funded path — bot holds debt asset, no flash loan needed.
        Swap calldata left empty; execution engine fills it at runtime.
        """
        # Encode: executeLiquidationDirect(collAsset, debtAsset, borrower, debtToCover, false, router, swapCalldata)
        calldata = EXECUTE_LIQUIDATION_DIRECT_SELECTOR + abi_encode(
            ["address", "address", "address", "uint256", "bool", "address", "bytes"],
            [
                _normalize_addr(coll_asset),
                _normalize_addr(debt_asset),
                _normalize_addr(user),
                debt_to_cover,
                False,           # receiveAToken — receive underlying asset
                "0x" + "0" * 40, # swapRouter — address(0) if no swap
                b"",             # swapCalldata — empty, filled at runtime
            ],
        )
        return calldata, FLASH_EXECUTOR_V3, 0

    def _build_flash_loan_bundle(
        self, user: str, debt_asset: str, coll_asset: str, debt_to_cover: int
    ) -> Tuple[bytes, str, int]:
        """
        Build calldata for FlashExecutorV3.executeLiquidation().
        Flash-loan path — bot borrows debt asset from Balancer V2, liquidates,
        swaps collateral, repays loan.
        Swap calldata and router left empty; execution engine fills at runtime.
        """
        # Encode: executeLiquidation(collAsset, debtAsset, borrower, debtToCover, false, router, swapCalldata)
        calldata = EXECUTE_LIQUIDATION_SELECTOR + abi_encode(
            ["address", "address", "address", "uint256", "bool", "address", "bytes"],
            [
                _normalize_addr(coll_asset),
                _normalize_addr(debt_asset),
                _normalize_addr(user),
                debt_to_cover,
                False,           # receiveAToken — receive underlying asset
                "0x" + "0" * 40, # swapRouter — address(0) if no swap
                b"",             # swapCalldata — empty, filled at runtime
            ],
        )
        return calldata, FLASH_EXECUTOR_V3, 0

    async def build_all_for_feed(
        self, feed_symbol: str, opportunities: List[dict], min_profit: Decimal
    ) -> int:
        """
        Build bundles for all profitable opportunities triggered by a feed.
        Returns count of bundles built.
        """
        count = 0
        for opp in opportunities:
            if Decimal(str(opp.get("net_profit_usd", 0))) < min_profit:
                continue

            debt_cover = opp.get("debt_to_cover_wei", 0)
            if not debt_cover:
                # Infer from debt_usd and asset price (approximate)
                debt_usd = Decimal(str(opp.get("debt_asset_usd", 0)))
                debt_cover = int(debt_usd * Decimal("1e18"))  # approximation

            bundle_id = await self.build_bundle(
                user_addr=opp["user_addr"],
                debt_asset_addr=opp.get("debt_asset_addr", ""),
                debt_asset_symbol=opp.get("debt_asset", "???"),
                coll_asset_addr=opp.get("coll_asset_addr", ""),
                coll_asset_symbol=opp.get("coll_asset", "???"),
                debt_to_cover_wei=debt_cover,
                trigger_feed=feed_symbol,
                expected_profit_usd=Decimal(str(opp.get("net_profit_usd", 0))),
                priority=opp.get("profit_rank", 99),
                use_flash_loan=True,
            )
            if bundle_id:
                count += 1

        return count

    async def purge_expired(self):
        """Clean up expired bundles."""
        try:
            async with self.pg.acquire() as conn:
                await conn.execute(
                    "DELETE FROM precomputed_bundles WHERE expires_at < NOW() AND is_consumed = false"
                )
        except Exception:
            pass


def _normalize_addr(addr: str) -> bytes:
    """Normalize an Ethereum address to 20 bytes."""
    addr = addr.lower().replace("0x", "")
    return bytes.fromhex(addr.zfill(40))
