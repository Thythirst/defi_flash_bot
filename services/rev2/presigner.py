#!/usr/bin/env python3
"""
presigner.py — Pre-builds and pre-signs liquidation txs.
Hot path: single eth_sendRawTransaction (~20ms).
Refreshes top-20 candidates every 30s with fresh gas prices.
"""
import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Dict, Optional

from web3 import Web3

logger = logging.getLogger(__name__)

FLASH_GAS_LIMIT = 400_000
DIRECT_GAS_LIMIT = 680_000
REFRESH_INTERVAL_S = 30
MAX_PRESIGNED = 20
GAS_PRICE_MULTIPLIER = 1.5


@dataclass
class PresignedTx:
    borrower: str
    collateral_asset: str
    debt_asset: str
    debt_to_cover: int
    path: str
    raw_tx: bytes
    signed_at: float
    estimated_profit_usd: float
    gas_price_gwei: float


class PreSigner:
    def __init__(self, w3: Web3, contract, wallet_addr: str, private_key: str):
        self.w3 = w3
        self.contract = contract
        self.wallet = Web3.to_checksum_address(wallet_addr)
        self.private_key = private_key
        self.cache: Dict[str, PresignedTx] = {}
        self._nonce = None
        self._running = False

    async def start(self, hf_engine, ev_estimator):
        self._running = True
        self._hf_engine = hf_engine
        self._ev_estimator = ev_estimator
        asyncio.create_task(self._refresh_loop())
        logger.info("[PreSigner] Started background refresh loop")

    async def stop(self):
        self._running = False

    async def fire(self, borrower: str) -> Optional[str]:
        presigned = self.cache.get(borrower)
        if presigned:
            age = time.time() - presigned.signed_at
            if age > 60:
                logger.warning(f"[PreSigner] Stale presigned tx ({age:.0f}s old) for {borrower[:8]}")
            try:
                tx_hash = self.w3.eth.send_raw_transaction(presigned.raw_tx)
                logger.info(f"[PreSigner] FIRED presigned {tx_hash.hex()[:12]} for {borrower[:8]}")
                return tx_hash.hex()
            except Exception as e:
                logger.error(f"[PreSigner] fire() failed: {e} — rebuilding")
        return await self._build_and_fire(borrower)

    async def refresh_candidate(self, borrower: str, collateral_asset: str,
                                 debt_asset: str, debt_to_cover: int,
                                 estimated_profit_usd: float, use_flash: bool = True):
        try:
            borrower = Web3.to_checksum_address(borrower)
            collateral_asset = Web3.to_checksum_address(collateral_asset)
            debt_asset = Web3.to_checksum_address(debt_asset)
            base_fee = self.w3.eth.get_block('pending')['baseFeePerGas']
            max_fee = int(base_fee * GAS_PRICE_MULTIPLIER)
            priority_fee = Web3.to_wei('0.05', 'gwei')
            gas_limit = FLASH_GAS_LIMIT if use_flash else DIRECT_GAS_LIMIT

            tx = self.contract.functions.executeLiquidation(
                collateral_asset, debt_asset, borrower, debt_to_cover, False
            ).build_transaction({
                'from': self.wallet,
                'gas': gas_limit,
                'maxFeePerGas': max_fee,
                'maxPriorityFeePerGas': priority_fee,
                'nonce': await self._get_nonce(),
                'chainId': 42161,
            })
            signed = self.w3.eth.account.sign_transaction(tx, self.private_key)
            self.cache[borrower] = PresignedTx(
                borrower=borrower, collateral_asset=collateral_asset,
                debt_asset=debt_asset, debt_to_cover=debt_to_cover,
                path='flash' if use_flash else 'direct',
                raw_tx=signed.raw_transaction, signed_at=time.time(),
                estimated_profit_usd=estimated_profit_usd,
                gas_price_gwei=max_fee / 1e9,
            )
        except Exception as e:
            logger.error(f"[PreSigner] refresh_candidate failed for {borrower[:8]}: {e}")

    async def _refresh_loop(self):
        while self._running:
            try:
                await self._do_refresh()
            except Exception as e:
                logger.error(f"[PreSigner] refresh loop error: {e}")
            await asyncio.sleep(REFRESH_INTERVAL_S)

    async def _do_refresh(self):
        candidates = self._hf_engine.get_sorted_candidates(top_n=MAX_PRESIGNED)
        refreshed = 0
        for addr, hf in candidates:
            if hf >= 1.0:
                continue
            pos = self._hf_engine.positions.get(addr)
            if not pos:
                continue
            ev = self._ev_estimator.compute(addr, hf, pos)
            if not ev.go:
                continue
            best_collateral = max(pos.collateral_assets,
                key=lambda a: (pos.collateral.get(a, 0) * self._hf_engine.prices.get(a, 0)))
            best_debt = max(pos.debt_assets,
                key=lambda a: (pos.debt.get(a, 0) * self._hf_engine.prices.get(a, 0)))
            await self.refresh_candidate(addr, best_collateral, best_debt,
                                         ev.debt_to_cover_raw, ev.net_ev_usd, True)
            refreshed += 1
        top_addrs = {Web3.to_checksum_address(a) for a, _ in candidates}
        stale = [k for k in self.cache if k not in top_addrs]
        for k in stale:
            del self.cache[k]
        if refreshed:
            logger.info(f"[PreSigner] Refreshed {refreshed} presigned txs")

    async def _build_and_fire(self, borrower: str) -> Optional[str]:
        pos = self._hf_engine.positions.get(borrower)
        if not pos:
            return None
        hf = self._hf_engine.compute_hf(pos)
        ev = self._ev_estimator.compute(borrower, hf, pos)
        if not ev.go:
            return None
        best_c = max(pos.collateral_assets,
            key=lambda a: pos.collateral.get(a, 0) * self._hf_engine.prices.get(a, 0))
        best_d = max(pos.debt_assets,
            key=lambda a: pos.debt.get(a, 0) * self._hf_engine.prices.get(a, 0))
        await self.refresh_candidate(borrower, best_c, best_d,
                                     ev.debt_to_cover_raw, ev.net_ev_usd)
        return await self.fire(borrower)

    async def _get_nonce(self) -> int:
        self._nonce = self.w3.eth.get_transaction_count(self.wallet, 'pending')
        return self._nonce

    @property
    def presigned_count(self) -> int:
        return len(self.cache)
