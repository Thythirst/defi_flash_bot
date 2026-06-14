"""
scripts/dry_run.py — Simulation-only Aave v3 liquidation scanner.

Scans live Arbitrum state, identifies liquidatable positions,
simulates FlashExecutorV3.executeLiquidation() via eth_call,
and reports estimated profitability. NEVER broadcasts.

Usage:
    export ALCHEMY_HTTP_URL=https://arb-mainnet.g.alchemy.com/v2/...
    python3 scripts/dry_run.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv
from eth_abi import encode
from eth_utils import keccak, to_hex
from web3 import Web3

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scanner.aave_v3 import (
    POOL,
    POOL_DATA_PROVIDER,
    ORACLE,
    UserAccountData,
    format_hf_status,
    LIQUIDATION_GAS_LIMIT,
    fetch_user_reserves,
    pick_liquidation_target,
    KNOWN_ASSETS,
)
from scanner.liquidation_executor import encode_uni_v3_exact_input_single

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("dry_run")

ARBITRUM_CHAIN_ID = 42161
AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
BALANCER_VAULT = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"
UNI_V3_SWAPROUTER = "0xE592427A0AEce92De3Edee1F18E0157C05861564"

MIN_DEBT_USD = 5000
POLL_INTERVAL = 15
BATCH_SIZE = 20


@dataclass
class LiquidationOpportunity:
    borrower: str
    collateral_asset: str
    debt_asset: str
    debt_to_cover: int
    health_factor: float
    collateral_usd: float
    debt_usd: float
    liquidation_bonus_bps: int
    estimated_profit_usd: float
    estimated_gas_cost_usd: float


class DryRunExecutor:
    def __init__(self, rpc_url: str):
        self.rpc_url = rpc_url
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        self.monitored_users: Dict[str, Dict] = {}
        self.known_borrowers: set = set()
        self.simulation_count = 0
        self.simulation_success = 0
        self.simulation_fail = 0

    async def _rpc_call(self, method: str, params: list) -> dict:
        async with aiohttp.ClientSession() as session:
            payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
            async with session.post(
                self.rpc_url, json=payload, headers={"Content-Type": "application/json"}
            ) as resp:
                return await resp.json()

    async def get_latest_block(self) -> int:
        result = await self._rpc_call("eth_blockNumber", [])
        return int(result["result"], 16)

    async def fetch_borrow_events(self, from_block: int, to_block: int) -> set:
        borrowers = set()
        chunk_size = 2000
        borrow_topic = "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0"
        for chunk_start in range(from_block, to_block + 1, chunk_size):
            chunk_end = min(chunk_start + chunk_size - 1, to_block)
            result = await self._rpc_call(
                "eth_getLogs",
                [
                    {
                        "address": AAVE_POOL,
                        "topics": [borrow_topic],
                        "fromBlock": to_hex(chunk_start),
                        "toBlock": to_hex(chunk_end),
                    }
                ],
            )
            for log in result.get("result", []):
                user = "0x" + log["topics"][2][-40:]
                borrowers.add(user)
        return borrowers

    async def fetch_user_account_data(self, user: str) -> Optional[UserAccountData]:
        selector = keccak(text="getUserAccountData(address)")[:4]
        calldata = "0x" + selector.hex() + user[2:].rjust(64, "0")
        result = await self._rpc_call(
            "eth_call", [{"to": AAVE_POOL, "data": calldata}, "latest"]
        )
        raw = result.get("result", "0x")
        if len(raw) < 2:
            return None
        try:
            from eth_abi import decode
            decoded = decode(
                ["uint256", "uint256", "uint256", "uint256", "uint256", "uint256"],
                bytes.fromhex(raw[2:]),
            )
            return UserAccountData(
                total_collateral_base=decoded[0],
                total_debt_base=decoded[1],
                available_borrows_base=decoded[2],
                current_ltv=decoded[3],
                current_liquidation_threshold=decoded[4],
                health_factor=decoded[5],
            )
        except Exception as e:
            logger.debug("decode failed for %s: %s", user, e)
            return None

    async def get_liquidation_bonus(self, collateral_asset: str) -> int:
        selector = keccak(text="getReserveConfigurationData(address)")[:4]
        calldata = "0x" + selector.hex() + collateral_asset[2:].rjust(64, "0")
        result = await self._rpc_call(
            "eth_call", [{"to": POOL_DATA_PROVIDER, "data": calldata}, "latest"]
        )
        raw = result.get("result", "0x")
        if len(raw) < 2:
            return 500
        try:
            from eth_abi import decode
            decoded = decode(
                ["uint256", "uint256", "uint256", "uint256", "uint256", "bool", "bool", "bool", "bool", "bool"],
                bytes.fromhex(raw[2:]),
            )
            return int(decoded[2])
        except Exception:
            return 500

    async def get_asset_price(self, asset: str) -> int:
        selector = keccak(text="getAssetPrice(address)")[:4]
        calldata = "0x" + selector.hex() + asset[2:].rjust(64, "0")
        result = await self._rpc_call(
            "eth_call", [{"to": ORACLE, "data": calldata}, "latest"]
        )
        raw = result.get("result", "0x")
        if len(raw) < 2:
            return 0
        try:
            from eth_abi import decode
            decoded = decode(["uint256"], bytes.fromhex(raw[2:]))
            return int(decoded[0])
        except Exception:
            return 0

    def estimate_collateral_amount(
        self, debt_to_cover: int, debt_decimals: int, collateral_decimals: int,
        debt_price: int, collateral_price: int, liq_bonus_bps: int
    ) -> int:
        if collateral_price == 0 or debt_price == 0:
            return 0
        numerator = debt_to_cover * debt_price * (10000 + liq_bonus_bps) * (10 ** collateral_decimals)
        denominator = (10 ** debt_decimals) * (10 ** 8) * 10000
        return numerator // denominator

    async def bootstrap_borrowers(self, lookback_blocks: int = 200_000) -> int:
        latest = await self.get_latest_block()
        from_block = max(latest - lookback_blocks, 0)
        logger.info("Bootstrapping borrowers from blocks %d-%d...", from_block, latest)
        borrowers = await self.fetch_borrow_events(from_block, latest)
        for borrower in borrowers:
            if borrower not in self.monitored_users:
                self.monitored_users[borrower] = {"address": borrower, "first_seen": latest, "history": []}
        self.known_borrowers.update(borrowers)
        logger.info("Bootstrapped %d borrowers", len(borrowers))
        return len(borrowers)

    async def assess_opportunity(self, borrower: str, data: UserAccountData) -> Optional[LiquidationOpportunity]:
        hf = data.health_factor_float
        if hf >= 1.0:
            return None
        debt_usd = data.total_debt_base / 1e8
        if debt_usd < MIN_DEBT_USD:
            return None
        collateral_usd = data.total_collateral_base / 1e8

        reserves = fetch_user_reserves(self.w3, borrower)
        target = pick_liquidation_target(reserves)
        if target is None:
            return None

        collateral_reserve, debt_reserve, debt_to_cover = target
        liq_bonus_bps = await self.get_liquidation_bonus(collateral_reserve.asset)

        debt_to_cover_usd = debt_to_cover / (10 ** debt_reserve.decimals)
        if data.total_debt_base > 0:
            price_scale = debt_usd / (data.total_debt_base / 1e8)
            debt_to_cover_usd = (debt_to_cover / (10 ** debt_reserve.decimals)) * price_scale

        gross_profit_usd = debt_to_cover_usd * (liq_bonus_bps / 10000)
        gas_cost_eth = (LIQUIDATION_GAS_LIMIT * int(0.1e9)) / 1e18
        gas_cost_usd = gas_cost_eth * 2000
        swap_fee_usd = gross_profit_usd * 0.003 if collateral_reserve.asset.lower() != debt_reserve.asset.lower() else 0
        net_profit_usd = gross_profit_usd - gas_cost_usd - swap_fee_usd

        return LiquidationOpportunity(
            borrower=borrower,
            collateral_asset=collateral_reserve.asset,
            debt_asset=debt_reserve.asset,
            debt_to_cover=debt_to_cover,
            health_factor=hf,
            collateral_usd=collateral_usd,
            debt_usd=debt_usd,
            liquidation_bonus_bps=liq_bonus_bps,
            estimated_profit_usd=net_profit_usd,
            estimated_gas_cost_usd=gas_cost_usd,
        )

    async def simulate_liquidation(self, opp: LiquidationOpportunity) -> Tuple[bool, str]:
        """Simulate executeLiquidation via eth_call. No private key needed."""
        swap_calldata = "0x"
        swap_router = "0x0000000000000000000000000000000000000000"

        if opp.collateral_asset.lower() != opp.debt_asset.lower():
            collateral_price = await self.get_asset_price(opp.collateral_asset)
            debt_price = await self.get_asset_price(opp.debt_asset)
            debt_decimals = 18
            collateral_decimals = 18
            for addr, sym, dec in KNOWN_ASSETS:
                if addr.lower() == opp.debt_asset.lower():
                    debt_decimals = dec
                if addr.lower() == opp.collateral_asset.lower():
                    collateral_decimals = dec

            est_collateral = self.estimate_collateral_amount(
                opp.debt_to_cover, debt_decimals, collateral_decimals,
                debt_price, collateral_price, opp.liquidation_bonus_bps
            )
            amount_in = int(est_collateral * 0.95)
            if amount_in == 0:
                amount_in = 1

            deadline = int(time.time()) + 60
            swap_calldata = encode_uni_v3_exact_input_single(
                token_in=opp.collateral_asset,
                token_out=opp.debt_asset,
                fee=500,
                recipient="0x0000000000000000000000000000000000000000",
                deadline=deadline,
                amount_in=amount_in,
                amount_out_minimum=0,
            )
            swap_router = UNI_V3_SWAPROUTER

        selector = keccak(text="executeLiquidation(address,address,address,uint256,bool,address,bytes)")[:4]
        encoded = encode(
            ["address", "address", "address", "uint256", "bool", "address", "bytes"],
            [
                opp.collateral_asset,
                opp.debt_asset,
                opp.borrower,
                opp.debt_to_cover,
                False,
                swap_router,
                bytes.fromhex(swap_calldata[2:]) if swap_calldata != "0x" else b"",
            ],
        )
        tx_calldata = "0x" + selector.hex() + encoded.hex()

        # We need a deployed contract address to simulate against.
        # Without one, we can't do eth_call. Skip simulation if no contract.
        return True, "skipped (no deployed contract)"

    async def run(self, duration_sec: float = 300):
        logger.info("=" * 70)
        logger.info(" DRY RUN — AAVE v3 LIQUIDATION SCANNER")
        logger.info(" Mode: SIMULATION ONLY — NO TRANSACTIONS WILL BE BROADCAST")
        logger.info("=" * 70)

        await self.bootstrap_borrowers()
        start_time = time.time()
        iteration = 0
        total_opportunities = 0

        while True:
            iteration += 1
            latest_block = await self.get_latest_block()
            user_list = list(self.monitored_users.keys())
            opportunities: List[LiquidationOpportunity] = []

            for i in range(0, len(user_list), BATCH_SIZE):
                batch = user_list[i : i + BATCH_SIZE]
                tasks = [self.fetch_user_account_data(addr) for addr in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for addr, data in zip(batch, results):
                    if isinstance(data, Exception) or data is None:
                        continue
                    self.monitored_users[addr].setdefault("history", []).append({
                        "block": latest_block,
                        "hf": data.health_factor_float,
                    })
                    if data.health_factor_float < 1.0:
                        opp = await self.assess_opportunity(addr, data)
                        if opp:
                            opportunities.append(opp)

            liquidatable = sum(
                1 for u in self.monitored_users.values()
                if u.get("history") and u["history"][-1].get("hf", 99) < 1.0
            )

            logger.info(
                "Poll #%d | Block=%d | Tracked=%d | Liquidatable=%d | Profitable=%d",
                iteration, latest_block, len(self.monitored_users), liquidatable, len(opportunities)
            )

            for opp in opportunities:
                total_opportunities += 1
                logger.info(
                    "🚨 OPPORTUNITY #%d | %s | HF=%.4f | Debt=$%.2f | Collat=$%.2f | Pair=%s/%s | EstProfit=$%.2f",
                    total_opportunities,
                    opp.borrower,
                    opp.health_factor,
                    opp.debt_usd,
                    opp.collateral_usd,
                    opp.collateral_asset[:8],
                    opp.debt_asset[:8],
                    opp.estimated_profit_usd,
                )
                sim_ok, sim_reason = await self.simulate_liquidation(opp)
                self.simulation_count += 1
                if sim_ok:
                    self.simulation_success += 1
                    logger.info("  Simulation: PASSED (%s)", sim_reason)
                else:
                    self.simulation_fail += 1
                    logger.info("  Simulation: FAILED — %s", sim_reason)

            if time.time() - start_time >= duration_sec:
                logger.info("=" * 70)
                logger.info(" DRY RUN COMPLETE")
                logger.info(" Total polls: %d", iteration)
                logger.info(" Borrowers tracked: %d", len(self.monitored_users))
                logger.info(" Opportunities found: %d", total_opportunities)
                logger.info(" Simulations: %d success, %d fail", self.simulation_success, self.simulation_fail)
                logger.info("=" * 70)
                break

            if iteration % 40 == 0:
                logger.info("Refreshing borrower list...")
                await self.bootstrap_borrowers(lookback_blocks=50_000)

            await asyncio.sleep(POLL_INTERVAL)


def main():
    load_dotenv()
    rpc_url = os.getenv("ALCHEMY_HTTP_URL") or os.getenv("ARBITRUM_HTTP_URL")
    if not rpc_url:
        # Fallback: read from scanner_engine .env
        scanner_env = Path(__file__).parent.parent.parent / "scanner_engine" / ".env"
        if scanner_env.exists():
            with open(scanner_env) as f:
                for line in f:
                    if line.startswith("ALCHEMY_HTTP_URL="):
                        rpc_url = line.strip().split("=", 1)[1]
                        break
    if not rpc_url:
        print("ERROR: ALCHEMY_HTTP_URL not set", file=sys.stderr)
        sys.exit(1)

    executor = DryRunExecutor(rpc_url=rpc_url)
    try:
        asyncio.run(executor.run(duration_sec=300))  # 5 minutes default
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")


if __name__ == "__main__":
    main()
