"""
scripts/live_executor_v2.py — Aave V2 Arbitrum liquidation executor.

Runs alongside live_executor.py (V3) as a separate process.
Shares the same FlashExecutorV3 contract and DEX router approvals.
V2 differences:
- Fixed 5% liquidation bonus (vs variable in V3)
- ETH-denominated account data (vs USD base in V3)  
- Different event signatures for borrow detection
- getReservesList() instead of getReservesData() for asset discovery

Usage:
    python -m scripts.live_executor_v2
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiohttp
from dotenv import load_dotenv
from eth_abi import encode as abi_encode, decode as abi_decode
from eth_utils import keccak
from web3 import Web3

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scanner.aave_v2 import (
    POOL_V2,
    POOL_DATA_PROVIDER_V2,
    BORROW_TOPIC_V2,
    LIQUIDATION_CALL_TOPIC_V2,
    LIQUIDATION_GAS_LIMIT_V2,
    V2UserAccountData,
    V2UserReserveData,
    decode_v2_user_account_data,
    calculate_v2_liquidation_profit,
    format_v2_hf_status,
)
from scanner.aave_v3 import (
    fetch_user_reserves,
    pick_liquidation_target,
)

# ─── Logging ──────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | v2_executor | %(message)s",
)
logger = logging.getLogger("v2_executor")

# ─── Constants ────────────────────────────────────────────────

ARBITRUM_CHAIN_ID = 42161
EXECUTE_HF_THRESHOLD = 1.0
MIN_DEBT_USD = 5000  # Positions smaller than this won't cover gas
BATCH_SIZE = 20

# DEX routers (shared with V3, already approved on contract)
UNI_V3_SWAPROUTER = "0xE592427A0AEce92De3Edee1F18E0157C05861564"
SUSHI_V3_ROUTER = "0x8A21F6768c1F8075791D08546dADF6daA0Be16eC"
CAMELOT_V3_ROUTER = "0xf5f4496219F31dDB12b336056fE74D0bB8405239"
PANCAKESWAP_V3_ROUTER = "0x1b81D678ffb9C0263b24A97847620C99d213eB14"

MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
MEV_BLOCKER_RPC = "https://rpc.mevblocker.io"

# Known V2 reserve assets on Arbitrum (active, with liquidity)
V2_RESERVE_LIST = [
    "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # WETH
    "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",  # USDC
    "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",  # USDT
    "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",  # WBTC
    "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",  # DAI
    "0x5979D7b546E38E414F7E9822514be443A4802509",  # wstETH
]


@dataclass
class V2LiquidationOpportunity:
    borrower: str
    collateral_asset: str
    debt_asset: str
    debt_to_cover: int
    health_factor: float
    collateral_usd: float
    debt_usd: float
    estimated_profit_usd: float


class AaveV2LiquidationExecutor:
    """Monitors Aave V2 borrowers and executes profitable liquidations."""

    def __init__(self, rpc_url: str, private_key: str, executor_address: str,
                 min_profit_usd: float = 25.0):
        self.rpc_url = rpc_url
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        self.account = self.w3.eth.account.from_key(private_key)
        self.executor_address = self.w3.to_checksum_address(executor_address)
        self.min_profit_usd = min_profit_usd

        # RPC fallback chain
        quicknode = os.getenv("QUICKNODE_HTTP_URL", "")
        self.rpc_urls = []
        if quicknode:
            self.rpc_urls.append(quicknode.strip())
        self.rpc_urls.append(rpc_url)
        self.rpc_urls.append("https://arb1.arbitrum.io/rpc")

        self.dry_run = os.getenv("DRY_RUN", "0") == "1"
        if self.dry_run:
            logger.warning("DRY RUN MODE — no real transactions")

        # State
        self.monitored_users: Dict[str, Dict] = {}
        self.last_alerted: Dict[str, float] = {}
        self.known_borrowers: set = set()
        self._last_scanned_block = 0
        self.consecutive_reverts = 0
        self.max_consecutive_reverts = 3

        # Telegram
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")

        # Cached reserve symbols
        self._reserve_symbols: Dict[str, str] = {}

    # ─── RPC Helpers ────────────────────────────────────────

    async def _rpc_call(self, method: str, params: list) -> dict:
        """Make a JSON-RPC call with fallback chain."""
        payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}

        for i, url in enumerate(self.rpc_urls):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(
                        url, json=payload,
                        headers={"Content-Type": "application/json"},
                        timeout=aiohttp.ClientTimeout(total=5),
                    ) as resp:
                        if resp.status == 200:
                            return await resp.json()
                        if resp.status in (429, 403):
                            if i < len(self.rpc_urls) - 1:
                                logger.warning("RPC retry %d/%d → %s", i+1, 3, url)
                                continue
            except Exception:
                if i < len(self.rpc_urls) - 1:
                    continue
        return {"error": "All RPC endpoints exhausted"}

    async def get_latest_block(self) -> int:
        result = await self._rpc_call("eth_blockNumber", [])
        return int(result.get("result", "0x0"), 16)

    # ─── Borrower Discovery ─────────────────────────────────

    async def fetch_borrow_events(self, from_block: int, to_block: int) -> List[str]:
        """Fetch unique borrower addresses from V2 Borrow events.
        
        Uses Alchemy RPC (not QuickNode) because QuickNode free tier limits
        eth_getLogs to 5-block ranges.
        """
        borrowers = set()
        batch = 20_000
        # Use Alchemy for eth_getLogs (QuickNode free blocks range > 5)
        alchemy_url = os.getenv("ALCHEMY_HTTP_URL", self.rpc_urls[0])
        
        for start in range(from_block, to_block + 1, batch):
            end = min(start + batch - 1, to_block)
            try:
                async with aiohttp.ClientSession() as session:
                    payload = {"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{
                        "fromBlock": hex(start), "toBlock": hex(end),
                        "address": POOL_V2, "topics": [BORROW_TOPIC_V2],
                    }]}
                    async with session.post(alchemy_url, json=payload,
                        headers={"Content-Type":"application/json"},
                        timeout=aiohttp.ClientTimeout(total=30)) as resp:
                        data = await resp.json()
                        if "error" in data:
                            continue
                        logs = data.get("result", []) or []
                        for log in logs:
                            if len(log.get("topics", [])) > 1:
                                topic = log["topics"][1]
                                addr = "0x" + topic[-40:]
                                borrowers.add(self.w3.to_checksum_address(addr))
            except Exception:
                continue
            await asyncio.sleep(0.5)  # Respect QuickNode free tier rate limits
        return list(borrowers)

    async def bootstrap_borrowers(self, lookback_blocks: int = 200_000) -> int:
        """Build initial borrower set from V2 Borrow events."""
        latest = await self.get_latest_block()
        from_block = max(latest - lookback_blocks, 0)
        logger.info("Bootstrapping V2 borrowers from blocks %d-%d...", from_block, latest)
        borrowers = await self.fetch_borrow_events(from_block, latest)

        for borrower in borrowers:
            if borrower not in self.monitored_users:
                self.monitored_users[borrower] = {
                    "address": borrower,
                    "first_seen": latest,
                    "history": [],
                }
        self.known_borrowers.update(borrowers)
        logger.info("Bootstrapped %d V2 borrowers", len(borrowers))
        return len(borrowers)

    # ─── Health Factor Batch Fetch ──────────────────────────

    async def batch_fetch_v2_health(self, addresses: List[str]) -> Dict[str, Optional[V2UserAccountData]]:
        """Fetch V2 health factors for all borrowers via Multicall3."""
        MULTICALL3_ADDR = "0xcA11bde05977b3631167028862bE2a173976CA11"
        sel = keccak(text="getUserAccountData(address)")[:4].hex()

        calls = []
        for addr in addresses:
            calldata = bytes.fromhex(sel + addr[2:].lower().rjust(64, "0"))
            calls.append((POOL_V2, True, calldata))

        agg_sel = keccak(text="aggregate3((address,bool,bytes)[])")[:4]
        encoded = abi_encode(["(address,bool,bytes)[]"], [calls])
        calldata = "0x" + agg_sel.hex() + encoded.hex()

        result = await self._rpc_call("eth_call", [
            {"to": MULTICALL3_ADDR, "data": calldata}, "latest",
        ])
        raw = result.get("result", "0x")
        if len(raw) < 10:
            return {}

        try:
            decoded = abi_decode(["(bool,bytes)[]"], bytes.fromhex(raw[2:]))[0]
        except Exception as e:
            logger.error("Multicall3 decode failed: %s", e)
            return {}

        output: Dict[str, Optional[V2UserAccountData]] = {}
        for addr, (success, ret_data) in zip(addresses, decoded):
            if not success:
                output[addr] = None
            else:
                output[addr] = decode_v2_user_account_data(ret_data)
        return output

    # ─── Assessment ─────────────────────────────────────────

    async def assess_v2_opportunity(
        self, borrower: str, data: V2UserAccountData,
    ) -> Optional[V2LiquidationOpportunity]:
        """Determine if a V2 position is profitable to liquidate."""
        hf = data.health_factor_float
        if hf >= 1.0:
            return None

        # Convert ETH-denominated to rough USD
        eth_price = 3000.0
        debt_usd = (data.total_debt_eth / 1e18) * eth_price
        if debt_usd < MIN_DEBT_USD:
            return None

        collateral_usd = (data.total_collateral_eth / 1e18) * eth_price

        # Discover reserves using V3's helper (contract-compatible)
        reserves = fetch_user_reserves(self.w3, borrower)
        target = pick_liquidation_target(reserves)
        if target is None:
            return None

        collateral_reserve, debt_reserve, debt_to_cover = target

        logger.info(
            "⬇️ V2 LIQUIDATABLE: %s | HF=%.4f | Debt=$%.2f | Collat=$%.2f | %s/%s",
            borrower, hf, debt_usd, collateral_usd,
            collateral_reserve.symbol, debt_reserve.symbol,
        )

        # V2 profit: fixed 5% bonus, 0.09% flash fee
        net_profit = calculate_v2_liquidation_profit(
            debt_to_cover=debt_to_cover,
            debt_decimals=debt_reserve.decimals,
            debt_usd_price=1.0,  # simplified — prod would use oracle
            eth_price_usd=eth_price,
        )

        if net_profit < self.min_profit_usd:
            logger.info("  Skipping V2: profit=$%.2f < min=$%.2f", net_profit, self.min_profit_usd)
            return None

        logger.info("  ✅ V2 PROFITABLE: $%.2f", net_profit)

        return V2LiquidationOpportunity(
            borrower=borrower,
            collateral_asset=collateral_reserve.asset,
            debt_asset=debt_reserve.asset,
            debt_to_cover=debt_to_cover,
            health_factor=hf,
            collateral_usd=collateral_usd,
            debt_usd=debt_usd,
            estimated_profit_usd=net_profit,
        )

    # ─── Telegram Alerts ────────────────────────────────────

    async def send_alert(self, message: str) -> None:
        """Send Telegram alert."""
        if not self.telegram_token or not self.telegram_chat_id:
            return
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {"chat_id": self.telegram_chat_id, "text": message, "parse_mode": "Markdown"}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        logger.warning("Telegram send failed: %s", await resp.text())
        except Exception as e:
            logger.warning("Telegram exception: %s", e)

    # ─── Main Loop ──────────────────────────────────────────

    async def run(self) -> None:
        """Main execution loop for V2 liquidation monitoring."""
        logger.info("=" * 80)
        logger.info(" AAVE V2 LIQUIDATION EXECUTOR")
        logger.info(" Account: %s", self.account.address)
        logger.info(" Pool V2: %s", POOL_V2)
        logger.info(" Min Profit: $%.2f", self.min_profit_usd)
        logger.info("=" * 80)

        await self.bootstrap_borrowers()
        iteration = 0

        while True:
            iteration += 1
            logger.info("--- V2 Poll #%d ---", iteration)

            latest_block = await self.get_latest_block()
            if latest_block <= self._last_scanned_block:
                await asyncio.sleep(2)
                continue
            self._last_scanned_block = latest_block

            user_list = list(self.monitored_users.keys())
            opportunities: List[V2LiquidationOpportunity] = []

            # Batch fetch all V2 health factors
            health_data = await self.batch_fetch_v2_health(user_list)
            liquidatable_count = 0

            for addr, data in health_data.items():
                if data is None:
                    continue

                # Update history
                self.monitored_users[addr].setdefault("history", []).append({
                    "block": latest_block,
                    "hf": data.health_factor_float,
                    "debt_eth": data.total_debt_eth,
                    "collat_eth": data.total_collateral_eth,
                })

                if data.health_factor_float < EXECUTE_HF_THRESHOLD:
                    liquidatable_count += 1
                    opp = await self.assess_v2_opportunity(addr, data)
                    if opp:
                        opportunities.append(opp)

            logger.info(
                "V2 Block=%d | Tracked=%d | Liquidatable=%d | Opportunities=%d",
                latest_block, len(self.monitored_users),
                liquidatable_count, len(opportunities),
            )

            # Execute opportunities
            for opp in opportunities:
                now = time.time()
                if opp.borrower in self.last_alerted and (now - self.last_alerted[opp.borrower]) < 300:
                    continue
                self.last_alerted[opp.borrower] = now

                alert_msg = (
                    f"🟠 *V2 LIQUIDATABLE*\n"
                    f"Borrower: `{opp.borrower}`\n"
                    f"HF: `{opp.health_factor:.4f}`\n"
                    f"Debt: `${opp.debt_usd:,.2f}` | Collat: `${opp.collateral_usd:,.2f}`\n"
                    f"Est. Profit: `${opp.estimated_profit_usd:,.2f}`"
                )
                await self.send_alert(alert_msg)

                # In dry-run mode, just log. Production would broadcast.
                if self.dry_run:
                    logger.info("DRY-RUN V2: would liquidate %s for $%.2f",
                                opp.borrower, opp.estimated_profit_usd)

            # Refresh borrower list periodically
            if iteration % 500 == 0:
                await self.bootstrap_borrowers(lookback_blocks=50_000)

            await asyncio.sleep(15)  # V2 polls less aggressively


def main():
    load_dotenv()

    private_key = os.getenv("BOT_PRIVATE_KEY")
    if not private_key:
        print("ERROR: BOT_PRIVATE_KEY not set", file=sys.stderr)
        sys.exit(1)

    executor_address = os.getenv("FLASH_EXECUTOR_V3")
    if not executor_address:
        print("ERROR: FLASH_EXECUTOR_V3 not set", file=sys.stderr)
        sys.exit(1)

    rpc_url = os.getenv("QUICKNODE_HTTP_URL") or os.getenv("ARBITRUM_HTTP_URL")
    if not rpc_url:
        print("ERROR: No RPC URL set", file=sys.stderr)
        sys.exit(1)

    min_profit = float(os.getenv("MIN_PROFIT_USD", "25.0"))

    executor = AaveV2LiquidationExecutor(
        rpc_url=rpc_url,
        private_key=private_key,
        executor_address=executor_address,
        min_profit_usd=min_profit,
    )

    try:
        asyncio.run(executor.run())
    except KeyboardInterrupt:
        logger.info("V2 executor shutting down.")


if __name__ == "__main__":
    main()
