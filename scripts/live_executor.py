"""
scripts/live_executor.py — Fully autonomous Aave v3 liquidation executor.

Watches Aave borrowers, identifies liquidatable positions, and automatically:
1. Simulates liquidation via eth_call (dry run)
2. Checks profit > $50 + gas costs
3. Signs and broadcasts FlashExecutorV3.executeLiquidation()
4. Sends Telegram alert with tx hash

Environment:
    BOT_PRIVATE_KEY          — Required. Hot wallet with ETH for gas.
    FLASH_EXECUTOR_V3        — Required. Deployed contract address.
    ALCHEMY_HTTP_URL         — Required. Arbitrum RPC endpoint.
    TELEGRAM_BOT_TOKEN       — Optional. For alerts.
    TELEGRAM_CHAT_ID         — Optional. Target chat.
    MIN_PROFIT_USD           — Optional. Override $50 default.

Usage:
    export BOT_PRIVATE_KEY=0x...
    export FLASH_EXECUTOR_V3=0x...
    export ALCHEMY_HTTP_URL=https://arb-mainnet.g.alchemy.com/v2/...
    python3 scripts/live_executor.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv
from eth_abi import encode
from eth_utils import keccak, to_hex
from web3 import Web3
from web3.types import TxParams

# ─── Local imports ──────────────────────────────────────────
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scanner.aave_v3 import (
    POOL,
    POOL_DATA_PROVIDER,
    ORACLE,
    UserAccountData,
    UserReserveData,
    format_hf_status,
    LIQUIDATION_GAS_LIMIT,
    fetch_user_reserves,
    pick_liquidation_target,
)
from scanner.liquidation_executor import encode_uni_v3_exact_input_single

# ─── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("live_executor")

# ─── Constants ──────────────────────────────────────────────
ARBITRUM_CHAIN_ID = 42161
ALERT_HF_THRESHOLD = 1.05   # Alert when HF below this
EXECUTE_HF_THRESHOLD = 1.0  # Strict: only execute when HF < 1.0

BALANCER_VAULT = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"
AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"

# DEX routers (must be approved on FlashExecutorV3)
UNI_V3_SWAPROUTER = "0xE592427A0AEce92De3Edee1F18E0157C05861564"

# Min debt to consider (in USD) — positions smaller than this won't cover gas
MIN_DEBT_USD = 5000

# Poll interval (seconds)
POLL_INTERVAL = 15  # ~1.25 Arbitrum blocks

# RPC batch size for borrower polling
BATCH_SIZE = 20


# ─── Dataclasses ────────────────────────────────────────────

@dataclass
class LiquidationOpportunity:
    borrower: str
    collateral_asset: str
    debt_asset: str
    debt_to_cover: int          # in wei
    health_factor: float
    collateral_usd: float
    debt_usd: float
    liquidation_bonus_bps: int  # fetched from Aave
    estimated_profit_wei: int
    estimated_gas_cost_wei: int


# ─── Balancer Flash Loan Calldata Helper ────────────────────

class FlashLoanCalldataBuilder:
    """Build calldata for FlashExecutorV3.executeLiquidation()"""

    @staticmethod
    def encode_execute_liquidation(
        collateral_asset: str,
        debt_asset: str,
        borrower: str,
        debt_to_cover: int,
        swap_router: str,          # address(0) if no swap
        swap_calldata: str,        # "0x" if no swap
        receive_a_token: bool = False,
    ) -> str:
        """
        Encode the executeLiquidation function call.
        Returns hex string calldata.
        """
        selector = keccak(
            text="executeLiquidation(address,address,address,uint256,bool,address,bytes)"
        )[:4]

        # Encode the parameters
        encoded = encode(
            ["address", "address", "address", "uint256", "bool", "address", "bytes"],
            [
                collateral_asset,
                debt_asset,
                borrower,
                debt_to_cover,
                receive_a_token,
                swap_router,
                bytes.fromhex(swap_calldata[2:]) if swap_calldata != "0x" else b"",
            ],
        )

        return "0x" + selector.hex() + encoded.hex()


# ─── Live Executor ──────────────────────────────────────────

class AaveLiquidationExecutor:
    """
    Monitors Aave borrowers and auto-executes profitable liquidations
    via FlashExecutorV3 + Balancer flash loans.
    """

    def __init__(
        self,
        rpc_url: str,
        private_key: str,
        executor_address: str,
        min_profit_usd: float = 50.0,
    ):
        self.rpc_url = rpc_url
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        self.account = self.w3.eth.account.from_key(private_key)
        self.executor_address = self.w3.to_checksum_address(executor_address)
        self.min_profit_usd = min_profit_usd

        # Load executor ABI
        self.executor_abi = self._load_executor_abi()
        self.executor = self.w3.eth.contract(
            address=self.executor_address,
            abi=self.executor_abi,
        )

        # Track state
        self.monitored_users: Dict[str, Dict] = {}
        self.last_alerted: Dict[str, float] = {}  # address -> timestamp
        self.known_borrowers: set = set()

        # Telegram
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")

    def _load_executor_abi(self) -> List[Dict]:
        """Load FlashExecutorV3 ABI from Forge output."""
        abi_path = (
            Path(__file__).parent.parent
            / "out"
            / "FlashExecutorV3.sol"
            / "FlashExecutorV3.json"
        )
        with open(abi_path) as f:
            artifact = json.load(f)
        return artifact["abi"]

    # ─── RPC Helpers ────────────────────────────────────────

    async def _rpc_call(self, method: str, params: list) -> dict:
        """Async RPC call."""
        async with aiohttp.ClientSession() as session:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }
            async with session.post(
                self.rpc_url,
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as resp:
                return await resp.json()

    async def get_latest_block(self) -> int:
        """Fetch current block number."""
        result = await self._rpc_call("eth_blockNumber", [])
        return int(result["result"], 16)

    async def fetch_borrow_events(self, from_block: int, to_block: int) -> set:
        """Fetch unique borrower addresses from Aave Borrow events."""
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
            logs = result.get("result", [])
            for log in logs:
                user = "0x" + log["topics"][2][-40:]
                borrowers.add(user)

        return borrowers

    async def fetch_user_account_data(self, user: str) -> Optional[UserAccountData]:
        """Call getUserAccountData on Aave Pool."""
        selector = keccak(text="getUserAccountData(address)")[:4]
        calldata = "0x" + selector.hex() + user[2:].rjust(64, "0")

        result = await self._rpc_call(
            "eth_call",
            [{"to": AAVE_POOL, "data": calldata}, "latest"],
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
        """Fetch liquidation bonus (in bps) for an asset from Aave PoolDataProvider."""
        selector = keccak(text="getReserveConfigurationData(address)")[:4]
        calldata = "0x" + selector.hex() + collateral_asset[2:].rjust(64, "0")

        result = await self._rpc_call(
            "eth_call",
            [{"to": POOL_DATA_PROVIDER, "data": calldata}, "latest"],
        )

        raw = result.get("result", "0x")
        if len(raw) < 2:
            return 500  # default 5%

        try:
            from eth_abi import decode
            # Returns: ltv, liqThreshold, liqBonus, decimals, reserveFactor, (bool flags...)
            decoded = decode(
                ["uint256", "uint256", "uint256", "uint256", "uint256", "bool", "bool", "bool", "bool", "bool"],
                bytes.fromhex(raw[2:]),
            )
            # liquidation bonus is expressed as basis points above 100%
            # e.g. 10500 means 5% bonus (105% - 100%)
            return int(decoded[2])
        except Exception as e:
            logger.warning("Failed to fetch liquidation bonus for %s: %s", collateral_asset, e)
            return 500  # default 5%

    # ─── Bootstrap ──────────────────────────────────────────

    async def bootstrap_borrowers(self, lookback_blocks: int = 200_000) -> int:
        """Build initial borrower set from recent events."""
        latest = await self.get_latest_block()
        from_block = max(latest - lookback_blocks, 0)

        logger.info("Bootstrapping borrowers from blocks %d-%d...", from_block, latest)
        borrowers = await self.fetch_borrow_events(from_block, latest)

        for borrower in borrowers:
            if borrower not in self.monitored_users:
                self.monitored_users[borrower] = {
                    "address": borrower,
                    "first_seen": latest,
                    "history": [],
                }

        self.known_borrowers.update(borrowers)
        logger.info("Bootstrapped %d borrowers", len(borrowers))
        return len(borrowers)

    # ─── Opportunity Assessment ─────────────────────────────

    async def assess_liquidation_opportunity(
        self,
        borrower: str,
        data: UserAccountData,
    ) -> Optional[LiquidationOpportunity]:
        """
        Determine if a borrower position is profitable to liquidate.
        Discovers actual collateral/debt reserves and computes real amounts.
        Returns None if not profitable or not liquidatable.
        """
        hf = data.health_factor_float

        # Must be strictly liquidatable
        if hf >= 1.0:
            return None

        # Must have meaningful debt
        debt_usd = data.total_debt_base / 1e8
        if debt_usd < MIN_DEBT_USD:
            return None

        collateral_usd = data.total_collateral_base / 1e8

        # Discover user's actual reserves
        reserves = fetch_user_reserves(self.w3, borrower)
        target = pick_liquidation_target(reserves)
        if target is None:
            logger.debug("No valid collateral/debt pair for %s", borrower)
            return None

        collateral_reserve, debt_reserve, debt_to_cover = target

        logger.info(
            "⬇️ LIQUIDATABLE: %s | HF=%.4f | Debt=$%.2f | Collat=$%.2f | Pair=%s/%s",
            borrower, hf, debt_usd, collateral_usd,
            collateral_reserve.symbol, debt_reserve.symbol
        )

        # Fetch actual liquidation bonus for this collateral
        liq_bonus_bps = await self.get_liquidation_bonus(collateral_reserve.asset)

        # Rough profit estimate in USD terms
        # debt_to_cover is in debt token units; convert to USD roughly
        debt_to_cover_usd = debt_to_cover / (10 ** debt_reserve.decimals)
        # Use a rough price from base currency
        if data.total_debt_base > 0:
            price_scale = debt_usd / (data.total_debt_base / 1e8)
            debt_to_cover_usd = (debt_to_cover / (10 ** debt_reserve.decimals)) * price_scale

        gross_profit_usd = debt_to_cover_usd * (liq_bonus_bps / 10000)
        flash_fee_usd = 0  # Balancer = 0%
        gas_cost_eth = (LIQUIDATION_GAS_LIMIT * int(0.1e9)) / 1e18
        gas_cost_usd = gas_cost_eth * 2000  # Approx ETH at $2000
        swap_fee_usd = gross_profit_usd * 0.003 if collateral_reserve.asset.lower() != debt_reserve.asset.lower() else 0

        net_profit_usd = gross_profit_usd - gas_cost_usd - swap_fee_usd

        if net_profit_usd < self.min_profit_usd:
            logger.info(
                "  Skipping: est_profit=$%.2f (gross=$%.2f, gas=$%.2f, swap=$%.2f) < min=$%.2f",
                net_profit_usd, gross_profit_usd, gas_cost_usd, swap_fee_usd, self.min_profit_usd
            )
            return None

        logger.info(
            "  ✅ PROFITABLE: est_net=$%.2f | gross=$%.2f | gas=$%.2f | swap=$%.2f | debt=%d %s",
            net_profit_usd, gross_profit_usd, gas_cost_usd, swap_fee_usd,
            debt_to_cover, debt_reserve.symbol
        )

        return LiquidationOpportunity(
            borrower=borrower,
            collateral_asset=collateral_reserve.asset,
            debt_asset=debt_reserve.asset,
            debt_to_cover=debt_to_cover,
            health_factor=hf,
            collateral_usd=collateral_usd,
            debt_usd=debt_usd,
            liquidation_bonus_bps=liq_bonus_bps,
            estimated_profit_wei=int(net_profit_usd * (10 ** debt_reserve.decimals)),
            estimated_gas_cost_wei=int(gas_cost_eth * 1e18),
        )

    # ─── Simulation ─────────────────────────────────────────

    async def get_asset_price(self, asset: str) -> int:
        """Fetch asset price from Aave Oracle (USD, 8 decimals)."""
        selector = keccak(text="getAssetPrice(address)")[:4]
        calldata = "0x" + selector.hex() + asset[2:].rjust(64, "0")
        result = await self._rpc_call(
            "eth_call",
            [{"to": ORACLE, "data": calldata}, "latest"],
        )
        raw = result.get("result", "0x")
        if len(raw) < 2:
            return 0
        try:
            from eth_abi import decode
            decoded = decode(["uint256"], bytes.fromhex(raw[2:]))
            return int(decoded[0])
        except Exception as e:
            logger.warning("Failed to fetch price for %s: %s", asset, e)
            return 0

    def estimate_collateral_amount(
        self,
        debt_to_cover: int,
        debt_decimals: int,
        collateral_decimals: int,
        debt_price: int,
        collateral_price: int,
        liq_bonus_bps: int,
    ) -> int:
        """
        Estimate collateral received from liquidation.
        Prices are Aave oracle prices (8 decimals).
        """
        if collateral_price == 0 or debt_price == 0:
            return 0
        # debt_value_usd = debt_to_cover / 10^debt_decimals * debt_price / 10^8
        # collateral_amount = debt_value_usd * (1 + bonus) / collateral_price * 10^collateral_decimals
        # = debt_to_cover * debt_price * (10000 + bonus) * 10^collateral_decimals / (10^debt_decimals * 10^8 * 10000)
        numerator = debt_to_cover * debt_price * (10000 + liq_bonus_bps) * (10 ** collateral_decimals)
        denominator = (10 ** debt_decimals) * (10 ** 8) * 10000
        return numerator // denominator

    async def simulate_liquidation(
        self,
        opp: LiquidationOpportunity,
    ) -> Tuple[bool, str]:
        """
        Simulate the liquidation transaction via eth_call.
        Returns (success: bool, revert_reason: str).
        """
        logger.info("Simulating liquidation for %s...", opp.borrower)

        # Build swap calldata if collateral != debt
        swap_calldata = "0x"
        swap_router = "0x0000000000000000000000000000000000000000"

        if opp.collateral_asset.lower() != opp.debt_asset.lower():
            # Estimate collateral amount for swap
            collateral_price = await self.get_asset_price(opp.collateral_asset)
            debt_price = await self.get_asset_price(opp.debt_asset)
            # Need decimals - fetch from known list or assume 18
            debt_decimals = 18
            collateral_decimals = 18
            # Try to get from known assets
            from scanner.aave_v3 import KNOWN_ASSETS
            for addr, sym, dec in KNOWN_ASSETS:
                if addr.lower() == opp.debt_asset.lower():
                    debt_decimals = dec
                if addr.lower() == opp.collateral_asset.lower():
                    collateral_decimals = dec

            est_collateral = self.estimate_collateral_amount(
                opp.debt_to_cover, debt_decimals, collateral_decimals,
                debt_price, collateral_price, opp.liquidation_bonus_bps
            )
            # Use 95% of estimate to avoid overestimation revert
            amount_in = int(est_collateral * 0.95)
            if amount_in == 0:
                amount_in = 1  # minimum non-zero

            deadline = int(time.time()) + 60
            swap_calldata = encode_uni_v3_exact_input_single(
                token_in=opp.collateral_asset,
                token_out=opp.debt_asset,
                fee=500,
                recipient=self.executor_address,  # contract receives output
                deadline=deadline,
                amount_in=amount_in,
                amount_out_minimum=0,
            )
            swap_router = UNI_V3_SWAPROUTER

        # Build executeLiquidation calldata
        tx_calldata = FlashLoanCalldataBuilder.encode_execute_liquidation(
            collateral_asset=opp.collateral_asset,
            debt_asset=opp.debt_asset,
            borrower=opp.borrower,
            debt_to_cover=opp.debt_to_cover,
            swap_router=swap_router,
            swap_calldata=swap_calldata,
            receive_a_token=False,
        )

        # Simulate
        try:
            result = await self._rpc_call(
                "eth_call",
                [
                    {
                        "from": self.account.address,
                        "to": self.executor_address,
                        "data": tx_calldata,
                    },
                    "latest",
                ],
            )

            if "error" in result:
                error_msg = result["error"].get("message", "unknown")
                error_data = result["error"].get("data", "")
                logger.warning("Simulation FAILED: %s | data=%s", error_msg, error_data)
                return False, error_msg

            logger.info("Simulation PASSED ✓")
            return True, ""

        except Exception as e:
            logger.error("Simulation exception: %s", e)
            return False, str(e)

    # ─── Transaction Broadcasting ───────────────────────────

    async def broadcast_liquidation(self, opp: LiquidationOpportunity) -> Optional[str]:
        """
        Sign and broadcast the liquidation transaction.
        Returns tx_hash hex string or None on failure.
        """
        logger.info("Broadcasting liquidation for %s...", opp.borrower)

        # Build swap calldata
        swap_calldata = "0x"
        swap_router = "0x0000000000000000000000000000000000000000"

        if opp.collateral_asset.lower() != opp.debt_asset.lower():
            # Estimate collateral amount for swap (same logic as simulation)
            collateral_price = await self.get_asset_price(opp.collateral_asset)
            debt_price = await self.get_asset_price(opp.debt_asset)
            debt_decimals = 18
            collateral_decimals = 18
            from scanner.aave_v3 import KNOWN_ASSETS
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
                recipient=self.executor_address,
                deadline=deadline,
                amount_in=amount_in,
                amount_out_minimum=0,
            )
            swap_router = UNI_V3_SWAPROUTER

        # Build tx
        tx_calldata = FlashLoanCalldataBuilder.encode_execute_liquidation(
            collateral_asset=opp.collateral_asset,
            debt_asset=opp.debt_asset,
            borrower=opp.borrower,
            debt_to_cover=opp.debt_to_cover,
            swap_router=swap_router,
            swap_calldata=swap_calldata,
            receive_a_token=False,
        )

        # Get nonce and gas
        nonce = self.w3.eth.get_transaction_count(self.account.address)
        # Estimate gas (with buffer)
        try:
            gas_estimate = self.w3.eth.estimate_gas({
                "from": self.account.address,
                "to": self.executor_address,
                "data": tx_calldata,
            })
            gas_limit = int(gas_estimate * 1.5)
        except Exception as e:
            logger.warning("Gas estimation failed: %s, using default", e)
            gas_limit = LIQUIDATION_GAS_LIMIT

        # Build EIP-1559 tx
        block = self.w3.eth.get_block("latest")
        base_fee = block["baseFeePerGas"]
        max_fee = base_fee * 2 + self.w3.to_wei("0.1", "gwei")
        priority_fee = self.w3.to_wei("0.05", "gwei")

        tx: TxParams = {
            "from": self.account.address,
            "to": self.executor_address,
            "data": tx_calldata,
            "nonce": nonce,
            "gas": gas_limit,
            "maxFeePerGas": max_fee,
            "maxPriorityFeePerGas": priority_fee,
            "chainId": ARBITRUM_CHAIN_ID,
            "type": 2,
        }

        # Sign
        private_key = os.getenv("BOT_PRIVATE_KEY")
        signed = self.w3.eth.account.sign_transaction(tx, private_key)

        # Broadcast
        try:
            tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            tx_hex = tx_hash.hex()
            logger.info("Transaction broadcasted: %s", tx_hex)
            return tx_hex
        except Exception as e:
            logger.error("Transaction broadcast FAILED: %s", e)
            return None

    # ─── Telegram Alerts ────────────────────────────────────

    async def send_alert(self, message: str) -> None:
        """Send Telegram alert if configured."""
        if not self.telegram_token or not self.telegram_chat_id:
            logger.info("[TELEGRAM] %s", message)
            return

        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {
            "chat_id": self.telegram_chat_id,
            "text": message,
            "parse_mode": "Markdown",
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        logger.warning("Telegram send failed: %s", await resp.text())
            except Exception as e:
                logger.warning("Telegram send exception: %s", e)

    # ─── Main Execution Loop ────────────────────────────────

    async def run(self, duration_sec: Optional[float] = None) -> None:
        """Main execution loop."""
        logger.info("=" * 80)
        logger.info(" AAVE v3 LIQUIDATION LIVE EXECUTOR")
        logger.info(" Account: %s", self.account.address)
        logger.info(" Executor: %s", self.executor_address)
        logger.info(" Min Profit: $%.2f", self.min_profit_usd)
        logger.info("=" * 80)

        # Bootstrap
        await self.bootstrap_borrowers()

        start_time = time.time()
        iteration = 0

        while True:
            iteration += 1
            logger.info("--- Poll #%d ---", iteration)

            # Get current block
            latest_block = await self.get_latest_block()

            # Scan known borrowers
            user_list = list(self.monitored_users.keys())
            opportunities: List[LiquidationOpportunity] = []

            for i in range(0, len(user_list), BATCH_SIZE):
                batch = user_list[i : i + BATCH_SIZE]
                tasks = [self.fetch_user_account_data(addr) for addr in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for addr, data in zip(batch, results):
                    if isinstance(data, Exception) or data is None:
                        continue

                    # Update history
                    self.monitored_users[addr].setdefault("history", []).append({
                        "block": latest_block,
                        "hf": data.health_factor_float,
                        "collateral": data.total_collateral_base,
                        "debt": data.total_debt_base,
                    })

                    # Check if liquidatable and profitable
                    if data.health_factor_float < EXECUTE_HF_THRESHOLD:
                        opp = await self.assess_liquidation_opportunity(addr, data)
                        if opp:
                            opportunities.append(opp)

            # Report status
            liquidatable_count = len([
                u for u in self.monitored_users.values()
                if u.get("history") and u["history"][-1].get("hf", 99) < 1.0
            ])
            logger.info(
                "Block=%d | Tracked=%d | Liquidatable=%d | Opportunities=%d",
                latest_block, len(self.monitored_users), liquidatable_count, len(opportunities)
            )

            # Execute opportunities
            for opp in opportunities:
                # Rate limit alerts (don't spam same borrower)
                now = time.time()
                if opp.borrower in self.last_alerted and (now - self.last_alerted[opp.borrower]) < 300:
                    logger.info("Skipping %s (alerted recently)", opp.borrower)
                    continue

                self.last_alerted[opp.borrower] = now

                # Step 1: Alert
                alert_msg = (
                    f"🚨 *LIQUIDATABLE POSITION DETECTED*\n"
                    f"Borrower: `{opp.borrower}`\n"
                    f"Health Factor: `{opp.health_factor:.4f}`\n"
                    f"Debt: `${opp.debt_usd:,.2f}`\n"
                    f"Collateral: `${opp.collateral_usd:,.2f}`\n"
                    f"Est. Net Profit: `${opp.estimated_profit_wei / 1e6:,.2f}`"
                )
                await self.send_alert(alert_msg)

                # Step 2: Simulate
                sim_ok, sim_reason = await self.simulate_liquidation(opp)
                if not sim_ok:
                    await self.send_alert(f"⚠️ Simulation failed for `{opp.borrower}`: {sim_reason}")
                    continue

                # Step 3: Execute
                tx_hash = await self.broadcast_liquidation(opp)
                if tx_hash:
                    await self.send_alert(
                        f"✅ *LIQUIDATION EXECUTED*\n"
                        f"Borrower: `{opp.borrower}`\n"
                        f"Tx: `{tx_hash}`\n"
                        f"[View on Arbiscan](https://arbiscan.io/tx/{tx_hash})"
                    )
                else:
                    await self.send_alert(f"❌ *BROADCAST FAILED* for `{opp.borrower}`")

            # Check duration
            if duration_sec and (time.time() - start_time) >= duration_sec:
                logger.info("Duration limit reached. Exiting.")
                break

            # Update borrower list periodically (every 10 min)
            if iteration % 40 == 0:
                logger.info("Refreshing borrower list...")
                await self.bootstrap_borrowers(lookback_blocks=50_000)

            await asyncio.sleep(POLL_INTERVAL)


# ─── Entry Point ────────────────────────────────────────────

def main():
    load_dotenv()

    private_key = os.getenv("BOT_PRIVATE_KEY")
    if not private_key:
        print("ERROR: BOT_PRIVATE_KEY not set", file=sys.stderr)
        sys.exit(1)

    executor_address = os.getenv("FLASH_EXECUTOR_V3")
    if not executor_address:
        print("ERROR: FLASH_EXECUTOR_V3 not set. Deploy the contract first.", file=sys.stderr)
        print("  python3 scripts/deploy_v3.py", file=sys.stderr)
        sys.exit(1)

    rpc_url = os.getenv("ALCHEMY_HTTP_URL") or os.getenv("ARBITRUM_HTTP_URL")
    if not rpc_url:
        print("ERROR: RPC URL not set", file=sys.stderr)
        sys.exit(1)

    min_profit = float(os.getenv("MIN_PROFIT_USD", "50.0"))

    executor = AaveLiquidationExecutor(
        rpc_url=rpc_url,
        private_key=private_key,
        executor_address=executor_address,
        min_profit_usd=min_profit,
    )

    try:
        asyncio.run(executor.run())
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Shutting down.")


if __name__ == "__main__":
    main()
