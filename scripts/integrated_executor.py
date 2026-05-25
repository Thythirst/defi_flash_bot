"""
scripts/integrated_executor.py — Production Aave V3 liquidation bot.

Architecture:
  1. WebSocket subscription to newHeads (~150ms block detection)
  2. Priority queue: only check top 20 at-risk borrowers per block
  3. Multicall3 batch: one RPC call for all 20 health checks
  4. Pre-built tx cache: unsigned transactions ready for at-risk borrowers
  5. Flashbots bundles (mainnet) or direct broadcast (Arbitrum)
  6. Telegram alerts for all actions

Environment:
    CHAIN                    — "arbitrum" or "mainnet" (default: arbitrum)
    BOT_PRIVATE_KEY          — Hot wallet with gas ETH
    FLASH_EXECUTOR_V3        — Deployed contract address
    ARBITRUM_HTTP_URL        — Arbitrum HTTP RPC
    ARBITRUM_WS_URL          — Arbitrum WebSocket RPC
    MAINNET_HTTP_URL         — Mainnet HTTP RPC
    MAINNET_WS_URL           — Mainnet WebSocket RPC
    FLASHBOTS_AUTH_KEY       — Separate auth key for Flashbots (mainnet only)
    TELEGRAM_BOT_TOKEN       — Optional alerts
    TELEGRAM_CHAT_ID         — Optional alerts
    MIN_PROFIT_USD           — Override default ($50 arb, $500 mainnet)
    DRY_RUN                  — "1" to simulate only, never broadcast

Usage:
    export CHAIN=mainnet
    export BOT_PRIVATE_KEY=0x...
    export FLASH_EXECUTOR_V3=0x...
    export MAINNET_HTTP_URL=https://eth-mainnet.g.alchemy.com/v2/...
    export MAINNET_WS_URL=wss://eth-mainnet.g.alchemy.com/v2/...
    export FLASHBOTS_AUTH_KEY=0x...
    python3 scripts/integrated_executor.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv
from eth_abi import encode
from eth_account.datastructures import SignedTransaction
from eth_utils import keccak, to_hex
from web3 import Web3
from web3.types import TxParams

# ─── Local imports ──────────────────────────────────────────
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scanner.chains import get_chain_config, ChainConfig
from scanner.aave_v3 import (
    UserAccountData,
    UserReserveData,
    format_hf_status,
    fetch_user_reserves,
    pick_liquidation_target,
)
from scanner.liquidation_executor import encode_uni_v3_exact_input_single
from scanner.multicall_batch import MulticallBatcher
from scanner.priority_queue import LiquidationPriorityQueue, BorrowerNode
from scanner.websocket_monitor import HybridMonitor
from scanner.flashbots_relay import FlashbotsRelay, MultiRelayManager

# ─── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("integrated_executor")

# ─── Constants ──────────────────────────────────────────────
ALERT_HF_THRESHOLD = 1.05
EXECUTE_HF_THRESHOLD = 1.0
BATCH_SIZE = 20          # Health checks per multicall batch
TX_CACHE_SIZE = 50       # Pre-built unsigned transactions
REFRESH_INTERVAL = 40    # Blocks between borrower list refresh


# ─── Dataclasses ────────────────────────────────────────────

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
    estimated_profit_wei: int
    estimated_gas_cost_wei: int
    block_number: int = 0


@dataclass
class CachedTransaction:
    """Pre-built unsigned transaction for a borrower."""
    borrower: str
    tx_params: TxParams
    built_at_block: int
    collateral_asset: str
    debt_asset: str
    debt_to_cover: int
    swap_calldata: str
    swap_router: str


# ─── Integrated Executor ────────────────────────────────────

class IntegratedLiquidationExecutor:
    """
    High-performance liquidation executor.

    Latency targets:
    - Block detection:     ~150ms  (WebSocket newHeads)
    - Health check batch:  ~50ms   (Multicall3, 20 borrowers)
    - Opportunity assess:  ~20ms   (local math)
    - TX build/sign:       ~30ms   (pre-built cache)
    - Broadcast:           ~50ms   (Flashbots or direct)
    - Total:               ~300ms  end-to-end
    """

    def __init__(
        self,
        chain_config: ChainConfig,
        private_key: str,
        executor_address: str,
        min_profit_usd: float,
        dry_run: bool = False,
    ):
        self.chain = chain_config
        self.dry_run = dry_run

        # Web3 connections
        http_url = os.getenv(self.chain.rpc_env_var)
        ws_url = os.getenv(self.chain.ws_env_var)
        if not http_url or not ws_url:
            raise ValueError(f"Set {self.chain.rpc_env_var} and {self.chain.ws_env_var}")

        self.w3 = Web3(Web3.HTTPProvider(http_url))
        self.account = self.w3.eth.account.from_key(private_key)
        self.executor_address = self.w3.to_checksum_address(executor_address)

        # Load executor ABI
        self.executor_abi = self._load_executor_abi()
        self.executor = self.w3.eth.contract(
            address=self.executor_address,
            abi=self.executor_abi,
        )

        # Subsystems
        self.queue = LiquidationPriorityQueue(max_size=10_000, check_top_n=BATCH_SIZE)
        self.batcher = MulticallBatcher(self.w3)
        self.tx_cache: Dict[str, CachedTransaction] = {}

        # Flashbots (mainnet only)
        self.flashbots: Optional[FlashbotsRelay] = None
        self.multi_relay: Optional[MultiRelayManager] = None
        if self.chain.uses_flashbots:
            fb_auth_key = os.getenv(self.chain.flashbots_auth_signer or "FLASHBOTS_AUTH_KEY")
            if fb_auth_key:
                self.flashbots = FlashbotsRelay(self.chain.flashbots_relay, fb_auth_key)
                self.multi_relay = MultiRelayManager(fb_auth_key)
                self.multi_relay.add_default_relays()
                logger.info("Flashbots relay configured")
            else:
                logger.warning("Mainnet requires FLASHBOTS_AUTH_KEY — broadcasts will fail")

        # Telegram
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")

        # State
        self.monitored_users: Dict[str, Dict] = {}
        self.last_alerted: Dict[str, float] = {}
        self.known_borrowers: set = set()
        self.current_block = 0
        self._running = False

        # Metrics
        self.blocks_processed = 0
        self.health_checks = 0
        self.opportunities_found = 0
        self.simulations_run = 0
        self.txs_broadcast = 0
        self.txs_confirmed = 0

    def _load_executor_abi(self) -> List[Dict]:
        abi_path = (
            Path(__file__).parent.parent
            / "out"
            / "FlashExecutorV3.sol"
            / "FlashExecutorV3.json"
        )
        with open(abi_path) as f:
            artifact = json.load(f)
        return artifact["abi"]

    # ─── Bootstrap ────────────────────────────────────────────

    async def bootstrap_borrowers(self, lookback_blocks: int = 200_000) -> int:
        """Build initial borrower set from Aave Borrow events."""
        latest = self.w3.eth.block_number
        from_block = max(latest - lookback_blocks, 0)

        logger.info("Bootstrapping borrowers from blocks %d-%d...", from_block, latest)
        borrow_topic = "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0"

        borrowers = set()
        chunk_size = 2000
        for chunk_start in range(from_block, latest + 1, chunk_size):
            chunk_end = min(chunk_start + chunk_size - 1, latest)
            try:
                logs = self.w3.eth.get_logs({
                    "address": self.chain.aave_pool,
                    "topics": [borrow_topic],
                    "fromBlock": chunk_start,
                    "toBlock": chunk_end,
                })
                for log in logs:
                    user = "0x" + log["topics"][2][-40:]
                    borrowers.add(user)
            except Exception as e:
                logger.debug("Bootstrap chunk error: %s", e)

        # Initialize queue with neutral HF (will be updated on first check)
        for borrower in borrowers:
            if borrower not in self.monitored_users:
                self.monitored_users[borrower] = {
                    "address": borrower,
                    "first_seen": latest,
                    "history": [],
                }
            self.queue.update(borrower, health_factor=999.0, block_number=latest)

        self.known_borrowers.update(borrowers)
        logger.info("Bootstrapped %d borrowers", len(borrowers))
        return len(borrowers)

    # ─── Block Handler (Critical Path) ────────────────────────

    async def on_new_block(self, block: Dict[str, Any]) -> None:
        """
        Handle new block from WebSocket.
        This is the hot path — must complete in <300ms.
        """
        start_time = time.time()
        self.current_block = int(block.get("number", "0x0"), 16)
        self.blocks_processed += 1

        # 1. Get top N at-risk borrowers from priority queue
        at_risk = self.queue.get_at_risk(threshold=1.15, n=BATCH_SIZE)
        if not at_risk:
            return

        addresses = [n.address for n in at_risk]

        # 2. Batch health check via Multicall3
        health_results = self.batcher.batch_health_factors(
            addresses, self.chain.aave_pool
        )
        self.health_checks += len(addresses)

        # 3. Update queue with fresh data
        opportunities: List[LiquidationOpportunity] = []
        for addr, result in zip(addresses, health_results):
            if result is None:
                continue

            collateral, debt, available, ltv, liq_thresh, hf_raw = result
            hf = hf_raw / 1e18

            # Update queue
            self.queue.update(
                addr,
                health_factor=hf,
                total_debt_base=debt,
                total_collateral_base=collateral,
                block_number=self.current_block,
            )

            # Update history
            self.monitored_users[addr].setdefault("history", []).append({
                "block": self.current_block,
                "hf": hf,
                "collateral": collateral,
                "debt": debt,
            })

            # Check liquidatable
            if hf < EXECUTE_HF_THRESHOLD and debt > 0:
                opp = await self._assess_opportunity(addr, hf, collateral, debt)
                if opp:
                    opportunities.append(opp)

        # 4. Process opportunities
        for opp in opportunities:
            self.opportunities_found += 1
            await self._process_opportunity(opp)

        # 5. Refresh pre-built tx cache for at-risk borrowers
        await self._refresh_tx_cache(at_risk)

        # 6. Periodic borrower refresh
        if self.blocks_processed % REFRESH_INTERVAL == 0:
            asyncio.create_task(self._refresh_borrowers())

        elapsed_ms = (time.time() - start_time) * 1000
        if elapsed_ms > 500:
            logger.warning("Slow block processing: %.1fms", elapsed_ms)

    # ─── Opportunity Assessment ───────────────────────────────

    async def _assess_opportunity(
        self,
        borrower: str,
        hf: float,
        total_collateral_base: int,
        total_debt_base: int,
    ) -> Optional[LiquidationOpportunity]:
        """Determine if a position is profitable to liquidate."""
        debt_usd = total_debt_base / 1e8
        if debt_usd < self.chain.min_debt_usd:
            return None

        collateral_usd = total_collateral_base / 1e8

        # Discover reserves
        reserves = fetch_user_reserves(self.w3, borrower)
        target = pick_liquidation_target(reserves)
        if target is None:
            return None

        collateral_reserve, debt_reserve, debt_to_cover = target

        # Fetch liquidation bonus
        liq_bonus_bps = await self._get_liquidation_bonus(collateral_reserve.asset)

        # Profit estimate
        debt_to_cover_usd = debt_to_cover / (10 ** debt_reserve.decimals)
        if total_debt_base > 0:
            price_scale = debt_usd / (total_debt_base / 1e8)
            debt_to_cover_usd *= price_scale

        gross_profit_usd = debt_to_cover_usd * (liq_bonus_bps / 10000)
        gas_cost_eth = (self.chain.default_gas_limit * int(0.1e9)) / 1e18
        gas_cost_usd = gas_cost_eth * 2000
        swap_fee_usd = gross_profit_usd * 0.003 if collateral_reserve.asset.lower() != debt_reserve.asset.lower() else 0

        net_profit_usd = gross_profit_usd - gas_cost_usd - swap_fee_usd
        min_profit = float(os.getenv("MIN_PROFIT_USD", "50.0"))

        if net_profit_usd < min_profit:
            return None

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
            block_number=self.current_block,
        )

    async def _get_liquidation_bonus(self, collateral_asset: str) -> int:
        """Fetch liquidation bonus in bps."""
        selector = keccak(text="getReserveConfigurationData(address)")[:4]
        calldata = "0x" + selector.hex() + collateral_asset[2:].rjust(64, "0")

        try:
            raw = self.w3.eth.call({
                "to": self.chain.aave_pool_data_provider,
                "data": calldata,
            })
            decoded = decode(
                ["uint256", "uint256", "uint256", "uint256", "uint256", "bool", "bool", "bool", "bool", "bool"],
                raw,
            )
            return int(decoded[2])
        except Exception:
            return 500  # default 5%

    # ─── Transaction Cache ────────────────────────────────────

    async def _refresh_tx_cache(self, at_risk_nodes: List[BorrowerNode]) -> None:
        """Pre-build unsigned transactions for at-risk borrowers."""
        for node in at_risk_nodes:
            if node.health_factor > 1.05:
                # Not close enough to liquidation
                if node.address in self.tx_cache:
                    del self.tx_cache[node.address]
                continue

            if node.address in self.tx_cache:
                cached = self.tx_cache[node.address]
                if cached.built_at_block >= self.current_block - 5:
                    continue  # Still fresh

            # Build pre-signed transaction skeleton
            try:
                tx = await self._build_unsigned_tx(node.address)
                if tx:
                    self.tx_cache[node.address] = tx
            except Exception as e:
                logger.debug("TX cache build failed for %s: %s", node.address, e)

        # Evict old entries
        if len(self.tx_cache) > TX_CACHE_SIZE:
            sorted_cache = sorted(
                self.tx_cache.items(),
                key=lambda x: x[1].built_at_block
            )
            for addr, _ in sorted_cache[:len(sorted_cache) - TX_CACHE_SIZE]:
                del self.tx_cache[addr]

    async def _build_unsigned_tx(self, borrower: str) -> Optional[CachedTransaction]:
        """Build an unsigned liquidation transaction for a borrower."""
        reserves = fetch_user_reserves(self.w3, borrower)
        target = pick_liquidation_target(reserves)
        if target is None:
            return None

        collateral_reserve, debt_reserve, debt_to_cover = target

        # Build swap calldata if needed
        swap_calldata = "0x"
        swap_router = "0x0000000000000000000000000000000000000000"

        if collateral_reserve.asset.lower() != debt_reserve.asset.lower():
            # Estimate collateral for swap
            collateral_price = await self._get_asset_price(collateral_reserve.asset)
            debt_price = await self._get_asset_price(debt_reserve.asset)

            est_collateral = self._estimate_collateral_amount(
                debt_to_cover,
                debt_reserve.decimals,
                collateral_reserve.decimals,
                debt_price,
                collateral_price,
                500,  # default bonus
            )
            amount_in = int(est_collateral * 0.95)
            if amount_in == 0:
                amount_in = 1

            deadline = int(time.time()) + 60
            swap_calldata = encode_uni_v3_exact_input_single(
                token_in=collateral_reserve.asset,
                token_out=debt_reserve.asset,
                fee=500,
                recipient=self.executor_address,
                deadline=deadline,
                amount_in=amount_in,
                amount_out_minimum=0,
            )
            swap_router = self.chain.uni_v3_swaprouter

        # Build executeLiquidation calldata
        selector = keccak(
            text="executeLiquidation(address,address,address,uint256,bool,address,bytes)"
        )[:4]
        encoded = encode(
            ["address", "address", "address", "uint256", "bool", "address", "bytes"],
            [
                collateral_reserve.asset,
                debt_reserve.asset,
                borrower,
                debt_to_cover,
                False,
                swap_router,
                bytes.fromhex(swap_calldata[2:]) if swap_calldata != "0x" else b"",
            ],
        )
        tx_calldata = "0x" + selector.hex() + encoded.hex()

        # Estimate gas
        try:
            gas_estimate = self.w3.eth.estimate_gas({
                "from": self.account.address,
                "to": self.executor_address,
                "data": tx_calldata,
            })
            gas_limit = int(gas_estimate * 1.5)
        except Exception:
            gas_limit = self.chain.default_gas_limit

        # Build EIP-1559 tx params
        block = self.w3.eth.get_block("latest")
        base_fee = block.get("baseFeePerGas", self.w3.to_wei("1", "gwei"))
        max_fee = base_fee * 2 + self.w3.to_wei("0.1", "gwei")
        priority_fee = self.w3.to_wei(str(self.chain.min_priority_fee_gwei), "gwei")

        tx_params: TxParams = {
            "from": self.account.address,
            "to": self.executor_address,
            "data": tx_calldata,
            "gas": gas_limit,
            "maxFeePerGas": max_fee,
            "maxPriorityFeePerGas": priority_fee,
            "chainId": self.chain.chain_id,
            "type": 2,
            "nonce": self.w3.eth.get_transaction_count(self.account.address),
        }

        return CachedTransaction(
            borrower=borrower,
            tx_params=tx_params,
            built_at_block=self.current_block,
            collateral_asset=collateral_reserve.asset,
            debt_asset=debt_reserve.asset,
            debt_to_cover=debt_to_cover,
            swap_calldata=swap_calldata,
            swap_router=swap_router,
        )

    def _estimate_collateral_amount(
        self,
        debt_to_cover: int,
        debt_decimals: int,
        collateral_decimals: int,
        debt_price: int,
        collateral_price: int,
        liq_bonus_bps: int,
    ) -> int:
        if collateral_price == 0 or debt_price == 0:
            return 0
        numerator = debt_to_cover * debt_price * (10000 + liq_bonus_bps) * (10 ** collateral_decimals)
        denominator = (10 ** debt_decimals) * (10 ** 8) * 10000
        return numerator // denominator

    async def _get_asset_price(self, asset: str) -> int:
        selector = keccak(text="getAssetPrice(address)")[:4]
        calldata = "0x" + selector.hex() + asset[2:].rjust(64, "0")
        try:
            raw = self.w3.eth.call({"to": self.chain.aave_oracle, "data": calldata})
            decoded = decode(["uint256"], raw)
            return int(decoded[0])
        except Exception:
            return 0

    # ─── Opportunity Execution ────────────────────────────────

    async def _process_opportunity(self, opp: LiquidationOpportunity) -> None:
        """Simulate and optionally execute a liquidation."""
        now = time.time()
        if opp.borrower in self.last_alerted and (now - self.last_alerted[opp.borrower]) < 300:
            return
        self.last_alerted[opp.borrower] = now

        # Alert
        await self._send_alert(
            f"🚨 *LIQUIDATABLE* `{opp.borrower[:8]}...`\n"
            f"HF: `{opp.health_factor:.4f}` | Debt: `${opp.debt_usd:,.0f}`\n"
            f"Est. profit: `${opp.estimated_profit_wei / 1e6:,.2f}`"
        )

        # Check cache
        cached = self.tx_cache.get(opp.borrower)
        if not cached:
            logger.warning("No cached tx for %s, building fresh...", opp.borrower)
            cached = await self._build_unsigned_tx(opp.borrower)
            if not cached:
                return

        # Update nonce (critical: may have changed since cache)
        cached.tx_params["nonce"] = self.w3.eth.get_transaction_count(self.account.address)

        # Simulate
        self.simulations_run += 1
        sim_ok = await self._simulate(cached)
        if not sim_ok:
            await self._send_alert(f"⚠️ Simulation failed for `{opp.borrower[:8]}...`")
            return

        if self.dry_run:
            logger.info("DRY RUN: Would execute liquidation for %s", opp.borrower)
            return

        # Execute
        tx_hash = await self._execute(cached)
        if tx_hash:
            self.txs_broadcast += 1
            await self._send_alert(
                f"✅ *EXECUTED* `{opp.borrower[:8]}...`\n"
                f"Tx: `{tx_hash}`"
            )
        else:
            await self._send_alert(f"❌ *BROADCAST FAILED* `{opp.borrower[:8]}...`")

    async def _simulate(self, cached: CachedTransaction) -> bool:
        """Simulate via eth_call."""
        try:
            self.w3.eth.call({
                "from": self.account.address,
                "to": self.executor_address,
                "data": cached.tx_params["data"],
            })
            return True
        except Exception as e:
            logger.warning("Simulation failed: %s", e)
            return False

    async def _execute(self, cached: CachedTransaction) -> Optional[str]:
        """Sign and broadcast the transaction."""
        private_key = os.getenv("BOT_PRIVATE_KEY")
        signed: SignedTransaction = self.w3.eth.account.sign_transaction(
            cached.tx_params, private_key
        )

        if self.chain.uses_flashbots and self.multi_relay:
            # Mainnet: submit via Flashbots/MEV-Share
            try:
                target_block = self.current_block + 1
                result = await self.multi_relay.broadcast_to_all(
                    [signed], target_block
                )
                bundle_hash = result.get("result", {}).get("bundleHash", "unknown")
                logger.info("Flashbots bundle submitted: %s", bundle_hash)
                return bundle_hash
            except Exception as e:
                logger.error("Flashbots submission failed: %s", e)
                return None
        else:
            # Arbitrum: direct broadcast
            try:
                tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
                return tx_hash.hex()
            except Exception as e:
                logger.error("Broadcast failed: %s", e)
                return None

    # ─── Maintenance ──────────────────────────────────────────

    async def _refresh_borrowers(self) -> None:
        """Periodically refresh borrower list."""
        logger.info("Refreshing borrower list...")
        await self.bootstrap_borrowers(lookback_blocks=50_000)

    # ─── Alerts ───────────────────────────────────────────────

    async def _send_alert(self, message: str) -> None:
        if not self.telegram_token or not self.telegram_chat_id:
            logger.info("[ALERT] %s", message)
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
                        logger.warning("Telegram failed: %s", await resp.text())
            except Exception as e:
                logger.warning("Telegram error: %s", e)

    # ─── Main Loop ────────────────────────────────────────────

    async def run(self) -> None:
        """Start the integrated executor."""
        logger.info("=" * 80)
        logger.info(" INTEGRATED LIQUIDATION EXECUTOR v2.0")
        logger.info(" Chain: %s (ID: %d)", self.chain.name, self.chain.chain_id)
        logger.info(" Account: %s", self.account.address)
        logger.info(" Executor: %s", self.executor_address)
        logger.info(" Mode: %s", "DRY RUN" if self.dry_run else "LIVE")
        logger.info(" Flashbots: %s", "YES" if self.chain.uses_flashbots else "NO")
        logger.info("=" * 80)

        # Bootstrap
        await self.bootstrap_borrowers()

        # Initial health check to populate queue
        await self._initial_health_check()

        # Start WebSocket monitor
        http_url = os.getenv(self.chain.rpc_env_var)
        ws_url = os.getenv(self.chain.ws_env_var)
        monitor = HybridMonitor(ws_url, http_url)
        monitor.on_block = lambda block: asyncio.create_task(self.on_new_block(block))

        self._running = True
        try:
            await monitor.start()
        except asyncio.CancelledError:
            logger.info("Shutdown requested")
        finally:
            self._running = False
            await monitor.stop()
            self._print_stats()

    async def _initial_health_check(self) -> None:
        """Run initial health check on all borrowers to populate queue."""
        logger.info("Running initial health check...")
        addresses = list(self.monitored_users.keys())

        for i in range(0, len(addresses), BATCH_SIZE):
            batch = addresses[i:i + BATCH_SIZE]
            results = self.batcher.batch_health_factors(batch, self.chain.aave_pool)

            for addr, result in zip(batch, results):
                if result is None:
                    continue
                collateral, debt, available, ltv, liq_thresh, hf_raw = result
                hf = hf_raw / 1e18
                self.queue.update(
                    addr,
                    health_factor=hf,
                    total_debt_base=debt,
                    total_collateral_base=collateral,
                    block_number=self.current_block,
                )

        stats = self.queue.stats()
        logger.info("Initial check complete: %s", stats)

    def _print_stats(self) -> None:
        logger.info("=" * 80)
        logger.info(" SHUTDOWN STATS")
        logger.info(" Blocks processed: %d", self.blocks_processed)
        logger.info(" Health checks: %d", self.health_checks)
        logger.info(" Opportunities found: %d", self.opportunities_found)
        logger.info(" Simulations run: %d", self.simulations_run)
        logger.info(" TXs broadcast: %d", self.txs_broadcast)
        logger.info("=" * 80)


# ─── Entry Point ────────────────────────────────────────────

def main():
    load_dotenv()

    chain_name = os.getenv("CHAIN", "arbitrum").lower()
    chain_config = get_chain_config(chain_name)

    private_key = os.getenv("BOT_PRIVATE_KEY")
    if not private_key:
        print("ERROR: BOT_PRIVATE_KEY not set", file=sys.stderr)
        sys.exit(1)

    executor_address = os.getenv("FLASH_EXECUTOR_V3")
    if not executor_address:
        print("ERROR: FLASH_EXECUTOR_V3 not set", file=sys.stderr)
        sys.exit(1)

    min_profit = float(os.getenv("MIN_PROFIT_USD", "50.0"))
    dry_run = os.getenv("DRY_RUN", "0") == "1"

    executor = IntegratedLiquidationExecutor(
        chain_config=chain_config,
        private_key=private_key,
        executor_address=executor_address,
        min_profit_usd=min_profit,
        dry_run=dry_run,
    )

    try:
        asyncio.run(executor.run())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")


if __name__ == "__main__":
    main()
