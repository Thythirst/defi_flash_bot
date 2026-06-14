#!/usr/bin/env python3
"""
pipeline.py — Main entry point for the Rev2 liquidation pipeline.

Architecture:
  WebSocket oracle log (0ms)
    → LocalHFEngine.update_price() (<1ms)
    → HF < 1.0? → EVEstimator.compute() (2ms)
    → EV > $8? → presigned[borrower] exists?
      → YES: blast_submit() (20ms)
      → NO: build+sign+blast_submit() (80ms)
    → outcome → SQLite
"""
import asyncio
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Dict

import websockets
from web3 import Web3
from dotenv import load_dotenv

# Add project root for .env loading and submodule imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "services" / "rev2"))

from local_hf_engine import LocalHFEngine
from presigner import PreSigner
from ev_estimator import EVEstimator
from blast_submit import blast_submit, configure_endpoints
from outcome_db import OutcomeDB

load_dotenv(dotenv_path=project_root / ".env")

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)-8s %(name)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline")

# ── Config (from .env) ────────────────────────────────────────
WS_RPC   = os.getenv("QUICKNODE_WS_URL") or os.getenv("ARBITRUM_WS_URL") or os.getenv("CHAINSTACK_ARBITRUM_WS_URL") or ""
HTTP_RPC = os.getenv("QUICKNODE_HTTP_URL", "")
HTTP_RPC_2 = os.getenv("ANKR_RPC_URL", "")
HTTP_RPC_3 = os.getenv("DRPC_RPC_URL", "")

WALLET_ADDR  = os.getenv("BOT_ADDRESS", "0x1269800101780229B50919e1e27be62DC6279e9B")
PRIVATE_KEY  = os.getenv("BOT_PRIVATE_KEY", "")
CONTRACT_ADDR = os.getenv("FLASH_EXECUTOR_V3", "0x4CdADEd4749FcB498e7E371EBF00C319674D3F8D")

AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"

# ── Chainlink Feeds (8 feeds → 8 Aave reserve assets) ─────────
# Format: aggregator_proxy_address → underlying_asset_address
CHAINLINK_FEEDS: Dict[str, str] = {
    "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # ETH/USD → WETH
    "0x6ce185860a4963106506C203335A2910413708e9": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",  # BTC/USD → WBTC
    "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",  # USDC/USD → USDC.e
    "0x3f3f5dF88dC9F13eaCF39f76967e5ae6a44E2713": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",  # USDT/USD → USDT
    "0xc5C8E77B397E531B8EC06BFb0048326F1d3aC21c": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",  # DAI/USD → DAI
    "0xb2A82404358D0F8eE4f33A9c4aE3CFa01dD42857": "0x912CE59144191C1204E64559FE8253a0e49E6548",  # ARB/USD → ARB
    "0x86E53CF1B870786351Da77A57575e79CB55812CB": "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4",  # LINK/USD → LINK
    "0xb523AE262D20A936BC152e60239920D1e3a3c3Ca": "0x5979D7b546E38E414F7E9822514be443A4800529",  # wstETH/USD → wstETH
}

ANSWER_UPDATED_TOPIC = Web3.keccak(text="AnswerUpdated(int256,uint256,uint256)").hex()
LIQUIDATION_CALL_TOPIC = Web3.keccak(
    text="LiquidationCall(address,address,address,uint256,uint256,address,bool)"
).hex()

# ── Token decimals ────────────────────────────────────────────
DECIMALS: Dict[str, int] = {
    "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1": 18,
    "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f": 8,
    "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8": 6,
    "0xaf88d065e77c8cC2239327C5EDb3A432268e5831": 6,
    "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9": 6,
    "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1": 18,
    "0x5979D7b546E38E414F7E9822514be443A4800529": 18,
    "0x912CE59144191C1204E64559FE8253a0e49E6548": 18,
    "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4": 18,
}

# ── ABI fragments ─────────────────────────────────────────────
FLASH_EXECUTOR_ABI = json.loads("""[
  {"name":"executeLiquidation","type":"function","stateMutability":"nonpayable",
   "inputs":[
     {"name":"collateralAsset","type":"address"},
     {"name":"debtAsset","type":"address"},
     {"name":"user","type":"address"},
     {"name":"debtToCover","type":"uint256"},
     {"name":"receiveAToken","type":"bool"}
   ],"outputs":[]}
]""")

QUOTER_ABI = json.loads("""[
  {"name":"quoteExactInputSingle","type":"function","stateMutability":"nonpayable",
   "inputs":[{"name":"params","type":"tuple","components":[
     {"name":"tokenIn","type":"address"},{"name":"tokenOut","type":"address"},
     {"name":"amountIn","type":"uint256"},{"name":"fee","type":"uint24"},
     {"name":"sqrtPriceLimitX96","type":"uint160"}
   ]}],
   "outputs":[
     {"name":"amountOut","type":"uint256"},{"name":"sqrtPriceX96After","type":"uint160"},
     {"name":"initializedTicksCrossed","type":"uint32"},{"name":"gasEstimate","type":"uint256"}
   ]}
]""")


class LiquidationPipeline:
    def __init__(self):
        self.w3 = Web3(Web3.HTTPProvider(HTTP_RPC))
        self.contract = self.w3.eth.contract(
            address=Web3.to_checksum_address(CONTRACT_ADDR),
            abi=FLASH_EXECUTOR_ABI,
        )
        self.db = OutcomeDB()
        self.db.init()
        self.wallet_balances: Dict[str, int] = {}

        self.hf_engine = LocalHFEngine(
            on_liquidatable=self._on_liquidatable,
            decimals=DECIMALS,
        )
        self.ev_estimator = EVEstimator(
            w3=self.w3,
            prices=self.hf_engine.prices,
            decimals=DECIMALS,
            wallet_balances=self.wallet_balances,
            quoter_abi=QUOTER_ABI,
        )
        self.presigner = PreSigner(
            w3=self.w3,
            contract=self.contract,
            wallet_addr=WALLET_ADDR,
            private_key=PRIVATE_KEY,
        )

        # Configure blast_submit RPC endpoints
        endpoints = [u for u in [HTTP_RPC, HTTP_RPC_2, HTTP_RPC_3] if u]
        if not endpoints:
            endpoints = ["https://arb1.arbitrum.io/rpc"]
        configure_endpoints(endpoints)

        self._in_flight: set = set()
        self._shutdown = asyncio.Event()

    async def run(self):
        logger.info("=" * 60)
        logger.info("  Liquidation Pipeline v2 Starting")
        logger.info(f"  WS: {WS_RPC[:50]}...")
        logger.info(f"  HTTP: {HTTP_RPC[:50]}...")
        logger.info(f"  Wallet: {WALLET_ADDR}")
        logger.info(f"  Contract: {CONTRACT_ADDR}")
        logger.info("=" * 60)

        await self._bootstrap()
        await self.presigner.start(self.hf_engine, self.ev_estimator)

        tasks = [
            asyncio.create_task(self._oracle_ws_loop(), name="oracle_ws"),
            asyncio.create_task(self._liquidation_ws_loop(), name="liquidation_ws"),
            asyncio.create_task(self._wallet_balance_loop(), name="wallet"),
            asyncio.create_task(self._stats_loop(), name="stats"),
            asyncio.create_task(self._shutdown_waiter(), name="shutdown"),
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Pipeline shutting down")
        finally:
            await self.presigner.stop()

    # ── Bootstrap ──────────────────────────────────────────────

    async def _bootstrap(self):
        """Load borrower positions from Redis watchlist + on-chain asset data."""
        logger.info("[Bootstrap] Loading positions...")
        try:
            import redis
            r = redis.from_url(
                os.getenv("REDIS_URL", "redis://localhost:6379"),
                decode_responses=True
            )
            watchlist = r.zrange("arb:watchlist:active", 0, 49, withscores=True)
            logger.info(f"[Bootstrap] {len(watchlist)} candidates from Redis")

            for addr, hf_score in watchlist:
                # Load full position data from existing classification
                # In production, this would query getUserAccountData + balanceOf for all 20 reserves
                # For now: seed with known candidates from classification_complete.csv
                pass
            r.close()
        except Exception as e:
            logger.warning(f"[Bootstrap] Redis load failed: {e}")

        # Seed from known classification data
        csv_path = project_root / "reports" / "classification_complete.csv"
        if csv_path.exists():
            import csv
            with open(csv_path) as f:
                for row in csv.DictReader(f):
                    addr = row["address"]
                    is_sa = row.get("is_same_asset", "") in ("True", "true", "1")
                    if is_sa:
                        continue

                    # Build position from asset strings — raw amounts not available
                    # without on-chain scan. Use USD-denominated placeholder amounts
                    # scaled by known prices for the HF engine to compute correctly.
                    coll_str = row.get("collateral_assets", "")
                    debt_str = row.get("debt_assets", "")
                    if not coll_str or not debt_str:
                        continue

                    coll_assets = [a.strip() for a in coll_str.split(",") if a.strip()]
                    debt_assets = [a.strip() for a in debt_str.split(",") if a.strip()]

                    # Map symbols → addresses from canonical registry
                    symbol_to_addr = {
                        "WETH": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                        "WBTC": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",
                        "USDC": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                        "USDC_n": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                        "USDC_e": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
                        "USDT": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",
                        "DAI": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",
                        "wstETH": "0x5979D7b546E38E414F7E9822514be443A4800529",
                        "weETH": "0x35751007a407ca6FEFfE80b3cB397736D2cf4dbe",
                        "ARB": "0x912CE59144191C1204E64559FE8253a0e49E6548",
                        "LINK": "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4",
                    }

                    collateral_addrs = {}
                    debt_addrs = {}
                    for sym in coll_assets:
                        a = symbol_to_addr.get(sym)
                        if a:
                            collateral_addrs[a] = int(float(row.get("collateral_usd", 0)) * 1e6)
                    for sym in debt_assets:
                        a = symbol_to_addr.get(sym)
                        if a:
                            debt_addrs[a] = int(float(row.get("debt_usd", 0)) * 1e6)

                    if collateral_addrs and debt_addrs:
                        liq_thresh = int(row.get("liq_threshold_bps", 8000)) / 10000
                        self.hf_engine.upsert_position(
                            address=addr,
                            collateral=collateral_addrs,
                            debt=debt_addrs,
                            liq_threshold={a: liq_thresh for a in collateral_addrs},
                            liq_bonus={a: 0.05 for a in collateral_addrs},
                        )

            logger.info(f"[Bootstrap] Loaded {self.hf_engine.borrower_count} positions")

        # Seed initial prices from Chainlink (fallback)
        # These will be overwritten by live oracle WS updates
        self.hf_engine.prices.update({
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1": 1625 * 10**8,
            "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f": 86500 * 10**8,
            "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8": 1_00000000,
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831": 1_00000000,
            "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9": 1_00000000,
            "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1": 1_00000000,
            "0x5979D7b546E38E414F7E9822514be443A4800529": 1940 * 10**8,
            "0x35751007a407ca6FEFfE80b3cB397736D2cf4dbe": 1610 * 10**8,
            "0x912CE59144191C1204E64559FE8253a0e49E6548": 0.35 * 10**8,
            "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4": 13.50 * 10**8,
        })

    # ── Oracle WebSocket ───────────────────────────────────────

    async def _oracle_ws_loop(self):
        backoff = 1
        while not self._shutdown.is_set():
            try:
                await self._oracle_ws_session()
                backoff = 1  # reset on clean disconnect
            except Exception as e:
                logger.warning(f"[OracleWS] Disconnected: {e} — reconnecting in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)  # exponential backoff, max 60s

    async def _oracle_ws_session(self):
        if not WS_RPC:
            logger.warning("[OracleWS] No WS_RPC configured — oracle loop disabled")
            await self._shutdown.wait()
            return

        async with websockets.connect(WS_RPC, ping_interval=10, ping_timeout=5) as ws:
            logger.info("[OracleWS] Connected")
            sub = json.dumps({
                "jsonrpc": "2.0", "id": 1, "method": "eth_subscribe",
                "params": ["logs", {
                    "address": list(CHAINLINK_FEEDS.keys()),
                    "topics": [ANSWER_UPDATED_TOPIC],
                }]
            })
            await ws.send(sub)
            async for raw in ws:
                msg = json.loads(raw)
                if 'params' not in msg:
                    continue
                await self._handle_oracle_log(msg['params']['result'])

    async def _handle_oracle_log(self, log: dict):
        t0 = time.monotonic()
        feed_addr = log['address'].lower()
        asset = CHAINLINK_FEEDS.get(feed_addr)
        if not asset:
            return
        new_price = int(log['data'][:66], 16)
        old_price = self.hf_engine.prices.get(asset, 0)
        if old_price:
            dev = abs(new_price - old_price) / old_price
            self.db.record_oracle_event(asset, old_price, new_price, dev > 0.001)
        self.hf_engine.update_price(asset, new_price)
        elapsed = (time.monotonic() - t0) * 1000
        if elapsed > 5:
            logger.warning(f"[OracleWS] Slow oracle handling: {elapsed:.1f}ms")

    # ── Liquidation event monitor ──────────────────────────────

    async def _liquidation_ws_loop(self):
        backoff = 1
        while not self._shutdown.is_set():
            try:
                await self._liquidation_ws_session()
                backoff = 1
            except Exception as e:
                logger.warning(f"[LiqWS] Disconnected: {e} — reconnecting in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _liquidation_ws_session(self):
        if not WS_RPC:
            await self._shutdown.wait()
            return

        async with websockets.connect(WS_RPC, ping_interval=10, ping_timeout=5) as ws:
            logger.info("[LiqWS] Connected")
            await ws.send(json.dumps({
                "jsonrpc": "2.0", "id": 2, "method": "eth_subscribe",
                "params": ["logs", {
                    "address": [AAVE_POOL],
                    "topics": [LIQUIDATION_CALL_TOPIC],
                }]
            }))
            async for raw in ws:
                msg = json.loads(raw)
                if 'params' not in msg:
                    continue
                await self._handle_liquidation_log(msg['params']['result'])

    async def _handle_liquidation_log(self, log: dict):
        topics = log.get('topics', [])
        if len(topics) < 3:
            return
        borrower = "0x" + topics[2][-40:]
        liquidator = "0x" + log['data'][26:66] if len(log['data']) > 66 else ""
        tx_hash = log.get('transactionHash', '')
        block = int(log.get('blockNumber', '0x0'), 16)

        if borrower.lower() in {a.lower() for a in self.hf_engine.positions}:
            if liquidator.lower() != WALLET_ADDR.lower():
                self.db.record_lost_race(borrower, liquidator, tx_hash, block)
                logger.warning(f"[Pipeline] LOST RACE: {borrower[:8]} → {liquidator[:8]}")
            self.hf_engine.remove_position(borrower)
            self.presigner.cache.pop(borrower, None)
            self._in_flight.discard(borrower)

    # ── Liquidation trigger ────────────────────────────────────

    def _on_liquidatable(self, address: str, hf: float, pos):
        if address in self._in_flight:
            return
        asyncio.create_task(self._execute_liquidation(address, hf, pos))

    async def _execute_liquidation(self, address: str, hf: float, pos):
        t0 = time.monotonic()
        self._in_flight.add(address)
        try:
            ev = self.ev_estimator.compute(address, hf, pos)
            if not ev.go:
                logger.info(f"[Pipeline] NO-GO {address[:8]}: {ev.reason}")
                return
            logger.info(f"[Pipeline] GO {address[:8]} HF={hf:.4f} EV=${ev.net_ev_usd:.2f} path={ev.execution_path}")
            tx_hash = await self.presigner.fire(address)
            if tx_hash:
                self.db.record_submission(tx_hash, address, ev.collateral_asset,
                                          ev.debt_asset, ev.execution_path, ev.net_ev_usd)
                elapsed = (time.monotonic() - t0) * 1000
                logger.info(f"[Pipeline] SUBMITTED {tx_hash[:12]} in {elapsed:.0f}ms")
                asyncio.create_task(self._watch_confirmation(tx_hash, address))
            else:
                logger.error(f"[Pipeline] Submission failed for {address[:8]}")
        except Exception as e:
            logger.error(f"[Pipeline] _execute_liquidation error: {e}", exc_info=True)
        finally:
            await asyncio.sleep(30)
            self._in_flight.discard(address)

    async def _watch_confirmation(self, tx_hash: str, borrower: str, timeout_s: int = 60):
        deadline = time.time() + timeout_s
        while time.time() < deadline:
            try:
                receipt = self.w3.eth.get_transaction_receipt(tx_hash)
                if receipt:
                    status = receipt['status']
                    block = receipt['blockNumber']
                    gas_used = receipt['gasUsed']
                    gas_price = receipt.get('effectiveGasPrice', 0)
                    gas_cost_eth = (gas_used * gas_price) / 10**18
                    eth_price = self.hf_engine.prices.get(
                        "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", 200000_00000000
                    ) / 10**8
                    gas_cost_usd = gas_cost_eth * eth_price
                    if status == 1:
                        self.db.record_confirmation(tx_hash, 0, gas_used, gas_cost_usd, 0, block)
                        logger.info(f"[Pipeline] CONFIRMED {tx_hash[:12]} block={block}")
                    else:
                        self.db.record_revert(tx_hash, block)
                        logger.warning(f"[Pipeline] REVERTED {tx_hash[:12]} block={block}")
                    return
            except Exception:
                pass
            await asyncio.sleep(2)
        logger.warning(f"[Pipeline] No receipt for {tx_hash[:12]} after {timeout_s}s")

    # ── Background loops ───────────────────────────────────────

    async def _wallet_balance_loop(self):
        while not self._shutdown.is_set():
            try:
                for asset in DECIMALS:
                    erc20 = self.w3.eth.contract(
                        address=Web3.to_checksum_address(asset),
                        abi=[{"name":"balanceOf","type":"function","stateMutability":"view",
                              "inputs":[{"name":"account","type":"address"}],
                              "outputs":[{"name":"","type":"uint256"}]}]
                    )
                    bal = erc20.functions.balanceOf(WALLET_ADDR).call()
                    self.wallet_balances[asset] = bal
                eth_bal = self.w3.eth.get_balance(WALLET_ADDR)
                logger.info(f"[Wallet] ETH={eth_bal/1e18:.4f} presigned={self.presigner.presigned_count} "
                            f"candidates={self.hf_engine.borrower_count}")
            except Exception as e:
                logger.warning(f"[Wallet] Balance refresh error: {e}")
            await asyncio.sleep(60)

    async def _stats_loop(self):
        while not self._shutdown.is_set():
            await asyncio.sleep(300)
            try:
                summary = self.db.pnl_summary()
                win_rates = self.db.win_rates()
                logger.info(f"[Stats] total={summary.get('total',0)} confirmed={(summary.get('confirmed') or 0)} "
                            f"lost={(summary.get('lost') or 0)} profit=${(summary.get('total_profit') or 0):.2f} "
                            f"gas=${(summary.get('total_gas') or 0):.2f} "
                            f"p50_latency={(summary.get('avg_latency_ms') or 0):.0f}ms")
                for path, wr in win_rates.items():
                    logger.info(f"[Stats] {path}: win_rate={wr['bayesian_win_rate']:.1%} "
                                f"({wr['wins']}W/{wr['losses']}L)")
                competitors = self.db.top_competitors(5)
                if competitors:
                    logger.info("[Stats] Top competitors:")
                    for c in competitors:
                        logger.info(f"  {c['address'][:10]} wins={c['wins']}")
            except Exception as e:
                logger.error(f"[Stats] error: {e}")

    async def _shutdown_waiter(self):
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self._shutdown.set)
            except NotImplementedError:
                pass
        await self._shutdown.wait()
        logger.info("Shutdown signal received")
        raise asyncio.CancelledError


if __name__ == "__main__":
    pipeline = LiquidationPipeline()
    asyncio.run(pipeline.run())
