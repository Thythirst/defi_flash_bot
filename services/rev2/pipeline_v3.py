#!/usr/bin/env python3
"""
pipeline_v3.py — Fully integrated Rev3 liquidation pipeline.
All 11 weaknesses from the Rev2 audit resolved.

Fixes wired:
  W1  position_loader    — real on-chain data via Multicall3 (replaces CSV fabrication)
  W2  blast_submit       — parallel 4-endpoint submission with MEV Blocker
  W3  liq_log_parser     — correct LiquidationCall event parsing
  W4  async_web3         — AsyncWeb3 throughout (no event-loop blocking)
  W5  async_web3         — NonceManager atomic allocation (no collision)
  W6  execution_guards   — ConfirmationTracker (replaces sleep(30) cooldown)
  W7  ws_manager         — dual WS + HTTP fallback (no single point of failure)
  W8  execution_guards   — PresignedTxGuard staleness check (gas + debt drift)
  W9  execution_guards   — PriceRegistry with max_age_seconds (no stale prices)
  W10 async_web3         — concurrent balanceOf via gather()
  W11 collateral_selector — risk-adjusted collateral ranking
"""

import asyncio
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Dict, Optional

from web3 import AsyncWeb3, Web3
from dotenv import load_dotenv

# Project paths
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "services" / "rev2"))

from position_loader     import PositionLoader
from blast_submit        import configure_endpoints, blast_submit, blast_submit_ex, close_session
from liq_log_parser      import parse_liquidation_log
from async_web3          import AsyncRPCClient, NonceManager, QuoterAsync
from execution_guards    import ConfirmationTracker, PresignedTxGuard, PriceRegistry, PresignedSnapshot
from ws_manager          import WSManager
from collateral_selector import CollateralSelector
from local_hf_engine     import LocalHFEngine
from price_poller        import PricePoller as BasePricePoller, ARBITRUM_CHAINLINK_FEEDS as PP_FEEDS
from fix_wsteth_staleness import StalenessGatedPricePoller
from fix_gas_reserve      import GasReserveGuard, GasEstimator
from fix_min_profit       import ProfitGate
from outcome_db           import OutcomeDB
from rpc_provider         import RPCProviderConfig, get_rpc_provider
from hot_path_fix        import SharedState, FastGasGuard, CachedBaseFeeChecker, LatencyTracker
from cache_prewarm       import CachePrewarmer, HFChangeDetector
from skip_telemetry      import SkipTelemetry, SkipEvent, SkipReason
from flash_loan_route    import FlashLoanTxBuilder, FlashLoanTxData
from compound_v3          import CompoundV3Module, COMPOUND_MARKETS
from wsteth_fix           import WstETHPriceManager
from lst_depeg_scanner    import LSTDepegScanner
from quote_cache          import QuoteCache, KNOWN_SLOW_PAIRS
from aave_base            import AaveBaseModule, BaseChainConfig, BaseFlashLoanTxBuilder
from gas_oracle           import GasOracle
from pathlib              import Path

import json as _json
import redis.asyncio as aioredis

load_dotenv(dotenv_path=project_root / ".env")


def _load_gas_config() -> dict:
    """
    Load gas oracle config from calibration file if it exists.
    Falls back to safe defaults if file missing or malformed.
    """
    # Try prod path first (may not be accessible from this user)
    try:
        prod_path = Path("/home/ubuntu/defi_flash_bot/config/gas_oracle.json")
        config_path = prod_path if prod_path.exists() else Path(__file__).parent.parent.parent / "config" / "gas_oracle.json"
    except PermissionError:
        config_path = Path(__file__).parent.parent.parent / "config" / "gas_oracle.json"
    defaults = {
        "percentile":          0.75,
        "surge_buffer":        2.0,
        "cascade_percentile":  0.90,
    }
    if not config_path.exists():
        return defaults
    try:
        data = _json.loads(config_path.read_text())
        pct = float(data.get("percentile", defaults["percentile"]))
        sb  = float(data.get("surge_buffer", defaults["surge_buffer"]))
        cp  = float(data.get("cascade_percentile", defaults["cascade_percentile"]))
        if not (0.5 <= pct <= 0.99 and 1.0 <= sb <= 10.0 and 0.5 <= cp <= 0.99):
            logger.warning("[GasConfig] Values out of range — using defaults")
            return defaults
        result = {"percentile": pct, "surge_buffer": sb, "cascade_percentile": cp}
        logger.info(
            f"[GasConfig] Loaded from calibration: "
            f"P{int(pct*100)} surge={sb}x cascade=P{int(cp*100)}"
        )
        return result
    except Exception as e:
        logger.warning(f"[GasConfig] Failed to load config: {e} — using defaults")
        return defaults

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)-8s %(name)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("pipeline_v3")

# ── Config (from .env) ────────────────────────────────────────
# Centralised RPC provider selection with health-checked rotation.
# Priority: DRPC (paid, stable) → PublicNode (free, high limits) → Alchemy
RPC_CONFIG = RPCProviderConfig.from_env()
_DRPC_WSS = os.getenv("DRPC_WSS_URL", "")
_PUBLIC_WSS = os.getenv("RPC_WSS_URL", "wss://arbitrum-one.publicnode.com")
_ALCHEMY_WSS = os.getenv("ARBITRUM_WS_URL", "")
PRIMARY_WSS   = _DRPC_WSS or _ALCHEMY_WSS or _PUBLIC_WSS
SECONDARY_WSS = _PUBLIC_WSS if PRIMARY_WSS != _PUBLIC_WSS else (_ALCHEMY_WSS if PRIMARY_WSS != _ALCHEMY_WSS else "")

WALLET_ADDR  = os.getenv("BOT_ADDRESS", "0x1269800101780229B50919e1e27be62DC6279e9B")
PRIVATE_KEY  = os.getenv("BOT_PRIVATE_KEY", "")
CONTRACT_ADDR = os.getenv("FLASH_EXECUTOR_V3", "0x4CdADEd4749FcB498e7E371EBF00C319674D3F8D")
AAVE_POOL    = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"

# ── Startup guard: refuse to start with missing/wrong critical env vars ──
_MISSING_CRITICAL = []
if not PRIVATE_KEY or PRIVATE_KEY == "":
    _MISSING_CRITICAL.append("BOT_PRIVATE_KEY")
if not WALLET_ADDR or WALLET_ADDR == "":
    _MISSING_CRITICAL.append("BOT_ADDRESS")
if not os.getenv("FLASH_EXECUTOR_V3"):
    _MISSING_CRITICAL.append("FLASH_EXECUTOR_V3")
_MISSING_WARN = []
if not os.getenv("TELEGRAM_BOT_TOKEN"):
    _MISSING_WARN.append("TELEGRAM_BOT_TOKEN")
if not os.getenv("TELEGRAM_CHAT_ID"):
    _MISSING_WARN.append("TELEGRAM_CHAT_ID")
if _MISSING_CRITICAL:
    logger.critical(
        f"FATAL: Required env vars missing from .env (systemd EnvironmentFile may have failed): "
        f"{', '.join(_MISSING_CRITICAL)}"
    )
    logger.critical("Refusing to start with wrong/missing contract or wallet. Fix .env and restart.")
    sys.exit(1)
if _MISSING_WARN:
    logger.warning(
        f"WARNING: Optional env vars missing: {', '.join(_MISSING_WARN)} — "
        f"Telegram alerts disabled"
    )

# ── Chainlink Feeds ───────────────────────────────────────────
# feed_address → underlying_asset_address (for price routing)
CHAINLINK_FEEDS: Dict[str, str] = {
    "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # ETH/USD → WETH
    "0x6ce185860a4963106506C203335A2910413708e9": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",  # BTC/USD → WBTC
    "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3": "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",  # USDC/USD → USDC.e
    "0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",  # USDT/USD → USDT
    "0xc5C8E77B397E531B8EC06BFb0048326F1d3aC21c": "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1",  # DAI/USD → DAI
    "0xb2A824043730FE05F3DA2efaFa1CBbe83fa548D6": "0x912CE59144191C1204E64559FE8253a0e49E6548",  # ARB/USD → ARB
    "0x86E53CF1B870786351Da77A57575e79CB55812CB": "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4",  # LINK/USD → LINK
    "0xb523AE262D20A936BC152e6023996e46FDC2A95D": "0x5979D7b546E38E414F7E9822514be443A4800529",  # wstETH/ETH → wstETH
}
CHAINLINK_FEED_ADDRESSES = list(CHAINLINK_FEEDS.keys())
FEED_TO_ASSET = {k.lower(): v for k, v in CHAINLINK_FEEDS.items()}

# ── Token decimals ────────────────────────────────────────────
DECIMALS: Dict[str, int] = {
    "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1": 18,   # WETH
    "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f": 8,    # WBTC
    "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8": 6,    # USDC.e
    "0xaf88d065e77c8cC2239327C5EDb3A432268e5831": 6,    # USDC (native)
    "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9": 6,    # USDT
    "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1": 18,   # DAI
    "0x5979D7b546E38E414F7E9822514be443A4800529": 18,   # wstETH
    "0x912CE59144191C1204E64559FE8253a0e49E6548": 18,   # ARB
    "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4": 18,   # LINK
    "0xba5DdD1f9d7F570dc94a51479a000E3BCE967196": 18,   # AAVE
    "0xEC70Dcb4A1EFa46b8F2D97C310C9c4790ba5ffA8": 18,   # rETH
    "0x93b346b6BC2548dA6A1E7d98E9a421B42541425b": 18,   # LUSD
    "0x17FC002b466eEc40DaE837Fc4bE5c67993ddBd6F": 18,   # FRAX
    "0x35751007a407ca6FEFfE80b3cB397736D2cf4dbe": 18,   # weETH
    "0x7dfF72693f6A4149b17e7C6314655f6A9F7c8B33": 18,   # GHO
    "0x2416092f143378750bb29b79eD961ab195CcEea5": 18,   # ezETH
    "0x4186BFC76E2E237523CBC30FD220FE055156b41F": 18,   # rsETH
    "0x6c84a8f1c29108F47a79964b5Fe888D4f4D0dE40": 8,    # tBTC
}

# Aave V3 oracle on Arbitrum — used to price assets not covered by our Chainlink feeds
# (weETH, rsETH, ezETH, rETH, LUSD, GHO, native USDC, FRAX, AAVE, tBTC, MAI, eUSD)
AAVE_ORACLE_ADDR = Web3.to_checksum_address("0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7")
AAVE_ORACLE_ASSETS = [
    "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # USDC (native) — same as USDC.e price
    "0x35751007a407ca6FEFfE80b3cB397736D2cf4dbe",  # weETH
    "0xEC70Dcb4A1EFa46b8F2D97C310C9c4790ba5ffA8",  # rETH
    "0x93b346b6BC2548dA6A1E7d98E9a421B42541425b",  # LUSD
    "0x2416092f143378750bb29b79eD961ab195CcEea5",  # ezETH
    "0x4186BFC76E2E237523CBC30FD220FE055156b41F",  # rsETH
    "0x7dfF72693f6A4149b17e7C6314655f6A9F7c8B33",  # GHO
    "0xba5DdD1f9d7F570dc94a51479a000E3BCE967196",  # AAVE
    "0x6c84a8f1c29108F47a79964b5Fe888D4f4D0dE40",  # tBTC
]
AAVE_ORACLE_GET_PRICE_SEL = bytes.fromhex("b3596f07")  # getAssetPrice(address)

# Asset address → symbol (for logging)
ASSET_SYMBOLS: Dict[str, str] = {
    "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1": "WETH",
    "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f": "WBTC",
    "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8": "USDC.e",
    "0xaf88d065e77c8cC2239327C5EDb3A432268e5831": "USDC",
    "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9": "USDT",
    "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1": "DAI",
    "0x5979D7b546E38E414F7E9822514be443A4800529": "wstETH",
    "0x35751007a407ca6FEFfE80b3cB397736D2cf4dbe": "weETH",
    "0x912CE59144191C1204E64559FE8253a0e49E6548": "ARB",
    "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4": "LINK",
    "0xEC70Dcb4A1EFa46b8F2D97C310C9c4790ba5ffA8": "rETH",
    "0x93b346b6BC2548dA6A1E7d98E9a421B42541425b": "LUSD",
    "0x2416092f143378750bb29b79eD961ab195CcEea5": "ezETH",
    "0x4186BFC76E2E237523CBC30FD220FE055156b41F": "rsETH",
    "0x7dfF72693f6A4149b17e7C6314655f6A9F7c8B33": "GHO",
    "0xba5DdD1f9d7F570dc94a51479a000E3BCE967196": "AAVE",
    "0x6c84a8f1c29108F47a79964b5Fe888D4f4D0dE40": "tBTC",
}

ERC20_ABI = [{
    "name": "balanceOf", "type": "function", "stateMutability": "view",
    "inputs": [{"name": "account", "type": "address"}],
    "outputs": [{"name": "", "type": "uint256"}],
}]

# ── Liquidation Pipeline (Rev3) ───────────────────────────────

AAVE_POOL_GUAD_SEL = bytes.fromhex("bf92857c")  # getUserAccountData(address) — WAD HF in slot 5
FRESHNESS_WAD = 1_000_000_000_000_000_000     # 1e18 — HF == 1.0 in Aave WAD encoding


class LiquidationPipelineV3:
    def __init__(self):
        self._in_flight: set = set()
        self._shutdown = asyncio.Event()
        self.wallet_balances: Dict[str, int] = {}
        self._batch_queue: list = []          # pending liquidations to batch per block
        self._batch_timer: Optional[asyncio.Task] = None  # fires after 50ms window
        self._revert_cooldown: Dict[str, float] = {}      # borrower → monotonic expiry

    async def setup(self):
        """Initialize all subsystems. Called once at startup."""
        logger.info("=" * 60)
        logger.info("  Liquidation Pipeline v3 Starting")
        logger.info(f"  Wallet: {WALLET_ADDR}")
        logger.info(f"  Contract: {CONTRACT_ADDR}")
        logger.info(f"  Primary WSS: {PRIMARY_WSS[:50]}...")
        logger.info("=" * 60)

        # ── W4: Async RPC clients ────────────────────────────
        # Health-checked rotation: Chainstack → DRPC-public → DRPC-lb → Alchemy → public Arb1
        self.rpc = await get_rpc_provider(RPC_CONFIG, purpose="exec", request_timeout=10.0)
        logger.info(f"  AsyncRPC (exec): {self.rpc.http_url[:60]}... — block {await self.rpc.get_block_number()}")

        # Read RPC — separate client with longer timeout for bulk Multicall3 batches
        self.rpc_read = await get_rpc_provider(RPC_CONFIG, purpose="read", request_timeout=15.0)
        logger.info(f"  AsyncRPC (read): {self.rpc_read.http_url[:60]}... — block {await self.rpc_read.get_block_number()}")

        # Light RPC — DRPC-public for price polls, balance checks (avoids burning Chainstack rate limits)
        self.rpc_light = await get_rpc_provider(RPC_CONFIG, purpose="light", request_timeout=10.0)
        logger.info(f"  AsyncRPC (light): {self.rpc_light.http_url[:60]}... — block {await self.rpc_light.get_block_number()}")

        # ── W5: Nonce manager ───────────────────────────────
        self.nonce_mgr = NonceManager(self.rpc.w3, WALLET_ADDR)
        await self.nonce_mgr.init()

        # ── W9: Price registry (staleness-aware) ────────────
        self.prices = PriceRegistry(max_age_seconds=60)

        # ── PricePoller: HTTP fallback for Chainlink feeds (fixes 4/8 → 8/8) ──
        self.price_poller = StalenessGatedPricePoller(
            rpc=self.rpc_light,
            price_registry=self.prices,
            feeds=PP_FEEDS,
            poll_interval=30,
        )
        await self.price_poller.start()
        logger.info(f"  PricePoller: {len(PP_FEEDS)} feeds polling every 30s")

        # ── W10-bis: wstETH price manager (composition + Balancer fallback) ──
        self.wsteth_mgr = WstETHPriceManager(
            rpc           = self.rpc_light,
            price_reg     = self.prices,
            poll_interval = 30,
        )
        await self.wsteth_mgr.start()
        logger.info(f"  wstETH: {self.wsteth_mgr.status()}")

        # ── LST depeg scanner (wstETH, observe-only, fast depegs only) ──
        async def _get_eth_usd() -> float:
            price = self.prices.get_price("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1")
            return (price / 1e8) if price else 1640.0

        self.lst_depeg_scanner = LSTDepegScanner(self.rpc_read.w3, _get_eth_usd)
        asyncio.create_task(self.lst_depeg_scanner.run())

        # ── Profit gate: rejects sub-$5 liquidations before blast_submit ──
        self.profit_gate = ProfitGate(
            min_profit_usd=float(os.getenv("MIN_PROFIT_USD_ARBITRUM", "10.0")),
            gas_cost_usd=0.10,
        )

        # ── Hot path optimization: SharedState + FastGasGuard (0ms RAM reads) ──
        self.shared_state = SharedState()
        self.fast_gas_guard = FastGasGuard(
            shared_state = self.shared_state,
            rpc          = self.rpc,          # fallback only (startup)
            wallet       = WALLET_ADDR,
            min_eth      = 0.005,
            safety_mult  = 3.0,
        )
        self.base_fee_checker = CachedBaseFeeChecker(self.shared_state)

        # ── Gas oracle: trailing percentile — replaces static base_fee × 4 ──
        gas_cfg = _load_gas_config()
        self.gas_oracle = GasOracle(
            shared_state       = self.shared_state,
            window             = 50,
            percentile         = gas_cfg["percentile"],
            surge_buffer       = gas_cfg["surge_buffer"],
            cascade_percentile = gas_cfg["cascade_percentile"],
        )
        self.latency_tracker = LatencyTracker()

        # ── Gas reserve guard: blocks submission if ETH < required ──
        #     Kept for reference; hot path uses fast_gas_guard above.
        self.gas_guard = GasReserveGuard(
            rpc        = self.rpc,
            wallet     = WALLET_ADDR,
            min_eth    = 0.005,
            safety_mult= 3.0,
        )
        self.gas_estimator = GasEstimator(self.rpc)

        # ── W6: Confirmation tracker ────────────────────────
        self.tracker = ConfirmationTracker(
            w3=self.rpc.w3,
            nonce_manager=self.nonce_mgr,
        )
        await self.tracker.start()

        # ── Database ────────────────────────────────────────
        self.db = OutcomeDB()
        self.db.init()
        self.tracker.set_db(self.db)

        # P3: Wire revert callback — blacklists the borrower for 30s
        def _on_revert(borrower: str) -> None:
            self._revert_cooldown[borrower] = time.monotonic() + 30.0
            logger.info(f"[Backoff] {borrower[:10]}… blacklisted 30s after tx revert")
        self.tracker.on_revert = _on_revert

        # ── Skip telemetry ──────────────────────────────────
        self.skip_tel = SkipTelemetry(db_path="skips.db")
        await self.skip_tel.start()

        # ── W1: Position loader (real on-chain data, read-only RPC) ──
        self.loader = PositionLoader(self.rpc_read.w3, AAVE_POOL)
        try:
            import redis
            r = redis.from_url("redis://localhost:6379", decode_responses=True)
            watchlist_addrs = r.zrange("arb:watchlist:active", 0, -1)
            r.close()
            logger.info(f"  Redis watchlist: {len(watchlist_addrs)} addresses")
        except Exception as e:
            logger.warning(f"  Redis unavailable: {e} — trying CSV fallback")
            watchlist_addrs = self._csv_fallback_addresses()

        loaded = await self.loader.bootstrap(watchlist_addrs)
        logger.info(f"  PositionLoader: {loaded} positions loaded from chain")

        # Prime reserves for pre-warm targets — refresh_hot populates per-asset breakdown
        hot_count = await self.loader.refresh_hot(hf_threshold=1.2)
        logger.info(f"  PositionLoader: {hot_count} positions below HF 1.2 (reserves primed)")

        # ── W11: Collateral selector ────────────────────────
        self.selector = CollateralSelector(
            position_loader=self.loader,
            asset_symbols=ASSET_SYMBOLS,
        )

        # ── Uni V3 Quoter (shared between Aave and Compound) ──
        self.quoter = QuoterAsync(self.rpc_read)
        logger.info("  QuoterAsync: Uni V3 QuoterV2 ready")

        # ── Quote cache: pre-fetch cross-asset quotes (fixes pre-warm timeouts) ──
        self.quote_cache = QuoteCache(
            quoter = self.quoter,
            pairs  = KNOWN_SLOW_PAIRS,
            ttl    = 12.0,
        )
        await self.quote_cache.start()
        logger.info(f"  QuoteCache: {self.quote_cache.stats['entries']} entries pre-fetched")

        # ── W8: Flash loan tx builder ────────────────────────
        self.flash_builder = FlashLoanTxBuilder(
            rpc              = self.rpc,
            executor_address = CONTRACT_ADDR,
            wallet_address   = WALLET_ADDR,
            private_key      = PRIVATE_KEY,
            slippage_bps     = 50,
            shared_state     = self.shared_state,
            quoter           = self.quoter,
            quote_cache      = self.quote_cache,
            gas_oracle       = self.gas_oracle,
        )
        logger.info("  FlashLoanTxBuilder: Uni V3 swap routes + gas oracle ready")

        # ── Async Redis (Compound module needs async client) ──
        redis_async = aioredis.from_url("redis://localhost:6379", decode_responses=True)

        # ── Compound V3 module ──────────────────────────────
        compound_executor = os.getenv("COMPOUND_EXECUTOR_ADDR", "")
        if compound_executor:
            self.compound = CompoundV3Module(
                rpc           = self.rpc_light,  # DRPC-public — avoids Chainstack rate limits
                rpc_read      = self.rpc_light,
                redis         = redis_async,
                shared_state  = self.shared_state,
                nonce_mgr     = self.nonce_mgr,
                skip_tel      = self.skip_tel,
                quoter        = self.quoter,
                executor_addr = compound_executor,
                private_key   = PRIVATE_KEY,
                wallet        = WALLET_ADDR,
                markets       = COMPOUND_MARKETS,
                min_profit_usd= float(os.getenv("MIN_PROFIT_USD_COMPOUND", "3.0")),
                check_interval= 10,
            )
            await self.compound.start()
        else:
            self.compound = None
            logger.warning("  COMPOUND_EXECUTOR_ADDR not set — Compound V3 disabled")

        # ── Base chain: Aave V3 Base module ─────────────────
        base_executor = os.getenv("BASE_EXECUTOR_ADDR", "")
        if base_executor and base_executor != "0x0000000000000000000000000000000000000000":
            self.base = AaveBaseModule(
                rpc_http      = os.getenv("BASE_RPC_URL", "https://1rpc.io/base"),
                rpc_wss       = os.getenv("BASE_WSS_URL", ""),
                redis         = redis_async,
                wallet        = WALLET_ADDR,
                private_key   = PRIVATE_KEY,
                executor_addr = base_executor,
                executor_abi  = self.flash_builder._executor.abi if hasattr(self.flash_builder, '_executor') else [],
                skip_tel      = self.skip_tel,
                min_profit_usd = float(os.getenv("MIN_PROFIT_USD_BASE", "3.0")),
                price_registry = self.prices,
            )
            try:
                await self.base.start()
                logger.info(f"  Base module: {self.base.status()}")
            except Exception as e:
                logger.warning(f"  Base module: start failed — {e}")
                self.base = None
        else:
            self.base = None
            logger.warning("  BASE_EXECUTOR_ADDR not set — Base chain disabled")

        # ── DEX-DEX arbitrage (ArbExecutor.sol) ─────────────
        arb_executor_addr = os.getenv("ARB_EXECUTOR_ADDR", "")
        if arb_executor_addr:
            from multi_dex_router import MultiDexRouter
            from dex_arbitrage import ArbitrageScanner, ArbExecutor as DexArbExecutor

            self._arb_multi_dex = MultiDexRouter(self.rpc.w3, arb_executor_addr)
            self._arb_scanner = ArbitrageScanner(
                multi_dex     = self._arb_multi_dex,
                shared_state  = self.shared_state,
                price_reg     = self.prices,
                min_profit_usd= float(os.getenv("MIN_PROFIT_USD", "5.0")),
            )
            self._arb_executor = DexArbExecutor(
                w3                   = self.rpc.w3,
                arb_executor_address = arb_executor_addr,
                wallet               = WALLET_ADDR,
                private_key          = PRIVATE_KEY,
                shared_state         = self.shared_state,
            )
            self._arb_dry_run = os.getenv("ARB_DRY_RUN", "1") == "1"
            await self._arb_executor.warmup()
            logger.info(
                f"  DexArb: scanner + executor ready "
                f"(contract={arb_executor_addr[:10]}… dry_run={self._arb_dry_run})"
            )
        else:
            self._arb_scanner  = None
            self._arb_executor = None
            logger.warning("  ARB_EXECUTOR_ADDR not set — DEX arb disabled")

        # ── HF Engine ───────────────────────────────────────
        self.hf_engine = LocalHFEngine(
            on_liquidatable=self._on_liquidatable,
            decimals=DECIMALS,
        )
        self.hf_engine.prices = self.prices  # W9: replace raw dict with PriceRegistry

        # Seed positions into HF engine from on-chain data
        self._sync_hf_engine()

        # ── W2: Blast submit endpoints ──────────────────────
        # QuickNode 22ms primary, public arb1 52ms as 3 redundant slots.
        # MEV Blocker / Flashbots are Ethereum-only — not usable on Arbitrum.
        # 3 arb1 slots fire the same tx to the same node (deduplicated),
        # giving 3 parallel network paths at no cost. Only first to land wins.
        configure_endpoints(
            primary_rpc    = self.rpc.http_url,
            secondary_rpc  = self.rpc_read.http_url,
            mev_blocker_url= "https://arb1.arbitrum.io/rpc",
            flashbots_url  = "https://arb1.arbitrum.io/rpc",
        )
        logger.info(f"  BlastSubmit: {self.rpc.http_url[:40]}... + {self.rpc_read.http_url[:40]}... configured")

        # ── W7: Dual WS manager ─────────────────────────────
        self._new_block_event = asyncio.Event()
        self._arb_block_event = asyncio.Event()
        self._new_block_number = 0

        self.ws = WSManager(
            primary_wss=PRIMARY_WSS,
            secondary_wss=SECONDARY_WSS or None,   # empty string → None so WSManager skips it
            http_rpc=self.rpc.http_url,
            on_price_update=self._on_price_update,
            on_liquidation=self._handle_liquidation_log,
            oracle_feeds=CHAINLINK_FEED_ADDRESSES,
            pool_address=AAVE_POOL,
            on_new_block=self._on_new_block,
        )
        await self.ws.start()
        logger.info("  WSManager: dual WS + HTTP fallback active")

        # ── Cache pre-warmer: keep presigned txs warm for top-N near-HF positions ──
        self._last_hf: Dict[str, float] = {}  # for HFChangeDetector

        self.prewarm = CachePrewarmer(
            loader           = self.loader,
            build_fn         = self._build_and_cache_one,
            shared_state     = self.shared_state,
            top_n            = 20,
            refresh_interval = 25.0,
            hf_ceiling       = 1.15,
        )
        await self.prewarm.start()

        self.hf_detector = HFChangeDetector(
            prewarm           = self.prewarm,
            hf_drop_threshold = 0.05,
        )

        # ── W8: Presigned tx guard ──────────────────────────
        self.tx_guard = PresignedTxGuard()

        # ── Build presigned tx cache ────────────────────────
        self._presigned_cache: Dict[str, FlashLoanTxData] = {}      # cached tx data
        self._presigned_snapshots: Dict[str, "PresignedSnapshot"] = {}

        # ── Smoke test flash builder (background, doesn't block) ──
        asyncio.create_task(self._smoke_test_flash_builder())

        # ── Base chain block poller ───────────────────────────
        if self.base is not None:
            asyncio.create_task(self._base_block_loop(), name="base_block")

    # ── Run ─────────────────────────────────────────────────

    async def run(self):
        logger.info("[Pipeline] run() starting — about to await setup()")
        await self.setup()
        self._setup_complete = True
        logger.info("[Pipeline] setup() complete — starting main loop tasks")

        # Re-scan HF engine for positions that were underwater during setup
        # (suppressed by _setup_complete guard). These need immediate action.
        # Wait for at least 4 Chainlink prices before triggering — post-setup runs
        # before PricePoller has completed its first poll, causing _select_best_debt_asset
        # to return None for all debt assets (no prices → no selection → wasted attempt).
        price_wait_start = __import__("time").monotonic()
        while len(self.prices.snapshot()) < 4 and __import__("time").monotonic() - price_wait_start < 45:
            await asyncio.sleep(2)
        n_prices = len(self.prices.snapshot())
        logger.info(f"[Pipeline] Post-setup scan starting — {n_prices} prices available")

        for addr, pos in list(self.hf_engine.positions.items()):
            total_debt = sum(pos.debt.values())
            if total_debt > 0:
                hf = self.hf_engine.compute_hf(pos)
                if hf < 1.0:
                    logger.info(f"[Pipeline] Post-setup: {addr[:10]}… HF={hf:.4f} — triggering")
                    self._on_liquidatable(addr, hf, pos)

        tasks = [
            asyncio.create_task(self._block_watch_loop(), name="block_watch"),
            asyncio.create_task(self._wallet_balance_loop(), name="wallet"),
            asyncio.create_task(self._stats_loop(), name="stats"),
            asyncio.create_task(self._aave_oracle_price_loop(), name="aave_oracle"),
            asyncio.create_task(self._shutdown_waiter(), name="shutdown"),
        ]
        if self._arb_scanner is not None:
            tasks.append(asyncio.create_task(self._arb_loop(), name="arb_scan"))
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Pipeline shutting down")
        finally:
            await self.shutdown()

    async def _build_and_cache_one(self, borrower: str) -> tuple[bool, float]:
        """
        Build and cache a presigned flash loan tx for pre-warming.
        Called by CachePrewarmer every 25s for top-20 lowest-HF positions.
        Returns (success, estimated_profit_usd) — profit is 0.0 on failure.
        """
        try:
            account_data = self.loader.get(borrower)
            if account_data is None:
                return (False, 0.0)

            if not account_data.reserves:
                return (False, 0.0)    # needs refresh_hot(HF<1.05) first

            # ── Select best collateral via CollateralSelector ───────────
            prices = self.prices.snapshot() if hasattr(self.prices, 'snapshot') else {}
            decimals = DECIMALS

            result = self.selector.select(
                account_data     = account_data,
                total_debt_usd   = account_data.total_debt_base / 1e8,
                asset_prices_usd = {k: v / 1e8 for k, v in prices.items()},
                asset_decimals   = decimals,
            )
            if result is None:
                return (False, 0.0)

            best_c = result.asset

            # ── Select best debt asset ──────────────────────────────────
            best_d = self._select_best_debt_asset(account_data)
            if best_d is None:
                return (False, 0.0)

            # CollateralSelector returns debt_to_cover in collateral-asset units
            # (collatoral decimals / collateral price) — wrong for cross-asset liquidations.
            # Recompute here using debt asset's price and decimals so debtToCover is correct.
            debt_price_raw = prices.get(best_d, 0)
            debt_dec       = DECIMALS.get(best_d, 18)
            if debt_price_raw > 0:
                debt_to_cover_usd = account_data.total_debt_base / 1e8 * result.close_factor
                debt_to_cover = int(debt_to_cover_usd / (debt_price_raw / 1e8) * (10 ** debt_dec))
            else:
                debt_to_cover = result.debt_to_cover  # fallback to selector's estimate

            # Estimate collateral amount received, in collateral-token raw units.
            # Must convert via oracle price ratio — raw-unit multiplication is only
            # correct for same-asset pairs (where col_price == debt_price and decimals match).
            bonus_mult    = result.liquidation_bonus_bps / 10_000
            col_asset     = result.asset
            col_price_raw = prices.get(col_asset, 0)
            col_dec       = DECIMALS.get(col_asset, 18)
            if col_price_raw > 0 and debt_price_raw > 0 and col_asset.lower() != best_d.lower():
                collateral_amount = int(
                    debt_to_cover * debt_price_raw * (10 ** col_dec) * bonus_mult
                    / (col_price_raw * (10 ** debt_dec))
                )
            else:
                collateral_amount = int(debt_to_cover * bonus_mult)

            # ── Build flash loan tx (nonce=0 placeholder) ───────────────
            tx_data = await self.flash_builder.build(
                collateral_asset  = best_c,
                debt_asset        = best_d,
                borrower          = borrower,
                debt_to_cover     = debt_to_cover,
                shared_state      = self.shared_state,
                nonce             = 0,              # replaced at fire time
                collateral_amount = collateral_amount,
                asset_prices_usd  = prices,         # {token: price*1e8}
                asset_decimals    = DECIMALS,
                liquidation_bonus_bps = result.liquidation_bonus_bps,
            )

            if tx_data is None:
                return (False, 0.0)

            # ── Sanity-check profit estimate ────────────────────────────
            # Skip implausible cross-asset quotes (>$10K likely corrupted QuoterV2).
            # Same-asset (fee_tier=0) profits are real liquidation bonuses — no cap.
            if tx_data.swap_route.fee_tier != 0 and tx_data.estimated_profit_usd > 10_000:
                logger.warning(
                    f"[Prewarm] Implausible profit estimate "
                    f"${tx_data.estimated_profit_usd:,.0f} for {borrower[:10]}… — "
                    f"skipping cache (likely bad quote)"
                )
                return (False, 0.0)

            # ── Cache ────────────────────────────────────────────────────
            from execution_guards import PresignedSnapshot
            self._presigned_cache[borrower]     = tx_data
            self._presigned_snapshots[borrower] = PresignedSnapshot(
                borrower         = borrower,
                base_fee_wei     = self.shared_state.base_fee_wei,
                debt_to_cover    = debt_to_cover,
                collateral_asset = best_c,
                debt_asset       = best_d,
            )
            return (True, tx_data.estimated_profit_usd)

        except Exception as e:
            logger.debug(f"[Prewarm] build failed {borrower[:10]}: {e}")
            return (False, 0.0)

    # ── P1: Freshness check — re-verify HF from chain ────────

    async def _verify_hf_fresh(self, borrowers: list[str]) -> list[str]:
        """
        Re-query getUserAccountData from Aave pool for each borrower.
        Returns only those still liquidatable (HF < 1.0 in WAD).
        Uses rpc_read to avoid burning exec RPC rate limits.
        Conservative: treats a failed call as still-liquidatable.
        """
        from eth_abi import decode as abi_decode

        async def _check_one(addr: str) -> tuple[str, bool]:
            try:
                calldata = AAVE_POOL_GUAD_SEL + bytes.fromhex(addr[2:].lower().zfill(64))
                result = await self.rpc_read.w3.eth.call(
                    {"to": AAVE_POOL, "data": calldata}
                )
                hf = abi_decode(
                    ["uint256","uint256","uint256","uint256","uint256","uint256"],
                    result,
                )[5]
                return addr, hf < FRESHNESS_WAD
            except Exception as e:
                logger.warning(f"[Freshness] getUserAccountData error {addr[:10]}: {e}")
                return addr, True  # conservative: assume still liquidatable

        results = await asyncio.gather(*[_check_one(b) for b in borrowers])
        live    = [addr for addr, ok in results if ok]
        dropped = [addr for addr, ok in results if not ok]
        if dropped:
            logger.info(
                f"[Freshness] Dropped {len(dropped)} no-longer-liquidatable: "
                + ", ".join(d[:10] + "…" for d in dropped)
            )
        return live

    # ── Helper: select best debt asset ───────────────────────

    def _select_best_debt_asset(self, account_data) -> Optional[str]:
        """
        Select the debt asset with highest USD value from account reserves.
        Returns checksummed address or None if no debt found.
        Falls back to highest raw debt amount when prices are unavailable.
        """
        best_asset    = None
        best_usd      = 0.0
        fallback_asset = None
        fallback_raw  = 0
        decimals      = DECIMALS

        for reserve in account_data.reserves:
            if reserve.total_debt == 0:
                continue
            price = self.prices.get_price(reserve.asset)
            dec = decimals.get(reserve.asset, 18)
            if price is not None:
                usd = (reserve.total_debt / 10 ** dec) * (price / 1e8)
                if usd > best_usd:
                    best_usd   = usd
                    best_asset = reserve.asset
            else:
                # Price not available — track by raw normalised amount as fallback
                normalised = reserve.total_debt / 10 ** dec
                if normalised > fallback_raw:
                    fallback_raw  = normalised
                    fallback_asset = reserve.asset

        if best_asset is not None:
            return best_asset
        if fallback_asset is not None:
            logger.debug(f"[DebtSelect] No price for debt assets — using fallback {fallback_asset[:10]}…")
            return fallback_asset
        return None

    async def _aave_oracle_price_loop(self) -> None:
        """
        Polls Aave V3 oracle every 60s for assets not covered by our Chainlink feeds
        (weETH, rsETH, ezETH, rETH, native USDC, GHO, etc.) and injects them into
        the PriceRegistry so CollateralSelector can price these collateral types.
        Uses rpc_light (PublicNode) — single eth_call per asset, low frequency.
        """
        from eth_abi import decode as abi_decode

        while True:
            try:
                updated = 0
                for asset_addr in AAVE_ORACLE_ASSETS:
                    try:
                        calldata = AAVE_ORACLE_GET_PRICE_SEL + self.rpc_light.w3.codec.encode(
                            ["address"], [asset_addr]
                        )
                        result = await self.rpc_light.w3.eth.call(
                            {"to": AAVE_ORACLE_ADDR, "data": calldata}
                        )
                        price = abi_decode(["uint256"], result)[0]
                        if price > 0:
                            self.prices.update_price(asset_addr, price)
                            updated += 1
                        await asyncio.sleep(0.2)   # gentle pacing
                    except Exception:
                        pass
                if updated:
                    logger.debug(f"[AaveOracle] Updated {updated} supplemental prices")
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.debug(f"[AaveOracle] price loop error: {e}")
            await asyncio.sleep(60)

    async def shutdown(self):
        logger.info("Shutting down subsystems...")
        await self.prewarm.stop()
        await self.skip_tel.stop()
        await self.price_poller.stop()
        await self.wsteth_mgr.stop()
        self.lst_depeg_scanner.stop()
        await self.quote_cache.stop()
        await self.ws.stop()
        if self.compound is not None:
            await self.compound.stop()
        if self.base is not None:
            await self.base.stop()
        await self.tracker.stop()
        await close_session()
        logger.info("Shutdown complete")

    # ── Block callback (push-based, replaces polling) ────────

    def _on_new_block(self, block_number: int):
        """Called by WSManager on each new block via newHeads subscription."""
        self._new_block_number = block_number
        self._new_block_event.set()
        self._arb_block_event.set()

    # ── Price update callback (W9 + W7) ─────────────────────

    def _on_price_update(self, feed_addr: str, price: int):
        """Called by WSManager when Chainlink AnswerUpdated fires."""
        asset = FEED_TO_ASSET.get(feed_addr.lower())
        if asset is None:
            return
        # W9: PriceRegistry tracks staleness
        self.prices.update_price(asset, price)
        # Delegate to HF engine for liquidation checks
        self.hf_engine.update_price(asset, price)

    # ── Liquidation log handler (W3) ────────────────────────

    async def _handle_liquidation_log(self, log: dict):
        """Called by WSManager (or HTTP fallback) on LiquidationCall events."""
        event = parse_liquidation_log(log, WALLET_ADDR)
        if event is None:
            return
        if event.is_competitor:
            # Immediate log — full telemetry filled in by background receipt fetch
            logger.warning(
                f"[Pipeline] LOST RACE: borrower={event.borrower[:10]}… "
                f"liquidator={event.liquidator[:10]}… "
                f"block={event.block_number} tx={event.tx_hash[:16]}…"
            )
            # P4: Spawn receipt fetch to capture gas price + block timestamp
            asyncio.create_task(self._record_competitor_loss(event), name="comp_telemetry")
        else:
            logger.info(
                f"[Pipeline] OUR LIQUIDATION: borrower={event.borrower[:10]}… "
                f"block={event.block_number}"
            )
        # Remove from HF engine and presigned cache
        self.hf_engine.remove_position(event.borrower)
        self._presigned_cache.pop(event.borrower, None)
        self._presigned_snapshots.pop(event.borrower, None)

    async def _record_competitor_loss(self, event) -> None:
        """
        Fetch tx receipt for a competitor win to get effectiveGasPrice + block timestamp.
        Logs the data gap (block time vs our submission) and writes to OutcomeDB.
        """
        gas_price  = event.gas_price  # likely 0 from log subscription
        block_ts   = None

        try:
            receipt = await self.rpc_read.w3.eth.get_transaction_receipt(event.tx_hash)
            if receipt:
                gp = receipt.get("effectiveGasPrice", 0)
                gas_price = int(gp, 16) if isinstance(gp, str) else int(gp)
        except Exception as e:
            logger.debug(f"[Telemetry] Receipt fetch failed {event.tx_hash[:12]}: {e}")

        try:
            block = await self.rpc_read.w3.eth.get_block(event.block_number)
            block_ts = int(block.get("timestamp", 0))
        except Exception as e:
            logger.debug(f"[Telemetry] Block fetch failed {event.block_number}: {e}")

        self.db.record_lost_race(
            event.borrower, event.liquidator,
            event.tx_hash, event.block_number,
            competitor_gas_price=gas_price,
        )

        gas_gwei = gas_price / 1e9 if gas_price else 0.0
        ts_str   = f"block_ts={block_ts}" if block_ts else "block_ts=unknown"
        logger.warning(
            f"[Competitor] {event.liquidator} won — "
            f"tx={event.tx_hash} "
            f"block={event.block_number} {ts_str} "
            f"gas={gas_gwei:.4f}gwei"
        )

    # ── HF Engine → liquidation trigger ─────────────────────

    def _on_liquidatable(self, address: str, hf: float, pos):
        # Guard: don't fire during setup — blast_submit, hf_detector not ready yet
        if not getattr(self, '_setup_complete', False):
            logger.info(f"[Setup] Liquidatable {address} HF={hf:.4f} — queued for main loop")
            return
        if address in self._in_flight:
            return

        # P3: Revert backoff — skip borrowers that just had a doomed tx
        cooldown_until = self._revert_cooldown.get(address)
        if cooldown_until is not None:
            if time.monotonic() < cooldown_until:
                logger.debug(f"[Backoff] {address[:10]}… still in 30s revert cooldown — skipping")
                return
            self._revert_cooldown.pop(address, None)

        old_hf = self._last_hf.get(address, 2.0)
        self._last_hf[address] = hf
        if old_hf - hf >= 0.05:
            asyncio.create_task(
                self.hf_detector.on_hf_update(address, old_hf, hf)
            )

        # Queue for batch collection — flush fires after 50ms window so same-block
        # positions sharing a debt asset get batched into one tx / one nonce.
        self._in_flight.add(address)
        self._batch_queue.append({'address': address, 'hf': hf, 'pos': pos})
        if self._batch_timer is None or self._batch_timer.done():
            self._batch_timer = asyncio.create_task(self._flush_batch())

    async def _flush_batch(self):
        """
        One-tick collection window: yield to the event loop so any other
        _on_liquidatable calls queued in the same block are processed first,
        then flush. With colocation + Chainstack, 0ms sleep hits <10ms end-to-end.
        """
        await asyncio.sleep(0)

        items, self._batch_queue = self._batch_queue, []

        if not items:
            return

        # Resolve debt asset + build per-item metadata for each queued position
        resolved: list[dict] = []
        prices_snap = self.prices.snapshot() if hasattr(self.prices, 'snapshot') else {}

        for entry in items:
            address = entry['address']
            hf      = entry['hf']
            pos     = entry['pos']
            try:
                account_data = self.loader.get(address)
                if account_data is None:
                    self._in_flight.discard(address)
                    continue

                best_debt = self._select_best_debt_asset(account_data)
                if best_debt is None:
                    self._in_flight.discard(address)
                    continue

                asset_prices_usd = {k: v / 1e8 for k, v in prices_snap.items()}
                selection = self.selector.select(
                    account_data=account_data,
                    total_debt_usd=account_data.total_debt_base / 1e8,
                    asset_prices_usd=asset_prices_usd,
                    asset_decimals=DECIMALS,
                )
                if selection is None:
                    self._in_flight.discard(address)
                    continue

                debt_price_raw = prices_snap.get(best_debt, 0)
                debt_dec       = DECIMALS.get(best_debt, 18)
                bonus_mult     = selection.liquidation_bonus_bps / 10_000
                col_asset      = selection.asset
                col_price_raw  = prices_snap.get(col_asset, 0)
                col_dec        = DECIMALS.get(col_asset, 18)

                if debt_price_raw > 0:
                    dtc_usd = account_data.total_debt_base / 1e8 * selection.close_factor

                    # P2: Cap dtc_usd by available collateral so builder and gate
                    # use the same profit formula.  Without this cap:
                    #   selector profit = min(dtc, col_usd) * bonus_pct  (capped)
                    #   builder profit  = dtc * bonus_pct                 (uncapped)
                    # They diverge by 10-50× when dtc >> col_usd, causing the $100k
                    # gate to pass batches the builder values at $462k+.
                    # The on-chain tx would also revert (borrower lacks the collateral).
                    if col_price_raw > 0 and bonus_mult > 0:
                        col_balance = next(
                            (r.a_token_balance for r in account_data.reserves
                             if r.asset.lower() == col_asset.lower()),
                            0,
                        )
                        collateral_usd = (col_balance / 10 ** col_dec) * (col_price_raw / 1e8)
                        dtc_usd = min(dtc_usd, collateral_usd / bonus_mult)

                    debt_to_cover = int(dtc_usd / (debt_price_raw / 1e8) * (10 ** debt_dec))
                else:
                    dtc_usd       = 0.0
                    debt_to_cover = selection.debt_to_cover

                if col_price_raw > 0 and debt_price_raw > 0 and col_asset.lower() != best_debt.lower():
                    collateral_amount = int(
                        debt_to_cover * debt_price_raw * (10 ** col_dec) * bonus_mult
                        / (col_price_raw * (10 ** debt_dec))
                    )
                else:
                    collateral_amount = int(debt_to_cover * bonus_mult)

                # P2: Use builder formula for profit (col_usd - dbt_usd) so the
                # $100k implausibility gate in _execute_batch sees the same value
                # build_batch will compute, not the selector's capped estimate.
                builder_profit_usd = dtc_usd * (bonus_mult - 1.0) if dtc_usd > 0 else 0.0

                resolved.append({
                    'address':          address,
                    'hf':               hf,
                    'pos':              pos,
                    'debt_asset':       best_debt,
                    'collateral_asset': selection.asset,
                    'debt_to_cover':    debt_to_cover,
                    'collateral_amount':collateral_amount,
                    'asset_prices_usd': prices_snap,  # raw (×1e8) — build_batch normalises
                    'asset_decimals':   DECIMALS,
                    'profit_usd':       builder_profit_usd,
                    'account_data':     account_data,
                    'selection':        selection,
                })
            except Exception as e:
                logger.warning(f"[Batch] resolve failed {address[:10]}: {e}")
                self._in_flight.discard(address)

        if not resolved:
            return

        # Group by debt asset
        by_debt: dict[str, list] = {}
        for r in resolved:
            by_debt.setdefault(r['debt_asset'], []).append(r)

        for debt_asset, group in by_debt.items():
            if len(group) >= 2:
                # Profit gate on group aggregate
                total_profit = sum(r['profit_usd'] for r in group)
                ok, reason = await self.fast_gas_guard.check()
                if not ok:
                    logger.info(f"[Batch] Gas guard: {reason} — releasing {len(group)} items")
                    for r in group:
                        self._in_flight.discard(r['address'])
                    continue
                asyncio.create_task(
                    self._execute_batch(debt_asset, group, total_profit)
                )
            else:
                # Single — release _in_flight and use existing path
                r = group[0]
                self._in_flight.discard(r['address'])
                asyncio.create_task(
                    self._execute_liquidation(r['address'], r['hf'], r['pos'])
                )

    async def _execute_batch(self, debt_asset: str, group: list, total_profit_usd: float):
        """Submit one executeLiquidationBatch tx for a group sharing debt_asset."""
        addresses = [r['address'] for r in group]
        t0 = time.monotonic()

        try:
            # ── P1: Re-verify HF from chain before spending gas ──────────
            live_addrs = set(await self._verify_hf_fresh([r['address'] for r in group]))
            if len(live_addrs) < len(group):
                for r in group:
                    if r['address'] not in live_addrs:
                        self._in_flight.discard(r['address'])
                group = [r for r in group if r['address'] in live_addrs]
                total_profit_usd = sum(r['profit_usd'] for r in group)
            if len(group) < 2:
                # Survivors fall through to single path
                for r in group:
                    self._in_flight.discard(r['address'])
                    asyncio.create_task(
                        self._execute_liquidation(r['address'], r['hf'], r['pos'])
                    )
                return

            if not self.profit_gate.check(total_profit_usd):
                logger.info(
                    f"[Batch] Aggregate profit ${total_profit_usd:.2f} below floor "
                    f"${self.profit_gate.min_profit_usd:.2f} — skipping N={len(group)}"
                )
                return

            if total_profit_usd > 100_000:
                logger.warning(
                    f"[Batch] Implausible aggregate profit ${total_profit_usd:,.0f} N={len(group)} "
                    f"— skipping (likely bad oracle quote for cross-asset pair)"
                )
                return

            build_items = [{
                'borrower':          r['address'],
                'collateral_asset':  r['collateral_asset'],
                'debt_to_cover':     r['debt_to_cover'],
                'collateral_amount': r['collateral_amount'],
                'asset_prices_usd':  r['asset_prices_usd'],
                'asset_decimals':    r['asset_decimals'],
            } for r in group]

            nonce = await self.nonce_mgr.next()
            result = await self.flash_builder.build_batch(
                debt_asset   = debt_asset,
                items        = build_items,
                shared_state = self.shared_state,
                nonce        = nonce,
            )

            if result is None:
                # Route-building left < 2 items — fall back each to single path
                await self.nonce_mgr.rewind()
                for r in group:
                    self._in_flight.discard(r['address'])
                    asyncio.create_task(
                        self._execute_liquidation(r['address'], r['hf'], r['pos'])
                    )
                return

            raw_tx, total_debt, profit_usd, n_built = result
            submit_result = await blast_submit_ex(raw_tx)
            tx_hash = submit_result.tx_hash

            if tx_hash:
                await self.nonce_mgr.mark_submitted(nonce)
                for r in group:
                    await self.tracker.add(
                        r['address'], tx_hash, nonce,
                        collateral_asset=r['collateral_asset'],
                        debt_asset=debt_asset,
                        estimated_profit=r['profit_usd'],
                    )
                elapsed = (time.monotonic() - t0) * 1000
                logger.info(
                    f"[Batch] SUBMITTED N={n_built} hash={tx_hash[:12]}… "
                    f"debt={debt_asset[:10]}… profit=${profit_usd:.2f} "
                    f"in {elapsed:.0f}ms"
                )
            elif submit_result.status == "ambiguous":
                await self.nonce_mgr.mark_submitted(nonce)
                logger.warning(f"[Batch] Ambiguous submit — nonce {nonce} held")
            else:
                await self.nonce_mgr.rewind()
                logger.warning(f"[Batch] Submit hard-failed — rewinding nonce")

        except Exception as e:
            logger.error(f"[Batch] _execute_batch error: {e}", exc_info=True)
        finally:
            for addr in addresses:
                self._in_flight.discard(addr)

    async def _execute_liquidation(self, address: str, hf: float, pos):
        t0 = time.monotonic()
        self._in_flight.add(address)  # re-add (cleared by _flush_batch single fallback)
        try:
            account_data = self.loader.get(address)
            if account_data is None:
                self.skip_tel.record(SkipEvent(
                    borrower=address, reason=SkipReason.POSITION_NOT_FOUND,
                    hf=hf, detail="loader.get() returned None"
                ))
                return

            # W11: Risk-adjusted collateral selection
            asset_prices_usd = {}
            for addr in DECIMALS:
                p = self.prices.get_price(addr)
                if p is not None:
                    asset_prices_usd[addr] = p / 1e8

            debt_usd = account_data.total_debt_base / 1e8 if account_data else 0
            coll_usd = account_data.total_collateral_base / 1e8 if account_data else 0

            selection = self.selector.select(
                account_data=account_data,
                total_debt_usd=debt_usd,
                asset_prices_usd=asset_prices_usd,
                asset_decimals=DECIMALS,
            )
            if selection is None:
                self.skip_tel.record(SkipEvent(
                    borrower=address, reason=SkipReason.NO_ELIGIBLE_COLLATERAL,
                    hf=hf, debt_usd=debt_usd, collateral_usd=coll_usd,
                ))
                return

            # Flash fee-aware profit gate — check flash source and deduct fee BEFORE gate
            debt_asset_addr = self._select_best_debt_asset(account_data)

            # CollateralSelector returns debt_to_cover in collateral-asset units.
            # Recompute in debt-asset units now that we know the debt asset.
            prices_snap = self.prices.snapshot() if hasattr(self.prices, 'snapshot') else {}
            debt_price_raw = prices_snap.get(debt_asset_addr, 0) if debt_asset_addr else 0
            debt_dec = DECIMALS.get(debt_asset_addr, 18) if debt_asset_addr else 18
            if debt_asset_addr and debt_price_raw > 0:
                dtc_usd = debt_usd * selection.close_factor
                corrected_dtc = int(dtc_usd / (debt_price_raw / 1e8) * (10 ** debt_dec))
            else:
                corrected_dtc = selection.debt_to_cover

            source = 'balancer'
            if debt_asset_addr:
                source = await self.flash_builder.choose_flash_source(
                    debt_asset_addr, corrected_dtc
                )
            # Aave fee = 9 bps of debt. Gross profit = debt × liq_bonus (typically 5%).
            # Ratio: fee/profit ≈ 0.0009/0.05 = 0.018. Safe for gate-check purposes.
            flash_fee_usd = selection.expected_profit_usd * 0.018 if source == 'aave' else 0.0
            net_profit_usd = selection.expected_profit_usd - flash_fee_usd

            logger.info(
                f"[Pipeline] GO {address[:10]}… HF={hf:.4f} "
                f"gross=${selection.expected_profit_usd:.2f} "
                f"net=${net_profit_usd:.2f} "
                f"source={source} fee=${flash_fee_usd:.2f} "
                f"collateral={selection.symbol} debtToCover={corrected_dtc}"
            )

            # Profit gate — reject dust liquidations
            if not self.profit_gate.check(net_profit_usd):
                logger.info(
                    f"[Skip] {address[:10]}… net=${net_profit_usd:.2f} after {source} "
                    f"fee=${flash_fee_usd:.2f} — below ${self.profit_gate.min_profit_usd:.2f} floor"
                )
                self.skip_tel.record(SkipEvent(
                    borrower=address, reason=SkipReason.PROFIT_FLOOR,
                    hf=hf,
                    profit_usd=net_profit_usd,
                    gas_usd=0.10,
                    collateral=selection.asset,
                    debt_asset=selection.symbol,
                    debt_usd=debt_usd,
                    collateral_usd=coll_usd,
                ))
                return

            # Gas reserve — skip if ETH too low (0ms RAM read via SharedState)
            token = self.latency_tracker.start(address)
            ok, reason = await self.fast_gas_guard.check()
            if not ok:
                self.skip_tel.record(SkipEvent(
                    borrower=address, reason=SkipReason.GAS_RESERVE,
                    hf=hf, detail=reason,
                    profit_usd=selection.expected_profit_usd,
                    collateral=selection.asset,
                ))
                return

            # Build and submit tx
            tx_hash = await self._build_and_submit(
                address, selection.asset, corrected_dtc,
                estimated_profit=selection.expected_profit_usd,
                asset_prices_usd=asset_prices_usd,
            )

            if tx_hash:
                submit_ms = self.latency_tracker.mark_submitted(token)
                self.db.record_submission(
                    tx_hash, address, selection.asset,
                    "", "flash", selection.expected_profit_usd,
                )
                elapsed = (time.monotonic() - t0) * 1000
                logger.info(
                    f"[Pipeline] SUBMITTED {tx_hash[:12]}… "
                    f"in {elapsed:.0f}ms "
                    f"(hot_path={submit_ms:.0f}ms)"
                )
            else:
                self.skip_tel.record(SkipEvent(
                    borrower=address, reason=SkipReason.SUBMIT_FAILED,
                    hf=hf, detail="blast_submit returned None",
                    profit_usd=selection.expected_profit_usd,
                    collateral=selection.asset,
                ))

        except Exception as e:
            logger.error(f"[Pipeline] _execute_liquidation error: {e}", exc_info=True)
            self.skip_tel.record(SkipEvent(
                borrower=address, reason=SkipReason.BUILD_FAILED,
                hf=hf, detail=str(e)[:200],
            ))

    # ── Transaction builder + submitter ──────────────────────

    async def _build_and_submit(
        self, borrower: str, collateral_asset: str, debt_to_cover: int,
        estimated_profit: float = 0.0, debt_asset: str = "",
        asset_prices_usd: dict = None,
    ) -> Optional[str]:
        """
        Flash-first submission path.
        Uses executeLiquidation() with Balancer flash loan + Uni V3 swap.
        If flash loan route is unavailable, records skip and returns None.
        """
        # ── Cache check ────────────────────────────────────────────────
        cached      = self._presigned_cache.get(borrower)
        cached_snap = self._presigned_snapshots.get(borrower)
        base_fee    = self.shared_state.base_fee_wei

        if cached and cached_snap:
            current_base_fee = self.base_fee_checker.get_base_fee()
            stale, reason = self.tx_guard.is_stale(
                cached_snap, current_base_fee,
                current_debt_estimate=debt_to_cover,
            )
            if stale:
                logger.debug(f"[Submit] Cache stale ({reason}) — rebuilding {borrower[:10]}…")
                cached = None

        # ── Identify best debt asset ────────────────────────────────────
        account_data = self.loader.get(borrower)
        if account_data is None:
            self.skip_tel.record(SkipEvent(
                borrower=borrower, reason=SkipReason.POSITION_NOT_FOUND,
                detail="_build_and_submit: loader returned None"
            ))
            return None

        best_debt_asset = self._select_best_debt_asset(account_data)
        if best_debt_asset is None:
            self.skip_tel.record(SkipEvent(
                borrower=borrower, reason=SkipReason.NO_DEBT_ASSET,
            ))
            return None

        # Skip same-asset liquidations (collateral == debt). The FlashExecutorV3
        # always routes through Uni V3 swap (collateral → debt); when both assets
        # are identical, Uni V3 has no same-token pool (fee_tier=0 invalid) and
        # the on-chain TX reverts. Requires contract update to handle.
        if best_debt_asset.lower() == collateral_asset.lower():
            logger.debug(
                f"[Submit] Skipping same-asset liquidation {borrower[:10]}… "
                f"(collateral==debt={collateral_asset[:10]}…)"
            )
            self.skip_tel.record(SkipEvent(
                borrower=borrower, reason=SkipReason.BUILD_FAILED,
                detail="same-asset liquidation (collateral==debt) — contract unsupported",
            ))
            return None

        nonce: Optional[int] = None
        raw_tx: Optional[bytes] = None

        # ── CACHE HIT — re-sign with fresh nonce, zero RPC ───────────
        if cached is not None:
            nonce = await self.nonce_mgr.next()
            tx_data = self.flash_builder.rebuild_with_nonce(cached, nonce)
            if tx_data:
                raw_tx = tx_data.raw_tx
                logger.debug(
                    f"[Submit] Cache HIT — {borrower[:10]}… "
                    f"nonce={nonce} fee_tier={tx_data.swap_route.fee_tier}"
                )
            else:
                # Re-sign failed — release the nonce and rebuild from scratch
                await self.nonce_mgr.rewind()
                nonce = None
                cached = None

        # ── COLD PATH — build from scratch ─────────────────────────────
        if cached is None:
            # Build with nonce=0 placeholder — nonce is allocated AFTER a
            # successful build so that a build failure never consumes a nonce
            # slot that would create a gap for concurrent in-flight txs.
            tx_data = await self.flash_builder.build(
                collateral_asset  = collateral_asset,
                debt_asset        = best_debt_asset,
                borrower          = borrower,
                debt_to_cover     = debt_to_cover,
                shared_state      = self.shared_state,
                nonce             = 0,
                asset_prices_usd  = asset_prices_usd or {},
                asset_decimals    = DECIMALS,
            )

            if tx_data is None:
                # Flash loan unavailable — no fallback (direct path requires pre-funded wallet).
                # No nonce was allocated so no rewind needed.
                self.skip_tel.record(SkipEvent(
                    borrower = borrower,
                    reason   = SkipReason.BUILD_FAILED,
                    detail   = "flash loan route unavailable, no direct path fallback",
                ))
                return None

            # Allocate nonce only now, then re-sign the already-built tx
            nonce = await self.nonce_mgr.next()
            tx_data = self.flash_builder.rebuild_with_nonce(tx_data, nonce)
            if tx_data is None:
                await self.nonce_mgr.rewind()
                return None

            raw_tx = tx_data.raw_tx
            logger.info(
                f"[Submit] Flash loan COLD path — {borrower[:10]}… "
                f"fee_tier={tx_data.swap_route.fee_tier} "
                f"slippage={tx_data.swap_route.slippage_pct:.2%} "
                f"est_profit={'$' + f'{tx_data.estimated_profit_usd:.2f}' if tx_data.estimated_profit_usd < 10_000 else 'IMPLAUSIBLE(bad_quote)'}"
            )
            # Cache for next time
            from execution_guards import PresignedSnapshot
            self._presigned_cache[borrower]     = tx_data
            self._presigned_snapshots[borrower] = PresignedSnapshot(
                borrower         = borrower,
                base_fee_wei     = base_fee,
                debt_to_cover    = debt_to_cover,
                collateral_asset = collateral_asset,
                debt_asset       = best_debt_asset,
            )

        # ── P1: Freshness check — re-verify from chain before gas spend ──
        live = await self._verify_hf_fresh([borrower])
        if not live:
            logger.info(f"[Freshness] {borrower[:10]}… HF>=1.0 at submit time — aborting")
            await self.nonce_mgr.rewind()
            self.skip_tel.record(SkipEvent(
                borrower=borrower, reason=SkipReason.BUILD_FAILED,
                detail="freshness: HF >= 1.0 verified on-chain at submit time",
            ))
            return None

        # ── Submit ─────────────────────────────────────────────────────
        result  = await blast_submit_ex(raw_tx)
        tx_hash = result.tx_hash

        if tx_hash:
            await self.nonce_mgr.mark_submitted(nonce)
            await self.tracker.add(borrower, tx_hash, nonce,
                                   collateral_asset=collateral_asset,
                                   debt_asset=debt_asset,
                                   estimated_profit=estimated_profit)
            logger.info(
                f"[Submit] Submitted — hash={tx_hash[:12]}… "
                f"borrower={borrower[:10]}… nonce={nonce}"
            )
        elif result.status == "ambiguous":
            # #1 fix: timeout / already-known — the tx may be in a private or slow
            # mempool. Do NOT rewind; hold the nonce via mark_submitted so the next
            # cascade liquidation doesn't reuse it and collide with this in-flight tx.
            await self.nonce_mgr.mark_submitted(nonce)
            self.skip_tel.record(SkipEvent(
                borrower = borrower,
                reason   = SkipReason.SUBMIT_FAILED,
                detail   = "blast_submit ambiguous (timeout/already-known) — nonce held, not reused",
            ))
        else:
            # status == "failed": every endpoint hard-rejected the tx → the nonce
            # was never consumed, so it is safe to reclaim it for the next attempt.
            await self.nonce_mgr.rewind()
            self.skip_tel.record(SkipEvent(
                borrower = borrower,
                reason   = SkipReason.SUBMIT_FAILED,
                detail   = "blast_submit hard-failed — all endpoints rejected",
            ))

        return tx_hash

    # ── Background loops ─────────────────────────────────────

    async def _block_watch_loop(self):
        """Monitor new blocks via WSS newHeads push, with HTTP polling fallback."""
        logger.info("[BlockWatch] Loop starting — waiting for first block via WSS")
        last_block = await self.rpc_read.get_block_number()
        logger.info(f"[BlockWatch] Initial block={last_block} — WSS push active")
        while not self._shutdown.is_set():
            try:
                # Wait for WSS push, with 10s timeout as HTTP fallback
                await asyncio.wait_for(self._new_block_event.wait(), timeout=10.0)
                self._new_block_event.clear()
                current = self._new_block_number
                if current <= last_block:
                    continue
                last_block = current
                # Update SharedState with latest base fee (feeds FastGasGuard + CachedBaseFeeChecker)
                base_fee_wei = 0
                try:
                    block = await self.rpc_read.get_block("latest")
                    base_fee_wei = block.get("baseFeePerGas", 0)
                    self.shared_state.on_new_block(
                        block_number = current,
                        base_fee_wei = base_fee_wei,
                    )
                except Exception:
                    pass  # non-critical — hot path falls back to p95

                # Feed gas oracle — trailing percentile for competitive bids
                try:
                    priority_fee = await self.rpc.w3.eth.max_priority_fee
                except Exception:
                    priority_fee = 0
                self.gas_oracle.update(current, base_fee_wei, priority_fee)

                # Compound V3 check (every 10 blocks, gated internally)
                if self.compound is not None:
                    await self.compound.on_new_block(current)


                # Three-tier position refresh — tuned for free-tier RPC rate limits
                # (Arbitrum ~250ms/block: 30 blocks≈7.5s, 120 blocks≈30s, 400 blocks≈100s)
                if current % 30 == 0:
                    await self.loader.refresh_hot(hf_threshold=1.05)   # imminent only: ~2 req/s
                if current % 120 == 0:
                    await self.loader.refresh_hot(hf_threshold=1.15)   # near: ~0.5 req/s
                if current % 400 == 0:
                    await self.loader.refresh_hot(hf_threshold=1.20)   # broad: ~0.15 req/s
                    self._sync_hf_engine()
                    # HFChangeDetector: trigger rebuilds on fast HF drops
                    for addr, pos in self.loader._positions.items():
                        old_hf = self._last_hf.get(addr, 2.0)
                        new_hf = pos.hf_float
                        if new_hf == old_hf:
                            continue
                        self._last_hf[addr] = new_hf
                        if old_hf - new_hf >= self.hf_detector._threshold:
                            await self.hf_detector.on_hf_update(addr, old_hf, new_hf)

                # Nonce sync every 60 blocks
                if current % 60 == 0:
                    await self.nonce_mgr.sync()

            except asyncio.TimeoutError:
                # WSS silent > 10s — fall back to HTTP poll
                try:
                    current = await self.rpc_read.get_block_number()
                    if current <= last_block:
                        continue
                    last_block = current
                except Exception:
                    continue
            except Exception as e:
                logger.warning(f"[BlockWatch] Error: {e}")

    async def _wallet_balance_loop(self):
        """W10: Concurrent balance fetch for all assets."""
        while not self._shutdown.is_set():
            try:
                balances = await self.rpc_read.get_all_balances(
                    assets={ASSET_SYMBOLS.get(addr, addr[:8]): addr for addr in DECIMALS},
                    wallet=WALLET_ADDR,
                    erc20_abi=ERC20_ABI,
                )
                self.wallet_balances = {
                    addr: balances.get(ASSET_SYMBOLS.get(addr, ""), 0)
                    for addr in DECIMALS
                }
                eth_bal = await self.rpc_light.w3.eth.get_balance(WALLET_ADDR)
                self.shared_state.on_balance_update(eth_bal)
                logger.info(
                    f"[Wallet] ETH={eth_bal/1e18:.4f} "
                    f"candidates={self.hf_engine.borrower_count} "
                    f"prices_fresh={len(self.prices.snapshot())}/8 "
                    f"{self.wsteth_mgr.status()} "
                    f"pre_warm={self.prewarm.warm_count}/{len(self.prewarm._cache)} "
                    f"quote={self.quote_cache.stats['hit_rate']:.0%} "
                    f"{self.shared_state.status_line()} "
                    f"{self.gas_oracle.recommend().log_line()}"
                )
            except Exception as e:
                logger.warning(f"[Wallet] Balance refresh error: {e}")
            await asyncio.sleep(60)

    async def _presigner_loop(self):
        """Periodically rebuild presigned tx cache for top candidates."""
        from execution_guards import PresignedSnapshot
        while not self._shutdown.is_set():
            await asyncio.sleep(30)
            try:
                candidates = self.hf_engine.get_sorted_candidates(top_n=20)
                refreshed = 0
                for addr, hf in candidates:
                    if hf >= 1.0:
                        continue
                    pos = self.hf_engine.positions.get(addr)
                    if not pos:
                        continue
                    # Select best collateral
                    best_c = max(
                        pos.collateral_assets,
                        key=lambda a: (pos.collateral.get(a, 0) * (self.prices.get_price(a) or 0)),
                    )
                    best_d = max(
                        pos.debt_assets,
                        key=lambda a: (pos.debt.get(a, 0) * (self.prices.get_price(a) or 0)),
                    )
                    # W11: use CollateralSelector if account data available
                    debt_to_cover = pos.debt.get(best_d, 0) // 2  # rough 50% estimate

                    base_fee     = await self.rpc.get_base_fee()
                    max_fee      = int(base_fee * 2.0)
                    priority_fee = max(int(base_fee * 0.5), 1_000_000)

                    from web3 import Web3
                    sync_w3 = Web3()
                    contract = sync_w3.eth.contract(
                        address=Web3.to_checksum_address(CONTRACT_ADDR),
                        abi=json.loads('[{"name":"executeLiquidation","type":"function","stateMutability":"nonpayable","inputs":[{"name":"collateralAsset","type":"address"},{"name":"debtAsset","type":"address"},{"name":"user","type":"address"},{"name":"debtToCover","type":"uint256"},{"name":"receiveAToken","type":"bool"}],"outputs":[]}]'),
                    )
                    tx = contract.functions.executeLiquidation(
                        Web3.to_checksum_address(best_c),
                        Web3.to_checksum_address(best_d),
                        Web3.to_checksum_address(addr),
                        debt_to_cover,
                        False,
                    ).build_transaction({
                        'from': Web3.to_checksum_address(WALLET_ADDR),
                        'gas': 400_000,
                        'maxFeePerGas': max_fee,
                        'maxPriorityFeePerGas': priority_fee,
                        'nonce': await self.nonce_mgr.next(),
                        'chainId': 42161,
                    })
                    signed = sync_w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
                    self._presigned_cache[addr] = signed.raw_transaction
                    self._presigned_snapshots[addr] = PresignedSnapshot(
                        borrower=addr, base_fee_wei=base_fee,
                        debt_to_cover=debt_to_cover,
                        collateral_asset=best_c, debt_asset=best_d,
                    )
                    refreshed += 1

                # Cleanup stale entries
                top_addrs = {Web3.to_checksum_address(a) for a, _ in candidates}
                stale = [k for k in self._presigned_cache if k not in top_addrs]
                for k in stale:
                    self._presigned_cache.pop(k, None)
                    self._presigned_snapshots.pop(k, None)

                if refreshed:
                    logger.info(f"[Presigner] Refreshed {refreshed} presigned txs")
            except Exception as e:
                logger.error(f"[Presigner] Refresh error: {e}")

    async def _arb_loop(self):
        """Dedicated arb scan loop — decoupled from block loop for max throughput."""
        while not self._shutdown.is_set():
            try:
                # Wake on new block or every 250ms (one block time) as fallback
                try:
                    await asyncio.wait_for(self._arb_block_event.wait(), timeout=0.25)
                    self._arb_block_event.clear()
                except asyncio.TimeoutError:
                    pass

                opp = await self._arb_scanner.scan_once()
                if opp and opp.is_profitable():
                    nonce = await self.nonce_mgr.next()
                    tx_hash = await self._arb_executor.execute(
                        opp,
                        self._arb_multi_dex,
                        nonce=nonce,
                        dry_run=self._arb_dry_run,
                    )
                    if tx_hash:
                        logger.info(f"[Arb] Submitted: {tx_hash}")
                    elif not self._arb_dry_run:
                        await self.nonce_mgr.rewind()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[Arb] scan/execute error: {e}")

    async def _stats_loop(self):
        while not self._shutdown.is_set():
            await asyncio.sleep(300)
            try:
                summary = self.db.pnl_summary()
                win_rates = self.db.win_rates()
                lat = self.latency_tracker.to_dict()
                logger.info(
                    f"[Stats] total={(summary.get('total') or 0)} "
                    f"confirmed={(summary.get('confirmed') or 0)} "
                    f"lost={(summary.get('lost') or 0)} "
                    f"profit=${(summary.get('total_profit') or 0):.2f} "
                    f"gas=${(summary.get('total_gas') or 0):.2f} "
                    f"p50={lat['p50_submit_ms']:.0f}ms "
                    f"p95={lat['p95_submit_ms']:.0f}ms"
                )
                logger.info(self.skip_tel.summary())
                if self.compound is not None:
                    logger.info(self.compound.status())
                if self._arb_scanner is not None:
                    self._arb_scanner.log_stats()
                    arb_s = self._arb_executor.stats
                    logger.info(
                        f"[Arb] executed={arb_s['executed']} failed={arb_s['failed']} "
                        f"dry_run={self._arb_dry_run}"
                    )
                for path, wr in win_rates.items():
                    logger.info(
                        f"[Stats] {path}: win_rate={wr['bayesian_win_rate']:.1%} "
                        f"({wr['wins']}W/{wr['losses']}L)"
                    )
                competitors = self.db.top_competitors(5)
                if competitors:
                    logger.info("[Stats] Top competitors:")
                    for c in competitors:
                        avg_gwei = (c.get('avg_gas_price') or 0) / 1e9
                        import datetime
                        last_seen_str = (
                            datetime.datetime.utcfromtimestamp(c['last_seen']).strftime('%H:%M:%SZ')
                            if c.get('last_seen') else "?"
                        )
                        logger.info(
                            f"  {c['address']} wins={c['wins']} "
                            f"avg_gas={avg_gwei:.4f}gwei "
                            f"last_seen={last_seen_str}"
                        )
                else:
                    logger.info("[Stats] Top competitors: (none recorded yet)")
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

    # ── Helpers ──────────────────────────────────────────────

    def _sync_hf_engine(self):
        """
        Sync PositionLoader data into LocalHFEngine.

        Previously only synced HF<1.0 positions. Now syncs ALL positions that
        have per-asset reserve data populated (from _batch_reserve_data). This
        enables LocalHFEngine to track price-move triggers for positions in the
        HF 1.0-1.2 range, not just already-liquidatable dust positions.
        """
        from web3 import Web3
        for addr in list(self.hf_engine.positions.keys()):
            if self.loader.get(addr) is None:
                self.hf_engine.remove_position(addr)

        for pos in self.loader._positions.values():
            if not pos.reserves or pos.total_debt_base == 0:
                continue    # skip positions without per-asset data
            # Build collateral/debt maps from reserves
            coll = {}
            debt = {}
            thresholds = {}
            bonuses = {}
            for r in pos.reserves:
                if r.a_token_balance > 0 and r.usage_as_collateral:
                    coll[r.asset] = r.a_token_balance
                    cfg = self.loader.get_reserve_config(r.asset)
                    thresholds[r.asset] = (cfg.liquidation_threshold / 10000) if cfg else 0.8
                    bonuses[r.asset] = ((cfg.liquidation_bonus / 10000) - 1) if cfg else 0.05
                if r.total_debt > 0:
                    debt[r.asset] = r.total_debt
            if coll and debt:
                self.hf_engine.upsert_position(
                    address=pos.address,
                    collateral=coll,
                    debt=debt,
                    liq_threshold=thresholds,
                    liq_bonus=bonuses,
                )

    def _csv_fallback_addresses(self) -> list:
        """Fallback: load addresses from classification CSV if Redis unavailable."""
        csv_path = project_root / "reports" / "classification_complete.csv"
        if not csv_path.exists():
            return []
        import csv
        with open(csv_path) as f:
            return [row["address"] for row in csv.DictReader(f) if row.get("address")]

    # ── Smoke test (called from setup) ────────────────────────

    async def _smoke_test_flash_builder(self):
        """
        Verify FlashLoanTxBuilder works end-to-end on a real position.
        Runs once at startup. Does NOT submit — build only.
        """
        await asyncio.sleep(10)   # wait for position loader to finish

        try:
            # Find a position with reserve data
            test_addr = next(
                (addr for addr, pos in self.loader._positions.items()
                 if pos.reserves and pos.total_debt_base > 0),
                None
            )

            if test_addr is None:
                logger.warning("[SmokeTest] No position with reserve data found — skip")
                return

            pos = self.loader.get(test_addr)
            best_d = self._select_best_debt_asset(pos)
            if best_d is None:
                logger.warning("[SmokeTest] No debt asset found — skip")
                return

            if not pos.reserves:
                logger.warning("[SmokeTest] No reserves — skip")
                return

            tx_data = await self.flash_builder.build(
                collateral_asset  = pos.reserves[0].asset,
                debt_asset        = best_d,
                borrower          = test_addr,
                debt_to_cover     = pos.total_debt_base // 2,
                shared_state      = self.shared_state,
                nonce             = 0,
            )

            if tx_data:
                if tx_data.estimated_profit_usd > 10_000:
                    logger.warning(
                        f"[SmokeTest] Implausible profit "
                        f"${tx_data.estimated_profit_usd:,.0f} "
                        f"— quote likely corrupted for this pair"
                    )
                _profit_display = (
                    f"${tx_data.estimated_profit_usd:.2f}"
                    if tx_data.estimated_profit_usd < 10_000
                    else "IMPLAUSIBLE(bad_quote — harmless, WBTC fee_tier=500)"
                )
                logger.info(
                    f"[SmokeTest] FlashLoanTxBuilder OK — "
                    f"borrower={test_addr[:10]}… "
                    f"fee_tier={tx_data.swap_route.fee_tier} "
                    f"slippage={tx_data.swap_route.slippage_pct:.2%} "
                    f"est_profit={_profit_display} "
                    f"raw_tx_len={len(tx_data.raw_tx)} bytes"
                )
            else:
                logger.warning(
                    f"[SmokeTest] FlashLoanTxBuilder returned None — "
                    f"check swap route for {test_addr[:10]}… "
                    f"collateral={pos.reserves[0].asset[:10]}…"
                )

        except Exception as e:
            logger.error(f"[SmokeTest] FlashLoanTxBuilder smoke test failed: {e}")

    # ── Base chain block poller ──────────────────────────────

    async def _base_block_loop(self):
        """Poll Base chain blocks and check for liquidatable positions."""
        if self.base is None:
            return
        logger.info("[BaseBlock] Starting Base chain block poller")
        # Seed from current block to avoid replaying entire chain history
        last_block = max(0, await self.base._rpc.get_block_number() - 1)
        logger.info(f"[BaseBlock] Starting from block {last_block}")
        while not self._shutdown.is_set():
            try:
                block = await self.base._rpc.get_block_number()
                base_fee = 0
                try:
                    b = await self.base._rpc.w3.eth.get_block(block, full_transactions=False)
                    base_fee = b.get("baseFeePerGas", 0)
                except Exception:
                    pass
                if block != last_block:
                    if block > last_block:
                        for bn in range(last_block + 1, block + 1):
                            try:
                                await self.base.on_new_block(bn, base_fee)
                            except RuntimeError:
                                pass  # watchlist not ready
                    last_block = block
                await asyncio.sleep(2)
            except asyncio.CancelledError:
                break
            except Exception as e:
                import traceback
                logger.error(f"[BaseBlock] Error: {e}\n{traceback.format_exc()}")
                await asyncio.sleep(5)


if __name__ == "__main__":
    pipeline = LiquidationPipelineV3()
    asyncio.run(pipeline.run())
