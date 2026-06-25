"""
swap_monitor.py — Event-driven large-swap detector for DEX-DEX arbitrage signals

Subscribes to Uniswap V3 and Camelot V3 Swap events via WebSocket. For each
swap above MIN_SWAP_USD, decodes the sqrtPriceX96 to get the post-swap DEX
price, then quotes the competing DEX to measure the cross-DEX spread.

Sends a Telegram alert when spread × position_size - gas > MIN_NET_PROFIT_USD.

Purpose: data collection to understand opportunity frequency and sizing
         before building the flash-loan arb executor.

Run standalone:
    cd ~/defi_flash_bot
    python services/rev2/swap_monitor.py

or with override env:
    MIN_SWAP_USD=5000 MIN_NET_PROFIT=2 python services/rev2/swap_monitor.py
"""

from __future__ import annotations

import asyncio
import itertools
import json
import logging
import math
import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import aiohttp
import websockets
import aiohttp
from eth_abi import decode as abi_decode
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider
from dotenv import load_dotenv

# ─── Bootstrap ───────────────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "services" / "rev2"))
load_dotenv(ROOT / ".env")

try:
    from dex_arbitrage import ArbOpportunity, ArbExecutor as _DexArbExecutor
    from multi_dex_router import MultiDexRouter as _MultiDexRouter, CAMELOT_ROUTER, UNIV3_ROUTER
    from sequencer_feed import SequencerFeedWatcher, PendingSwap as _PendingSwap
    _ARB_IMPORTS_OK = True
except ImportError as _ie:
    _ARB_IMPORTS_OK = False
    CAMELOT_ROUTER = ""
    UNIV3_ROUTER   = ""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)-8s %(name)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("swap_monitor")

# ─── On-chain constants (Arbitrum) ───────────────────────────────────────────

SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"

WETH   = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC   = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
USDC_E = "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8"
USDT   = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"
WBTC   = "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f"
ARB    = "0x912CE59144191C1204E64559FE8253a0e49E6548"

TOKEN_DECIMALS = {WETH: 18, USDC: 6, USDC_E: 6, USDT: 6, WBTC: 8, ARB: 18}
TOKEN_SYMBOLS  = {WETH: "WETH", USDC: "USDC", USDC_E: "USDC.e", USDT: "USDT",
                  WBTC: "WBTC", ARB: "ARB"}

# Seed prices — updated at startup via Chainlink, used for USD sizing
SEED_PRICES_USD: dict[str, float] = {
    WETH: 3500.0, USDC: 1.0, USDC_E: 1.0, USDT: 1.0, WBTC: 65000.0, ARB: 0.8,
}

UNIV3_FACTORY   = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
UNIV3_QUOTER    = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"  # QuoterV2
CAMELOT_FACTORY = "0x1a3c9B1d2F0529D97f2afC5136Cc23e58f1FD35B"
CAMELOT_QUOTER  = "0x0Fc73040b26E9bC8514fA028D998E73A254Fa76e"

# Pairs to monitor: (tokenA, tokenB, [univ3_fee_tiers])
MONITORED_PAIRS = [
    (WETH, USDC,  [500, 3000]),
    (WETH, USDT,  [500, 3000]),
    (WBTC, WETH,  [500]),
    (WBTC, USDC,  [500]),
    (ARB,  USDC,  [500, 3000]),
    (ARB,  WETH,  [3000]),
]

# Chainlink USD feeds (Arbitrum) for seed prices
CHAINLINK_FEEDS = {
    WETH: "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",
    WBTC: "0x6ce185860a4963106506C203335A2910413708e9",
    # ARB/USD not available on Arbitrum mainnet Chainlink — uses seed price $0.80
}
CHAINLINK_ABI = [{
    "name": "latestRoundData", "type": "function", "stateMutability": "view",
    "inputs": [], "outputs": [
        {"name": "roundId",         "type": "uint80"},
        {"name": "answer",          "type": "int256"},
        {"name": "startedAt",       "type": "uint256"},
        {"name": "updatedAt",       "type": "uint256"},
        {"name": "answeredInRound", "type": "uint80"},
    ],
}]

# Thresholds
MIN_SWAP_USD   = float(os.getenv("MIN_SWAP_USD",   "1000"))
MIN_SPREAD_PCT = float(os.getenv("MIN_SPREAD_PCT", "0.15"))
MIN_NET_PROFIT = float(os.getenv("MIN_NET_PROFIT", "2.0"))
ARB_GAS_UNITS  = 450_000

# Arb execution
ARB_EXECUTOR_ADDR = os.getenv("ARB_EXECUTOR_ADDR", "")
ARB_DRY_RUN       = os.getenv("ARB_DRY_RUN", "1") == "1"
MIN_EXECUTE_USD   = float(os.getenv("MIN_EXECUTE_USD", "3.0"))

# Whether THIS process (swap_monitor) executes arbs. Default OFF: swap_monitor's
# spread detection is spot-price based (ignores price impact + both pool fees),
# so it produces phantom spreads whose executions revert at the sell leg's
# profit guard (verified: 13/13 sampled txs reverted, burning gas). The
# pipeline's ArbitrageScanner validates with a REAL two-leg round-trip quote
# and is the sole executor. Set SWAP_MONITOR_EXECUTE=1 to re-enable (not
# recommended until the detection model here is fixed to quote real round-trips).
SWAP_MONITOR_EXECUTE = os.getenv("SWAP_MONITOR_EXECUTE", "0") == "1"

# Minimum probe sizes per token (caps rival quote to avoid RPC timeout on huge sizes)
MAX_PROBE: dict[str, int] = {WETH: int(20e18), USDC: int(50_000e6), USDT: int(50_000e6),
                              WBTC: int(1e8),   ARB: int(50_000e18)}

# ─── Same-block (sequencer feed) backrun config ──────────────────────────────
# Enable the sequencer-feed watcher that reacts to a pending swap in the SAME
# Arbitrum block. When SWAP_MONITOR_EXECUTE=0 it runs in SHADOW mode: it
# predicts the post-swap dislocation and logs what it WOULD fire, then quotes
# the real round-trip ~1 block later to score the prediction — no broadcast.
FEED_SAME_BLOCK     = os.getenv("FEED_SAME_BLOCK", "1") == "1"
# Required margin of the PREDICTED price gap ABOVE the round-trip fee floor
# (both pools' fees). The old feed path gated on 0.15% — below the 0.35% fee
# floor of a 500↔3000 round trip — which is why it fired phantom reverts.
MIN_BACKRUN_EDGE_PCT = float(os.getenv("MIN_BACKRUN_EDGE_PCT", "0.0"))
# Assumed Camelot (Algebra dynamic-fee) fee for the fee-floor calc. Camelot's
# quoter here doesn't return the applied fee, so we assume a conservative value
# (over-estimating fees makes us fire LESS, never more).
CAMELOT_FEE_FRAC    = float(os.getenv("CAMELOT_FEE_FRAC", "0.003"))
# Fraction of the post-swap edge our backrun is assumed to actually capture
# (our own trade closes part of the gap — triangle area ≈ half).
BACKRUN_CAPTURE     = float(os.getenv("BACKRUN_CAPTURE", "0.5"))

Q96 = 1 << 96


def predict_next_sqrt(sqrt_x96: int, liquidity: int, amount_in: int,
                      zero_for_one: bool, fee_ppm: int) -> int:
    """
    Uniswap V3 exact-input post-swap sqrtPriceX96, single-tick approximation.

    Uses the canonical SqrtPriceMath step formulas with liquidity held constant.
    This is EXACT while the swap stays within the current initialised tick range
    and slightly OVER-states the move once it crosses ticks (more liquidity would
    absorb it) — i.e. it errs toward predicting a larger dislocation, so the
    fee-floor gate stays honest and the on-chain profit guard catches any
    over-estimate. The fee is removed first (fee stays in the pool, doesn't move
    price).
    """
    if sqrt_x96 <= 0 or liquidity <= 0 or amount_in <= 0:
        return 0
    amt = amount_in * (1_000_000 - fee_ppm) // 1_000_000
    if amt <= 0:
        return 0
    if zero_for_one:
        # token0 in → price decreases. getNextSqrtPriceFromAmount0RoundingUp:
        #   sqrtP' = (L<<96)*sqrtP / ((L<<96) + amt*sqrtP)
        num = liquidity * Q96 * sqrt_x96
        den = liquidity * Q96 + amt * sqrt_x96
        return num // den if den else 0
    else:
        # token1 in → price increases. getNextSqrtPriceFromAmount1RoundingDown:
        #   sqrtP' = sqrtP + amt*Q96/L
        return sqrt_x96 + (amt * Q96) // liquidity


def pool_fee_frac(pool: "PoolInfo") -> float:
    """Round-trip fee contribution of one pool, as a fraction (0.0005 = 0.05%)."""
    if pool.dex == "uniswap":
        return pool.fee / 1_000_000
    # Camelot/Algebra: use the live dynamic fee if we have it, else a safe default.
    if pool.last_fee > 0:
        return pool.last_fee / 1_000_000
    return CAMELOT_FEE_FRAC

# ─── Minimal ABIs ────────────────────────────────────────────────────────────

UNIV3_FACTORY_ABI = [{
    "name": "getPool", "type": "function", "stateMutability": "view",
    "inputs":  [{"name": "tokenA", "type": "address"},
                {"name": "tokenB", "type": "address"},
                {"name": "fee",    "type": "uint24"}],
    "outputs": [{"name": "pool",   "type": "address"}],
}]

CAMELOT_FACTORY_ABI = [{
    "name": "poolByPair", "type": "function", "stateMutability": "view",
    "inputs":  [{"name": "tokenA", "type": "address"},
                {"name": "tokenB", "type": "address"}],
    "outputs": [{"name": "pool",   "type": "address"}],
}]

UNIV3_QUOTER_ABI = [{
    "name": "quoteExactInputSingle", "type": "function", "stateMutability": "nonpayable",
    "inputs": [{"name": "params", "type": "tuple", "components": [
        {"name": "tokenIn",           "type": "address"},
        {"name": "tokenOut",          "type": "address"},
        {"name": "amountIn",          "type": "uint256"},
        {"name": "fee",               "type": "uint24"},
        {"name": "sqrtPriceLimitX96", "type": "uint160"},
    ]}],
    "outputs": [
        {"name": "amountOut",               "type": "uint256"},
        {"name": "sqrtPriceX96After",       "type": "uint160"},
        {"name": "initializedTicksCrossed", "type": "uint32"},
        {"name": "gasEstimate",             "type": "uint256"},
    ],
}]

CAMELOT_QUOTER_ABI = [{
    "name": "quoteExactInputSingle", "type": "function", "stateMutability": "nonpayable",
    "inputs": [
        {"name": "tokenIn",        "type": "address"},
        {"name": "tokenOut",       "type": "address"},
        {"name": "amountIn",       "type": "uint256"},
        {"name": "limitSqrtPrice", "type": "uint160"},
    ],
    "outputs": [
        {"name": "amountOut",               "type": "uint256"},
        {"name": "sqrtPriceX96After",       "type": "uint160"},
        {"name": "initializedTicksCrossed", "type": "uint32"},
    ],
}]

# Pool state reads — to seed price/liquidity at startup so the same-block feed
# path is productive immediately (rather than waiting for random WS events).
UNIV3_POOL_ABI = [
    {"name": "slot0", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [
        {"name": "sqrtPriceX96", "type": "uint160"}, {"name": "tick", "type": "int24"},
        {"name": "observationIndex", "type": "uint16"}, {"name": "observationCardinality", "type": "uint16"},
        {"name": "observationCardinalityNext", "type": "uint16"}, {"name": "feeProtocol", "type": "uint8"},
        {"name": "unlocked", "type": "bool"}]},
    {"name": "liquidity", "type": "function", "stateMutability": "view", "inputs": [],
     "outputs": [{"name": "l", "type": "uint128"}]},
]
# Camelot/Algebra pool: globalState() — first field is the sqrtPriceX96.
ALGEBRA_POOL_ABI = [
    {"name": "globalState", "type": "function", "stateMutability": "view", "inputs": [], "outputs": [
        {"name": "price", "type": "uint160"}, {"name": "tick", "type": "int24"},
        {"name": "fee", "type": "uint16"}, {"name": "timepointIndex", "type": "uint16"},
        {"name": "communityFeeToken0", "type": "uint8"}, {"name": "communityFeeToken1", "type": "uint8"},
        {"name": "unlocked", "type": "bool"}]},
]


# ─── Data classes ────────────────────────────────────────────────────────────

@dataclass
class PoolInfo:
    address:      str
    token0:       str   # lower address (as returned by factory)
    token1:       str
    fee:          int   # 0 for Camelot
    dex:          str   # "uniswap" | "camelot"
    last_price:   float = 0.0   # token1_human per token0_human, updated from events
    last_event_t: float = 0.0
    last_sqrt_x96:  int = 0     # raw sqrtPriceX96 from last Swap event / seed
    last_liquidity: int = 0     # in-range liquidity from last Swap event / seed
    last_fee:       int = 0     # current fee in ppm (Camelot dynamic; UniV3 uses .fee)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def sym(addr: str) -> str:
    return TOKEN_SYMBOLS.get(addr, addr[:8])

def pool_label(p: "PoolInfo") -> str:
    if p.dex == "uniswap":
        return f"UniV3-{p.fee}"
    return "Camelot"

def sqrt_price_to_human(sqrt_x96: int, token0: str, token1: str) -> float:
    """sqrtPriceX96 → human-readable price (token1 per token0)."""
    if sqrt_x96 == 0:
        return 0.0
    dec0 = TOKEN_DECIMALS.get(_to_checksum(token0), 18)
    dec1 = TOKEN_DECIMALS.get(_to_checksum(token1), 18)
    # (sqrtPriceX96 / 2^96)^2 = token1_raw / token0_raw
    price_raw = (sqrt_x96 ** 2) / (2 ** 192)
    return price_raw * (10 ** dec0) / (10 ** dec1)

def _to_checksum(addr: str) -> str:
    """Convert any-case address to checksum format for dict lookup."""
    try:
        return AsyncWeb3.to_checksum_address(addr)
    except Exception:
        return addr

def swap_usd_size(amount0: int, amount1: int, pool: PoolInfo,
                  prices: dict[str, float]) -> float:
    """Estimate USD size of a swap from amount deltas."""
    t0 = _to_checksum(pool.token0)
    t1 = _to_checksum(pool.token1)
    p0 = prices.get(t0, 0.0)
    p1 = prices.get(t1, 0.0)
    d0 = TOKEN_DECIMALS.get(t0, 18)
    d1 = TOKEN_DECIMALS.get(t1, 18)
    v0 = abs(amount0) / (10 ** d0) * p0 if p0 else 0.0
    v1 = abs(amount1) / (10 ** d1) * p1 if p1 else 0.0
    return max(v0, v1)


# ─── PoolRegistry ────────────────────────────────────────────────────────────

class PoolRegistry:
    """Discovers pool addresses at startup via factory contracts."""

    def __init__(self, w3: AsyncWeb3):
        self._w3 = w3
        self._uni_factory     = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(UNIV3_FACTORY),
            abi=UNIV3_FACTORY_ABI,
        )
        self._camelot_factory = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(CAMELOT_FACTORY),
            abi=CAMELOT_FACTORY_ABI,
        )

    async def discover(self) -> list[PoolInfo]:
        """Query factories for all monitored pairs. Returns list of active pools."""
        pools: list[PoolInfo] = []
        # Stagger calls to avoid hitting rate limits on public RPCs (max ~3 concurrent)
        tasks_batches = []
        batch: list = []
        for token_a, token_b, fees in MONITORED_PAIRS:
            for fee in fees:
                batch.append(self._get_univ3_pool(token_a, token_b, fee))
            batch.append(self._get_camelot_pool(token_a, token_b))
            if len(batch) >= 3:
                tasks_batches.append(batch)
                batch = []
        if batch:
            tasks_batches.append(batch)

        for i, batch in enumerate(tasks_batches):
            if i > 0:
                await asyncio.sleep(0.4)  # 400ms between batches avoids 429
            results = await asyncio.gather(*batch, return_exceptions=True)
            for r in results:
                if isinstance(r, Exception) or r is None:
                    continue
                pools.append(r)

        logger.info(f"[PoolRegistry] Discovered {len(pools)} active pools")
        for p in pools:
            label = f"UniV3 fee={p.fee}" if p.dex == "uniswap" else "Camelot"
            logger.info(f"  {sym(p.token0)}/{sym(p.token1)}  {label}  {p.address}")
        return pools

    async def _get_univ3_pool(self, ta: str, tb: str, fee: int) -> Optional[PoolInfo]:
        try:
            addr = await self._uni_factory.functions.getPool(
                AsyncWeb3.to_checksum_address(ta),
                AsyncWeb3.to_checksum_address(tb),
                fee,
            ).call()
            if addr == "0x0000000000000000000000000000000000000000":
                return None
            # Determine token ordering (factory returns sorted: token0 < token1)
            token0, token1 = self._sort_tokens(ta, tb)
            return PoolInfo(address=addr.lower(), token0=token0, token1=token1,
                            fee=fee, dex="uniswap")
        except Exception as e:
            logger.warning(f"[PoolRegistry] Uni getPool({sym(ta)}/{sym(tb)} fee={fee}): {e}")
            return None

    async def _get_camelot_pool(self, ta: str, tb: str) -> Optional[PoolInfo]:
        try:
            addr = await self._camelot_factory.functions.poolByPair(
                AsyncWeb3.to_checksum_address(ta),
                AsyncWeb3.to_checksum_address(tb),
            ).call()
            if addr == "0x0000000000000000000000000000000000000000":
                return None
            token0, token1 = self._sort_tokens(ta, tb)
            return PoolInfo(address=addr.lower(), token0=token0, token1=token1,
                            fee=0, dex="camelot")
        except Exception as e:
            logger.warning(f"[PoolRegistry] Camelot poolByPair({sym(ta)}/{sym(tb)}): {e}")
            return None

    @staticmethod
    def _sort_tokens(ta: str, tb: str) -> tuple[str, str]:
        a, b = ta.lower(), tb.lower()
        return (a, b) if a < b else (b, a)


# ─── PriceEngine ─────────────────────────────────────────────────────────────

class PriceEngine:
    """Quotes rival DEX via on-chain quoters. Rate-limited per pair."""

    def __init__(self, w3: AsyncWeb3):
        self._w3 = w3
        self._uni_quoter = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(UNIV3_QUOTER),
            abi=UNIV3_QUOTER_ABI,
        )
        self._camelot_quoter = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(CAMELOT_QUOTER),
            abi=CAMELOT_QUOTER_ABI,
        )
        self._last_quote_t: dict[tuple, float] = {}
        self.prices_usd: dict[str, float] = dict(SEED_PRICES_USD)

    async def init_prices(self) -> None:
        """Fetch live Chainlink prices to replace seed values."""
        for token, feed_addr in CHAINLINK_FEEDS.items():
            try:
                feed = self._w3.eth.contract(
                    address=AsyncWeb3.to_checksum_address(feed_addr),
                    abi=CHAINLINK_ABI,
                )
                data = await feed.functions.latestRoundData().call()
                price_usd = data[1] / 1e8
                self.prices_usd[token] = price_usd
                logger.info(f"[PriceEngine] {sym(token)} = ${price_usd:,.2f}")
            except Exception as e:
                logger.warning(f"[PriceEngine] Chainlink fetch failed for {sym(token)}: {e}")

    async def quote_rival(
        self, rival: PoolInfo, token_in: str, token_out: str, amount_in: int,
    ) -> Optional[int]:
        """Quote rival pool for amount_in → amount_out. Rate-limited to 1 call/2s per pool."""
        key = (rival.token0, rival.token1, rival.dex, rival.fee)
        now = time.time()
        if now - self._last_quote_t.get(key, 0) < 2.0:
            return None
        self._last_quote_t[key] = now

        # Cap amount to avoid huge quote calls
        cap = MAX_PROBE.get(token_in)
        if cap and amount_in > cap:
            amount_in = cap

        try:
            if rival.dex == "uniswap":
                return await self._quote_univ3(token_in, token_out, amount_in, rival.fee)
            else:
                return await self._quote_camelot(token_in, token_out, amount_in)
        except Exception as e:
            logger.debug(f"[PriceEngine] rival quote failed ({rival.dex}): {e}")
            return None

    async def _quote_univ3(
        self, token_in: str, token_out: str, amount_in: int, fee: int,
    ) -> Optional[int]:
        result = await asyncio.wait_for(
            self._uni_quoter.functions.quoteExactInputSingle({
                "tokenIn":           AsyncWeb3.to_checksum_address(token_in),
                "tokenOut":          AsyncWeb3.to_checksum_address(token_out),
                "amountIn":          amount_in,
                "fee":               fee,
                "sqrtPriceLimitX96": 0,
            }).call(),
            timeout=4.0,
        )
        return result[0]  # amountOut

    async def _quote_camelot(
        self, token_in: str, token_out: str, amount_in: int,
    ) -> Optional[int]:
        result = await asyncio.wait_for(
            self._camelot_quoter.functions.quoteExactInputSingle(
                AsyncWeb3.to_checksum_address(token_in),
                AsyncWeb3.to_checksum_address(token_out),
                amount_in,
                0,  # limitSqrtPrice = 0 (no limit)
            ).call(),
            timeout=4.0,
        )
        return result[0]  # amountOut

    def gas_cost_usd(self, base_fee_gwei: float = 0.1) -> float:
        gas_price_wei = int(base_fee_gwei * 2 * 1e9)  # 2× base as rough total
        eth_cost = ARB_GAS_UNITS * gas_price_wei / 1e18
        return eth_cost * self.prices_usd.get(WETH, 3500.0)


# ─── TelegramAlerter ─────────────────────────────────────────────────────────

class TelegramAlerter:
    def __init__(self, token: str, chat_id: str):
        self._token   = token
        self._chat_id = chat_id
        self._last_alert: dict[str, float] = {}

    async def send(self, key: str, text: str, cooldown: float = 60.0) -> None:
        if time.time() - self._last_alert.get(key, 0) < cooldown:
            return
        self._last_alert[key] = time.time()
        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        try:
            async with aiohttp.ClientSession() as session:
                await session.post(url, json={
                    "chat_id":    self._chat_id,
                    "text":       text,
                    "parse_mode": "HTML",
                }, timeout=aiohttp.ClientTimeout(total=5))
        except Exception as e:
            logger.warning(f"[Telegram] send failed: {e}")


# ─── SwapMonitor ─────────────────────────────────────────────────────────────

class SwapMonitor:
    """
    Subscribes to Swap events on monitored pools, detects large cross-DEX spreads,
    and alerts via Telegram.
    """

    def __init__(
        self,
        ws_url:   str | list[str],
        http_url: str,
        telegram: Optional[TelegramAlerter] = None,
    ):
        self._ws_urls  = [ws_url] if isinstance(ws_url, str) else ws_url
        self._http_url = http_url
        self._telegram = telegram
        self._w3: Optional[AsyncWeb3] = None
        self._pools_by_addr: dict[str, PoolInfo] = {}
        self._pair_to_pools: dict[tuple, list[PoolInfo]] = {}
        self._price_engine: Optional[PriceEngine] = None
        self._running = False

        # WS liveness — updated each time a WS message arrives
        self._last_ws_msg_t: float = 0.0

        # Dedup for polling fallback: (txHash:logIndex) evicted after MAX_SEEN entries
        self._seen_log_ids: set[str] = set()
        self._seen_log_ids_lru: deque[str] = deque()
        self._last_block_polled: int = 0

        # Arb executor
        self._arb_executor:  object = None
        self._arb_multi_dex: object = None
        self._arb_dry_run    = ARB_DRY_RUN
        self._arb_live       = False   # set from SWAP_MONITOR_EXECUTE at init
        self._arb_lock       = asyncio.Lock()
        self._arb_pair_cooldown: dict[str, float] = {}
        self._arb_executed   = 0

        # Sequencer feed
        self._feed_pair_cooldown: dict[str, float] = {}  # 5s cooldown per pair (feed path)
        self._feed_watcher: Optional[object] = None
        self._feed_fired = 0

        # Stats
        self._swaps_seen     = 0
        self._swaps_large    = 0
        self._opportunities  = 0
        self._alerts_sent    = 0
        self._poll_events    = 0

    async def start(self) -> None:
        connector = aiohttp.TCPConnector(limit=10, keepalive_timeout=30, enable_cleanup_closed=True)
        session   = aiohttp.ClientSession(connector=connector)
        provider  = AsyncHTTPProvider(self._http_url)
        await provider.cache_async_session(session)
        self._w3 = AsyncWeb3(provider)
        self._price_engine = PriceEngine(self._w3)

        logger.info("[SwapMonitor] Fetching live token prices…")
        await self._price_engine.init_prices()

        logger.info("[SwapMonitor] Discovering pools…")
        registry = PoolRegistry(self._w3)
        pools = await registry.discover()

        if not pools:
            logger.error("[SwapMonitor] No pools discovered — check factory addresses")
            return

        # pool_by_key: (token0_lower, token1_lower, fee) → PoolInfo — used by feed watcher
        pool_by_key: dict = {}
        camelot_by_pair: dict = {}        # (t0,t1) → Camelot pool (router swaps)
        univ3_primary_by_pair: dict = {}  # (t0,t1) → deepest UniV3 pool (1inch best-guess)
        for p in pools:
            self._pools_by_addr[p.address] = p
            key = (p.token0, p.token1)
            self._pair_to_pools.setdefault(key, []).append(p)
            pk = (p.token0.lower(), p.token1.lower())
            if p.dex == "uniswap":
                pool_by_key[(pk[0], pk[1], p.fee)] = p
                # primary = lowest fee tier (usually deepest liquidity)
                cur = univ3_primary_by_pair.get(pk)
                if cur is None or p.fee < cur.fee:
                    univ3_primary_by_pair[pk] = p
            elif p.dex == "camelot":
                camelot_by_pair[pk] = p

        # Seed pool prices/liquidity so the same-block feed path works from t=0
        # instead of waiting for confirmed WS events to warm each pool.
        if FEED_SAME_BLOCK:
            await self._seed_pool_state(pools)

        # Initialise the arb executor. It is needed for BOTH live execution and
        # SHADOW mode (which builds dry txs + scores predictions). The WS spread
        # path never executes (its spot-price detection is unsound); only the
        # sequencer-feed backrun path may broadcast, and only when live.
        self._arb_live = SWAP_MONITOR_EXECUTE
        self._arb_dry_run = not SWAP_MONITOR_EXECUTE
        _want_executor = (
            _ARB_IMPORTS_OK and ARB_EXECUTOR_ADDR
            and (SWAP_MONITOR_EXECUTE or FEED_SAME_BLOCK)
        )
        if _want_executor:
            wallet = os.getenv("BOT_ADDRESS", "")
            pk     = os.getenv("BOT_PRIVATE_KEY", "")
            if wallet and pk:
                try:
                    self._arb_multi_dex = _MultiDexRouter(self._w3, ARB_EXECUTOR_ADDR)
                    self._arb_executor  = _DexArbExecutor(
                        w3                   = self._w3,
                        arb_executor_address = ARB_EXECUTOR_ADDR,
                        wallet               = wallet,
                        private_key          = pk,
                    )
                    mode = "LIVE (broadcasts)" if self._arb_live else "SHADOW (no broadcast)"
                    logger.info(
                        f"[ArbExec] Feed-path executor ready — "
                        f"contract={ARB_EXECUTOR_ADDR[:12]}… mode={mode} "
                        f"min_execute=${MIN_EXECUTE_USD:.0f} "
                        f"edge>fee_floor+{MIN_BACKRUN_EDGE_PCT:.2f}%"
                    )
                    # Pre-warm TCP connections and nonce cache so first execution
                    # doesn't pay TLS handshake + nonce-fetch latency
                    await self._arb_executor.warmup()
                except Exception as _e:
                    logger.warning(f"[ArbExec] Init failed: {_e}")
            else:
                logger.warning("[ArbExec] BOT_ADDRESS/BOT_PRIVATE_KEY not set — execution disabled")
        else:
            logger.info(
                "[ArbExec] Disabled — set FEED_SAME_BLOCK=1 (shadow) or "
                "SWAP_MONITOR_EXECUTE=1 (live) to enable the same-block path."
            )

        # Start sequencer feed watcher for same-block arb (shadow or live)
        if _ARB_IMPORTS_OK and self._arb_executor is not None and FEED_SAME_BLOCK:
            pool_by_addr_lower = {k.lower(): v for k, v in self._pools_by_addr.items()}
            self._feed_watcher = SequencerFeedWatcher(
                pool_by_addr = pool_by_addr_lower,
                pool_by_key  = pool_by_key,
                on_pending_swap = self._on_feed_swap,
                camelot_by_pair = camelot_by_pair,
                univ3_primary_by_pair = univ3_primary_by_pair,
            )
            logger.info(
                f"[SeqFeed] Watcher ready — "
                f"{len(pool_by_addr_lower)} pools, {len(pool_by_key)} pair-fee keys"
            )

        self._running = True
        await asyncio.gather(
            self._ws_loop(pools),
            self._poll_loop(pools),
            self._stats_loop(),
            self._feed_watcher.run() if self._feed_watcher else asyncio.sleep(0),
        )

    async def _ws_loop(self, pools: list[PoolInfo]) -> None:
        """WebSocket loop with per-URL backoff and silence-timeout detection."""
        pool_addrs = [AsyncWeb3.to_checksum_address(p.address) for p in pools]
        subscribe_msg = json.dumps({
            "id": 1, "method": "eth_subscribe", "jsonrpc": "2.0",
            "params": ["logs", {
                "address": pool_addrs,
                "topics":  [SWAP_TOPIC],
            }],
        })

        # Per-URL backoff so a bad URL doesn't inflate delay on the good one
        backoffs: dict[str, float] = {u: 1.0 for u in self._ws_urls}
        url_cycle = itertools.cycle(self._ws_urls)
        current_url = next(url_cycle)
        SILENCE_TIMEOUT = 90.0  # seconds — PublicNode connects but delivers nothing

        while self._running:
            try:
                logger.info(f"[WS] Connecting to {current_url[:55]}…")
                async with websockets.connect(
                    current_url, ping_interval=30, ping_timeout=25, close_timeout=5,
                ) as ws:
                    await ws.send(subscribe_msg)
                    sub_resp = await asyncio.wait_for(ws.recv(), timeout=10.0)
                    parsed = json.loads(sub_resp)
                    if "error" in parsed:
                        raise RuntimeError(
                            f"Subscription rejected: {parsed['error'].get('message', sub_resp[:120])}"
                        )
                    logger.info(f"[WS] Subscribed — {sub_resp[:80]}")
                    backoffs[current_url] = 1.0  # reset on successful subscribe

                    # Use wait_for on each recv so silence > SILENCE_TIMEOUT raises TimeoutError
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=SILENCE_TIMEOUT)
                        self._last_ws_msg_t = time.time()
                        try:
                            msg = json.loads(raw)
                            log = msg.get("params", {}).get("result")
                            if log:
                                await self._on_swap_log(log)
                        except Exception as e:
                            logger.warning(f"[WS] message error: {e}")

            except asyncio.CancelledError:
                break
            except asyncio.TimeoutError:
                if self._running:
                    delay = backoffs[current_url]
                    backoffs[current_url] = min(delay * 2, 60.0)
                    current_url = next(url_cycle)
                    logger.warning(
                        f"[WS] Silent for {SILENCE_TIMEOUT:.0f}s — dropping. "
                        f"Waiting {delay:.0f}s, then → {current_url[:55]}"
                    )
                    await asyncio.sleep(delay)
            except Exception as e:
                if self._running:
                    delay = backoffs[current_url]
                    backoffs[current_url] = min(delay * 2, 60.0)
                    current_url = next(url_cycle)
                    logger.warning(
                        f"[WS] Failed: {e}. Waiting {delay:.0f}s, then → {current_url[:55]}"
                    )
                    await asyncio.sleep(delay)

    async def _seed_pool_state(self, pools: list[PoolInfo]) -> None:
        """
        Read each pool's current price (and UniV3 liquidity) once at startup so
        `_on_feed_swap` can predict + compare immediately. UniV3 → slot0()+
        liquidity(); Camelot/Algebra → globalState(). Best-effort: a pool that
        fails to seed simply stays cold until its first WS event.
        """
        gs_sel  = AsyncWeb3.to_hex(AsyncWeb3.keccak(text="globalState()")[:4])
        liq_sel = AsyncWeb3.to_hex(AsyncWeb3.keccak(text="liquidity()")[:4])

        async def _seed(p: PoolInfo) -> None:
            try:
                addr = AsyncWeb3.to_checksum_address(p.address)
                if p.dex == "uniswap":
                    c = self._w3.eth.contract(address=addr, abi=UNIV3_POOL_ABI)
                    slot0, liq = await asyncio.gather(
                        c.functions.slot0().call(), c.functions.liquidity().call())
                    sqrtp = int(slot0[0]); p.last_liquidity = int(liq); p.last_fee = p.fee
                else:
                    # Camelot/Algebra: globalState() tuple layout varies by version,
                    # so read raw words — word0=sqrtPrice(uint160), word2=current fee.
                    raw = await self._w3.eth.call({"to": addr, "data": gs_sel})
                    raw = bytes(raw)
                    sqrtp = int.from_bytes(raw[0:32], "big")
                    p.last_fee = int.from_bytes(raw[64:96], "big") if len(raw) >= 96 else 0
                    lraw = bytes(await self._w3.eth.call({"to": addr, "data": liq_sel}))
                    p.last_liquidity = int.from_bytes(lraw[0:32], "big")
                if sqrtp > 0:
                    p.last_sqrt_x96 = sqrtp
                    p.last_price    = sqrt_price_to_human(sqrtp, p.token0, p.token1)
                    p.last_event_t  = time.time()
            except Exception as e:
                logger.debug(f"[Seed] {pool_label(p)} {sym(p.token0)}/{sym(p.token1)} failed: {e}")

        # Stagger in small batches to avoid RPC rate limits.
        for i in range(0, len(pools), 4):
            await asyncio.gather(*[_seed(p) for p in pools[i:i + 4]])
            if i + 4 < len(pools):
                await asyncio.sleep(0.3)
        seeded = sum(1 for p in pools if p.last_price > 0)
        logger.info(f"[SwapMonitor] Seeded state for {seeded}/{len(pools)} pools")

    async def _on_swap_log(self, log: dict) -> None:
        addr = log.get("address", "").lower()
        pool = self._pools_by_addr.get(addr)
        if pool is None:
            return

        # Decode Swap event data: (int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)
        try:
            raw = bytes.fromhex(log["data"][2:])
            amount0, amount1, sqrt_x96, liquidity, tick = abi_decode(
                ["int256", "int256", "uint160", "uint128", "int24"], raw,
            )
        except Exception as e:
            logger.debug(f"[Monitor] decode failed: {e}")
            return

        self._swaps_seen += 1

        # Update pool's tracked price + raw state (state used for same-block
        # post-swap price prediction in the sequencer-feed path).
        price = sqrt_price_to_human(sqrt_x96, pool.token0, pool.token1)
        pool.last_price     = price
        pool.last_event_t   = time.time()
        pool.last_sqrt_x96  = int(sqrt_x96)
        pool.last_liquidity = int(liquidity)

        # USD size filter
        usd = swap_usd_size(amount0, amount1, pool, self._price_engine.prices_usd)
        if usd < MIN_SWAP_USD:
            return

        self._swaps_large += 1
        logger.info(
            f"[SWAP] ${usd:>9,.0f}  {sym(pool.token0)}/{sym(pool.token1)}"
            f"  {pool_label(pool)}"
            f"  price={price:.6g}"
        )

        # Check cross-DEX spread
        await self._check_spread(pool, amount0, amount1, usd, price)

    async def _check_spread(
        self,
        pool: PoolInfo,
        amount0: int, amount1: int,
        swap_usd: float,
        price_post_swap: float,
    ) -> None:
        """Quote rival DEX, compute spread, alert if profitable."""
        pair_key = (pool.token0, pool.token1)
        rival_pools = [p for p in self._pair_to_pools.get(pair_key, [])
                       if p.address != pool.address]
        if not rival_pools:
            return

        # Determine swap direction from signs of amount0/amount1
        # amount0 < 0 → pool sold token0 (user bought token0)
        # amount0 > 0 → pool received token0 (user sold token0)
        if amount0 < 0:
            # User bought token0 with token1 → token_in=token1, amount_in=amount1
            token_in, token_out = pool.token1, pool.token0
            amount_in = abs(amount1)
        else:
            # User bought token1 with token0 → token_in=token0, amount_in=amount0
            token_in, token_out = pool.token0, pool.token1
            amount_in = abs(amount0)

        # Expected output based on post-swap sqrtPriceX96 price
        dec_in  = TOKEN_DECIMALS.get(_to_checksum(token_in),  18)
        dec_out = TOKEN_DECIMALS.get(_to_checksum(token_out), 18)
        if token_in == pool.token0:
            spot_price = price_post_swap  # token1 per token0
        else:
            spot_price = 1.0 / price_post_swap if price_post_swap > 0 else None
        if not spot_price:
            return
        expected_out = int(amount_in / (10 ** dec_in) * spot_price * (10 ** dec_out))

        # Quote each rival pool
        for rival in rival_pools:
            rival_out = await self._price_engine.quote_rival(
                rival, token_in, token_out, amount_in,
            )
            if rival_out is None:
                continue

            # Keep rival.last_price fresh so _on_feed_swap can use it
            if rival_out > 0 and amount_in > 0:
                if token_in.lower() == rival.token0.lower():
                    rival.last_price = (rival_out / 10**dec_out) / (amount_in / 10**dec_in)
                elif rival_out > 0:
                    rival.last_price = (amount_in / 10**dec_in) / (rival_out / 10**dec_out)
                rival.last_event_t = time.time()

            if expected_out <= 0:
                continue

            spread = (rival_out - expected_out) / expected_out

            price_usd_out = self._price_engine.prices_usd.get(_to_checksum(token_out), 1.0)
            gross_usd     = abs(rival_out - expected_out) / (10 ** dec_out) * price_usd_out
            gas_usd       = self._price_engine.gas_cost_usd()
            net_usd       = gross_usd - gas_usd

            if rival_out > expected_out:
                direction = f"buy {sym(token_out)} on {pool_label(pool)}, sell on {pool_label(rival)}"
            else:
                direction = f"buy {sym(token_out)} on {pool_label(rival)}, sell on {pool_label(pool)}"

            spread_pct = abs(spread) * 100
            logger.info(
                f"[SPREAD] {sym(pool.token0)}/{sym(pool.token1)}"
                f"  {pool_label(pool)}↔{pool_label(rival)}"
                f"  spot_spread={spread_pct:.3f}%  (pre-filter; real net checked below)"
            )

            # The spot spread above is only a CHEAP PRE-FILTER: it compares a
            # post-swap spot price against a rival quote and ignores price impact
            # + both pool fees, so it MASSIVELY over-states profit (verified: its
            # "opportunities" reverted 13/13 on-chain). Before alerting we now run
            # the REAL two-leg round trip (both legs on-chain quotes) so the
            # Telegram "Net" is actually-executable P&L, not a phantom.
            if spread_pct < MIN_SPREAD_PCT:
                continue
            rt = await self._real_round_trip(pool, rival, token_in, token_out, amount_in)
            if rt is None:
                continue
            real_net, real_gross, buy_pool, sell_pool, probe_usd = rt
            if real_net < MIN_NET_PROFIT:
                logger.info(
                    f"[SPREAD] spot={spread_pct:.3f}% but REAL round-trip "
                    f"net=${real_net:+.2f} @ ${probe_usd:,.0f} "
                    f"(< ${MIN_NET_PROFIT:.2f}) — phantom, no alert"
                )
                continue

            self._opportunities += 1
            direction = (f"buy {sym(token_out)} on {pool_label(buy_pool)}, "
                         f"sell on {pool_label(sell_pool)}")
            asyncio.create_task(self._alert(
                pool, rival, swap_usd, spread_pct, direction,
                real_gross, gas_usd, real_net, amount0, amount1, probe_usd))

    async def _alert(
        self,
        pool: PoolInfo, rival: PoolInfo,
        swap_usd: float, spread_pct: float,
        direction: str, gross_usd: float, gas_usd: float, net_usd: float,
        amount0: int, amount1: int, probe_usd: float = 0.0,
    ) -> None:
        # net_usd here is the REAL two-leg round-trip P&L (fees + impact), not the
        # spot spread — only genuinely-executable signals reach this point.
        logger.warning(
            f"[OPPORTUNITY] real_net=${net_usd:.2f} @ ${probe_usd:,.0f}"
            f"  (spot_spread={spread_pct:.3f}%)  pair={sym(pool.token0)}/{sym(pool.token1)}"
            f"  → {direction}"
        )
        if self._telegram is None:
            return

        pair_key = f"{sym(pool.token0)}{sym(pool.token1)}"
        dec0 = TOKEN_DECIMALS.get(pool.token0, 18)
        dec1 = TOKEN_DECIMALS.get(pool.token1, 18)
        a0_h = abs(amount0) / 10**dec0
        a1_h = abs(amount1) / 10**dec1

        text = (
            f"<b>ARB SIGNAL — {sym(pool.token0)}/{sym(pool.token1)}</b>\n\n"
            f"Swap: ${swap_usd:,.0f}  "
            f"({a0_h:.4g} {sym(pool.token0)} / {a1_h:.4g} {sym(pool.token1)})\n"
            f"On: {pool.dex} {'fee=' + str(pool.fee) if pool.dex == 'uniswap' else ''}\n\n"
            f"Spot spread: {spread_pct:.3f}% (signal only)\n"
            f"Action: {direction}\n"
            f"Tested size: ${probe_usd:,.0f}\n\n"
            f"Gross (real round-trip): <b>${gross_usd:.2f}</b>\n"
            f"Gas:   ${gas_usd:.2f}\n"
            f"Net (real, executable): <b>${net_usd:.2f}</b>"
        )
        self._alerts_sent += 1
        await self._telegram.send(pair_key, text, cooldown=30.0)

    async def _try_execute_arb(
        self,
        token_in:     str,
        token_out:    str,
        amount_in:    int,
        buy_pool:     PoolInfo,
        sell_pool:    PoolInfo,
        gross_usd:    float,
        gas_usd:      float,
        net_usd:      float,
        spread_pct:   float,
        pre_buy_out:  int = 0,
        slippage_bps: int = 30,
    ) -> None:
        """Build and submit arb tx. Non-blocking — called via create_task."""
        pair_key = f"{token_in[:8]}:{token_out[:8]}"
        now = time.time()

        # Per-pair cooldown — only blocks re-entry after a successful submission
        if now - self._arb_pair_cooldown.get(pair_key, 0) < 30.0:
            return

        # Skip if another execution is already in flight
        if self._arb_lock.locked():
            logger.debug("[ArbExec] Lock held — skipping signal")
            return

        async with self._arb_lock:
            try:
                # Cap flash-borrow amount to MAX_PROBE to avoid outsized slippage
                cap = MAX_PROBE.get(_to_checksum(token_in))
                opp_amount = min(amount_in, cap) if cap else amount_in

                def _dex_name(p: PoolInfo) -> str:
                    return "camelot" if p.dex == "camelot" else "univ3"

                def _router(p: PoolInfo) -> str:
                    return CAMELOT_ROUTER if p.dex == "camelot" else UNIV3_ROUTER

                # Scale pre_buy_out if amount was capped
                scaled_pre_buy_out = (
                    int(pre_buy_out * opp_amount / amount_in)
                    if pre_buy_out and amount_in > 0 else pre_buy_out
                )

                opp = ArbOpportunity(
                    token_in         = _to_checksum(token_in),
                    token_out        = _to_checksum(token_out),
                    amount_in        = opp_amount,
                    buy_dex          = _dex_name(buy_pool),
                    sell_dex         = _dex_name(sell_pool),
                    buy_router       = _router(buy_pool),
                    sell_router      = _router(sell_pool),
                    expected_out     = int(opp_amount * (1 + spread_pct / 100)),
                    gross_profit     = int(opp_amount * spread_pct / 100),
                    gross_profit_usd = gross_usd,
                    gas_cost_usd     = gas_usd,
                    net_profit_usd   = net_usd,
                    spread_pct       = spread_pct,
                    buy_amount_out   = scaled_pre_buy_out,
                    buy_fee          = buy_pool.fee if buy_pool.dex == "uniswap" else 0,
                    sell_fee_tier    = sell_pool.fee if sell_pool.dex == "uniswap" else 0,
                )

                dec_in = TOKEN_DECIMALS.get(_to_checksum(token_in), 18)
                mode   = "DRY RUN" if self._arb_dry_run else "LIVE"
                logger.info(
                    f"[ArbExec] {mode}: {sym(token_in)}→{sym(token_out)}"
                    f" buy={opp.buy_dex} sell={opp.sell_dex}"
                    f" amount={opp_amount / 10**dec_in:.4g}"
                    f" net=${net_usd:.2f}"
                )

                t_exec = time.time()
                tx_hash = await self._arb_executor.execute(
                    opp          = opp,
                    multi_dex    = self._arb_multi_dex,
                    dry_run      = self._arb_dry_run,
                    slippage_bps = slippage_bps,
                )
                exec_ms = int((time.time() - t_exec) * 1000)
                if tx_hash:
                    self._arb_pair_cooldown[pair_key] = now
                    self._arb_executed += 1
                    logger.info(f"[ArbExec] Submitted in {exec_ms}ms: {tx_hash}")
                else:
                    logger.info("[ArbExec] No tx — spread closed or dry run")

            except Exception as _e:
                logger.error(f"[ArbExec] Execute error: {_e}", exc_info=True)

    async def _on_feed_swap(self, ps: "_PendingSwap") -> None:
        """
        Same-block backrun. Called by SequencerFeedWatcher for each pending swap
        on a monitored UniV3 pool, BEFORE it confirms.

        The pending swap is what *creates* the dislocation, so we cannot use the
        target pool's last (pre-swap) price — we PREDICT its post-swap price from
        the pending swap amount via Uniswap V3 SqrtPriceMath (single-tick), then
        compare to the rival pool's current price. We fire a flash-loan backrun
        only when the predicted gap clears the round-trip FEE FLOOR (both pools'
        fees) by MIN_BACKRUN_EDGE_PCT — the gate the old spot-price path lacked.

        SHADOW mode (SWAP_MONITOR_EXECUTE=0): logs the decision and, ~1 block
        later, quotes the REAL round trip to score the prediction — no broadcast.
        LIVE mode: broadcasts the backrun (on-chain profit guard bounds downside
        to gas).
        """
        FEED_COOLDOWN     = 5.0
        FEED_SLIPPAGE     = 80           # bps — backrun lands behind a fresh quote-able state
        FEED_MIN_SWAP_USD = 500.0        # LIVE pre-filter; net≥$3 + edge>0 is the real gate
        SHADOW_MIN_SWAP_USD = 400.0      # SHADOW: wider net — score predictions vs reality

        # Flash-borrow preference: borrow the most flash-liquid token of the pair
        # (Balancer has deep USDC/WETH; ARB flash liquidity is thin).
        FLASH_RANK = {USDC: 0, USDT: 1, WETH: 2, WBTC: 3, ARB: 4}

        if self._arb_executor is None:
            return

        # ── locate target pool + require state for prediction ────────────────
        addr_cs = _to_checksum(ps.pool_addr) if len(ps.pool_addr) == 42 else ps.pool_addr
        pool = (self._pools_by_addr.get(addr_cs)
                or self._pools_by_addr.get(addr_cs.lower())
                or self._pools_by_addr.get(addr_cs.upper()))
        if pool is None:
            return
        if pool.last_sqrt_x96 <= 0 or pool.last_liquidity <= 0:
            return   # no state to predict from (UniV3 slot0 / Camelot globalState)
        # Fee for the price-move prediction: UniV3 static tier, Camelot dynamic.
        target_fee_ppm = pool.fee if pool.dex == "uniswap" else pool.last_fee
        if target_fee_ppm <= 0:
            return

        if self._arb_lock.locked() and self._arb_live:
            return

        # ── size filter ──────────────────────────────────────────────────────
        ti_cs    = _to_checksum(ps.token_in)
        dec_ti   = TOKEN_DECIMALS.get(ti_cs, 18)
        price_ti = self._price_engine.prices_usd.get(ti_cs, 0.0)
        if ps.amount_in <= 0 or price_ti <= 0:
            return
        swap_usd = ps.amount_in / 10 ** dec_ti * price_ti
        if swap_usd < (FEED_MIN_SWAP_USD if self._arb_live else SHADOW_MIN_SWAP_USD):
            return

        # ── predict target pool's POST-swap price (token1 per token0) ────────
        sqrt_next = predict_next_sqrt(
            pool.last_sqrt_x96, pool.last_liquidity,
            ps.amount_in, ps.zero_for_one, target_fee_ppm,
        )
        if sqrt_next <= 0:
            return
        pP = sqrt_price_to_human(sqrt_next, pool.token0, pool.token1)
        if pP <= 0:
            return

        # ── pick the rival pool that gives the biggest edge over the fee floor ─
        rivals = [p for p in self._pair_to_pools.get((pool.token0, pool.token1), [])
                  if p.address != pool.address and p.last_price > 0]
        if not rivals:
            return

        best = None  # (rival, pR, gap, fee_floor, edge)
        floor_target = pool_fee_frac(pool)
        for r in rivals:
            pR  = r.last_price
            gap = abs(pP - pR) / min(pP, pR)
            fee_floor = floor_target + pool_fee_frac(r)
            edge = gap - fee_floor
            if best is None or edge > best[4]:
                best = (r, pR, gap, fee_floor, edge)
        rival, pR, gap, fee_floor, edge = best

        # NOTE: the edge gate (edge >= MIN_BACKRUN_EDGE_PCT) is applied below as
        # part of `would_fire`. In SHADOW we proceed past it so we can score
        # below-threshold candidates too — that's how we learn if the gate is
        # well-calibrated. In LIVE, would_fire must be true to broadcast.

        # ── choose flash token + arb direction ───────────────────────────────
        # Higher price (token1/token0) ⇒ token0 is dearer there. To capture the
        # gap we buy token0 on the cheaper pool and sell on the dearer.
        hi_pool, hi_price = (pool, pP) if pP > pR else (rival, pR)
        lo_pool, lo_price = (rival, pR) if pP > pR else (pool, pP)

        # Borrow the more flash-liquid token; orient legs around it.
        t0_cs, t1_cs = _to_checksum(pool.token0), _to_checksum(pool.token1)
        if FLASH_RANK.get(t0_cs, 9) <= FLASH_RANK.get(t1_cs, 9):
            # token_in = token0, token_out = token1.
            # buy leg token0→token1 yields most token1 on the HIGHER-price pool.
            token_in, token_out = pool.token0, pool.token1
            buy_pool, sell_pool = hi_pool, lo_pool
            price_buy = hi_price                      # token1 per token0
            out_per_in = price_buy                    # token_out per token_in
        else:
            # token_in = token1, token_out = token0.
            # buy leg token1→token0 yields most token0 on the LOWER-price pool.
            token_in, token_out = pool.token1, pool.token0
            buy_pool, sell_pool = lo_pool, hi_pool
            price_buy = lo_price
            out_per_in = (1.0 / price_buy) if price_buy > 0 else 0.0
        if out_per_in <= 0:
            return

        tin_cs  = _to_checksum(token_in)
        tout_cs = _to_checksum(token_out)
        dec_in  = TOKEN_DECIMALS.get(tin_cs, 18)
        dec_out = TOKEN_DECIMALS.get(tout_cs, 18)
        price_in_usd = self._price_engine.prices_usd.get(tin_cs, 0.0)
        if price_in_usd <= 0:
            return

        # ── size the backrun: scale to the pending swap, capped by MAX_PROBE ──
        cap = MAX_PROBE.get(tin_cs)
        notional_in = int(swap_usd / price_in_usd * 10 ** dec_in)
        amount_in = min(notional_in, cap) if cap else notional_in
        if amount_in <= 0:
            return

        estimated_out = int(amount_in / 10 ** dec_in * out_per_in * 10 ** dec_out)
        if estimated_out <= 0:
            return

        # Profit ≈ captured fraction of the edge on the traded notional (our own
        # trade closes part of the gap → BACKRUN_CAPTURE, ~half).
        notional_usd = amount_in / 10 ** dec_in * price_in_usd
        gross_usd = edge * BACKRUN_CAPTURE * notional_usd
        gas_usd   = self._price_engine.gas_cost_usd()
        net_usd   = gross_usd - gas_usd

        # Would this fire for real money? Execute only when the predicted spread
        # stays POSITIVE after all costs (edge > 0 ⇒ gap clears both pools' fees)
        # and clears the $3 floor. The on-chain sell-leg guard (principal +
        # minProfit) is the capital backstop: a misprediction reverts (gas only).
        would_fire = (
            swap_usd >= FEED_MIN_SWAP_USD
            and edge > (MIN_BACKRUN_EDGE_PCT / 100)
            and net_usd >= MIN_EXECUTE_USD
        )
        live = self._arb_live
        if live and not would_fire:
            return   # LIVE only acts on the strict gate

        # ── cooldown ──────────────────────────────────────────────────────────
        pair_kstr = f"{token_in[:8]}:{token_out[:8]}"
        now = time.time()
        if now - self._feed_pair_cooldown.get(pair_kstr, 0) < FEED_COOLDOWN:
            return
        self._feed_pair_cooldown[pair_kstr] = now
        self._feed_fired += 1

        mode   = "LIVE" if live else "SHADOW"
        marker = "★" if would_fire else "◦"
        logger.info(
            f"[SeqFeed] {marker} {mode} backrun {sym(token_in)}→{sym(token_out)} "
            f"pending=${swap_usd:,.0f} on {pool_label(pool)} "
            f"pred_gap={gap*100:.3f}% fee_floor={fee_floor*100:.3f}% "
            f"edge={edge*100:.3f}% est_net=${net_usd:.2f} would_fire={would_fire} "
            f"buy={pool_label(buy_pool)} sell={pool_label(sell_pool)}"
        )

        if live:
            # REAL-round-trip gate for the live broadcast.
            # The same-block edge only exists AFTER the pending swap lands, so a
            # full round-trip eth_call *now* would measure pre-dislocation state
            # and is NOT a valid profit gate — the atomic on-chain guard
            # (sellMinOut = amountIn + minProfit) is what protects principal: a
            # missed prediction reverts, costing gas only. But we DO re-quote the
            # BUY leg against current state so the buy-leg slippage guard is
            # anchored to a real price (not the predicted estimate), and we abort
            # if the buy venue can't be quoted at all.
            real_buy_out = await self._quote_pool(buy_pool, token_in, token_out, amount_in)
            if not real_buy_out:
                logger.info(f"[SeqFeed] LIVE abort — buy leg unquotable on {pool_label(buy_pool)}")
                return
            # Guard against a predicted edge that a real buy quote already refutes
            # (real buy fill materially worse than predicted ⇒ the gap isn't there).
            if real_buy_out < estimated_out * 0.97:
                logger.info(
                    f"[SeqFeed] LIVE abort — real buy {real_buy_out} << predicted "
                    f"{estimated_out} (>3% worse), edge not real"
                )
                return
            asyncio.create_task(self._try_execute_arb(
                token_in     = token_in,
                token_out    = token_out,
                amount_in    = amount_in,
                buy_pool     = buy_pool,
                sell_pool    = sell_pool,
                gross_usd    = gross_usd,
                gas_usd      = gas_usd,
                net_usd      = net_usd,
                spread_pct   = edge * 100,
                pre_buy_out  = real_buy_out,
                slippage_bps = FEED_SLIPPAGE,
            ))
    async def _quote_pool(self, pool: PoolInfo, token_in: str, token_out: str,
                          amount_in: int) -> Optional[int]:
        """Direct single-pool quote (bypasses the rate limiter). Returns amount_out."""
        pe = self._price_engine
        if pool.dex == "camelot":
            return await pe._quote_camelot(token_in, token_out, amount_in)
        return await pe._quote_univ3(token_in, token_out, amount_in, pool.fee)

    async def _real_round_trip(
        self, pool: PoolInfo, rival: PoolInfo,
        token_in: str, token_out: str, amount_in: int,
    ) -> Optional[tuple]:
        """
        Ground-truth economics for a detected spread: buy token_out where it's
        cheapest, sell it back on the other venue — both legs REAL on-chain
        QuoterV2 quotes (price-impact + both pool fees included). A positive net
        here is genuinely executable, unlike the spot-price spread that triggered
        the check. Returns (net_usd, gross_usd, buy_pool, sell_pool, probe_usd)
        or None if either leg is unquotable.
        """
        # Cap the probe so a whale swap doesn't make us quote an absurd notional
        # (which would only show a hugely negative net from its own impact).
        cap = MAX_PROBE.get(_to_checksum(token_in))
        if cap:
            amount_in = min(amount_in, cap)
        if amount_in <= 0:
            return None

        out_pool  = await self._quote_pool(pool,  token_in, token_out, amount_in)
        out_rival = await self._quote_pool(rival, token_in, token_out, amount_in)
        cands = [(p, o) for p, o in ((pool, out_pool), (rival, out_rival)) if o]
        if len(cands) < 2:
            return None
        buy_pool, buy_out = max(cands, key=lambda x: x[1])   # cheapest place to buy
        sell_pool = rival if buy_pool is pool else pool
        a_back = await self._quote_pool(sell_pool, token_out, token_in, buy_out)
        if not a_back:
            return None

        dec_in    = TOKEN_DECIMALS.get(_to_checksum(token_in), 18)
        price_in  = self._price_engine.prices_usd.get(_to_checksum(token_in), 0.0)
        gross_usd = (a_back - amount_in) / 10 ** dec_in * price_in   # real round-trip P&L
        net_usd   = gross_usd - self._price_engine.gas_cost_usd()
        probe_usd = amount_in / 10 ** dec_in * price_in
        return net_usd, gross_usd, buy_pool, sell_pool, probe_usd

    async def _poll_loop(self, pools: list[PoolInfo]) -> None:
        """
        HTTP eth_getLogs fallback — activates when WS has been silent for WS_DEAD seconds.
        Deduplicates against WS events via (txHash:logIndex) so both can run simultaneously
        without double-processing during WS reconnects.
        """
        WS_DEAD       = 10.0   # seconds without a WS message before polling kicks in
        POLL_INTERVAL = 2.0    # seconds between polls
        LOOKBACK      = 5      # extra blocks to re-fetch on reconnect (covers gaps)
        MAX_SEEN      = 3000   # dedup LRU size

        pool_addrs = [AsyncWeb3.to_checksum_address(p.address) for p in pools]

        await asyncio.sleep(12)  # let WS settle first

        # Anchor to current chain tip so first poll doesn't scan from block 0
        try:
            self._last_block_polled = await self._w3.eth.block_number
        except Exception:
            pass

        _polling_active = False

        while self._running:
            await asyncio.sleep(POLL_INTERVAL)

            # Always fetch latest so _last_block_polled stays anchored near the tip.
            # Without this, after a long WS-alive stretch, the first poll on WS drop
            # would either miss blocks or issue a huge backfill query.
            try:
                latest = await asyncio.wait_for(
                    self._w3.eth.block_number, timeout=5.0
                )
            except Exception:
                continue

            ws_alive = time.time() - self._last_ws_msg_t < WS_DEAD
            if ws_alive:
                if _polling_active:
                    logger.info("[Poll] WS recovered — polling paused")
                    _polling_active = False
                self._last_block_polled = latest
                continue

            if not _polling_active:
                logger.info("[Poll] WS silent — HTTP polling active")
                _polling_active = True

            try:
                from_block = max(self._last_block_polled - LOOKBACK + 1, 0)

                logs = await asyncio.wait_for(
                    self._w3.eth.get_logs({
                        "fromBlock": from_block,
                        "toBlock":   latest,
                        "address":   pool_addrs,
                        "topics":    [SWAP_TOPIC],
                    }),
                    timeout=6.0,
                )

                self._last_block_polled = latest

                new = 0
                for log in logs:
                    tx  = log["transactionHash"].hex()
                    lid = f"{tx}:{log['logIndex']}"
                    if lid in self._seen_log_ids:
                        continue
                    self._seen_log_ids.add(lid)
                    self._seen_log_ids_lru.append(lid)
                    if len(self._seen_log_ids_lru) > MAX_SEEN:
                        self._seen_log_ids.discard(self._seen_log_ids_lru.popleft())

                    data = log["data"]
                    if isinstance(data, bytes):
                        data = "0x" + data.hex()
                    await self._on_swap_log({"address": log["address"], "data": data})
                    new += 1
                    self._poll_events += 1

                if new:
                    logger.debug(f"[Poll] {new} new event(s) from blocks {from_block}–{latest}")

            except asyncio.TimeoutError:
                logger.debug("[Poll] eth_getLogs timed out")
            except Exception as e:
                logger.debug(f"[Poll] {e}")

    async def _stats_loop(self) -> None:
        while self._running:
            await asyncio.sleep(60)
            ws_age = time.time() - self._last_ws_msg_t
            ws_status = "live" if ws_age < 10 else f"silent {ws_age:.0f}s"
            feed_msgs = getattr(self._feed_watcher, "_msgs_seen",  0) if self._feed_watcher else 0
            feed_swaps= getattr(self._feed_watcher, "_swaps_seen", 0) if self._feed_watcher else 0
            feed_mode = ("off" if self._arb_executor is None
                         else ("live" if self._arb_live else "shadow"))
            logger.info(
                f"[Stats] swaps_seen={self._swaps_seen}"
                f"  large={self._swaps_large}"
                f"  opportunities={self._opportunities}"
                f"  alerts={self._alerts_sent}"
                f"  arb_executed={self._arb_executed}"
                f"  feed_msgs={feed_msgs}  feed_swaps={feed_swaps}"
                f"  feed[{feed_mode}]_fired={self._feed_fired}"
                f"  poll_events={self._poll_events}"
                f"  ws={ws_status}"
            )


# ─── Entry point ─────────────────────────────────────────────────────────────

async def main() -> None:
    # Priority: Chainstack/Alchemy first (reliable), DRPC second (works but flaky/short-lived),
    # PublicNode last (accepts subscription but delivers no events — 90s silence sink).
    # Deduplicate while preserving order (QUICKNODE_WS_URL often == CHAINSTACK_ARBITRUM_WS_URL).
    _seen: set[str] = set()
    ws_candidates: list[str] = []
    for _url in [
        os.getenv("CHAINSTACK_ARBITRUM_WS_URL"),
        os.getenv("ARBITRUM_WS_URL"),         # Alchemy
        os.getenv("QUICKNODE_WS_URL"),
        os.getenv("DRPC_WSS_URL"),            # intermittently supports log subscriptions
        os.getenv("RPC_WSS_URL"),             # PublicNode — silent, last resort
    ]:
        if _url and _url not in _seen:
            ws_candidates.append(_url)
            _seen.add(_url)
    http_url = (
        os.getenv("ARBITRUM_HTTP_URL") or
        os.getenv("ALCHEMY_HTTP_URL") or
        os.getenv("RPC_PUBLICNODE", "https://arbitrum-one.publicnode.com")
    )

    if not ws_candidates:
        logger.error("No WebSocket URL found. Set CHAINSTACK_ARBITRUM_WS_URL or ARBITRUM_WS_URL.")
        sys.exit(1)

    tg_token   = os.getenv("TELEGRAM_BOT_TOKEN")
    tg_chat_id = os.getenv("TELEGRAM_CHAT_ID")
    telegram   = TelegramAlerter(tg_token, tg_chat_id) if tg_token and tg_chat_id else None

    if telegram:
        logger.info("[Telegram] Alerts enabled")
    else:
        logger.warning("[Telegram] No token/chat_id — alerts disabled")

    logger.info(
        f"[Config] MIN_SWAP_USD=${MIN_SWAP_USD:,.0f}"
        f"  MIN_SPREAD={MIN_SPREAD_PCT:.2f}%"
        f"  MIN_NET=${MIN_NET_PROFIT:.2f}"
    )

    monitor = SwapMonitor(ws_url=ws_candidates, http_url=http_url, telegram=telegram)
    await monitor.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted — bye")
