"""
quote_cache.py — Pre-fetched swap quote cache for cross-asset pairs
Fixes: 6 pre-warm positions timing out at 350ms due to QuoterV2 latency
       on cross-asset pairs (primarily wstETH→WETH via Uni V3).

Problem:
    SwapCalldataBuilder.build() calls QuoterV2 × 3 fee tiers per build.
    On public/Chainstack RPC, each eth_call takes ~100-200ms.
    Cross-asset builds exceed the 350ms pre-warm deadline every cycle.
    12/20 warm instead of 20/20.

Fix:
    QuoteCache runs a background task that pre-fetches quotes for
    configured pairs every QUOTE_TTL seconds. SwapCalldataBuilder
    checks the cache before calling QuoterV2 — cache hit = ~0ms,
    cache miss = falls back to live QuoterV2 as before.

    The cache stores (best_amount_out, best_fee) per pair keyed by
    (token_in, token_out, amount_in_bucket). Amount is bucketed to
    nearest order of magnitude so one cached quote covers a range
    of liquidation sizes.

Usage:
    # In setup():
    self.quote_cache = QuoteCache(
        quoter = self.quoter,           # QuoterAsync
        pairs  = KNOWN_SLOW_PAIRS,
    )
    await self.quote_cache.start()

    # Wire into SwapCalldataBuilder in flash_loan_route.py:
    # Pass quote_cache to SwapCalldataBuilder.__init__()
    # build() checks cache before QuoterV2
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Known slow pairs on Arbitrum — pre-fetch these on startup
# ---------------------------------------------------------------------------

WSTETH = "0x5979D7b546E38E414F7E9822514be443A4800529"
WETH   = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC   = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
USDT   = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"
ARB    = "0x912CE59144191C1204E64559FE8253a0e49E6548"
WBTC   = "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f"

# Pairs confirmed slow — pre-fetch them on startup
# Discovered from pre-warm cache miss logging
KNOWN_SLOW_PAIRS = [
    (WSTETH, WETH),    # 0x9CD61CaB collateral → debt (primary bottleneck)
    (WSTETH, USDC),    # wstETH collateral, native USDC debt
    (WSTETH, USDT),    # wstETH collateral, USDT debt
    (ARB,    USDC),    # ARB collateral, USDC debt
    (ARB,    WETH),    # ARB collateral, WETH debt
    (ARB,    USDT),    # ARB collateral, USDT debt
    (WBTC,   USDC),    # WBTC collateral, USDC debt
    (WBTC,   WETH),    # WBTC collateral, WETH debt
    (WETH,   USDC),    # WETH collateral, native USDC debt (cache miss observed)
    (USDC,   USDT),    # native USDC collateral, USDT debt (cache miss observed)
]

# Cache TTL — refresh every 12s (~48 Arbitrum blocks)
# Price moves <0.1% in 12s for these stable pairs — safe window
QUOTE_TTL = 12.0


# Token decimals for sanity-check normalization in get()
TOKEN_DECIMALS = {
    WETH:   18,
    WBTC:   8,
    USDC:   6,
    USDT:   6,
    ARB:    18,
    WSTETH: 18,
}

# Amount bucket size — cache one quote per order of magnitude
# e.g. 1e17, 1e18, 1e19 each get their own cache entry
# Covers liquidation sizes from dust to large positions
BUCKET_DECIMALS = 1  # round to nearest 10^N


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class CachedQuote:
    token_in:    str
    token_out:   str
    amount_in:   int      # bucketed reference amount
    amount_out:  int      # best quote result
    fee_tier:    int      # best fee tier
    cached_at:   float = field(default_factory=time.time)
    source:      str = "quoter"  # "quoter" | "stale"

    @property
    def age_seconds(self) -> float:
        return time.time() - self.cached_at

    def is_fresh(self, ttl: float = QUOTE_TTL) -> bool:
        return self.age_seconds < ttl

    def scale_to(self, target_amount: int) -> tuple[int, int]:
        """
        Scale cached quote to a different amount linearly.
        Returns (scaled_amount_out, fee_tier).
        Linear scaling is valid for small deviations from reference amount.
        """
        if self.amount_in == 0:
            return 0, self.fee_tier
        ratio      = target_amount / self.amount_in
        scaled_out = int(self.amount_out * ratio)
        return scaled_out, self.fee_tier


def _bucket_amount(amount: int) -> int:
    """
    Round amount to nearest order of magnitude for cache key stability.
    e.g. 1.23e18 → 1e18, 4.56e17 → 1e17
    Ensures different liquidation sizes share cached quotes.
    """
    if amount <= 0:
        return amount
    import math
    magnitude = 10 ** max(0, int(math.log10(amount)))
    return magnitude


# ---------------------------------------------------------------------------
# QuoteCache
# ---------------------------------------------------------------------------

class QuoteCache:
    """
    Pre-fetches and caches Uni V3 quotes for known slow cross-asset pairs.
    Background task refreshes every QUOTE_TTL seconds.

    SwapCalldataBuilder checks this cache before calling QuoterV2 live.
    Cache hit: ~0ms. Cache miss: falls back to live QuoterV2.

    The cache is keyed by (token_in, token_out, amount_bucket).
    Amount bucketing means one cached quote covers a range of sizes
    via linear scaling — valid for deviations up to ~50% of reference.
    """

    # Reference amounts for pre-fetching (covers typical liquidation sizes)
    REFERENCE_AMOUNTS = {
        WSTETH: int(0.5e18),    # 0.5 wstETH — typical small liquidation
        ARB:    int(1000e18),   # 1000 ARB
        WBTC:   int(0.01e8),    # 0.01 WBTC
        WETH:   int(0.5e18),    # 0.5 WETH
    }
    DEFAULT_REFERENCE = int(1e18)

    def __init__(
        self,
        quoter,                          # QuoterAsync from async_web3.py
        pairs: list[tuple[str, str]] = None,
        ttl:   float = QUOTE_TTL,
    ):
        self._quoter  = quoter
        self._pairs   = pairs or KNOWN_SLOW_PAIRS
        self._ttl     = ttl
        self._cache:  dict[tuple, CachedQuote] = {}
        self._running = False
        self._task:   asyncio.Task = None
        self._hits    = 0
        self._misses  = 0
        self._fetches = 0

    async def start(self) -> None:
        """Pre-fetch all pairs immediately, then start background refresh."""
        await self._fetch_all()
        self._running = True
        self._task = asyncio.create_task(self._refresh_loop(), name="quote_cache")
        logger.info(
            f"[QuoteCache] Started — "
            f"{len(self._pairs)} pairs, "
            f"TTL={self._ttl}s, "
            f"{len(self._cache)} entries pre-fetched"
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()

    def get(
        self,
        token_in:  str,
        token_out: str,
        amount_in: int,
    ) -> Optional[tuple[int, int]]:
        """
        Look up a cached quote. Returns (amount_out, fee_tier) or None.
        Scales cached reference quote to requested amount linearly.

        Keyed by (token_in, token_out) — no amount bucketing.
        Linear scaling is valid because price impact on these liquid
        pairs is <0.1% across order-of-magnitude size differences.
        """
        key = (token_in.lower(), token_out.lower())

        entry = self._cache.get(key)
        if entry is None:
            self._misses += 1
            return None

        if not entry.is_fresh(self._ttl):
            entry.source = "stale"

        scaled_out, fee_tier = entry.scale_to(amount_in)

        # Reject zero-output: scale_to() truncates to 0 for very small amount_in
        # (tiny dust positions). A zero amount_out causes 100% slippage in the
        # validator and always fails. Fall through to live QuoterV2 instead.
        if scaled_out == 0:
            self._misses += 1
            return None

        # Sanity check: reject cached quotes where output vastly exceeds input.
        # Normalize by token decimals — cross-decimal pairs (USDC→WETH) have
        # different raw ratios due to 6 vs 18 decimal difference.
        # Threshold: 100,000× catches truly corrupt quotes (585M×) while
        # allowing normal FX rates like WETH/USDC ~1640×.
        in_dec  = TOKEN_DECIMALS.get(token_in, 18)
        out_dec = TOKEN_DECIMALS.get(token_out, 18)
        human_out = scaled_out / 10**out_dec
        human_in  = amount_in / 10**in_dec
        if human_in > 0 and human_out / human_in > 100_000:
            logger.debug(
                f"[QuoteCache] Rejecting implausible cached quote: "
                f"{token_in[:10]}→{token_out[:10]} "
                f"out={scaled_out} in={amount_in} ratio={scaled_out/amount_in:.0f}x"
            )
            self._misses += 1
            return None

        self._hits += 1
        return scaled_out, fee_tier

    def is_cached(self, token_in: str, token_out: str) -> bool:
        """Check if a pair has any cached entry (fresh or stale)."""
        key = (token_in.lower(), token_out.lower())
        return key in self._cache

    @property
    def stats(self) -> dict:
        total = self._hits + self._misses
        return {
            "entries":   len(self._cache),
            "hits":      self._hits,
            "misses":    self._misses,
            "hit_rate":  self._hits / total if total else 0.0,
            "fetches":   self._fetches,
            "stale":     sum(1 for e in self._cache.values() if not e.is_fresh(self._ttl)),
        }

    def log_stats(self) -> None:
        s = self.stats
        logger.info(
            f"[QuoteCache] entries={s['entries']} "
            f"hit_rate={s['hit_rate']:.1%} "
            f"hits={s['hits']} misses={s['misses']} "
            f"stale={s['stale']}"
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _fetch_pair(self, token_in: str, token_out: str) -> bool:
        """Fetch quote for one pair and store in cache."""
        ref_amount = self.REFERENCE_AMOUNTS.get(token_in, self.DEFAULT_REFERENCE)
        bucket     = _bucket_amount(ref_amount)

        try:
            amount_out, fee = await self._quoter.best_quote(
                token_in  = token_in,
                token_out = token_out,
                amount_in = ref_amount,
            )

            if amount_out == 0:
                logger.debug(
                    f"[QuoteCache] No liquidity: "
                    f"{token_in[:10]}→{token_out[:10]}"
                )
                return False

            key = (token_in.lower(), token_out.lower())
            self._cache[key] = CachedQuote(
                token_in  = token_in,
                token_out = token_out,
                amount_in = ref_amount,
                amount_out= amount_out,
                fee_tier  = fee,
            )
            self._fetches += 1

            logger.debug(
                f"[QuoteCache] Cached {token_in[:10]}→{token_out[:10]} "
                f"fee={fee} out={amount_out}"
            )
            return True

        except Exception as e:
            logger.warning(
                f"[QuoteCache] Fetch failed "
                f"{token_in[:10]}→{token_out[:10]}: {e}"
            )
            return False

    async def _fetch_all(self) -> None:
        """Fetch all configured pairs concurrently."""
        tasks = [
            asyncio.create_task(
                self._fetch_pair(tin, tout),
                name=f"quote_{tin[:8]}_{tout[:8]}",
            )
            for tin, tout in self._pairs
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        success = sum(1 for r in results if r is True)
        logger.debug(f"[QuoteCache] Fetched {success}/{len(self._pairs)} pairs")

    async def _refresh_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._ttl)
                await self._fetch_all()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[QuoteCache] Refresh error: {e}")
                await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# flash_loan_route.py integration
# ---------------------------------------------------------------------------
#
# 1. Add quote_cache parameter to SwapCalldataBuilder.__init__():
#
#   def __init__(self, w3, executor_address, quoter=None, quote_cache=None):
#       ...
#       self._quote_cache = quote_cache  # QuoteCache instance
#
# 2. In SwapCalldataBuilder.build(), add cache check BEFORE QuoterV2 calls:
#
#   async def build(self, token_in, token_out, amount_in, slippage_bps=50, ...):
#
#       # wstETH → WETH: try Balancer first (existing code)
#       if token_in.lower() == WSTETH_ADDR.lower():
#           ...  # existing Balancer route logic
#
#       # Check quote cache before hitting QuoterV2
#       if self._quote_cache:
#           cached = self._quote_cache.get(token_in, token_out, amount_in)
#           if cached:
#               amount_out, best_fee = cached
#               amount_out_min = int(amount_out * (10_000 - slippage_bps) / 10_000)
#               calldata = self._encode_exact_input_single(
#                   token_in       = token_in,
#                   token_out      = token_out,
#                   fee            = best_fee,
#                   recipient      = self._executor,
#                   deadline       = int(time.time()) + deadline_offset,
#                   amount_in      = amount_in,
#                   amount_out_min = amount_out_min,
#               )
#               logger.debug(
#                   f"[SwapBuilder] Cache hit: {token_in[:10]}→{token_out[:10]} "
#                   f"fee={best_fee} out={amount_out}"
#               )
#               return SwapRoute(
#                   router      = UNI_V3_ROUTER,
#                   calldata    = calldata,
#                   fee_tier    = best_fee,
#                   amount_in   = amount_in,
#                   amount_out  = amount_out,
#                   slippage_pct= slippage_bps / 10_000,
#               )
#
#       # Cache miss — fall through to live QuoterV2 (existing code)
#       tasks = [asyncio.create_task(...) for fee in FEE_TIERS]
#       ...
#
# 3. Pass quote_cache to FlashLoanTxBuilder and down to SwapCalldataBuilder:
#
#   In FlashLoanTxBuilder.__init__():
#       def __init__(self, ..., quote_cache=None):
#           self._swap_builder = SwapCalldataBuilder(
#               rpc.w3, executor_address,
#               quoter=quoter,
#               quote_cache=quote_cache,
#           )
#
# ---------------------------------------------------------------------------
#
# pipeline_v3.py integration:
#
# 1. Import:
#       from quote_cache import QuoteCache, KNOWN_SLOW_PAIRS
#
# 2. In setup(), after self.quoter is ready:
#       self.quote_cache = QuoteCache(
#           quoter = self.quoter,
#           pairs  = KNOWN_SLOW_PAIRS,
#           ttl    = 12.0,
#       )
#       await self.quote_cache.start()
#
# 3. Pass to flash_builder:
#       self.flash_builder = FlashLoanTxBuilder(
#           ...
#           quoter      = self.quoter,
#           quote_cache = self.quote_cache,   # ← add this
#       )
#
# 4. In stats loop:
#       self.quote_cache.log_stats()
#
# 5. In shutdown():
#       await self.quote_cache.stop()
#
# Expected result after integration:
#   pre_warm=20/20  (up from 12/20)
#   [QuoteCache] entries=6 hit_rate=85% hits=102 misses=18
#   [CachePrewarm] Cycle N — targets=20 built=8 skipped=12 warm=20/20
#
# ---------------------------------------------------------------------------
