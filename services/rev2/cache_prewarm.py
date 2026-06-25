"""
cache_prewarm.py — Pre-warm presigned tx cache for near-liquidatable positions
Fixes: cold path penalty (+50ms) on first opportunity after idle period.

Problem:
    The presigner only builds and caches txs for positions already flagged
    as liquidatable (HF < 1.0). When the market is quiet, the cache is empty.
    The first real opportunity hits the cold path:
        build tx → sign → submit = +50ms over a cache hit.
    In a competitive race, 50ms is the margin between winning and losing.

Fix:
    Pre-warm the cache for the top-N positions by lowest HF, regardless of
    whether they're currently liquidatable. When any of them crosses HF=1.0,
    the presigned tx is already in RAM — hot path from the first millisecond.

    The pre-warm refreshes every REFRESH_INTERVAL seconds (default: 25s,
    slightly under the 30s presigner refresh cycle) so the cache never goes
    cold between cycles.

Architecture:
    CachePrewarmer runs as a background task.
    - Reads top-N positions from PositionLoader sorted by HF ascending
    - For each position: calls presigner._build_and_cache(borrower) if not
      already cached or cache is stale
    - Tracks cache hit rate and warm ratio for diagnostics
    - Yields between builds to avoid starving the event loop

Usage:
    prewarm = CachePrewarmer(
        loader      = self.loader,        # PositionLoader
        presigner   = self.presigner,     # Presigner
        shared_state= self.shared_state,  # SharedState
        top_n       = 20,
        refresh_interval = 25.0,
    )
    await prewarm.start()
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional, Callable, Coroutine

logger = logging.getLogger(__name__)

WAD = 10 ** 18


# ---------------------------------------------------------------------------
# CacheEntry — tracks per-borrower cache state
# ---------------------------------------------------------------------------

@dataclass
class CacheEntry:
    borrower: str
    hf: float
    built_at: float
    base_fee_at_build: int
    is_warm: bool = True

    @property
    def age_seconds(self) -> float:
        return time.time() - self.built_at

    def is_stale(self, max_age: float = 30.0, current_base_fee: int = 0,
                 gas_drift_factor: float = 1.20) -> bool:
        """
        Mirror PresignedTxGuard.is_stale() logic for consistency.
        max_age default is 30s — 5s headroom over the 25s refresh_interval
        so entries don't expire right as the next cycle starts.
        """
        if self.age_seconds > max_age:
            return True
        # Skip gas drift check if either value is zero —
        # base_fee_at_build=0 means SharedState hadn't populated yet
        # when this entry was built (Cycle 1 race). Don't false-expire.
        if self.base_fee_at_build > 0 and current_base_fee > 0:
            drift = current_base_fee / self.base_fee_at_build
            if drift > gas_drift_factor:
                return True
        return False


# ---------------------------------------------------------------------------
# CachePrewarmer
# ---------------------------------------------------------------------------

class CachePrewarmer:
    """
    Background task that pre-builds presigned txs for the top-N
    lowest-HF borrowers, keeping them warm across refresh cycles.

    The key insight: you don't need HF < 1.0 to pre-sign a liquidation tx.
    A tx for a borrower at HF=1.04 is structurally identical to one at HF=0.98
    — same collateral asset, same debt asset, same executor call. The only
    thing that changes when HF crosses 1.0 is that the tx becomes profitable
    to submit. Having it pre-built means zero build latency at that moment.

    Staleness handling:
        Pre-warmed txs expire on the same schedule as live txs (25s age or
        20% gas drift). The refresh loop rebuilds them before expiry so there's
        always a warm tx ready. The window between expiry and rebuild is <1s
        since the refresh runs continuously.
    """

    def __init__(
        self,
        loader,                     # PositionLoader
        build_fn: Callable,         # async fn(borrower: str) → bool
                                    # presigner._build_and_cache equivalent
        shared_state,               # SharedState from hot_path_fix.py
        top_n: int = 20,
        refresh_interval: float = 25.0,   # seconds between full refresh cycles
        hf_ceiling: float = 1.15,         # only pre-warm positions below this HF
        max_build_time_ms: float = 1500.0, # was 350 — Aave V3 flash loan quotes routinely >350ms
        max_cache_size: int = 40,         # hard ceiling — evict only above this
    ):
        self._loader        = loader
        self._build_fn      = build_fn
        self._state         = shared_state
        self._top_n         = top_n
        self._interval      = refresh_interval
        self._hf_ceil       = hf_ceiling
        self._max_build_ms  = max_build_time_ms
        self._max_cache_size= max_cache_size

        self._cache: dict[str, CacheEntry] = {}
        self._running = False
        self._task: Optional[asyncio.Task] = None

        # Dust cache: skip rebuild for positions that failed or produced
        # sub-threshold profit recently — the top-N is dominated by dead
        # borrowers whose positions were closed but HF engine hasn't pruned yet.
        self._dust_cache: dict[str, tuple[float, float]] = {}
        #                                   (hf_when_checked, timestamp)

        # Profit cache: skip rebuild when last build produced <$0.01 profit
        # and HF hasn't moved — catches ghost positions that "build" but at $0.
        self._profit_cache: dict[str, tuple[float, float]] = {}
        #                                    (profit, timestamp)

        # Stats
        self._builds_total    = 0
        self._builds_skipped  = 0
        self._builds_failed   = 0
        self._cycles          = 0
        self._last_cycle_ms   = 0.0

    async def start(self) -> None:
        """Start background pre-warm loop. Non-blocking."""
        self._running = True
        # Run first cycle immediately so cache is warm before first opportunity
        await self._run_cycle()
        self._task = asyncio.create_task(self._loop(), name="cache_prewarm")
        logger.info(
            f"[CachePrewarm] Started — top_n={self._top_n}, "
            f"hf_ceiling={self._hf_ceil}, "
            f"refresh={self._interval}s"
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()

    @property
    def warm_count(self) -> int:
        """Number of currently warm (non-stale) cached txs."""
        base_fee = self._state.base_fee_wei if self._state else 0
        return sum(
            1 for e in self._cache.values()
            if not e.is_stale(current_base_fee=base_fee)
        )

    @property
    def targets(self) -> list[str]:
        """Current set of pre-warm target addresses."""
        return list(self._cache.keys())

    def is_warm(self, borrower: str) -> bool:
        """Check if a specific borrower has a warm cached tx."""
        entry = self._cache.get(borrower.lower())
        if not entry:
            return False
        base_fee = self._state.base_fee_wei if self._state else 0
        return not entry.is_stale(current_base_fee=base_fee)

    def log_status(self) -> None:
        base_fee = self._state.base_fee_wei if self._state else 0
        warm  = self.warm_count
        total = len(self._cache)
        logger.info(
            f"[CachePrewarm] warm={warm}/{total} "
            f"builds={self._builds_total} "
            f"failed={self._builds_failed} "
            f"cycles={self._cycles} "
            f"last_cycle={self._last_cycle_ms:.0f}ms"
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get_top_n_targets(self, n: Optional[int] = None) -> list[tuple[str, float]]:
        """
        Get top-N borrowers by lowest HF from PositionLoader.
        Returns list of (address, hf_float) sorted ascending by HF.
        Filters to positions below hf_ceiling.
        n defaults to self._top_n if not specified.
        """
        limit = n if n is not None else self._top_n
        all_positions = [
            (addr, pos.hf_float)
            for addr, pos in self._loader._positions.items()
            if pos.hf_float < self._hf_ceil
            and pos.total_debt_base > 0
        ]

        # Sort ascending — lowest HF first (most urgent)
        all_positions.sort(key=lambda x: x[1])
        return all_positions[:limit]

    async def _run_cycle(self) -> None:
        """
        One full pre-warm cycle:
        1. Get top-N targets from PositionLoader
        2. For each: build tx if not warm
        3. Evict targets no longer in top-N
        """
        t0      = time.perf_counter()
        targets = self._get_top_n_targets()  # top_n, default

        if not targets:
            logger.debug("[CachePrewarm] No targets below HF ceiling — idle")
            self._cycles += 1
            return

        target_addrs = {addr.lower() for addr, _ in targets}
        base_fee     = self._state.base_fee_wei if self._state else 0

        builds_this_cycle = 0
        skips_this_cycle  = 0
        dust_skips = 0

        for addr, hf in targets:
            addr_lower = addr.lower()
            existing   = self._cache.get(addr_lower)

            # Skip if already warm
            if existing and not existing.is_stale(current_base_fee=base_fee):
                skips_this_cycle += 1
                continue

            # Skip if in dust cache — position was dead/dust <5 min ago
            if addr_lower in self._dust_cache:
                cached_hf, cached_ts = self._dust_cache[addr_lower]
                hf_unchanged = abs(hf - cached_hf) < 0.001
                if hf_unchanged and (time.time() - cached_ts) < 300:
                    dust_skips += 1
                    continue  # still dust, skip rebuild

            # Skip if in profit cache — last build was $0 profit, HF unchanged
            if addr_lower in self._profit_cache:
                cached_profit, cached_ts = self._profit_cache[addr_lower]
                if cached_profit < 0.01 and (time.time() - cached_ts) < 300:
                    dust_skips += 1
                    continue  # sub-cent profit, skip rebuild

            # Build presigned tx
            build_ok, profit = await self._build_one(addr, hf, base_fee)
            if build_ok:
                builds_this_cycle += 1
                self._dust_cache.pop(addr_lower, None)  # clear dust flag
                self._profit_cache[addr_lower] = (profit, time.time())
            else:
                self._builds_failed += 1
                # Only dust-cache genuine ghost positions (never built profitably).
                # Previously-profitable positions may fail transiently (slippage spike,
                # QuoterV2 hiccup) — let them retry next cycle instead of blocking for 5 min.
                prev_profit = self._profit_cache.get(addr_lower, (0.0, 0.0))[0]
                if prev_profit < 0.01:
                    self._dust_cache[addr_lower] = (hf, time.time())

            # Yield between builds — don't starve oracle processing
            await asyncio.sleep(0)

        # ── Soft eviction ────────────────────────────────────────────────
        # top_n is a BUILD CAP, not an eviction boundary.
        # Keep warm entries even if they've drifted outside the current
        # top-20 — the HF ranking shifts every cycle as prices update and
        # the 19th-25th positions constantly swap. Hard-evicting on every
        # cycle causes churn: delete → rebuild → delete → rebuild.
        #
        # Eviction rules (in priority order):
        #   1. Always evict: HF has recovered above hf_ceiling (no longer at risk)
        #   2. Always evict: entry is stale AND address is outside top-N*2 buffer
        #   3. Trim to max_cache_size if cache exceeds hard ceiling (evict by HF desc)

        target_addrs = {addr.lower() for addr, _ in targets}

        # Rule 1 — evict positions that have recovered above ceiling
        recovered = [
            addr for addr, entry in self._cache.items()
            if entry.hf >= self._hf_ceil
        ]
        for addr in recovered:
            del self._cache[addr]
            logger.debug(f"[CachePrewarm] Evicted {addr[:10]}… HF recovered above ceiling")

        # Rule 2 — evict stale entries outside top_n * 2 buffer
        buffer_addrs = set()
        all_positions = self._get_top_n_targets(n=self._top_n * 2)
        buffer_addrs  = {a.lower() for a, _ in all_positions}

        stale_outside = [
            addr for addr, entry in self._cache.items()
            if addr not in buffer_addrs
            and entry.is_stale(current_base_fee=base_fee)
        ]
        for addr in stale_outside:
            del self._cache[addr]
            logger.debug(f"[CachePrewarm] Evicted stale+out-of-buffer {addr[:10]}…")

        # Rule 3 — trim hard ceiling (evict highest HF first — least urgent)
        if len(self._cache) > self._max_cache_size:
            by_hf = sorted(self._cache.items(), key=lambda x: x[1].hf, reverse=True)
            excess = len(self._cache) - self._max_cache_size
            for addr, _ in by_hf[:excess]:
                del self._cache[addr]
            logger.debug(f"[CachePrewarm] Trimmed {excess} entries to max_cache_size={self._max_cache_size}")

        evicted = 0  # kept for log compat — total removed this cycle
        evicted = len(recovered) + len(stale_outside)

        self._cycles         += 1
        self._last_cycle_ms   = (time.perf_counter() - t0) * 1000

        if builds_this_cycle > 0 or evicted > 0 or dust_skips > 0:
            logger.info(
                f"[CachePrewarm] Cycle {self._cycles} — "
                f"targets={len(targets)} "
                f"built={builds_this_cycle} "
                f"skipped={skips_this_cycle} "
                f"dust={dust_skips} "
                f"evicted={evicted} "
                f"warm={self.warm_count}/{len(targets)} "
                f"cycle_time={self._last_cycle_ms:.0f}ms"
            )
        else:
            logger.debug(
                f"[CachePrewarm] Cycle {self._cycles} — "
                f"all {len(targets)} warm, nothing to do "
                f"({self._last_cycle_ms:.0f}ms)"
            )

    async def _build_one(self, addr: str, hf: float, base_fee: int) -> tuple[bool, float]:
        """
        Build and cache a presigned tx for one borrower.
        Returns (success, estimated_profit_usd). Profit is 0.0 on failure.
        """
        t0 = time.perf_counter()
        try:
            result = await asyncio.wait_for(
                self._build_fn(addr),
                timeout=self._max_build_ms / 1000,
            )
            ms = (time.perf_counter() - t0) * 1000

            # Unpack (success, profit) tuple
            if isinstance(result, tuple):
                success, profit = result
            else:
                # Backward compat: old bool-only return
                success, profit = result, 0.0

            if success:
                self._cache[addr.lower()] = CacheEntry(
                    borrower          = addr.lower(),
                    hf                = hf,
                    built_at          = time.time(),
                    base_fee_at_build = base_fee,
                )
                self._builds_total += 1
                logger.debug(
                    f"[CachePrewarm] Built tx for {addr[:10]}… "
                    f"HF={hf:.4f} profit=\${profit:.2f} in {ms:.0f}ms"
                )
                return (True, profit)
            else:
                logger.debug(f"[CachePrewarm] build_fn returned False for {addr[:10]}…")
                return (False, 0.0)

        except asyncio.TimeoutError:
            ms = (time.perf_counter() - t0) * 1000
            logger.warning(
                f"[CachePrewarm] Build timeout for {addr[:10]}… "
                f"after {ms:.0f}ms — skipping"
            )
            self._builds_skipped += 1
            return (False, 0.0)

        except Exception as e:
            logger.warning(f"[CachePrewarm] Build error for {addr[:10]}…: {e}")
            return (False, 0.0)

    async def _loop(self) -> None:
        """Background refresh loop."""
        while self._running:
            try:
                await asyncio.sleep(self._interval)
                await self._run_cycle()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[CachePrewarm] Loop error: {e}")
                await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# HF change detector — triggers immediate rebuild on fast HF moves
# ---------------------------------------------------------------------------

class HFChangeDetector:
    """
    Detects rapid HF deterioration and triggers immediate cache rebuild.

    When oracle prices update, HF can drop quickly. If a position moves
    from HF=1.05 to HF=0.99 between pre-warm cycles, the cached tx
    might be built with stale debt_to_cover values.

    This detector hooks into the oracle price update path and triggers
    an immediate rebuild for any position whose HF drops by more than
    hf_drop_threshold in a single oracle update.

    Usage:
        detector = HFChangeDetector(prewarm, hf_drop_threshold=0.05)

        # In pipeline oracle callback, after HF recompute:
        await detector.on_hf_update(borrower, old_hf, new_hf)
    """

    def __init__(
        self,
        prewarm: CachePrewarmer,
        hf_drop_threshold: float = 0.05,  # trigger rebuild if HF drops >5%
    ):
        self._prewarm   = prewarm
        self._threshold = hf_drop_threshold
        self._triggers  = 0

    async def on_hf_update(
        self,
        borrower: str,
        old_hf: float,
        new_hf: float,
    ) -> None:
        """
        Call after every HF recompute.
        Triggers immediate cache rebuild if HF dropped significantly.
        """
        if old_hf <= 0:
            return

        drop = old_hf - new_hf
        if drop >= self._threshold:
            self._triggers += 1
            logger.info(
                f"[HFDetector] Fast HF drop on {borrower[:10]}…: "
                f"{old_hf:.4f} → {new_hf:.4f} (Δ{drop:.4f}) — "
                f"triggering immediate cache rebuild"
            )
            # Force rebuild regardless of current cache state
            base_fee = self._prewarm._state.base_fee_wei
            await self._prewarm._build_one(borrower, new_hf, base_fee)


# ---------------------------------------------------------------------------
# pipeline.py integration
# ---------------------------------------------------------------------------
#
# 1. Import:
#       from cache_prewarm import CachePrewarmer, HFChangeDetector
#
# 2. Define the build function that wraps your presigner:
#
#       async def _prewarm_build(borrower: str) -> bool:
#           """Build and cache presigned tx. Returns True on success."""
#           try:
#               # This calls your existing presigner build logic
#               # without submitting — just builds and caches the tx
#               await self.presigner.build_and_cache(borrower)
#               return True
#           except Exception as e:
#               logger.debug(f"Prewarm build failed for {borrower[:10]}: {e}")
#               return False
#
# 3. In setup(), after PositionLoader and SharedState are ready:
#
#       self.prewarm = CachePrewarmer(
#           loader           = self.loader,
#           build_fn         = self._prewarm_build,
#           shared_state     = self.shared_state,
#           top_n            = 20,
#           refresh_interval = 25.0,
#           hf_ceiling       = 1.15,
#       )
#       await self.prewarm.start()
#
#       self.hf_detector = HFChangeDetector(
#           prewarm           = self.prewarm,
#           hf_drop_threshold = 0.05,
#       )
#
# 4. In HF engine callback, after recomputing HF:
#
#       old_hf = self._last_hf.get(borrower, 2.0)
#       new_hf = pos.hf_float
#       self._last_hf[borrower] = new_hf
#
#       await self.hf_detector.on_hf_update(borrower, old_hf, new_hf)
#
#       if new_hf < 1.0:
#           await self.attempt_liquidation(borrower)  # cache already warm
#
# 5. In attempt_liquidation(), log cache hit vs miss:
#
#       cache_hit = self.prewarm.is_warm(borrower)
#       logger.info(
#           f"[Liquidation] {borrower[:10]}… "
#           f"HF={pos.hf_float:.4f} "
#           f"cache={'HIT' if cache_hit else 'MISS'}"
#       )
#       token = self.latency_tracker.start(borrower)
#       # ... rest of submission path
#
# 6. In stats/wallet loop:
#       self.prewarm.log_status()
#       # Expected output when market is quiet:
#       # [CachePrewarm] warm=20/20 builds=20 failed=0 cycles=47
#       # Expected output on first opportunity:
#       # [Liquidation] 0xborrow… HF=0.9987 cache=HIT
#
# 7. In shutdown():
#       await self.prewarm.stop()
#
# ---------------------------------------------------------------------------
#
# Presigner change — expose build_and_cache as a public method:
#
#   In presigner.py, add:
#       async def build_and_cache(self, borrower: str) -> None:
#           """
#           Build presigned tx and store in cache without submitting.
#           Called by CachePrewarmer for pre-warming.
#           """
#           presigned = await self._build_tx(borrower)
#           if presigned:
#               self._cache[borrower] = presigned
#               logger.debug(f"[Presigner] Pre-warmed {borrower[:10]}…")
#
# ---------------------------------------------------------------------------
