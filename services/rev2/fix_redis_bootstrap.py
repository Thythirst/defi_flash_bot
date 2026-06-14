"""
fix_redis_bootstrap.py — Periodic Redis watchlist re-bootstrap
Fixes W3: 2,884 addresses loaded once at startup, never refreshed.
          New Aave borrowers are never added.
          Exited borrowers (fully repaid) waste refresh cycles.

The problem:
    PositionLoader.bootstrap() runs once. Redis ZSET is the source of truth
    for which addresses to watch. If new borrowers open positions after startup
    they're invisible to the bot. If borrowers fully repay, the bot still
    polls them every refresh cycle — wasted RPC budget.

This module:
    WatchlistManager wraps PositionLoader and Redis.
    - Re-bootstraps from Redis every bootstrap_interval seconds
    - Diffs old vs new: logs adds/removes
    - Re-bootstraps at off-peak times (configurable)
    - Integrates with the existing block loop (no new thread)

Bootstrap frequency recommendation:
    Every 4-6h is sufficient for normal conditions.
    During high-volatility periods (large price moves), new borrowers
    open faster. If you want finer coverage, trigger re-bootstrap when
    any asset price moves >5% in a single update.

Usage:
    mgr = WatchlistManager(
        loader       = self.loader,
        redis_client = redis_client,
        redis_key    = "watchlist",             # ZSET key
        bootstrap_interval = 4 * 3600,          # 4 hours
    )
    await mgr.start()

    # In block handler:
    await mgr.on_block(block_number)

    # Optional: trigger on large price move
    if price_move_pct > 0.05:
        await mgr.force_bootstrap("large price move")
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# WatchlistManager
# ---------------------------------------------------------------------------

@dataclass
class BootstrapStats:
    timestamp: float
    total_loaded: int
    added: int
    removed: int
    duration_seconds: float
    trigger: str           # "startup" | "scheduled" | "forced" | "price_spike"


class WatchlistManager:
    """
    Manages periodic re-bootstrap of the Aave watchlist from Redis.

    Wraps PositionLoader.bootstrap() with:
    - Scheduled re-runs every bootstrap_interval seconds
    - Diff tracking (new borrowers added, exited borrowers removed)
    - Price-spike trigger (optional) for volatile periods
    - Non-blocking: bootstrap runs as a background task, pipeline continues
    - Off-peak scheduling: avoids bootstrapping during London/NY open
    """

    def __init__(
        self,
        loader,                         # PositionLoader from position_loader.py
        redis_client,                   # redis.asyncio.Redis instance
        redis_key: str = "watchlist",   # ZSET or SET key containing addresses
        bootstrap_interval: float = 4 * 3600,   # 4 hours default
        off_peak_only: bool = False,    # if True, skip during peak trading hours
        peak_hours_utc: tuple = (7, 16),# UTC hours to avoid (London open → NY close)
    ):
        self._loader    = loader
        self._redis     = redis_client
        self._key       = redis_key
        self._interval  = bootstrap_interval
        self._off_peak  = off_peak_only
        self._peak_start, self._peak_end = peak_hours_utc

        self._last_bootstrap: float = 0.0
        self._known_addresses: set[str] = set()
        self._bootstrap_history: list[BootstrapStats] = []
        self._bootstrapping = False
        self._task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        """
        Performs initial bootstrap and starts the background refresh loop.
        Call after PositionLoader.bootstrap() has already run once.
        """
        # Snapshot the current watchlist so we can diff on next bootstrap
        self._known_addresses = await self._fetch_watchlist()
        self._last_bootstrap  = time.time()
        self._running = True
        self._task = asyncio.create_task(self._schedule_loop(), name="watchlist_manager")
        logger.info(
            f"[WatchlistManager] Started — {len(self._known_addresses)} addresses, "
            f"re-bootstrap every {self._interval/3600:.1f}h"
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()

    async def on_block(self, block_number: int) -> None:
        """
        Hook into existing block handler.
        Triggers bootstrap if interval elapsed and not already running.
        Lightweight — just checks a timestamp, no RPC.
        """
        if self._bootstrapping:
            return
        elapsed = time.time() - self._last_bootstrap
        if elapsed >= self._interval:
            if self._off_peak and self._is_peak_hours():
                logger.debug("[WatchlistManager] Deferring bootstrap — peak trading hours")
                return
            asyncio.create_task(
                self._run_bootstrap(trigger="scheduled"),
                name="watchlist_bootstrap",
            )

    async def force_bootstrap(self, reason: str = "manual") -> None:
        """
        Immediately trigger a re-bootstrap regardless of interval.
        Call when a large price move suggests new positions may have opened.
        """
        if self._bootstrapping:
            logger.debug("[WatchlistManager] Bootstrap already in progress — skip force")
            return
        logger.info(f"[WatchlistManager] Force bootstrap triggered: {reason}")
        await self._run_bootstrap(trigger=reason)

    @property
    def last_bootstrap_age(self) -> float:
        """Seconds since last successful bootstrap."""
        return time.time() - self._last_bootstrap

    @property
    def stats(self) -> list[BootstrapStats]:
        return self._bootstrap_history[-10:]  # last 10 runs

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _fetch_watchlist(self) -> set[str]:
        """
        Fetch current watchlist from Redis.
        Supports both ZSET (zrange) and SET (smembers).
        Returns set of lowercase address strings.
        """
        try:
            # Try ZSET first (scored set — common pattern for HF-ranked watchlists)
            members = await self._redis.zrange(self._key, 0, -1)
            if not members:
                # Fallback to plain SET
                members = await self._redis.smembers(self._key)

            decoded = set()
            for m in members:
                addr = m.decode() if isinstance(m, bytes) else m
                decoded.add(addr.lower())
            return decoded

        except Exception as e:
            logger.error(f"[WatchlistManager] Redis fetch failed: {e}")
            return set()

    async def _run_bootstrap(self, trigger: str) -> None:
        """Run bootstrap, compute diff, update PositionLoader."""
        if self._bootstrapping:
            return

        self._bootstrapping = True
        t0 = time.time()

        try:
            new_addresses = await self._fetch_watchlist()
            if not new_addresses:
                logger.warning("[WatchlistManager] Redis returned empty watchlist — aborting bootstrap")
                return

            # Diff
            added   = new_addresses - self._known_addresses
            removed = self._known_addresses - new_addresses

            if added:
                logger.info(
                    f"[WatchlistManager] {len(added)} new borrowers since last bootstrap"
                )
            if removed:
                logger.info(
                    f"[WatchlistManager] {len(removed)} borrowers exited since last bootstrap"
                )

            # Full re-bootstrap with new list
            loaded = await self._loader.bootstrap(list(new_addresses))

            duration = time.time() - t0
            stats = BootstrapStats(
                timestamp=time.time(),
                total_loaded=loaded,
                added=len(added),
                removed=len(removed),
                duration_seconds=duration,
                trigger=trigger,
            )
            self._bootstrap_history.append(stats)

            self._known_addresses = new_addresses
            self._last_bootstrap  = time.time()

            logger.info(
                f"[WatchlistManager] Bootstrap complete — "
                f"{loaded} positions, "
                f"+{len(added)} -{len(removed)} vs previous, "
                f"{duration:.1f}s elapsed, "
                f"trigger={trigger}"
            )

        except Exception as e:
            logger.error(f"[WatchlistManager] Bootstrap failed: {e}")
        finally:
            self._bootstrapping = False

    async def _schedule_loop(self) -> None:
        """Background loop — checks every 60s if bootstrap is due."""
        while self._running:
            await asyncio.sleep(60)
            elapsed = time.time() - self._last_bootstrap
            if elapsed >= self._interval and not self._bootstrapping:
                if self._off_peak and self._is_peak_hours():
                    continue
                await self._run_bootstrap(trigger="scheduled")

    def _is_peak_hours(self) -> bool:
        """True if current UTC hour is within peak trading window."""
        import datetime
        utc_hour = datetime.datetime.utcnow().hour
        if self._peak_start < self._peak_end:
            return self._peak_start <= utc_hour < self._peak_end
        else:  # wraps midnight
            return utc_hour >= self._peak_start or utc_hour < self._peak_end


# ---------------------------------------------------------------------------
# Price-spike trigger integration
# ---------------------------------------------------------------------------
#
# In PriceRegistry.update_price() or WSManager._handle_oracle_message(),
# detect large single-update moves and trigger re-bootstrap:
#
#     # In pipeline.py — track last prices for spike detection
#     _last_prices: dict[str, int] = {}
#
#     async def on_price_update(self, asset: str, new_price: int):
#         old = self._last_prices.get(asset, new_price)
#         if old > 0:
#             move = abs(new_price - old) / old
#             if move > 0.05:  # 5% single-update move
#                 await self.watchlist_mgr.force_bootstrap(
#                     f"price spike {asset[:8]} {move:.1%}"
#                 )
#         self._last_prices[asset] = new_price
#         self.prices.update_price(asset, new_price)
#
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# pipeline.py integration
# ---------------------------------------------------------------------------
#
# 1. Import:
#       from fix_redis_bootstrap import WatchlistManager
#
# 2. In setup(), AFTER initial PositionLoader.bootstrap():
#       self.watchlist_mgr = WatchlistManager(
#           loader            = self.loader,
#           redis_client      = redis_client,
#           redis_key         = "watchlist",
#           bootstrap_interval= 4 * 3600,   # 4 hours
#       )
#       await self.watchlist_mgr.start()
#
# 3. In block handler:
#       async def on_new_block(self, block_number: int):
#           await self.watchlist_mgr.on_block(block_number)
#           if block_number % 5 == 0:
#               await self.loader.refresh_hot(hf_threshold=1.2)
#
# 4. In shutdown():
#       await self.watchlist_mgr.stop()
#
# 5. Log bootstrap stats periodically:
#       for s in self.watchlist_mgr.stats:
#           logger.info(
#               f"[Bootstrap] {s.trigger} — "
#               f"{s.total_loaded} loaded, "
#               f"+{s.added}/-{s.removed}, "
#               f"{s.duration_seconds:.1f}s"
#           )
#
# ---------------------------------------------------------------------------
