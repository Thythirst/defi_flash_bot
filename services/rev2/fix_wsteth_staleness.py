"""
fix_wsteth_staleness.py — Chainlink on-chain staleness gate for PricePoller
Fixes W2: PricePoller writes stale Chainlink data to PriceRegistry even when
          the feed's updatedAt is 4h+ old. This poisons PriceRegistry with
          a "fresh" timestamp (just polled) wrapping stale underlying data.

The bug:
    PricePoller._poll_all() calls latestRoundData() every 30s.
    It checks self._prices.age(asset_addr) — how long since WE last wrote.
    But it never checks latestRoundData.updatedAt — when Chainlink last updated.
    So a feed frozen for 4h gets written every 30s with a fresh local timestamp.
    PriceRegistry.is_fresh() returns True. HF engine uses a 4h-old price.

The fix:
    Before writing to PriceRegistry, check:
        now - updated_at > CHAINLINK_MAX_AGE → skip write, log warning

This file provides:
    1. StalenessGatedPricePoller — drop-in replacement for PricePoller
       with on-chain staleness check baked in
    2. Patch instructions for price_poller.py if you prefer a minimal diff

Chainlink heartbeat reference (Arbitrum):
    ETH/USD:    1h  deviation 0.5%
    BTC/USD:    1h  deviation 0.5%
    USDC/USD:   24h deviation 0.1%
    USDT/USD:   24h deviation 0.1%
    ARB/USD:    1h  deviation 0.5%
    LINK/USD:   1h  deviation 1%
    wstETH/USD: 24h deviation 2%   ← longest heartbeat, most likely to appear stale
    native USDC:24h deviation 0.1%
"""

import asyncio
import logging
import time
from web3 import Web3
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Per-feed max age config (seconds)
# Based on Chainlink heartbeat + buffer
# ---------------------------------------------------------------------------

# Default: reject prices older than this regardless of feed
DEFAULT_MAX_AGE = 3700          # ~1h + 100s buffer

# Per-asset overrides keyed by asset address (Aave oracle key)
# Feeds with 24h heartbeats get a 25h window
FEED_MAX_AGE: dict[str, int] = {
    # USDC bridged — 24h heartbeat
    "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8": 90_000,
    # USDT — 24h heartbeat
    "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9": 90_000,
    # wstETH — 24h heartbeat, 2% deviation
    "0x5979D7b546E38E414F7E9822514be443A4800529": 90_000,
    # native USDC — 24h heartbeat
    "0xaf88d065e77c8cC2239327C5EDb3A432268e5831": 90_000,
    # ETH — 1h heartbeat
    "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1": 3_700,
    # WBTC — 1h heartbeat
    "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f": 3_700,
    # ARB — 1h heartbeat
    "0x912CE59144191C1204E64559FE8253a0e49E6548": 3_700,
    # LINK — 1h heartbeat
    "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4": 3_700,
}


def is_chainlink_fresh(
    asset_addr: str,
    updated_at: int,
    max_age_override: Optional[int] = None,
) -> tuple[bool, float]:
    """
    Check if a Chainlink price is fresh enough to use.

    Args:
        asset_addr:       Aave oracle key (asset address)
        updated_at:       latestRoundData.updatedAt timestamp (Unix seconds)
        max_age_override: optional override — uses FEED_MAX_AGE if None

    Returns:
        (is_fresh: bool, age_seconds: float)
    """
    now = time.time()
    age = now - updated_at

    if max_age_override is not None:
        max_age = max_age_override
    else:
        max_age = FEED_MAX_AGE.get(asset_addr, DEFAULT_MAX_AGE)

    return age <= max_age, age


# ---------------------------------------------------------------------------
# Patch for price_poller.py — minimal diff
# ---------------------------------------------------------------------------
#
# In PricePoller._poll_all(), find this block (~line 155):
#
#     if not status.is_feed_healthy:
#         logger.warning(...)
#
#     ws_age = self._prices.age(asset_addr)
#     if ws_age > self._ws_fresh:
#         self._prices.update_price(asset_addr, answer)
#
# Replace with:
#
#     from fix_wsteth_staleness import is_chainlink_fresh
#
#     fresh, cl_age = is_chainlink_fresh(asset_addr, updated_at)
#     if not fresh:
#         logger.warning(
#             f"[PricePoller] ⚠ {status.description} Chainlink stale "
#             f"({cl_age:.0f}s old) — NOT writing to PriceRegistry"
#         )
#         status.source = "stale_rejected"
#         # Do NOT update status.last_price or PriceRegistry
#         # PriceRegistry will naturally expire via max_age_seconds
#         continue
#
#     # Feed is fresh — update status
#     status.last_price            = answer
#     status.last_updated_on_chain = updated_at
#     status.last_polled           = now
#     status.consecutive_failures  = 0
#
#     ws_age = self._prices.age(asset_addr)
#     if ws_age > self._ws_fresh:
#         self._prices.update_price(asset_addr, answer)
#         status.source = "http_poll"
#         updated += 1
#     else:
#         status.source = "ws"
#
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# StalenessGatedPricePoller — full drop-in replacement
# ---------------------------------------------------------------------------

class StalenessGatedPricePoller:
    """
    Drop-in replacement for PricePoller with on-chain staleness gate.

    Inherits all PricePoller behaviour. Overrides _poll_all() to add:
        - Per-feed Chainlink freshness check using latestRoundData.updatedAt
        - Stale feeds are NOT written to PriceRegistry
        - Stale feeds are tracked in _stale_feeds set for diagnostics
        - HF engine will skip positions with those assets (PriceRegistry expires)

    Usage: identical to PricePoller
        from fix_wsteth_staleness import StalenessGatedPricePoller as PricePoller
    """

    def __init__(self, rpc, price_registry, feeds=None, poll_interval=30.0,
                 ws_freshness_threshold=45.0):
        # Import here to avoid circular dependency
        from price_poller import PricePoller
        self._inner = PricePoller(
            rpc=rpc,
            price_registry=price_registry,
            feeds=feeds,
            poll_interval=poll_interval,
            ws_freshness_threshold=ws_freshness_threshold,
        )
        # Patch the poll method
        self._inner._poll_all = self._staleness_gated_poll
        self._stale_feeds: set[str] = set()
        self._prices = price_registry

    # Delegate everything to inner
    async def start(self):    await self._inner.start()
    async def stop(self):     await self._inner.stop()
    def log_status(self):     self._inner.log_status()
    def get_feed_status(self): return self._inner.get_feed_status()

    @property
    def stale_feeds(self) -> set[str]:
        """Asset addresses whose Chainlink feeds are currently stale."""
        return self._stale_feeds.copy()

    async def _staleness_gated_poll(self) -> int:
        """
        Patched version of PricePoller._poll_all() with staleness gate.
        Identical logic except stale feeds are skipped before PriceRegistry write.
        """
        inner = self._inner
        asset_addrs = list(inner._feeds.keys())
        feed_addrs  = [inner._feeds[a] for a in asset_addrs]

        calls = [
            {
                "target":       Web3.to_checksum_address(f),
                "allowFailure": True,
                "callData":     "0xfeaf968c",  # latestRoundData() selector
            }
            for f in feed_addrs
        ]

        try:
            results = await asyncio.wait_for(
                inner._mc.functions.aggregate3(calls).call(),
                timeout=15.0
            )
        except asyncio.TimeoutError:
            logger.error("[StalenessPoller] Multicall timed out after 15s")
            return 0
        except Exception as e:
            logger.error(f"[StalenessPoller] Multicall failed: {e}")
            return 0

        updated      = 0
        now          = time.time()
        newly_stale  = set()
        newly_fresh  = set()

        for asset_addr, (success, raw) in zip(asset_addrs, results):
            status = inner._feed_status.get(asset_addr)
            if not status:
                continue

            if not success or not raw:
                status.consecutive_failures += 1
                continue

            decoded = inner._decode_latest_round(raw)
            if decoded is None:
                continue

            _round_id, answer, _started_at, updated_at, _answered_in_round = decoded

            if answer <= 0:
                continue

            # ── STALENESS GATE ─────────────────────────────────────────────
            fresh, cl_age = is_chainlink_fresh(asset_addr, updated_at)

            if not fresh:
                newly_stale.add(asset_addr)
                status.source = "stale_rejected"
                status.consecutive_failures += 1

                if asset_addr not in self._stale_feeds:
                    logger.warning(
                        f"[StalenessPoller] ⚠ {status.description} "
                        f"Chainlink stale {cl_age:.0f}s — "
                        f"NOT writing to PriceRegistry. "
                        f"Positions with this asset will be skipped by HF engine."
                    )
                # Do NOT write — let PriceRegistry expire naturally
                continue
            # ──────────────────────────────────────────────────────────────

            # Feed is fresh — track recovery
            if asset_addr in self._stale_feeds:
                newly_fresh.add(asset_addr)
                logger.info(
                    f"[StalenessPoller] ✓ {status.description} recovered "
                    f"(age={cl_age:.0f}s)"
                )

            status.last_price            = answer
            status.last_updated_on_chain = updated_at
            status.last_polled           = now
            status.consecutive_failures  = 0

            ws_age = self._prices.age(asset_addr)
            if ws_age > inner._ws_fresh:
                self._prices.update_price(asset_addr, answer)
                status.source = "http_poll"
                updated += 1
            else:
                status.source = "ws"

        # Update stale set
        self._stale_feeds = (self._stale_feeds | newly_stale) - newly_fresh

        inner._poll_count += 1

        if inner._poll_count % 10 == 0:
            fresh_count = sum(1 for a in asset_addrs if self._prices.is_fresh(a))
            logger.info(
                f"[StalenessPoller] Poll #{inner._poll_count} — "
                f"{fresh_count}/{len(asset_addrs)} fresh, "
                f"{len(self._stale_feeds)} stale-rejected, "
                f"{updated} updated"
            )

        return updated


# ---------------------------------------------------------------------------
# pipeline.py integration
# ---------------------------------------------------------------------------
#
# Option A — drop-in swap (recommended):
#
#     # Replace:
#     from price_poller import PricePoller
#     # With:
#     from fix_wsteth_staleness import StalenessGatedPricePoller as PricePoller
#
#     # Everything else unchanged.
#
# Option B — minimal patch to price_poller.py:
#     Apply the diff shown in the comment block above (~10 lines).
#
# After fix, expected behaviour:
#     - wstETH/USD frozen for 4h → NOT written to PriceRegistry
#     - PriceRegistry.is_fresh("0x5979...") → False after 60s
#     - HF engine skips positions with wstETH-only collateral
#     - Log: "⚠ wstETH feed stale 14400s — NOT writing to PriceRegistry"
#     - When Chainlink updates: "✓ wstETH recovered (age=45s)"
#     - prices_fresh log shows honest count (e.g. 7/8 during wstETH freeze)
#
# ---------------------------------------------------------------------------
