"""
execution_guards.py — Runtime safeguards for the hot execution path
Fixes W6:  30s sleep cooldown replaced with confirmation-driven _in_flight clear
Fixes W8:  presigned tx staleness — gas + debt_to_cover stale after 30s
Fixes W9:  price staleness — 5-min-old price treated same as fresh

These are drop-in components. Wire them into pipeline.py and presigner.py
as described in the integration guide at the bottom.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Coroutine, Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Fix W6 — ConfirmationTracker (replaces asyncio.sleep(30) cooldown)
# ---------------------------------------------------------------------------

@dataclass
class InFlightEntry:
    borrower: str
    tx_hash: str
    nonce: int
    collateral_asset: str = ""
    debt_asset: str = ""
    estimated_profit: float = 0.0
    submitted_at: float = field(default_factory=time.time)
    expiry_seconds: float = 60.0

    @property
    def is_expired(self) -> bool:
        return time.time() - self.submitted_at > self.expiry_seconds


class ConfirmationTracker:
    """
    Tracks in-flight liquidation txs and clears them on confirmation or expiry.

    Fixes W6: the old pipeline did await asyncio.sleep(30) in a finally block,
    blocking re-liquidation of the same borrower for 30 seconds regardless of
    whether the tx landed. In cascading liquidation events this guarantees
    missed opportunities.

    New behaviour:
    - _in_flight[borrower] is set when tx is submitted
    - Cleared immediately when receipt is received (success or revert)
    - Cleared after expiry_seconds if no receipt (chain congestion fallback)
    - A borrower can be re-attempted as soon as their prior tx is resolved
    """

    def __init__(
        self,
        w3,  # AsyncWeb3 instance
        nonce_manager=None,  # optional NonceManager for rewind on drop
        poll_interval: float = 2.0,
        expiry_seconds: float = 60.0,
    ):
        self._w3 = w3
        self._nonce_manager = nonce_manager
        self._poll_interval = poll_interval
        self._expiry_seconds = expiry_seconds
        self._in_flight: dict[str, InFlightEntry] = {}
        self._lock = asyncio.Lock()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._db = None   # set via set_db() after pipeline db is ready
        # P3: revert callback — called with borrower address when a tx reverts.
        # Wire to pipeline._revert_cooldown for 30s re-submission blacklist.
        self.on_revert: Optional[Callable[[str], None]] = None

    def set_db(self, db) -> None:
        """
        Wire in the outcome DB after init.
        Call once in pipeline setup after both tracker and db are ready:
            self.tracker.set_db(self.db)
        """
        self._db = db

    async def start(self) -> None:
        """Start the background confirmation polling loop."""
        self._running = True
        self._task = asyncio.create_task(self._poll_loop(), name="confirmation_tracker")
        logger.info("[ConfirmationTracker] Started")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()

    async def add(self, borrower: str, tx_hash: str, nonce: int,
                  collateral_asset: str = "", debt_asset: str = "",
                  estimated_profit: float = 0.0) -> None:
        """Register a submitted tx for tracking."""
        async with self._lock:
            self._in_flight[borrower] = InFlightEntry(
                borrower=borrower,
                tx_hash=tx_hash,
                nonce=nonce,
                collateral_asset=collateral_asset,
                debt_asset=debt_asset,
                estimated_profit=estimated_profit,
                expiry_seconds=self._expiry_seconds,
            )
            logger.debug(f"[ConfirmationTracker] Tracking {borrower[:10]}… tx={tx_hash[:12]}…")

    async def is_in_flight(self, borrower: str) -> bool:
        async with self._lock:
            return borrower in self._in_flight

    async def _clear(self, borrower: str, reason: str) -> None:
        async with self._lock:
            entry = self._in_flight.pop(borrower, None)
        if entry:
            logger.info(
                f"[ConfirmationTracker] Cleared {borrower[:10]}… "
                f"tx={entry.tx_hash[:12]}… reason={reason}"
            )

    async def _poll_loop(self) -> None:
        """
        Background task — polls for receipts every poll_interval seconds.
        Clears _in_flight entries on confirm, revert, or expiry.
        """
        while self._running:
            await asyncio.sleep(self._poll_interval)

            async with self._lock:
                entries = list(self._in_flight.values())

            for entry in entries:
                # Check expiry first (no RPC needed)
                if entry.is_expired:
                    logger.warning(
                        f"[ConfirmationTracker] {entry.tx_hash[:12]}… expired after "
                        f"{self._expiry_seconds}s — clearing {entry.borrower[:10]}…"
                    )
                    if self._nonce_manager:
                        await self._nonce_manager.rewind()
                    await self._clear(entry.borrower, "expired")
                    continue

                # Poll for receipt
                try:
                    receipt = await self._w3.eth.get_transaction_receipt(entry.tx_hash)
                    if receipt is None:
                        continue  # still pending

                    status = receipt.get("status", 0)
                    block  = receipt["blockNumber"]
                    if status == 1:
                        logger.info(
                            f"[ConfirmationTracker] CONFIRMED {entry.tx_hash[:12]}… "
                            f"block={block}"
                        )
                        if self._nonce_manager:
                            await self._nonce_manager.confirm()
                        # Write confirmed outcome to DB
                        if self._db:
                            try:
                                self._db.record_confirmation(
                                    tx_hash=entry.tx_hash,
                                    actual_profit=entry.estimated_profit,
                                    gas_used=receipt.get("gasUsed", 0),
                                    gas_cost_usd=0.0,
                                    slippage_actual=0.0,
                                    block_number=block,
                                )
                            except Exception as _e:
                                logger.warning(
                                    f"[ConfirmationTracker] DB confirm failed: {_e}"
                                )
                        await self._clear(entry.borrower, "confirmed")
                    else:
                        logger.warning(
                            f"[ConfirmationTracker] REVERTED {entry.tx_hash[:12]}… "
                            f"block={block}"
                        )
                        if self._nonce_manager:
                            await self._nonce_manager.rewind()
                        # Write reverted outcome to DB
                        if self._db:
                            try:
                                self._db.mark_reverted(entry.tx_hash, block)
                            except Exception as _e:
                                logger.warning(
                                    f"[ConfirmationTracker] DB revert failed: {_e}"
                                )
                        await self._clear(entry.borrower, "reverted")
                        # P3: notify pipeline to blacklist this borrower for 30s
                        if self.on_revert:
                            try:
                                self.on_revert(entry.borrower)
                            except Exception as _cb_e:
                                logger.debug(f"[ConfirmationTracker] on_revert callback error: {_cb_e}")

                except Exception as e:
                    logger.debug(f"[ConfirmationTracker] Receipt poll error: {e}")


# ---------------------------------------------------------------------------
# Fix W8 — PresignedTxGuard (staleness check before firing cached tx)
# ---------------------------------------------------------------------------

@dataclass
class PresignedSnapshot:
    """Metadata captured at presign time for staleness evaluation."""
    borrower: str
    base_fee_wei: int       # base fee when tx was built
    debt_to_cover: int      # debt amount at presign time
    collateral_asset: str
    debt_asset: str
    built_at: float = field(default_factory=time.time)


class PresignedTxGuard:
    """
    Validates a cached presigned tx before firing.
    Fixes W8: gas price and debt_to_cover were frozen at presign time.
    A 30s-old tx with 1.5× base fee multiplier may sit in mempool if
    base fee spiked, or revert if the borrower's debt composition changed.

    Checks:
    1. Age: reject if older than max_age_seconds
    2. Gas drift: reject if current base fee > presigned base fee × gas_drift_factor
    3. Debt drift: reject if debt_to_cover differs from current estimate by > debt_drift_pct
    """

    def __init__(
        self,
        max_age_seconds: float = 25.0,
        gas_drift_factor: float = 1.20,   # reject if base fee rose >20%
        debt_drift_pct: float   = 0.05,   # reject if debt drifted >5%
    ):
        self.max_age_seconds  = max_age_seconds
        self.gas_drift_factor = gas_drift_factor
        self.debt_drift_pct   = debt_drift_pct

    def is_stale(
        self,
        snapshot: PresignedSnapshot,
        current_base_fee: int,
        current_debt_estimate: Optional[int] = None,
    ) -> tuple[bool, str]:
        """
        Returns (is_stale, reason).
        Call before presigner.fire() — if stale, rebuild instead of using cache.
        """
        age = time.time() - snapshot.built_at

        # Check 1: age
        if age > self.max_age_seconds:
            return True, f"age={age:.1f}s > max={self.max_age_seconds}s"

        # Check 2: gas drift
        if snapshot.base_fee_wei > 0:
            drift = current_base_fee / snapshot.base_fee_wei
            if drift > self.gas_drift_factor:
                return True, (
                    f"base_fee drifted {drift:.2f}× "
                    f"(snapshot={snapshot.base_fee_wei}, current={current_base_fee})"
                )

        # Check 3: debt drift (optional — only if caller provides current estimate)
        if current_debt_estimate is not None and snapshot.debt_to_cover > 0:
            debt_drift = abs(current_debt_estimate - snapshot.debt_to_cover) / snapshot.debt_to_cover
            if debt_drift > self.debt_drift_pct:
                return True, (
                    f"debt_to_cover drifted {debt_drift:.1%} "
                    f"(snapshot={snapshot.debt_to_cover}, current={current_debt_estimate})"
                )

        return False, "ok"


# ---------------------------------------------------------------------------
# Fix W9 — PriceRegistry (staleness-aware price store)
# ---------------------------------------------------------------------------

class PriceRegistry:
    """
    Staleness-aware price store. Replaces raw dict in local_hf_engine.py.
    Fixes W9: prices[asset] was overwritten unconditionally, with no
    last_updated tracking. WS reconnect bursts fired on stale intermediate
    prices. A 5-minute-old price was treated identically to a fresh one.

    Changes:
    - update_price() records timestamp per asset
    - get_price() returns None if price is older than max_age_seconds
    - is_fresh() lets HF engine gate on data quality before firing
    - get_stale_assets() surfaces which feeds need attention
    """

    def __init__(self, max_age_seconds: float = 60.0):
        self.max_age_seconds = max_age_seconds
        self._prices: dict[str, int]   = {}
        self._timestamps: dict[str, float] = {}

    def update_price(self, asset: str, price: int) -> None:
        """
        Record a new price with current timestamp.
        Replaces: self.prices[asset] = new_price
        """
        now = time.time()
        old = self._prices.get(asset)
        self._prices[asset]     = price
        self._timestamps[asset] = now

        if old and old > 0:
            drift = abs(price - old) / old
            if drift > 0.10:  # log >10% single-update moves (spoofing signal)
                logger.warning(
                    f"[PriceRegistry] Large price move on {asset[:8]}…: "
                    f"{old} → {price} ({drift:.1%})"
                )

    def get_price(self, asset: str) -> Optional[int]:
        """
        Returns current price, or None if stale / not seen.
        Replaces: self.prices.get(asset)
        In HF engine: if get_price(asset) is None: skip position
        """
        price = self._prices.get(asset)
        if price is None:
            return None

        age = time.time() - self._timestamps.get(asset, 0)
        if age > self.max_age_seconds:
            logger.debug(f"[PriceRegistry] {asset[:8]}… price is stale ({age:.0f}s old)")
            return None

        return price

    def is_fresh(self, asset: str) -> bool:
        return self.get_price(asset) is not None

    def age(self, asset: str) -> float:
        """Returns price age in seconds, or inf if never seen."""
        ts = self._timestamps.get(asset)
        return time.time() - ts if ts else float("inf")

    def get_stale_assets(self) -> list[str]:
        """Returns list of assets whose prices exceed max_age_seconds."""
        return [a for a in self._prices if self.age(a) > self.max_age_seconds]

    def all_fresh(self, assets: list[str]) -> bool:
        """True only if every asset in the list has a fresh price."""
        return all(self.is_fresh(a) for a in assets)

    def snapshot(self) -> dict[str, int]:
        """
        Returns dict of all currently-fresh prices.
        Safe to pass to LocalHFEngine for batch compute.
        """
        return {
            asset: price
            for asset, price in self._prices.items()
            if self.is_fresh(asset)
        }


# ---------------------------------------------------------------------------
# Integration guide
# ---------------------------------------------------------------------------
#
# ── W6 fix (pipeline.py) ──────────────────────────────────────────────────
#
# At pipeline startup:
#     tracker = ConfirmationTracker(w3=rpc_client.w3, nonce_manager=nonce_mgr)
#     await tracker.start()
#     tracker.set_db(outcome_db)
#
# Replace the finally block in the liquidation attempt:
#     OLD:
#         finally:
#             await asyncio.sleep(30)
#             self._in_flight.discard(address)
#
#     NEW:
#         # After blast_submit():
#         if tx_hash:
#             await tracker.add(borrower=address, tx_hash=tx_hash, nonce=nonce)
#         # (tracker clears _in_flight automatically on confirm/revert/expiry)
#
# Guard at attempt entry:
#     if await tracker.is_in_flight(address):
#         return  # already has a pending tx
#
# ── W8 fix (presigner.py) ─────────────────────────────────────────────────
#
# Add guard before using cached tx:
#     guard = PresignedTxGuard()
#
#     stale, reason = guard.is_stale(
#         snapshot=cached_snapshot,
#         current_base_fee=await rpc_client.get_base_fee(),
#         current_debt_estimate=current_debt,
#     )
#     if stale:
#         logger.info(f"[Presigner] Cache miss — {reason}. Rebuilding.")
#         presigned = await self._build_tx(borrower)  # force rebuild
#     else:
#         presigned = cached_tx
#
# ── W9 fix (local_hf_engine.py) ───────────────────────────────────────────
#
# Replace raw dict:
#     OLD: self.prices: dict[str, int] = {}
#     NEW: from execution_guards import PriceRegistry
#          self.prices = PriceRegistry(max_age_seconds=60)
#
# Replace update_price:
#     OLD: self.prices[asset] = new_price
#     NEW: self.prices.update_price(asset, new_price)
#
# Gate HF compute:
#     OLD: price = self.prices.get(asset, 0)
#     NEW: price = self.prices.get_price(asset)
#          if price is None:
#              logger.debug(f"Skipping {asset} — stale price")
#              continue
#
# ---------------------------------------------------------------------------
