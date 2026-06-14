"""
skip_telemetry.py — Structured skip telemetry for liquidation candidates
Fixes: every rejected candidate logs "SKIP" with no structured reason code.
       Can't analyze why opportunities are missed or parameters are wrong.

Problem:
    Current pipeline logs something like:
        "SKIP 0xborrow... HF=1.002"
    No reason code. No profit figure. No indication whether the skip was
    correct (genuine unprofitable trade) or a misconfigured parameter
    (profit floor too high, staleness gate over-triggering, wrong collateral).

    Without this data you cannot answer:
        - Is $5 profit floor too high? Rejecting $4.80 trades that would win?
        - Is staleness gate firing on feeds that are actually fine?
        - Is collateral selection rejecting positions EVEstimator would approve?
        - How many candidates per hour? Per day?

This module:
    SkipReason     — enum of all rejection points in the pipeline
    SkipEvent      — structured record of one rejected candidate
    SkipTelemetry  — collector, aggregator, SQLite persistence
    SkipAnalyzer   — parameter tuning recommendations from accumulated data

Usage:
    telemetry = SkipTelemetry(db_path="skips.db")
    await telemetry.start()

    # In _execute_liquidation(), replace scattered "SKIP" logs with:
    telemetry.record(SkipEvent(
        borrower   = address,
        reason     = SkipReason.PROFIT_FLOOR,
        hf         = pos.hf_float,
        profit_usd = ev_result.expected_profit_usd,
        gas_usd    = ev_result.gas_cost_usd,
        collateral = collateral_asset,
        debt_asset = debt_asset,
    ))
"""

import asyncio
import logging
import sqlite3
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SkipReason — every rejection point in the pipeline
# ---------------------------------------------------------------------------

class SkipReason(Enum):
    """
    Ordered by pipeline position — first gate that fires wins.
    This ordering matters for analysis: if POSITION_NOT_FOUND fires often,
    earlier gates (watchlist, loader) need fixing before tuning later ones.
    """

    # Data quality gates (before any financial evaluation)
    POSITION_NOT_FOUND    = "position_not_found"     # loader.get() returned None
    NO_RESERVE_DATA       = "no_reserve_data"         # reserves not populated (HF > 1.05 at refresh)
    STALE_PRICE           = "stale_price"             # PriceRegistry rejected asset price
    HF_NOT_LIQUIDATABLE   = "hf_not_liquidatable"     # HF >= 1.0 at execution time (race)
    IN_FLIGHT             = "in_flight"               # tx already pending for this borrower

    # Collateral / debt selection
    NO_ELIGIBLE_COLLATERAL= "no_eligible_collateral"  # CollateralSelector returned None
    COLLATERAL_ZERO_BONUS = "collateral_zero_bonus"   # all collateral has liq_bonus <= 10000
    NO_DEBT_ASSET         = "no_debt_asset"           # couldn't identify debt asset

    # Financial gates
    PROFIT_FLOOR          = "profit_floor"            # ProfitGate: expected_profit < min_usd
    GAS_MULTIPLE          = "gas_multiple"            # ProfitGate: profit < N× gas cost
    GAS_RESERVE           = "gas_reserve"             # FastGasGuard: ETH balance too low
    EV_NEGATIVE           = "ev_negative"             # EVEstimator: net EV < 0 after slippage

    # Execution gates
    NONCE_UNAVAILABLE     = "nonce_unavailable"       # NonceManager failed
    BUILD_FAILED          = "build_failed"            # tx build/sign threw exception
    SUBMIT_FAILED         = "submit_failed"           # blast_submit returned None
    TX_REVERTED           = "tx_reverted"             # confirmed but status=0

    # External / market
    RACE_LOST             = "race_lost"               # competitor liquidated first
    ALREADY_LIQUIDATED    = "already_liquidated"      # position no longer exists on-chain


# Human-readable descriptions for each reason
REASON_DESCRIPTIONS = {
    SkipReason.POSITION_NOT_FOUND:     "Position not in PositionLoader — watchlist gap or bootstrap stale",
    SkipReason.NO_RESERVE_DATA:        "Per-asset reserve data missing — needs refresh_hot(HF<1.05)",
    SkipReason.STALE_PRICE:            "Asset price too old — PriceRegistry max_age exceeded",
    SkipReason.HF_NOT_LIQUIDATABLE:    "HF >= 1.0 at execution — price moved between detection and attempt",
    SkipReason.IN_FLIGHT:              "Tx already pending for this borrower — ConfirmationTracker gate",
    SkipReason.NO_ELIGIBLE_COLLATERAL: "CollateralSelector found no eligible collateral",
    SkipReason.COLLATERAL_ZERO_BONUS:  "All collateral has liquidation bonus <= 0 — misconfigured asset",
    SkipReason.NO_DEBT_ASSET:          "Could not identify debt asset for liquidation",
    SkipReason.PROFIT_FLOOR:           "Expected profit below $5 floor — may need floor adjustment",
    SkipReason.GAS_MULTIPLE:           "Profit < 2× gas cost — marginal trade not worth the risk",
    SkipReason.GAS_RESERVE:            "ETH balance below gas reserve threshold",
    SkipReason.EV_NEGATIVE:            "Net EV negative after slippage — Quoter rejected",
    SkipReason.NONCE_UNAVAILABLE:      "NonceManager could not allocate nonce",
    SkipReason.BUILD_FAILED:           "Transaction build or signing threw exception",
    SkipReason.SUBMIT_FAILED:          "blast_submit returned None — all endpoints failed",
    SkipReason.TX_REVERTED:            "Transaction confirmed but reverted — contract rejected",
    SkipReason.RACE_LOST:              "Competitor liquidated position before our tx confirmed",
    SkipReason.ALREADY_LIQUIDATED:     "Position fully liquidated — no remaining debt",
}


# ---------------------------------------------------------------------------
# SkipEvent — one structured rejection record
# ---------------------------------------------------------------------------

@dataclass
class SkipEvent:
    borrower:      str
    reason:        SkipReason
    hf:            float = 0.0
    profit_usd:    float = 0.0        # expected profit before rejection
    gas_usd:       float = 0.0        # estimated gas cost
    collateral:    Optional[str] = None
    debt_asset:    Optional[str] = None
    debt_usd:      float = 0.0        # total debt in USD
    collateral_usd:float = 0.0        # total collateral in USD
    detail:        str = ""           # free-form extra context
    timestamp:     float = field(default_factory=time.time)
    block:         int = 0

    @property
    def net_profit_usd(self) -> float:
        return self.profit_usd - self.gas_usd

    @property
    def reason_code(self) -> str:
        return self.reason.value

    def to_tuple(self) -> tuple:
        """For SQLite insertion."""
        return (
            self.timestamp,
            self.borrower,
            self.reason.value,
            self.hf,
            self.profit_usd,
            self.gas_usd,
            self.net_profit_usd,
            self.collateral or "",
            self.debt_asset or "",
            self.debt_usd,
            self.collateral_usd,
            self.detail,
            self.block,
        )


# ---------------------------------------------------------------------------
# SkipTelemetry — collector and persistence
# ---------------------------------------------------------------------------

class SkipTelemetry:
    """
    Collects structured skip events, persists to SQLite, exposes aggregates.

    Thread-safe via asyncio queue — record() is sync (safe to call from
    anywhere in the pipeline), persistence runs in background.
    """

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS skip_events (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp       REAL NOT NULL,
        borrower        TEXT NOT NULL,
        reason          TEXT NOT NULL,
        hf              REAL,
        profit_usd      REAL,
        gas_usd         REAL,
        net_profit_usd  REAL,
        collateral      TEXT,
        debt_asset      TEXT,
        debt_usd        REAL,
        collateral_usd  REAL,
        detail          TEXT,
        block           INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_reason    ON skip_events(reason);
    CREATE INDEX IF NOT EXISTS idx_timestamp ON skip_events(timestamp);
    CREATE INDEX IF NOT EXISTS idx_borrower  ON skip_events(borrower);
    """

    def __init__(
        self,
        db_path: str = "skips.db",
        window: int = 1000,           # in-memory rolling window for fast aggregates
        flush_interval: float = 30.0, # seconds between SQLite flushes
    ):
        self._db_path       = db_path
        self._window        = window
        self._flush_interval= flush_interval

        # In-memory collections
        self._recent: deque = deque(maxlen=window)
        self._counts: dict[str, int] = defaultdict(int)
        self._total  = 0

        # Async queue for non-blocking record()
        self._queue: asyncio.Queue = asyncio.Queue()
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._pending_flush: list[SkipEvent] = []

    async def start(self) -> None:
        """Initialise SQLite schema and start background flush loop."""
        self._init_db()
        self._running = True
        self._task = asyncio.create_task(self._flush_loop(), name="skip_telemetry")
        logger.info(f"[SkipTelemetry] Started — db={self._db_path}")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
        # Final flush
        await self._flush_to_db()

    def record(self, event: SkipEvent) -> None:
        """
        Record a skip event. Sync — safe to call from anywhere.
        Logs at appropriate level and queues for persistence.

        Replace all scattered "SKIP" log lines with this call.
        """
        self._total            += 1
        self._counts[event.reason.value] += 1
        self._recent.append(event)

        # Log with structured context
        log_level = self._log_level(event.reason)
        logger.log(
            log_level,
            f"[Skip] {event.reason.value:30s} "
            f"borrower={event.borrower[:10]}… "
            f"HF={event.hf:.4f} "
            f"profit=${event.profit_usd:.2f} "
            f"gas=${event.gas_usd:.2f} "
            + (f"detail={event.detail}" if event.detail else "")
        )

        # Queue for async SQLite write
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.debug("[SkipTelemetry] Queue full — event dropped from persistence")

    def counts(self) -> dict[str, int]:
        """Counts by reason code — current session."""
        return dict(self._counts)

    def rate_per_hour(self) -> dict[str, float]:
        """
        Skip rates per hour based on recent window.
        Useful for detecting parameter misconfiguration:
        if PROFIT_FLOOR is firing 50x/hour, the floor may be too high.
        """
        if not self._recent:
            return {}
        oldest = self._recent[0].timestamp
        elapsed_hours = max((time.time() - oldest) / 3600, 1 / 3600)
        rates = defaultdict(float)
        for event in self._recent:
            rates[event.reason.value] += 1
        return {k: v / elapsed_hours for k, v in rates.items()}

    def top_skipped_profit(self, n: int = 10) -> list[SkipEvent]:
        """
        Top-N skipped candidates by expected profit.
        If PROFIT_FLOOR events appear here with profit=$4-5, the floor
        is too conservative — lower it or it's costing you real money.
        """
        profit_skips = [
            e for e in self._recent
            if e.reason in (SkipReason.PROFIT_FLOOR, SkipReason.GAS_MULTIPLE, SkipReason.EV_NEGATIVE)
        ]
        return sorted(profit_skips, key=lambda e: e.profit_usd, reverse=True)[:n]

    def summary(self) -> str:
        """One-line summary for stats loop."""
        if self._total == 0:
            return "[SkipTelemetry] 0 skips recorded"
        top = sorted(self._counts.items(), key=lambda x: x[1], reverse=True)[:3]
        top_str = " | ".join(f"{k}={v}" for k, v in top)
        return f"[SkipTelemetry] total={self._total} top3=[{top_str}]"

    def detailed_report(self) -> str:
        """Multi-line report for periodic logging or diagnostics."""
        if self._total == 0:
            return "No skip events recorded yet."

        lines = [f"Skip Telemetry Report — {self._total} total skips"]
        lines.append("─" * 55)

        rates = self.rate_per_hour()
        for reason in SkipReason:
            count = self._counts.get(reason.value, 0)
            if count == 0:
                continue
            rate  = rates.get(reason.value, 0.0)
            desc  = REASON_DESCRIPTIONS[reason]
            lines.append(
                f"  {reason.value:30s}  {count:>5}x  "
                f"({rate:>5.1f}/hr)  {desc}"
            )

        # Highlight potential misconfigurations
        lines.append("─" * 55)
        issues = self._detect_issues()
        if issues:
            lines.append("⚠ Potential parameter issues:")
            for issue in issues:
                lines.append(f"  {issue}")
        else:
            lines.append("✓ No parameter issues detected")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _log_level(self, reason: SkipReason) -> int:
        """
        Log level by reason — not all skips are equal.
        Data quality failures are warnings. Financial gates are debug.
        """
        import logging as _logging
        warning_reasons = {
            SkipReason.POSITION_NOT_FOUND,
            SkipReason.NO_RESERVE_DATA,
            SkipReason.SUBMIT_FAILED,
            SkipReason.BUILD_FAILED,
            SkipReason.TX_REVERTED,
        }
        info_reasons = {
            SkipReason.RACE_LOST,
            SkipReason.PROFIT_FLOOR,
            SkipReason.GAS_RESERVE,
        }
        if reason in warning_reasons:
            return _logging.WARNING
        if reason in info_reasons:
            return _logging.INFO
        return _logging.DEBUG

    def _detect_issues(self) -> list[str]:
        """
        Heuristic checks for parameter misconfiguration.
        Returns list of human-readable issue strings.
        """
        issues = []
        rates  = self.rate_per_hour()

        # PROFIT_FLOOR firing heavily — floor may be too high
        pf_rate = rates.get(SkipReason.PROFIT_FLOOR.value, 0)
        if pf_rate > 10:
            top = self.top_skipped_profit(5)
            if top:
                avg_skipped = sum(e.profit_usd for e in top) / len(top)
                issues.append(
                    f"PROFIT_FLOOR firing {pf_rate:.0f}x/hr — "
                    f"avg skipped profit=${avg_skipped:.2f}. "
                    f"Consider lowering min_profit_usd if avg > $3."
                )

        # STALE_PRICE firing heavily — feeds may need attention
        sp_rate = rates.get(SkipReason.STALE_PRICE.value, 0)
        if sp_rate > 5:
            issues.append(
                f"STALE_PRICE firing {sp_rate:.0f}x/hr — "
                f"check PricePoller feed health and max_age_seconds config."
            )

        # POSITION_NOT_FOUND firing — watchlist coverage gap
        pnf_rate = rates.get(SkipReason.POSITION_NOT_FOUND.value, 0)
        if pnf_rate > 2:
            issues.append(
                f"POSITION_NOT_FOUND {pnf_rate:.0f}x/hr — "
                f"watchlist may be stale. Check WatchlistBuilder and PositionLoader bootstrap."
            )

        # GAS_RESERVE firing — ETH running low
        gr_rate = rates.get(SkipReason.GAS_RESERVE.value, 0)
        if gr_rate > 0:
            issues.append(
                f"GAS_RESERVE firing {gr_rate:.0f}x/hr — "
                f"wallet ETH below reserve threshold. Top up."
            )

        # NO_RESERVE_DATA high — refresh_hot threshold too conservative
        nrd_rate = rates.get(SkipReason.NO_RESERVE_DATA.value, 0)
        if nrd_rate > 5:
            issues.append(
                f"NO_RESERVE_DATA {nrd_rate:.0f}x/hr — "
                f"consider lowering refresh_hot hf_threshold from 1.05 to 1.10."
            )

        return issues

    def _init_db(self) -> None:
        """Create SQLite schema if not exists."""
        try:
            conn = sqlite3.connect(self._db_path)
            conn.executescript(self.SCHEMA)
            conn.commit()
            conn.close()
            logger.debug(f"[SkipTelemetry] SQLite ready at {self._db_path}")
        except Exception as e:
            logger.error(f"[SkipTelemetry] DB init failed: {e}")

    async def _flush_loop(self) -> None:
        """Drain queue to SQLite every flush_interval seconds."""
        while self._running:
            try:
                await asyncio.sleep(self._flush_interval)
                await self._flush_to_db()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[SkipTelemetry] Flush error: {e}")

    async def _flush_to_db(self) -> None:
        """Drain all queued events to SQLite in one transaction."""
        batch = []
        while not self._queue.empty():
            try:
                batch.append(self._queue.get_nowait())
            except asyncio.QueueEmpty:
                break

        if not batch:
            return

        try:
            conn = sqlite3.connect(self._db_path)
            conn.executemany(
                """INSERT INTO skip_events
                   (timestamp, borrower, reason, hf, profit_usd, gas_usd,
                    net_profit_usd, collateral, debt_asset, debt_usd,
                    collateral_usd, detail, block)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [e.to_tuple() for e in batch],
            )
            conn.commit()
            conn.close()
            logger.debug(f"[SkipTelemetry] Flushed {len(batch)} events to SQLite")
        except Exception as e:
            logger.error(f"[SkipTelemetry] DB write failed: {e}")


# ---------------------------------------------------------------------------
# SkipAnalyzer — query SQLite for parameter tuning
# ---------------------------------------------------------------------------

class SkipAnalyzer:
    """
    Queries the skip_events SQLite table for parameter tuning insights.
    Run ad-hoc from CLI or schedule in stats loop.

    Usage:
        analyzer = SkipAnalyzer(db_path="skips.db")
        analyzer.profit_floor_analysis()
        analyzer.hourly_breakdown(hours=24)
        analyzer.top_missed_opportunities(n=20)
    """

    def __init__(self, db_path: str = "skips.db"):
        self._db = db_path

    def _query(self, sql: str, params: tuple = ()) -> list[tuple]:
        try:
            conn = sqlite3.connect(self._db)
            rows = conn.execute(sql, params).fetchall()
            conn.close()
            return rows
        except Exception as e:
            logger.error(f"[SkipAnalyzer] Query failed: {e}")
            return []

    def profit_floor_analysis(self) -> None:
        """
        Shows distribution of skipped profits around the current floor.
        If many events cluster just below $5, lower the floor.
        """
        rows = self._query("""
            SELECT
                ROUND(profit_usd, 1) as profit_bucket,
                COUNT(*) as count
            FROM skip_events
            WHERE reason = 'profit_floor'
              AND timestamp > strftime('%s', 'now') - 86400
            GROUP BY profit_bucket
            ORDER BY profit_bucket DESC
            LIMIT 20
        """)
        print("\n── Profit Floor Analysis (last 24h) ───────────────────")
        print(f"  {'Profit':>10}  {'Count':>8}")
        for profit, count in rows:
            bar = "█" * min(count, 40)
            print(f"  ${profit:>9.1f}  {count:>8}  {bar}")
        print()

    def hourly_breakdown(self, hours: int = 24) -> None:
        """Skip counts by reason over the last N hours."""
        rows = self._query("""
            SELECT reason, COUNT(*) as count
            FROM skip_events
            WHERE timestamp > strftime('%s', 'now') - ?
            GROUP BY reason
            ORDER BY count DESC
        """, (hours * 3600,))
        print(f"\n── Skip Breakdown (last {hours}h) ──────────────────────")
        for reason, count in rows:
            desc = REASON_DESCRIPTIONS.get(
                next((r for r in SkipReason if r.value == reason), None), ""
            )
            print(f"  {reason:30s}  {count:>6}x  {desc}")
        print()

    def top_missed_opportunities(self, n: int = 20) -> None:
        """
        Top-N skipped candidates by profit — the ones that hurt most.
        Focus on PROFIT_FLOOR and EV_NEGATIVE entries here.
        """
        rows = self._query("""
            SELECT borrower, reason, hf, profit_usd, gas_usd, collateral,
                   datetime(timestamp, 'unixepoch') as ts
            FROM skip_events
            WHERE reason IN ('profit_floor', 'ev_negative', 'gas_multiple')
              AND profit_usd > 0
            ORDER BY profit_usd DESC
            LIMIT ?
        """, (n,))
        print(f"\n── Top {n} Missed Opportunities ─────────────────────────")
        print(f"  {'Borrower':12}  {'Reason':20}  {'HF':>6}  {'Profit':>8}  {'Gas':>6}  {'Time'}")
        for row in rows:
            borrower, reason, hf, profit, gas, collateral, ts = row
            print(
                f"  {borrower[:10]}…  {reason:20}  {hf:>6.4f}  "
                f"${profit:>7.2f}  ${gas:>5.2f}  {ts}"
            )
        print()

    def stale_price_assets(self) -> None:
        """Which assets are triggering STALE_PRICE most often."""
        rows = self._query("""
            SELECT collateral, COUNT(*) as count
            FROM skip_events
            WHERE reason = 'stale_price'
              AND timestamp > strftime('%s', 'now') - 86400
            GROUP BY collateral
            ORDER BY count DESC
        """)
        print("\n── Stale Price — Top Assets (last 24h) ─────────────────")
        for asset, count in rows:
            print(f"  {asset[:20]:20}  {count}x")
        print()


# ---------------------------------------------------------------------------
# pipeline.py integration
# ---------------------------------------------------------------------------
#
# 1. Import:
#       from skip_telemetry import SkipTelemetry, SkipEvent, SkipReason
#
# 2. In setup():
#       self.skip_tel = SkipTelemetry(db_path="skips.db")
#       await self.skip_tel.start()
#
# 3. In _execute_liquidation(), replace scattered logger calls with records:
#
#       # BEFORE:
#       if account_data is None:
#           logger.debug(f"SKIP {address} — not in loader")
#           return
#
#       # AFTER:
#       if account_data is None:
#           self.skip_tel.record(SkipEvent(
#               borrower=address, reason=SkipReason.POSITION_NOT_FOUND,
#               hf=hf, detail="loader.get() returned None"
#           ))
#           return
#
#       # ProfitGate rejection:
#       if not self.profit_gate.check(ev_result.expected_profit_usd):
#           self.skip_tel.record(SkipEvent(
#               borrower       = address,
#               reason         = SkipReason.PROFIT_FLOOR,
#               hf             = hf,
#               profit_usd     = ev_result.expected_profit_usd,
#               gas_usd        = ev_result.gas_cost_usd,
#               collateral     = collateral_asset,
#               debt_asset     = debt_asset,
#               debt_usd       = account_data.total_debt_base / 1e8,
#               collateral_usd = account_data.total_collateral_base / 1e8,
#           ))
#           return
#
#       # FastGasGuard rejection:
#       ok, reason = await self.fast_gas_guard.check()
#       if not ok:
#           self.skip_tel.record(SkipEvent(
#               borrower=address, reason=SkipReason.GAS_RESERVE,
#               hf=hf, detail=reason
#           ))
#           return
#
#       # CollateralSelector rejection:
#       result = self.selector.select(...)
#       if result is None:
#           self.skip_tel.record(SkipEvent(
#               borrower=address, reason=SkipReason.NO_ELIGIBLE_COLLATERAL,
#               hf=hf, debt_usd=account_data.total_debt_base / 1e8,
#           ))
#           return
#
#       # blast_submit failure:
#       if tx_hash is None:
#           self.skip_tel.record(SkipEvent(
#               borrower=address, reason=SkipReason.SUBMIT_FAILED,
#               hf=hf, detail="all 4 endpoints returned None"
#           ))
#           return
#
# 4. In stats loop, add:
#       logger.info(self.skip_tel.summary())
#       # Every 6 hours, log full report:
#       if hour_tick % 6 == 0:
#           logger.info(self.skip_tel.detailed_report())
#
# 5. In shutdown():
#       await self.skip_tel.stop()
#
# 6. Ad-hoc analysis from CLI:
#       python -c "
#       from skip_telemetry import SkipAnalyzer
#       a = SkipAnalyzer('skips.db')
#       a.profit_floor_analysis()
#       a.top_missed_opportunities()
#       a.hourly_breakdown(24)
#       "
#
# ---------------------------------------------------------------------------
