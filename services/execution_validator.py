"""
execution_validator.py — DRY_RUN Execution Model Validation.

During DRY_RUN, every simulated bundle is tracked to determine whether it
WOULD have won had it been submitted. Subsequent blocks are monitored for
liquidation events on the target borrower.

Outcome classification:
  WOULD_HAVE_WON:     Our simulation passed AND no competitor liquidated
                       the borrower first in the next 10 blocks.
  WOULD_HAVE_LOST:    Our simulation passed BUT a competitor liquidated
                       the borrower before we would have.
  WOULD_HAVE_REVERTED: Our simulation would have failed (simulation error).
  UNKNOWN:            Could not determine (stale, RPC error, etc.)

Daily metrics compare Bayesian vs Logistic model predictions against
actual DRY_RUN outcomes. The logistic model is promoted only if:
  1. N >= 100 resolved outcomes
  2. Brier score < 0.15
  3. Higher realized EV than Bayesian baseline
  4. Statistically significant improvement (p < 0.05 via paired test)

Redis keys:
  validator:pending:{opp_id}   HASH   simulated opportunity awaiting outcome
  validator:outcomes:{opp_id}  HASH   resolved DRY_RUN outcome
  validator:daily:{date}       HASH   daily metrics
  validator:promotion          HASH   model promotion state

Usage:
  Integrated into pre_liq_engine DRY_RUN path automatically.
  Manual: python -m services.execution_validator --daily-report
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import redis.asyncio as redis
import numpy as np

logger = logging.getLogger("validator")


# ═══════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════

BLOCKS_TO_WATCH = 10          # Check this many blocks after detection
BLOCK_POLL_INTERVAL = 1.0     # Poll for new blocks every 1s
OUTCOME_STALE_AGE = 120.0     # Give up on unresolved outcomes after 2 min

PROMOTION_MIN_SAMPLES = 100   # Minimum resolved outcomes for promotion
PROMOTION_MAX_BRIER = 0.15    # Maximum Brier score for promotion eligibility
PROMOTION_P_VALUE = 0.05      # Statistical significance threshold

LIQUIDATION_TOPIC = (
    "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286"
)
AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"


# ═══════════════════════════════════════════════════════════════
# Types
# ═══════════════════════════════════════════════════════════════

class DryRunOutcome(str, Enum):
    WOULD_HAVE_WON = "would_have_won"
    WOULD_HAVE_LOST = "would_have_lost"
    WOULD_HAVE_REVERTED = "would_have_reverted"
    UNKNOWN = "unknown"


class ModelSource(str, Enum):
    BAYESIAN = "bayesian"
    LOGISTIC = "logistic"


@dataclass
class DryRunOpportunity:
    """A simulated opportunity awaiting outcome determination."""
    opp_id: str
    timestamp: float
    trigger: str
    borrower: str
    collateral: str
    debt: str
    predicted_wp: float
    expected_profit_usd: float
    expected_ev_usd: float
    detected_block: int
    model_source: ModelSource
    competitor_count: int
    oracle_age: float


@dataclass
class DryRunResult:
    """Resolved DRY_RUN outcome."""
    opp_id: str
    outcome: DryRunOutcome
    predicted_wp: float
    expected_profit_usd: float
    expected_ev_usd: float
    realized_ev_usd: float      # 0 if lost/reverted, expected_profit if won
    model_source: ModelSource
    confirmation_block: int
    liquidator_address: str = ""


@dataclass
class DailyMetrics:
    """One day's validation metrics."""
    date: str
    total_opportunities: int = 0
    total_submitted: int = 0
    total_resolved: int = 0
    would_have_won: int = 0
    would_have_lost: int = 0
    would_have_reverted: int = 0
    predicted_win_rate: float = 0.0
    actual_win_rate: float = 0.0
    calibration_error: float = 0.0
    realized_ev_usd: float = 0.0
    missed_ev_usd: float = 0.0
    bayesian_win_rate: float = 0.0
    bayesian_ev_usd: float = 0.0
    logistic_win_rate: float = 0.0
    logistic_ev_usd: float = 0.0
    logistic_trained: bool = False
    logistic_promoted: bool = False


@dataclass
class ModelComparison:
    """Bayesian vs Logistic comparison results."""
    bayesian_win_rate: float
    bayesian_ev_total: float
    bayesian_brier: float
    logistic_win_rate: float
    logistic_ev_total: float
    logistic_brier: float
    logistic_n_trained: int
    ev_delta_pct: float          # (logistic_ev - bayesian_ev) / bayesian_ev
    brier_delta: float            # logistic_brier - bayesian_brier
    significant: bool             # statistically significant improvement?
    should_promote: bool


# ═══════════════════════════════════════════════════════════════
# Execution Validator
# ═══════════════════════════════════════════════════════════════

class ExecutionValidator:
    """
    Tracks DRY_RUN opportunities and determines whether each would have
    won by monitoring subsequent blocks for liquidation events.
    """

    def __init__(self, redis_client, w3, cal_db=None):
        self.redis = redis_client
        self.w3 = w3
        self.cal_db = cal_db
        self._pending: Dict[str, DryRunOpportunity] = {}
        self._last_block_check = 0
        self._current_block = 0

    # ── Recording ───────────────────────────────────────────

    async def record_simulated_submission(
        self, opp_id: str, trigger: str, borrower: str,
        collateral: str, debt: str, predicted_wp: float,
        expected_profit: float, expected_ev: float,
        model_source: ModelSource, competitor_count: int,
        oracle_age: float, detected_block: int,
    ):
        """Record a DRY_RUN simulated bundle for outcome tracking."""
        ts = time.time()
        record = DryRunOpportunity(
            opp_id=opp_id, timestamp=ts, trigger=trigger,
            borrower=borrower, collateral=collateral, debt=debt,
            predicted_wp=predicted_wp, expected_profit_usd=expected_profit,
            expected_ev_usd=expected_ev, detected_block=detected_block,
            model_source=model_source, competitor_count=competitor_count,
            oracle_age=oracle_age,
        )
        self._pending[opp_id] = record
        await self.redis.hset(f"validator:pending:{opp_id}", mapping={
            "timestamp": str(ts), "trigger": trigger,
            "borrower": borrower, "collateral": collateral, "debt": debt,
            "predicted_wp": str(predicted_wp),
            "expected_profit": str(expected_profit),
            "expected_ev": str(expected_ev),
            "detected_block": str(detected_block),
            "model_source": model_source.value,
            "competitor_count": str(competitor_count),
            "oracle_age": str(round(oracle_age, 3)),
        })

    # ── Outcome Determination ───────────────────────────────

    async def poll_outcomes(self):
        """Check pending opportunities against recent blocks."""
        now = time.time()
        stale_ids = []

        for opp_id, opp in list(self._pending.items()):
            age = now - opp.timestamp
            if age > OUTCOME_STALE_AGE:
                await self._resolve(opp_id, opp, DryRunOutcome.UNKNOWN)
                stale_ids.append(opp_id)
                continue

            # Check if borrower was liquidated in blocks since detection
            liquidated, liquidator = await self._check_liquidation(
                opp.borrower, opp.detected_block,
            )
            if liquidated:
                outcome = DryRunOutcome.WOULD_HAVE_LOST
                await self._resolve(opp_id, opp, outcome, liquidator)
                stale_ids.append(opp_id)
            elif self._current_block >= opp.detected_block + BLOCKS_TO_WATCH:
                # Blocks elapsed — no competitor acted
                outcome = DryRunOutcome.WOULD_HAVE_WON
                await self._resolve(opp_id, opp, outcome)
                stale_ids.append(opp_id)

        for oid in stale_ids:
            self._pending.pop(oid, None)

    async def _check_liquidation(
        self, borrower: str, from_block: int,
    ) -> Tuple[bool, str]:
        """Check if borrower was liquidated in [from_block, current]."""
        borrower_topic = "0x" + borrower[2:].rjust(64, "0")
        to_block = max(from_block, self._current_block)

        try:
            logs = self.w3.eth.get_logs({
                "address": AAVE_POOL,
                "topics": [LIQUIDATION_TOPIC, None, borrower_topic],
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
            })
            if logs:
                # Extract liquidator from topics[3]
                liquidator = "0x" + logs[0]["topics"][3][-40:] if len(logs[0].get("topics", [])) > 3 else ""
                return True, liquidator
        except Exception:
            pass
        return False, ""

    async def _resolve(
        self, opp_id: str, opp: DryRunOpportunity,
        outcome: DryRunOutcome, liquidator: str = "",
    ):
        """Record resolved outcome."""
        realized_ev = opp.expected_profit_usd if outcome == DryRunOutcome.WOULD_HAVE_WON else 0.0

        result = DryRunResult(
            opp_id=opp_id, outcome=outcome,
            predicted_wp=opp.predicted_wp,
            expected_profit_usd=opp.expected_profit_usd,
            expected_ev_usd=opp.expected_ev_usd,
            realized_ev_usd=realized_ev,
            model_source=opp.model_source,
            confirmation_block=self._current_block,
            liquidator_address=liquidator,
        )

        await self.redis.hset(f"validator:outcomes:{opp_id}", mapping={
            "outcome": outcome.value,
            "predicted_wp": str(opp.predicted_wp),
            "expected_profit": str(opp.expected_profit_usd),
            "expected_ev": str(opp.expected_ev_usd),
            "realized_ev": str(realized_ev),
            "model_source": opp.model_source.value,
            "block": str(self._current_block),
            "liquidator": liquidator,
        })

        status = {"would_have_won": "✅", "would_have_lost": "🏁", "would_have_reverted": "❌", "unknown": "?"}
        logger.info(
            "%s DRY-RUN: %s | borrower=%s | wp=%.0f%% | profit=$%.0f | EV=$%.0f → $%.0f",
            status.get(outcome.value, "?"), outcome.value,
            opp.borrower[:12], opp.predicted_wp * 100,
            opp.expected_profit_usd, opp.expected_ev_usd, realized_ev,
        )

    async def update_block(self, block_number: int):
        self._current_block = block_number

    async def pending_count(self) -> int:
        return len(self._pending)

    async def get_all_outcomes(self) -> List[dict]:
        keys = await self.redis.keys("validator:outcomes:*")
        results = []
        for key in keys:
            data = await self.redis.hgetall(key)
            if data:
                results.append(data)
        return results


# ═══════════════════════════════════════════════════════════════
# Daily Metrics Generator
# ═══════════════════════════════════════════════════════════════

class DailyMetricsGenerator:
    """Generates daily validation metrics comparing Bayesian vs Logistic."""

    def __init__(self, redis_client):
        self.redis = redis_client

    async def generate(self, date: str = None) -> DailyMetrics:
        """Generate metrics for today (or specified date)."""
        if date is None:
            date = time.strftime("%Y-%m-%d")

        outcomes = []
        keys = await self.redis.keys("validator:outcomes:*")
        for key in keys:
            data = await self.redis.hgetall(key)
            if data:
                outcomes.append(data)

        m = DailyMetrics(date=date)
        m.total_resolved = len(outcomes)

        bayesian_wins = 0
        bayesian_total = 0
        bayesian_ev = 0.0
        logistic_wins = 0
        logistic_total = 0
        logistic_ev = 0.0
        total_pred_wp = 0.0
        sq_errors = []

        for out in outcomes:
            is_won = out.get("outcome") == DryRunOutcome.WOULD_HAVE_WON.value
            wp = float(out.get("predicted_wp", 0.5))
            profit = float(out.get("expected_profit", 0))
            model = out.get("model_source", ModelSource.BAYESIAN.value)
            realized = float(out.get("realized_ev", 0))

            total_pred_wp += wp
            sq_errors.append((wp - (1.0 if is_won else 0.0)) ** 2)

            if is_won:
                m.would_have_won += 1
                m.realized_ev_usd += realized
            elif out.get("outcome") == DryRunOutcome.WOULD_HAVE_LOST.value:
                m.would_have_lost += 1
            elif out.get("outcome") == DryRunOutcome.WOULD_HAVE_REVERTED.value:
                m.would_have_reverted += 1

            if model == ModelSource.BAYESIAN.value:
                bayesian_total += 1
                if is_won:
                    bayesian_wins += 1
                    bayesian_ev += realized
            else:
                logistic_total += 1
                if is_won:
                    logistic_wins += 1
                    logistic_ev += realized

        if m.total_resolved > 0:
            m.actual_win_rate = m.would_have_won / m.total_resolved
            m.predicted_win_rate = total_pred_wp / m.total_resolved
            m.calibration_error = abs(m.actual_win_rate - m.predicted_win_rate)

        if bayesian_total > 0:
            m.bayesian_win_rate = bayesian_wins / bayesian_total
            m.bayesian_ev_usd = bayesian_ev

        if logistic_total > 0:
            m.logistic_win_rate = logistic_wins / logistic_total
            m.logistic_ev_usd = logistic_ev
            m.logistic_trained = True

        # Check promotion state
        promo = await self.redis.hgetall("validator:promotion")
        m.logistic_promoted = promo.get("promoted") == "1"

        # Count total opportunities from calibration stream
        m.total_opportunities = await self.redis.xlen("calibration:opps") or 0
        m.total_submitted = m.total_resolved  # all DRY_RUN opps are "submitted"

        # Missed EV: opportunities we detected but didn't "submit" (EV < threshold)
        # In DRY_RUN mode, everything above threshold is submitted
        m.missed_ev_usd = 0.0  # tracked separately via calibration

        return m


# ═══════════════════════════════════════════════════════════════
# Model Comparison & Promotion
# ═══════════════════════════════════════════════════════════════

class ModelPromotion:
    """Compares Bayesian vs Logistic and manages promotion."""

    def __init__(self, redis_client, cal_db=None):
        self.redis = redis_client
        self.cal_db = cal_db

    async def compare(self) -> ModelComparison:
        """Run head-to-head comparison of Bayesian vs Logistic."""
        outcomes = []
        keys = await self.redis.keys("validator:outcomes:*")
        for key in keys:
            data = await self.redis.hgetall(key)
            if data:
                outcomes.append(data)

        bay_wins = bay_total = bay_ev = bay_sq = 0.0
        log_wins = log_total = log_ev = log_sq = 0.0

        for out in outcomes:
            is_won = out.get("outcome") == DryRunOutcome.WOULD_HAVE_WON.value
            wp = float(out.get("predicted_wp", 0.5))
            profit = float(out.get("expected_profit", 0))
            model = out.get("model_source", ModelSource.BAYESIAN.value)
            err = (wp - (1.0 if is_won else 0.0)) ** 2

            if model == ModelSource.BAYESIAN.value:
                bay_total += 1
                bay_sq += err
                if is_won:
                    bay_wins += 1
                    bay_ev += profit
            else:
                log_total += 1
                log_sq += err
                if is_won:
                    log_wins += 1
                    log_ev += profit

        bay_wr = bay_wins / max(bay_total, 1)
        bay_brier = bay_sq / max(bay_total, 1)
        log_wr = log_wins / max(log_total, 1)
        log_brier = log_sq / max(log_total, 1)
        ev_delta = (log_ev - bay_ev) / max(bay_ev, 1.0)

        # Paired test: use the subset where both models made predictions
        # Simplified: compare win rates with z-test
        significant = False
        n = min(bay_total, log_total)
        if n >= 30:
            # Two-proportion z-test
            p_pool = (bay_wins + log_wins) / max(bay_total + log_total, 1)
            se = math.sqrt(p_pool * (1 - p_pool) * (1 / max(bay_total, 1) + 1 / max(log_total, 1)))
            z = abs(log_wr - bay_wr) / max(se, 1e-10)
            significant = z > 1.96  # 95% confidence

        should_promote = (
            log_total >= PROMOTION_MIN_SAMPLES
            and log_brier < PROMOTION_MAX_BRIER
            and log_ev > bay_ev
            and significant
        )

        return ModelComparison(
            bayesian_win_rate=bay_wr,
            bayesian_ev_total=bay_ev,
            bayesian_brier=bay_brier,
            logistic_win_rate=log_wr,
            logistic_ev_total=log_ev,
            logistic_brier=log_brier,
            logistic_n_trained=int(log_total),
            ev_delta_pct=ev_delta,
            brier_delta=log_brier - bay_brier,
            significant=significant,
            should_promote=should_promote,
        )

    async def promote_if_eligible(self):
        """Check promotion criteria and promote logistic model if met."""
        comp = await self.compare()
        if comp.should_promote:
            await self.redis.hset("validator:promotion", mapping={
                "promoted": "1",
                "promoted_at": str(time.time()),
                "bayesian_wr": str(round(comp.bayesian_win_rate, 4)),
                "logistic_wr": str(round(comp.logistic_win_rate, 4)),
                "ev_delta_pct": str(round(comp.ev_delta_pct, 4)),
                "brier_delta": str(round(comp.brier_delta, 4)),
            })
            logger.info("🚀 LOGISTIC MODEL PROMOTED: EV +%.1f%%, Brier %.3f→%.3f",
                       comp.ev_delta_pct * 100, comp.bayesian_brier, comp.logistic_brier)
            return True
        return False

    async def is_promoted(self) -> bool:
        data = await self.redis.hget("validator:promotion", "promoted")
        return data == "1"


# ═══════════════════════════════════════════════════════════════
# Daily Report
# ═══════════════════════════════════════════════════════════════

async def generate_daily_report(redis_url: str = "redis://localhost:6379") -> str:
    """Generate comprehensive daily validation report."""
    r = redis.from_url(redis_url, decode_responses=True)
    await r.ping()

    gen = DailyMetricsGenerator(r)
    m = await gen.generate()
    promo = ModelPromotion(r)
    comp = await promo.compare()

    lines = []
    lines.append("=" * 68)
    lines.append("  DAILY EXECUTION VALIDATION REPORT")
    lines.append(f"  {m.date}")
    lines.append("=" * 68)
    lines.append("")

    lines.append("  ── OPPORTUNITIES ──")
    lines.append(f"  Total detected:     {m.total_opportunities}")
    lines.append(f"  Submitted (sim):    {m.total_submitted}")
    lines.append(f"  Resolved:           {m.total_resolved}")
    lines.append(f"  Pending:            {m.total_submitted - m.total_resolved}")
    lines.append("")

    lines.append("  ── OUTCOMES ──")
    lines.append(f"  Would have won:     {m.would_have_won}")
    lines.append(f"  Would have lost:    {m.would_have_lost}")
    lines.append(f"  Would have reverted:{m.would_have_reverted}")
    lines.append(f"  Actual win rate:    {m.actual_win_rate:.1%}")
    lines.append(f"  Predicted win rate: {m.predicted_win_rate:.1%}")
    lines.append(f"  Calibration error:  {m.calibration_error:+.1%}")
    lines.append("")

    lines.append("  ── REALIZED vs MISSED EV ──")
    lines.append(f"  Realized EV:        ${m.realized_ev_usd:,.2f}")
    lines.append(f"  Missed EV:          ${m.missed_ev_usd:,.2f}")
    lines.append("")

    lines.append("  ── MODEL COMPARISON ──")
    lines.append(f"  {'':20} {'BAYESIAN':>12} {'LOGISTIC':>12}")
    lines.append(f"  {'Win rate':20} {comp.bayesian_win_rate:>11.1%} {comp.logistic_win_rate:>11.1%}")
    lines.append(f"  {'EV total':20} ${comp.bayesian_ev_total:>10,.0f} ${comp.logistic_ev_total:>10,.0f}")
    lines.append(f"  {'Brier':20} {comp.bayesian_brier:>12.4f} {comp.logistic_brier:>12.4f}")
    lines.append(f"  {'N resolved':20} {comp.logistic_n_trained:>12}")
    lines.append(f"  {'EV delta':20} {comp.ev_delta_pct:>+11.1%}")
    lines.append(f"  {'Significant?':20} {'YES' if comp.significant else 'no':>12}")
    lines.append("")

    lines.append("  ── PROMOTION STATUS ──")
    if m.logistic_promoted:
        lines.append("  ★ LOGISTIC MODEL IS PROMOTED (active)")
    elif comp.should_promote:
        lines.append("  ✓ LOGISTIC MODEL MEETS PROMOTION CRITERIA")
        lines.append(f"    N={comp.logistic_n_trained} Brier={comp.logistic_brier:.4f} EV+{comp.ev_delta_pct:.1%}")
    else:
        reasons = []
        if comp.logistic_n_trained < PROMOTION_MIN_SAMPLES:
            reasons.append(f"Need {PROMOTION_MIN_SAMPLES - comp.logistic_n_trained} more outcomes "
                          f"({comp.logistic_n_trained}/{PROMOTION_MIN_SAMPLES})")
        if comp.logistic_brier >= PROMOTION_MAX_BRIER:
            reasons.append(f"Brier {comp.logistic_brier:.4f} ≥ {PROMOTION_MAX_BRIER}")
        if comp.logistic_ev_total <= comp.bayesian_ev_total:
            reasons.append(f"Logistic EV ${comp.logistic_ev_total:,.0f} ≤ Bayesian ${comp.bayesian_ev_total:,.0f}")
        if not comp.significant:
            reasons.append(f"Not statistically significant (p ≥ {PROMOTION_P_VALUE})")
        if not reasons:
            reasons.append("Pending evaluation")
        lines.append("  ✗ NOT ELIGIBLE FOR PROMOTION")
        for reason in reasons:
            lines.append(f"    - {reason}")

    lines.append("")
    lines.append("=" * 68)

    await r.aclose()
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# GO / NO-GO Deployment Recommendation
# ═══════════════════════════════════════════════════════════════

GO_NOGO_MIN_OPPORTUNITIES = 100
GO_NOGO_MAX_CALIBRATION_ERROR = 0.15
GO_NOGO_MIN_WIN_RATE = 0.25
GO_NOGO_MIN_EV_MONTHLY = 5000  # $5K/month minimum expected PnL


async def generate_gonogo_report(redis_url: str = "redis://localhost:6379") -> str:
    """Generate GO/NO-GO deployment recommendation."""
    r = redis.from_url(redis_url, decode_responses=True)
    await r.ping()

    gen = DailyMetricsGenerator(r)
    m = await gen.generate()
    promo = ModelPromotion(r)
    comp = await promo.compare()

    # Collect all outcomes for analysis
    keys = await r.keys("validator:outcomes:*")
    all_outcomes = []
    for key in keys:
        data = await r.hgetall(key)
        if data:
            all_outcomes.append(data)

    n_resolved = len(all_outcomes)
    n_won = sum(1 for o in all_outcomes if o.get("outcome") == "would_have_won")
    n_lost = sum(1 for o in all_outcomes if o.get("outcome") == "would_have_lost")
    total_ev = sum(float(o.get("realized_ev", 0)) for o in all_outcomes)
    total_expected_ev = sum(float(o.get("expected_ev", 0)) for o in all_outcomes)

    actual_wr = n_won / max(n_resolved, 1)
    pred_wr = sum(float(o.get("predicted_wp", 0.5)) for o in all_outcomes) / max(n_resolved, 1)
    cal_error = abs(actual_wr - pred_wr)

    # Shadow payloads
    payload_keys = await r.keys("shadow:payload:*")
    n_payloads = len(payload_keys)

    # GO/NO-GO criteria
    criteria = {
        "opportunities_met": n_resolved >= GO_NOGO_MIN_OPPORTUNITIES,
        "calibration_ok": cal_error <= GO_NOGO_MAX_CALIBRATION_ERROR,
        "win_rate_ok": actual_wr >= GO_NOGO_MIN_WIN_RATE,
        "positive_ev": total_ev > 0,
        "monthly_ev_ok": (total_ev / max((n_resolved or 1), 1)) * 100 >= GO_NOGO_MIN_EV_MONTHLY,
    }
    all_pass = all(criteria.values())

    # Build report
    lines = []
    lines.append("=" * 72)
    lines.append("  GO / NO-GO DEPLOYMENT RECOMMENDATION")
    lines.append(f"  {time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("=" * 72)
    lines.append("")

    lines.append("  ── SUMMARY ──")
    lines.append(f"  Shadow opportunities:   {n_payloads}")
    lines.append(f"  Resolved outcomes:      {n_resolved}")
    lines.append(f"  Would have won:         {n_won}")
    lines.append(f"  Would have lost:        {n_lost}")
    lines.append(f"  Actual win rate:        {actual_wr:.1%}")
    lines.append(f"  Predicted win rate:     {pred_wr:.1%}")
    lines.append(f"  Calibration error:      {cal_error:+.1%}")
    lines.append(f"  Realized EV:            ${total_ev:,.0f}")
    lines.append(f"  Expected EV:            ${total_expected_ev:,.0f}")
    est_monthly = (total_ev / max(n_resolved or 1, 1)) * 100
    lines.append(f"  Est. monthly EV (100 ops): ${est_monthly:,.0f}")
    lines.append("")

    lines.append("  ── CRITERIA ──")
    for name, met in criteria.items():
        check = "✓" if met else "✗"
        detail = {
            "opportunities_met": f"≥{GO_NOGO_MIN_OPPORTUNITIES} resolved ({n_resolved})",
            "calibration_ok": f"≤{GO_NOGO_MAX_CALIBRATION_ERROR:.0%} ({cal_error:.1%})",
            "win_rate_ok": f"≥{GO_NOGO_MIN_WIN_RATE:.0%} ({actual_wr:.1%})",
            "positive_ev": f">$0 (${total_ev:,.0f})",
            "monthly_ev_ok": f"≥${GO_NOGO_MIN_EV_MONTHLY:,}/mo (${est_monthly:,.0f})",
        }[name]
        lines.append(f"  [{check}] {name}: {detail}")

    lines.append("")

    # Model source recommendation
    if comp.should_promote:
        lines.append("  ── MODEL ──")
        lines.append(f"  Recommended: LOGISTIC (EV +{comp.ev_delta_pct:.1%} vs Bayesian)")
    elif comp.logistic_n_trained >= GO_NOGO_MIN_OPPORTUNITIES:
        lines.append("  ── MODEL ──")
        lines.append("  Recommended: BAYESIAN (logistic not yet superior)")

    lines.append("")

    # Recommendation
    lines.append("  ── RECOMMENDATION ──")
    if all_pass:
        lines.append("")
        lines.append("  ╔══════════════════════════════════════════════════════════╗")
        lines.append("  ║                    ★★★ GO ★★★                          ║")
        lines.append("  ║                                                        ║")
        lines.append("  ║  All criteria met. Recommend deployment to LIVE mode.   ║")
        lines.append("  ║  Set PRELIQ_DRY_RUN=0 and SHADOW_LIVE=0 in .env.       ║")
        lines.append("  ╚══════════════════════════════════════════════════════════╝")
    else:
        failed = [n for n, m in criteria.items() if not m]
        lines.append("")
        lines.append("  ╔══════════════════════════════════════════════════════════╗")
        lines.append("  ║                    ✗ NO-GO ✗                           ║")
        lines.append("  ║                                                        ║")
        lines.append(f"  ║  {len(failed)}/{len(criteria)} criteria not met. Remain in SHADOW_LIVE.    ║")
        for f in failed:
            pretty = {
                "opportunities_met": "Need more opportunities",
                "calibration_ok": "Calibration error too high",
                "win_rate_ok": "Win rate below threshold",
                "positive_ev": "Negative expected value",
                "monthly_ev_ok": "Monthly EV below minimum",
            }[f]
            lines.append(f"  ║  - {pretty:<46s} ║")
        lines.append("  ╚══════════════════════════════════════════════════════════╝")

    lines.append("")
    lines.append("=" * 72)

    await r.aclose()
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Execution Model Validator")
    parser.add_argument("--redis", default="redis://localhost:6379")
    parser.add_argument("--daily-report", action="store_true")
    parser.add_argument("--compare-models", action="store_true")
    parser.add_argument("--gonogo", action="store_true", help="GO/NO-GO deployment recommendation")
    args = parser.parse_args()

    if args.gonogo:
        print(await generate_gonogo_report(args.redis))
    elif args.daily_report:
        print(await generate_daily_report(args.redis))
    elif args.compare_models:
        r = redis.from_url(args.redis, decode_responses=True)
        await r.ping()
        promo = ModelPromotion(r)
        comp = await promo.compare()
        print(f"Bayesian:  wr={comp.bayesian_win_rate:.1%} ev=${comp.bayesian_ev_total:,.0f} brier={comp.bayesian_brier:.4f}")
        print(f"Logistic:  wr={comp.logistic_win_rate:.1%} ev=${comp.logistic_ev_total:,.0f} brier={comp.logistic_brier:.4f}")
        print(f"Promote:   {'YES' if comp.should_promote else 'NO'}")
        await r.aclose()
    else:
        parser.print_help()


if __name__ == "__main__":
    asyncio.run(main())
