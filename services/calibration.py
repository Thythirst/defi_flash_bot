"""
calibration.py — Production Calibration Framework.

Records every opportunity and outcome. Computes reliability diagrams,
Brier scores, ECE. Trains a logistic regression model. Replaces hardcoded
win probabilities at N ≥ 100 observations.

Architecture:
  pre_liq_engine → CalibrationDB.record_opportunity()  [every detection]
  pre_liq_engine → CalibrationDB.record_outcome()      [every resolved bundle]
  weekly cron    → CalibrationAnalyzer.report()         [every Monday]
  pre_liq_engine → LogisticModel.predict_proba()       [once trained]

Redis keys:
  calibration:opps             STREAM   every opportunity
  calibration:outcomes:{hash}   HASH     resolved outcomes
  calibration:model             HASH     serialized logistic model coefficients
  calibration:model:meta        HASH     training metadata (N, last_trained, Brier)

Usage:
  from services.calibration import CalibrationDB, CalibrationAnalyzer, LogisticModel
"""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import redis.asyncio as redis

# ── NumPy is available in project venv ──
import numpy as np


# ═══════════════════════════════════════════════════════════════
# Calibration Database
# ═══════════════════════════════════════════════════════════════

@dataclass
class OpportunityRecord:
    """One detection event — recorded BEFORE outcome is known."""
    id: str                          # unique: f"{ts_ms}_{borrower[:8]}"
    timestamp: float                 # epoch seconds
    trigger: str                     # oracle_update | borrow | withdraw
    oracle_age: float                # seconds since oracle update (0 if N/A)
    competitor_count: int            # competitors seen in mempool window
    expected_profit_usd: float
    predicted_wp: float              # model's predicted win probability [0,1]
    borrower: str                    # target borrower address
    collateral_symbol: str
    debt_symbol: str
    submitted: bool                  # did we submit a bundle?
    tx_hash: str = ""                # filled after submission


@dataclass
class OutcomeRecord:
    """One resolved bundle — recorded AFTER receipt is known."""
    tx_hash: str
    trigger: str
    predicted_wp: float
    expected_profit_usd: float
    builder_accepted: bool           # tx made it on-chain (has receipt)
    executed: bool                   # status=1 (liquidation succeeded)
    profit_realized_usd: float       # actual profit (0 if failed)
    lost_to_competitor: bool         # our tx reverted, borrower was liquidated
    reverted: bool                   # our tx reverted, no liquidation occurred
    confirmation_block: int = 0
    confirmation_time: float = 0.0   # seconds from submission to confirmation


class CalibrationDB:
    """Persistent store for calibration data."""

    STREAM_KEY = "calibration:opps"
    OUTCOME_PREFIX = "calibration:outcomes"
    MAX_STREAM_LEN = 100_000

    def __init__(self, redis_client):
        self.redis = redis_client

    async def record_opportunity(self, opp: OpportunityRecord):
        """Store an opportunity detection event."""
        ts_ms = int(opp.timestamp * 1000)
        await self.redis.xadd(self.STREAM_KEY, {
            "id": opp.id,
            "ts": str(ts_ms),
            "trigger": opp.trigger,
            "oracle_age": str(round(opp.oracle_age, 3)),
            "competitor_count": str(opp.competitor_count),
            "expected_profit": str(round(opp.expected_profit_usd, 2)),
            "predicted_wp": str(round(opp.predicted_wp, 4)),
            "borrower": opp.borrower,
            "collateral": opp.collateral_symbol,
            "debt": opp.debt_symbol,
            "submitted": "1" if opp.submitted else "0",
            "tx_hash": opp.tx_hash,
        }, maxlen=self.MAX_STREAM_LEN, approximate=True)

    async def record_outcome(self, outcome: OutcomeRecord):
        """Store a resolved bundle outcome."""
        ts = int(time.time() * 1000)
        await self.redis.hset(f"{self.OUTCOME_PREFIX}:{outcome.tx_hash}", mapping={
            "trigger": outcome.trigger,
            "predicted_wp": str(round(outcome.predicted_wp, 4)),
            "expected_profit": str(round(outcome.expected_profit_usd, 2)),
            "builder_accepted": "1" if outcome.builder_accepted else "0",
            "executed": "1" if outcome.executed else "0",
            "profit_realized": str(round(outcome.profit_realized_usd, 2)),
            "lost_to_competitor": "1" if outcome.lost_to_competitor else "0",
            "reverted": "1" if outcome.reverted else "0",
            "block": str(outcome.confirmation_block),
            "confirmation_time": str(round(outcome.confirmation_time, 1)),
            "resolved_at": str(ts),
        })

    async def get_all_outcomes(self) -> List[dict]:
        """Return all resolved outcomes as dicts for training."""
        keys = await self.redis.keys(f"{self.OUTCOME_PREFIX}:*")
        results = []
        for key in keys:
            data = await self.redis.hgetall(key)
            if data:
                results.append(data)
        return results

    async def get_training_data(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        Return (X, y, wp_pred) for logistic regression training.

        X columns: [oracle_age, competitor_count, log_expected_profit,
                     is_oracle, is_borrow, is_withdraw]
        y: 1 if executed (won), 0 otherwise
        wp_pred: original predicted win probability
        """
        # Also read from the opportunity stream for feature data
        opps = await self._read_opps()
        if not opps:
            return np.array([]).reshape(0, 6), np.array([]), np.array([])

        X_rows = []
        y_rows = []
        wp_rows = []

        for opp in opps:
            if not opp.get("submitted") or opp["submitted"] != "1":
                continue
            tx_hash = opp.get("tx_hash", "")
            if not tx_hash:
                continue

            outcome = await self.redis.hgetall(f"{self.OUTCOME_PREFIX}:{tx_hash}")
            if not outcome:
                continue  # not yet resolved

            features = self._extract_features(opp)
            X_rows.append(features)
            y_rows.append(1 if outcome.get("executed") == "1" else 0)
            wp_rows.append(float(opp.get("predicted_wp", 0.5)))

        if not X_rows:
            return np.array([]).reshape(0, 6), np.array([]), np.array([])

        return np.array(X_rows), np.array(y_rows), np.array(wp_rows)

    async def _read_opps(self) -> List[dict]:
        """Read all opportunities from stream."""
        opps = []
        # Read stream in chunks
        last_id = "0-0"
        while True:
            batch = await self.redis.xrange(self.STREAM_KEY, min=last_id, count=500)
            if not batch:
                break
            for msg_id, data in batch:
                opps.append(data)
                last_id = msg_id
            if len(batch) < 500:
                break
        return opps

    @staticmethod
    def _extract_features(opp: dict) -> List[float]:
        trigger = opp.get("trigger", "")
        return [
            float(opp.get("oracle_age", 0)),
            float(opp.get("competitor_count", 0)),
            math.log(max(float(opp.get("expected_profit", 50)), 1.0)),
            1.0 if trigger == "oracle_update" else 0.0,
            1.0 if trigger == "borrow" else 0.0,
            1.0 if trigger == "withdraw" else 0.0,
        ]

    async def outcome_count(self) -> int:
        keys = await self.redis.keys(f"{self.OUTCOME_PREFIX}:*")
        return len(keys)

    async def get_opportunity_by_id(self, opp_id: str) -> Optional[dict]:
        """Read an opportunity from the stream by its id field."""
        opps = await self._read_opps()
        for opp in opps:
            if opp.get("id") == opp_id:
                return opp
        return None


# ═══════════════════════════════════════════════════════════════
# Calibration Analyzer
# ═══════════════════════════════════════════════════════════════

@dataclass
class ReliabilityBin:
    """One bin in a reliability diagram."""
    bin_low: float           # lower bound of predicted probability
    bin_high: float          # upper bound
    count: int               # number of observations in this bin
    predicted_mean: float    # mean predicted probability
    actual_mean: float       # mean observed outcome
    wins: int
    total: int


@dataclass
class CalibrationReport:
    """Full calibration analysis output."""
    timestamp: float
    total_observations: int
    total_submitted: int
    total_won: int
    total_lost: int
    overall_win_rate: float
    brier_score: float
    ece: float                    # Expected Calibration Error
    mce: float                    # Maximum Calibration Error
    reliability_bins: List[ReliabilityBin]
    feature_importance: Dict[str, float]
    realized_pnl: float
    missed_opportunities: int     # detected but not submitted
    calibration_drift: float      # change in ECE since last calibration


class CalibrationAnalyzer:
    """Computes calibration metrics from observed data."""

    def __init__(self, db: CalibrationDB):
        self.db = db

    async def analyze(self, n_bins: int = 10) -> CalibrationReport:
        """Run full calibration analysis."""

        # Get training data
        all_outcomes = await self.db.get_all_outcomes()
        if not all_outcomes:
            return self._empty_report()

        # Build y_true, y_pred pairs from resolved outcomes
        y_true = []
        y_pred = []
        profits = []

        for out in all_outcomes:
            executed = out.get("executed") == "1"
            y_true.append(1 if executed else 0)
            y_pred.append(float(out.get("predicted_wp", 0.5)))
            profits.append(float(out.get("profit_realized", 0)))

        y_true = np.array(y_true)
        y_pred = np.array(y_pred)
        profits = np.array(profits)

        # Get full opportunity data for missed detection stats
        X, y_all, wp_all = await self.db.get_training_data()
        missed = len([o for o in await self.db._read_opps()
                      if o.get("submitted") == "0"])

        # Compute metrics
        brier = self._brier_score(y_true, y_pred)
        ece, mce, bins = self._expected_calibration_error(y_true, y_pred, n_bins)

        # Feature importance (if we have training data)
        fi = {}
        if len(X) >= 20:
            fi = self._compute_feature_importance(X, y_all) if len(y_all) > 0 else {}

        # Calibration drift (compare with last stored ECE)
        last_ece = float(
            (await self.db.redis.hget("calibration:model:meta", "last_ece")) or "0.0"
        )
        drift = abs(ece - last_ece) if last_ece > 0 else 0.0

        total_won = int(np.sum(y_true))
        total_lost = len(y_true) - total_won

        return CalibrationReport(
            timestamp=time.time(),
            total_observations=len(y_true),
            total_submitted=len(y_true),
            total_won=total_won,
            total_lost=total_lost,
            overall_win_rate=total_won / len(y_true) if len(y_true) > 0 else 0.0,
            brier_score=brier,
            ece=ece,
            mce=mce,
            reliability_bins=bins,
            feature_importance=fi,
            realized_pnl=float(np.sum(profits)),
            missed_opportunities=missed,
            calibration_drift=drift,
        )

    @staticmethod
    def _brier_score(y_true: np.ndarray, y_pred: np.ndarray) -> float:
        """Brier score = mean((predicted - actual)²). Lower is better. 0 = perfect."""
        if len(y_true) == 0:
            return 1.0
        return float(np.mean((y_pred - y_true) ** 2))

    @staticmethod
    def _expected_calibration_error(
        y_true: np.ndarray, y_pred: np.ndarray, n_bins: int = 10
    ) -> Tuple[float, float, List[ReliabilityBin]]:
        """
        ECE = Σ (|B_m|/n) × |accuracy(B_m) − confidence(B_m)|

        Returns (ECE, MCE, bins).
        """
        if len(y_true) == 0:
            return 1.0, 1.0, []

        bin_edges = np.linspace(0, 1, n_bins + 1)
        bins = []
        ece_total = 0.0
        mce = 0.0
        n = len(y_true)

        for i in range(n_bins):
            low, high = bin_edges[i], bin_edges[i + 1]
            mask = (y_pred >= low) & (y_pred < high)
            # Include upper edge in last bin
            if i == n_bins - 1:
                mask = (y_pred >= low) & (y_pred <= high)

            count = int(np.sum(mask))
            if count == 0:
                bins.append(ReliabilityBin(
                    bin_low=low, bin_high=high, count=0,
                    predicted_mean=0.0, actual_mean=0.0, wins=0, total=0,
                ))
                continue

            pred_mean = float(np.mean(y_pred[mask]))
            actual_mean = float(np.mean(y_true[mask]))
            wins = int(np.sum(y_true[mask]))
            diff = abs(actual_mean - pred_mean)

            ece_total += (count / n) * diff
            mce = max(mce, diff)

            bins.append(ReliabilityBin(
                bin_low=low, bin_high=high, count=count,
                predicted_mean=pred_mean, actual_mean=actual_mean,
                wins=wins, total=count,
            ))

        return ece_total, mce, bins

    @staticmethod
    def _compute_feature_importance(X: np.ndarray, y: np.ndarray) -> Dict[str, float]:
        """Simple correlation-based feature importance."""
        feature_names = [
            "oracle_age", "competitor_count", "log_expected_profit",
            "is_oracle", "is_borrow", "is_withdraw",
        ]
        importances = {}
        for i, name in enumerate(feature_names):
            if i < X.shape[1]:
                corr = np.corrcoef(X[:, i], y)[0, 1]
                importances[name] = abs(corr) if not np.isnan(corr) else 0.0
        # Normalize
        total = sum(importances.values()) or 1.0
        return {k: v / total for k, v in importances.items()}

    @staticmethod
    def _empty_report() -> CalibrationReport:
        return CalibrationReport(
            timestamp=time.time(),
            total_observations=0, total_submitted=0,
            total_won=0, total_lost=0, overall_win_rate=0.0,
            brier_score=1.0, ece=1.0, mce=1.0,
            reliability_bins=[], feature_importance={},
            realized_pnl=0.0, missed_opportunities=0,
            calibration_drift=0.0,
        )


# ═══════════════════════════════════════════════════════════════
# Logistic Regression Win-Probability Model
# ═══════════════════════════════════════════════════════════════

class LogisticModel:
    """
    Logistic regression: P(win | features).

    Features: oracle_age, competitor_count, log(expected_profit),
              is_oracle, is_borrow, is_withdraw

    Serialized to Redis as JSON so predictions survive restarts.
    Retrained when N >= 100 new observations since last training.
    """

    FEATURE_NAMES = [
        "oracle_age", "competitor_count", "log_profit",
        "is_oracle", "is_borrow", "is_withdraw",
    ]

    def __init__(self, redis_client):
        self.redis = redis_client
        self.coef_: Optional[np.ndarray] = None
        self.intercept_: float = 0.0
        self.n_trained_: int = 0
        self.last_trained_: float = 0.0
        self.brier_: float = 1.0

    async def load(self):
        """Load serialized model from Redis."""
        raw = await self.redis.hgetall("calibration:model")
        if raw and "coef" in raw:
            self.coef_ = np.array(json.loads(raw["coef"]))
            self.intercept_ = float(raw.get("intercept", 0.0))
            self.n_trained_ = int(raw.get("n_trained", 0))
            self.last_trained_ = float(raw.get("last_trained", 0))
            self.brier_ = float(raw.get("brier", 1.0))

    async def save(self):
        """Serialize model to Redis."""
        if self.coef_ is None:
            return
        pipe = self.redis.pipeline()
        pipe.hset("calibration:model", mapping={
            "coef": json.dumps(self.coef_.tolist()),
            "intercept": str(self.intercept_),
            "n_trained": str(self.n_trained_),
            "last_trained": str(self.last_trained_),
            "brier": str(self.brier_),
        })
        pipe.hset("calibration:model:meta", mapping={
            "last_ece": str(self.brier_),
            "last_trained": str(self.last_trained_),
        })
        await pipe.execute()

    @property
    def is_trained(self) -> bool:
        return self.coef_ is not None and self.n_trained_ >= 100

    async def requires_retrain(self, db: CalibrationDB) -> bool:
        """Check if enough new data exists to retrain."""
        n_outcomes = await db.outcome_count()
        new_samples = n_outcomes - self.n_trained_
        return new_samples >= 100

    def predict_proba(self, features: List[float]) -> float:
        """
        Predict P(win) using logistic regression.

        features: [oracle_age, competitor_count, log_profit,
                   is_oracle, is_borrow, is_withdraw]
        """
        if self.coef_ is None:
            return 0.50  # fallback
        z = self.intercept_ + np.dot(self.coef_, features)
        # Clip to avoid overflow
        z = max(-50.0, min(z, 50.0))
        return 1.0 / (1.0 + math.exp(-z))

    async def train(self, db: CalibrationDB):
        """Train logistic regression on all observed data."""
        X, y, _ = await db.get_training_data()
        if len(X) < 10:
            return False

        # Standardize features
        mean = X.mean(axis=0)
        std = X.std(axis=0)
        std[std == 0] = 1.0
        X_scaled = (X - mean) / std

        # Gradient descent for logistic regression
        n_samples, n_features = X_scaled.shape
        coef = np.zeros(n_features)
        intercept = 0.0
        lr = 0.1
        n_iter = 1000

        for _ in range(n_iter):
            z = intercept + np.dot(X_scaled, coef)
            z = np.clip(z, -50, 50)
            p = 1.0 / (1.0 + np.exp(-z))
            error = p - y

            grad_coef = np.dot(X_scaled.T, error) / n_samples
            grad_intercept = np.mean(error)

            coef -= lr * grad_coef
            intercept -= lr * grad_intercept

        # Unscale coefficients
        self.coef_ = coef / std
        self.intercept_ = intercept - np.dot(coef / std, mean)
        self.n_trained_ = len(y)
        self.last_trained_ = time.time()

        # Compute Brier score
        z = self.intercept_ + np.dot(X, self.coef_)
        z = np.clip(z, -50, 50)
        p = 1.0 / (1.0 + np.exp(-z))
        self.brier_ = float(np.mean((p - y) ** 2))

        await self.save()
        return True

    def features_from_opp(self, trigger: str, oracle_age: float,
                          competitor_count: int, expected_profit: float) -> List[float]:
        """Convert opportunity metadata to feature vector."""
        return [
            oracle_age,
            float(competitor_count),
            math.log(max(expected_profit, 1.0)),
            1.0 if trigger == "oracle_update" else 0.0,
            1.0 if trigger == "borrow" else 0.0,
            1.0 if trigger == "withdraw" else 0.0,
        ]


# ═══════════════════════════════════════════════════════════════
# Weekly Report Generator
# ═══════════════════════════════════════════════════════════════

def format_reliability_diagram(bins: List[ReliabilityBin]) -> str:
    """Generate ASCII reliability diagram."""
    if not bins:
        return "  No data."

    lines = []
    lines.append(f"  {'BIN':>12} | {'COUNT':>6} | {'PREDICTED':>10} | {'ACTUAL':>8} | {'GAP':>8}")
    lines.append("  " + "-" * 56)

    for b in bins:
        if b.count == 0:
            continue
        gap = b.actual_mean - b.predicted_mean
        gap_str = f"+{gap:.3f}" if gap > 0 else f"{gap:.3f}"
        lines.append(
            f"  [{b.bin_low:.1f}-{b.bin_high:.1f})  "
            f"| {b.count:>6} | {b.predicted_mean:>9.3f} | {b.actual_mean:>7.3f} | {gap_str:>8}"
        )
    return "\n".join(lines)


async def generate_weekly_report(redis_url: str = "redis://localhost:6379") -> str:
    """Generate a comprehensive calibration report."""
    r = redis.from_url(redis_url, decode_responses=True)
    await r.ping()

    db = CalibrationDB(r)
    analyzer = CalibrationAnalyzer(db)
    model = LogisticModel(r)
    await model.load()

    report = await analyzer.analyze(n_bins=10)

    lines = []
    lines.append("=" * 72)
    lines.append("  WEEKLY CALIBRATION REPORT")
    lines.append(f"  {time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("=" * 72)
    lines.append("")

    # Summary
    lines.append("  ── OBSERVATIONS ──")
    lines.append(f"  Total resolved:  {report.total_observations}")
    lines.append(f"  Won:             {report.total_won}")
    lines.append(f"  Lost:            {report.total_lost}")
    lines.append(f"  Overall win:     {report.overall_win_rate:.1%}")
    lines.append(f"  Missed opps:     {report.missed_opportunities}")
    lines.append("")

    # PnL
    lines.append("  ── REALIZED PnL ──")
    lines.append(f"  Total PnL:       ${report.realized_pnl:,.2f}")
    lines.append("")

    # Calibration metrics
    lines.append("  ── CALIBRATION ──")
    brier_grade = "A" if report.brier_score < 0.10 else ("B" if report.brier_score < 0.20 else ("C" if report.brier_score < 0.30 else "D"))
    lines.append(f"  Brier Score:     {report.brier_score:.4f}  (Grade: {brier_grade})")
    lines.append(f"  ECE:             {report.ece:.4f}")
    lines.append(f"  MCE:             {report.mce:.4f}")
    lines.append(f"  Calibration drift: {report.calibration_drift:.4f}")
    lines.append(f"  Model trained:   {'Yes' if model.is_trained else 'No'} (N={model.n_trained_})")
    lines.append(f"  Model Brier:     {model.brier_:.4f}")
    lines.append("")

    # Reliability diagram
    lines.append("  ── RELIABILITY DIAGRAM ──")
    lines.append(format_reliability_diagram(report.reliability_bins))
    lines.append("")

    # Feature importance
    if report.feature_importance:
        lines.append("  ── FEATURE IMPORTANCE ──")
        sorted_fi = sorted(report.feature_importance.items(), key=lambda x: -x[1])
        for name, imp in sorted_fi:
            bar = "█" * int(imp * 20)
            lines.append(f"  {name:>25}: {bar} {imp:.3f}")
        lines.append("")

    # Recommendations
    lines.append("  ── RECOMMENDATIONS ──")
    if report.ece > 0.15:
        lines.append("  ⚠ ECE > 0.15 — model is miscalibrated. Consider retraining.")
    if report.overall_win_rate < 0.25:
        lines.append("  ⚠ Win rate < 25% — EV thresholds may need adjustment.")
    if report.calibration_drift > 0.05:
        lines.append("  ⚠ Calibration drift detected — market conditions may have changed.")
    if report.missed_opportunities > report.total_submitted:
        lines.append("  ⚠ More opportunities missed than submitted — EV threshold too high?")
    if not model.is_trained:
        lines.append(f"  ℹ Logistic model not yet trained ({model.n_trained_}/100). Using Bayesian prior.")
    if report.total_observations == 0:
        lines.append("  ℹ No observations yet — calibration report is empty.")

    lines.append("")
    lines.append("=" * 72)

    await r.aclose()
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Calibration Framework")
    parser.add_argument("--redis", default="redis://localhost:6379")
    parser.add_argument("--report", action="store_true", help="Generate weekly report")
    parser.add_argument("--train", action="store_true", help="Force retrain logistic model")
    args = parser.parse_args()

    if args.report:
        report = await generate_weekly_report(args.redis)
        print(report)
    elif args.train:
        r = redis.from_url(args.redis, decode_responses=True)
        await r.ping()
        db = CalibrationDB(r)
        model = LogisticModel(r)
        await model.load()
        success = await model.train(db)
        if success:
            print(f"Model trained on {model.n_trained_} observations. Brier={model.brier_:.4f}")
        else:
            print("Not enough data to train (need ≥ 10).")
        await r.aclose()
    else:
        print("Usage: python -m services.calibration --report | --train")


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
