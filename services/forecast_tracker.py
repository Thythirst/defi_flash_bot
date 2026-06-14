"""
services/forecast_tracker.py — Forecast Prediction vs Actual Liquidation Tracker.

Monitors liq_forecast predictions against on-chain reconciled liquidation
outcomes. Records precision/recall/f1 statistics for model calibration.

Architecture:
  1. Each cycle: snapshot forecast:ranking (users with buffer < threshold)
  2. Compare aave:liquidatable ZSET against previous snapshot
  3. For NEW liquidatable users: check if they were predicted recently → TP/FN
  4. For expired predictions (no liquidation within window) → FP
  5. Write metrics to Redis

Redis keys:
  forecast:predicted:{ts}        ZSET   predicted users with buffer_pct (TTL 30min)
  forecast:tracker:last_liq_set  SET    previous aave:liquidatable snapshot
  forecast:tracker:tp            INT    true positives
  forecast:tracker:fp            INT    false positives
  forecast:tracker:fn            INT    false negatives
  forecast:tracker:precision     FLOAT
  forecast:tracker:recall        FLOAT
  forecast:tracker:f1            FLOAT
  forecast:tracker:last_cycle    INT    unix timestamp

Usage:
  python -m services.forecast_tracker --watch --interval 30
  python -m services.forecast_tracker --stats          # one-shot stats dump
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Set

import redis.asyncio as redis
from dotenv import load_dotenv

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
load_dotenv(project_root / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | tracker | %(message)s",
)
logger = logging.getLogger("forecast_tracker")

# ── Constants ─────────────────────────────────────────────────

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
CYCLE_INTERVAL = int(os.getenv("TRACKER_INTERVAL", "30"))
PREDICTION_WINDOW = int(os.getenv("TRACKER_PREDICTION_WINDOW", "900"))  # 15 min
RISK_THRESHOLD = float(os.getenv("TRACKER_RISK_THRESHOLD", "5.0"))     # buffer < 5%

# Redis metric keys
K_PREDICTED_ACTIVE = "forecast:tracker:predicted_active"  # HASH {user: ts}
K_LIQ_SET_OLD      = "forecast:tracker:last_liq_set"
K_FP_SEEN          = "forecast:tracker:fp_seen"           # SET of FP-counted users
K_TP               = "forecast:tracker:tp"
K_FP               = "forecast:tracker:fp"
K_FN               = "forecast:tracker:fn"
K_PRECISION        = "forecast:tracker:precision"
K_RECALL           = "forecast:tracker:recall"
K_F1               = "forecast:tracker:f1"
K_LAST_CYCLE       = "forecast:tracker:last_cycle"
K_TOTAL_PRED       = "forecast:tracker:total_predicted"
K_TOTAL_LIQ        = "forecast:tracker:total_liquidated"


@dataclass
class CycleResult:
    new_liquidatable: int = 0
    tp: int = 0
    fn: int = 0
    fp: int = 0
    predicted_count: int = 0
    elapsed_ms: float = 0.0


class ForecastTracker:
    """Tracks forecast predictions vs actual liquidation outcomes."""

    def __init__(self, redis_url: str = ""):
        self.redis_url = redis_url or REDIS_URL
        self.redis: Optional[redis.Redis] = None

    async def start(self):
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        await self.redis.ping()
        logger.info(
            "started window=%ds threshold=%.1f%% interval=%ds",
            PREDICTION_WINDOW, RISK_THRESHOLD, CYCLE_INTERVAL,
        )

    async def stop(self):
        if self.redis:
            await self.redis.aclose()

    # ── Main loop ────────────────────────────────────────────

    async def run_forever(self):
        await self.start()
        try:
            while True:
                t0 = time.monotonic()
                r = await self._cycle()
                r.elapsed_ms = (time.monotonic() - t0) * 1000
                await self._update_metrics(r)
                logger.info(
                    "new_liq=%d tp=%d fn=%d fp=%d pred=%d %.0fms",
                    r.new_liquidatable, r.tp, r.fn, r.fp,
                    r.predicted_count, r.elapsed_ms,
                )
                await asyncio.sleep(CYCLE_INTERVAL)
        finally:
            await self.stop()

    async def _cycle(self) -> CycleResult:
        r = CycleResult()
        now = int(time.time())
        cutoff = now - PREDICTION_WINDOW

        # ── 1. Snapshot current predictions ──────────────────
        try:
            ranking = await self.redis.zrangebyscore(
                "forecast:ranking", float("-inf"), RISK_THRESHOLD,
            )
            predicted_now = set(ranking)
            r.predicted_count = len(predicted_now)
        except Exception as e:
            logger.warning("forecast:ranking read: %s", e)
            predicted_now = set()

        # Update active predictions HASH: {user: latest_ts}
        if predicted_now:
            pipe = self.redis.pipeline()
            for user in predicted_now:
                pipe.hset(K_PREDICTED_ACTIVE, user, str(now))
            await pipe.execute()

        # ── 2. Expire old predictions → FP ──────────────────
        try:
            active = await self.redis.hgetall(K_PREDICTED_ACTIVE)
            expired_users = []
            for user, ts_str in active.items():
                if int(ts_str) < cutoff:
                    expired_users.append(user)

            if expired_users:
                pipe = self.redis.pipeline()
                for user in expired_users:
                    pipe.hdel(K_PREDICTED_ACTIVE, user)
                await pipe.execute()

                # For each expired prediction: FP if user never liquidated
                for user in expired_users:
                    was_liq = await self.redis.zscore("aave:liquidatable", user)
                    already_fp = await self.redis.sismember(K_FP_SEEN, user)
                    if was_liq is None and not already_fp:
                        r.fp += 1
                        await self.redis.sadd(K_FP_SEEN, user)
        except Exception as e:
            logger.debug("expiry: %s", e)

        # ── 3. Get current liquidatable set ──────────────────
        current_liq: Set[str] = set()
        try:
            liq_users = await self.redis.zrangebyscore(
                "aave:liquidatable", 0.000001, 0.999999,
            )
            current_liq = set(liq_users)
        except Exception as e:
            logger.warning("aave:liquidatable read: %s", e)

        # ── 4. Find NEW liquidatable users (delta) ───────────
        previous_liq: Set[str] = set()
        try:
            prev = await self.redis.smembers(K_LIQ_SET_OLD)
            previous_liq = set(prev)
        except Exception:
            pass

        new_liq = current_liq - previous_liq
        r.new_liquidatable = len(new_liq)

        # ── 5. Classify each new liquidation ──────────────────
        for user in new_liq:
            was_predicted = await self.redis.hexists(K_PREDICTED_ACTIVE, user)
            if was_predicted:
                r.tp += 1
            else:
                r.fn += 1

        # ── 6. Update last liquidatable set ──────────────────
        try:
            pipe = self.redis.pipeline()
            pipe.delete(K_LIQ_SET_OLD)
            if current_liq:
                pipe.sadd(K_LIQ_SET_OLD, *current_liq)
            await pipe.execute()
        except Exception as e:
            logger.debug("liq_set update: %s", e)

        return r

    # ── Metrics ───────────────────────────────────────────────

    async def _update_metrics(self, r: CycleResult):
        try:
            pipe = self.redis.pipeline()
            if r.tp:
                pipe.incrby(K_TP, r.tp)
            if r.fp:
                pipe.incrby(K_FP, r.fp)
            if r.fn:
                pipe.incrby(K_FN, r.fn)
            pipe.incrby(K_TOTAL_PRED, r.predicted_count)
            pipe.incrby(K_TOTAL_LIQ, r.new_liquidatable)
            pipe.set(K_LAST_CYCLE, str(int(time.time())))
            await pipe.execute()

            # Compute precision/recall/f1
            tp = int(await self.redis.get(K_TP) or 0)
            fp = int(await self.redis.get(K_FP) or 0)
            fn = int(await self.redis.get(K_FN) or 0)

            precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
            f1 = (2 * precision * recall / (precision + recall)
                  if (precision + recall) > 0 else 0.0)

            pipe2 = self.redis.pipeline()
            pipe2.set(K_PRECISION, str(round(precision, 4)))
            pipe2.set(K_RECALL, str(round(recall, 4)))
            pipe2.set(K_F1, str(round(f1, 4)))
            await pipe2.execute()

        except Exception as e:
            logger.warning("metrics: %s", e)

    # ── Stats output ──────────────────────────────────────────

    async def print_stats(self):
        """One-shot stats dump."""
        await self.start()
        try:
            tp = int(await self.redis.get(K_TP) or 0)
            fp = int(await self.redis.get(K_FP) or 0)
            fn = int(await self.redis.get(K_FN) or 0)
            total_pred = int(await self.redis.get(K_TOTAL_PRED) or 0)
            total_liq = int(await self.redis.get(K_TOTAL_LIQ) or 0)
            precision = float(await self.redis.get(K_PRECISION) or 0)
            recall = float(await self.redis.get(K_RECALL) or 0)
            f1 = float(await self.redis.get(K_F1) or 0)

            print(f"\n{'='*60}")
            print(f"  FORECAST TRACKER — {time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            print(f"{'='*60}")
            print(f"  Window: {PREDICTION_WINDOW}s | Threshold: buffer < {RISK_THRESHOLD}%")
            print(f"")
            print(f"  True Positives:  {tp:>8}  (predicted → liquidated)")
            print(f"  False Positives: {fp:>8}  (predicted → not liquidated)")
            print(f"  False Negatives: {fn:>8}  (not predicted → liquidated)")
            print(f"")
            print(f"  Total Predicted:  {total_pred:>8}")
            print(f"  Total Liquidated: {total_liq:>8}")
            print(f"")
            print(f"  Precision: {precision:.1%}  (TP / (TP+FP))")
            print(f"  Recall:    {recall:.1%}  (TP / (TP+FN))")
            print(f"  F1 Score:  {f1:.3f}")

            # Current snapshot
            pred_now = await self.redis.zcount(
                "forecast:ranking", float("-inf"), RISK_THRESHOLD,
            )
            liq_now = await self.redis.zcount(
                "aave:liquidatable", 0.000001, 0.999999,
            )
            print(f"")
            print(f"  Now: {pred_now} predicted at-risk | {liq_now} liquidatable")
            print(f"{'='*60}\n")
        finally:
            await self.stop()


# ── CLI ──────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(
        description="Forecast Prediction Tracker — precision/recall statistics",
    )
    parser.add_argument("--redis", default="redis://localhost:6379")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--stats", action="store_true")
    parser.add_argument("--reset", action="store_true",
                       help="Reset all counters to zero")
    args = parser.parse_args()

    tracker = ForecastTracker(redis_url=args.redis)

    if args.reset:
        await tracker.start()
        pipe = tracker.redis.pipeline()
        for k in [K_TP, K_FP, K_FN, K_PRECISION, K_RECALL, K_F1,
                  K_TOTAL_PRED, K_TOTAL_LIQ, K_LIQ_SET_OLD,
                  K_FP_SEEN, K_PREDICTED_ACTIVE]:
            pipe.delete(k)
        await pipe.execute()
        print("Counters reset.")
        await tracker.stop()
        return

    if args.stats:
        await tracker.print_stats()
    elif args.watch:
        await tracker.run_forever()
    else:
        await tracker.print_stats()


if __name__ == "__main__":
    asyncio.run(main())
