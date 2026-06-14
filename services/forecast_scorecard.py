"""
services/forecast_scorecard.py — Per-User Liquidation Forecast Scorecard.

Tracks each user's journey from first forecast entry through liquidation
(or expiry). Produces rolling precision, recall, lead time, and
forecast-to-liquidation conversion rates.

Per-user HASH:  forecast:scorecard:{user}
  first_seen_ts             — unix timestamp of first forecast entry
  first_seen_hf             — HF at first entry
  first_seen_rank           — rank position (1-indexed) at first entry
  first_seen_buffer         — buffer_pct at first entry
  first_seen_debt_usd       — total debt USD at first entry
  first_seen_coll_usd       — total collateral USD at first entry
  entered_liq_ts            — timestamp when first appeared in aave:liquidatable
  entered_liq_hf            — HF when entered liquidatable
  time_to_liq_s             — seconds from first_seen to liquidatable
  exited_liq_ts             — timestamp when exited aave:liquidatable
  was_liquidated            — "1" if liquidated (coll=0 debt=0 after HF<1)
  bot_would_submit          — "1" if bot would have submitted a bundle
  est_profit_usd            — estimated profit from liquidation
  times_in_ranking          — count of forecast appearances
  last_seen_ts              — last time in forecast:ranking
  outcome                   — pending | liquidatable | liquidated | expired_safe

Aggregate metrics:
  forecast:scorecard:total_tracked       INT
  forecast:scorecard:total_liquidatable  INT
  forecast:scorecard:total_liquidated    INT
  forecast:scorecard:total_profitable    INT
  forecast:scorecard:precision           FLOAT
  forecast:scorecard:recall              FLOAT
  forecast:scorecard:avg_lead_time_s     FLOAT
  forecast:scorecard:conversion_rate     FLOAT  (liquidatable / tracked)
  forecast:scorecard:liquidation_rate    FLOAT  (liquidated / liquidatable)

Usage:
  python -m services.forecast_scorecard --watch --interval 30
  python -m services.forecast_scorecard --dashboard        # full scorecard
  python -m services.forecast_scorecard --dashboard --top 10
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set

import redis.asyncio as redis
from dotenv import load_dotenv

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
load_dotenv(project_root / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | scorecard | %(message)s",
)
logger = logging.getLogger("forecast_scorecard")

# ── Constants ─────────────────────────────────────────────────

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
CYCLE_INTERVAL = int(os.getenv("SCORECARD_INTERVAL", "30"))

# Redis keys
SC_PREFIX     = "forecast:scorecard:"
SC_TOTAL      = "forecast:scorecard:total_tracked"
SC_LIQ_TOTAL  = "forecast:scorecard:total_liquidatable"
SC_LIQ_DONE   = "forecast:scorecard:total_liquidated"
SC_PROFITABLE = "forecast:scorecard:total_profitable"
SC_PRECISION  = "forecast:scorecard:precision"
SC_RECALL     = "forecast:scorecard:recall"
SC_AVG_LEAD   = "forecast:scorecard:avg_lead_time_s"
SC_CONVERSION = "forecast:scorecard:conversion_rate"
SC_LIQ_RATE   = "forecast:scorecard:liquidation_rate"
SC_LAST_CYCLE = "forecast:scorecard:last_cycle"
SC_LIQ_PREV   = "forecast:scorecard:prev_liq_set"  # previous liquidatable snapshot

# ADDR_TO_SYMBOL from liq_forecast for reserve lookups
ADDR_TO_SYMBOL: Dict[str, str] = {
    "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": "WETH",
    "0xaf88d065e77c8cc2239327c5edb3a432268e5831": "USDC",
    "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": "USDT",
    "0x912ce59144191c1204e64559fe8253a0e49e6548": "ARB",
    "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f": "WBTC",
    "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": "DAI",
    "0xf97f4df75117a78c1a5a0dbb814af92458539fb4": "LINK",
    "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8": "USDC.e",
}


def _safe_float(val) -> float:
    """Parse float, returning 0.0 for empty/missing values."""
    if val is None or val == "":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


@dataclass
class ScorecardEntry:
    user: str
    first_seen_ts: float = 0
    first_seen_hf: float = 0
    first_seen_rank: int = 0
    first_seen_buffer: float = 0
    first_seen_debt_usd: float = 0
    first_seen_coll_usd: float = 0
    entered_liq_ts: float = 0
    time_to_liq_s: int = 0
    was_liquidated: bool = False
    outcome: str = "pending"
    times_in_ranking: int = 0


class ForecastScorecard:
    """Per-user liquidation forecast scorecard with rolling metrics."""

    def __init__(self, redis_url: str = ""):
        self.redis_url = redis_url or REDIS_URL
        self.redis: Optional[redis.Redis] = None

    async def start(self):
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        await self.redis.ping()
        logger.info("started interval=%ds", CYCLE_INTERVAL)

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
                elapsed = (time.monotonic() - t0) * 1000
                await self._compute_aggregates()
                logger.info(
                    "new_cards=%d entered_liq=%d exited_liq=%d %.0fms",
                    r["new_cards"], r["entered_liq"], r["exited_liq"], elapsed,
                )
                await asyncio.sleep(CYCLE_INTERVAL)
        finally:
            await self.stop()

    async def _cycle(self) -> dict:
        now = int(time.time())

        # ── 1. Snapshot forecast:ranking — ALL users ──────────
        ranking: List[tuple] = []
        try:
            raw = await self.redis.zrange(
                "forecast:ranking", 0, -1, withscores=True,
            )
            ranking = [(u, s) for u, s in raw]
        except Exception as e:
            logger.warning("forecast:ranking read: %s", e)

        new_cards = 0
        for rank_0, (user, buffer_pct) in enumerate(ranking):
            rank = rank_0 + 1
            key = f"{SC_PREFIX}{user}"
            exists = await self.redis.exists(key)

            if not exists:
                # ── New user: create scorecard ────────────────
                hf = await self._get_hf(user)
                debt_usd, coll_usd = await self._get_exposure(user)

                await self.redis.hset(key, mapping={
                    "first_seen_ts": str(now),
                    "first_seen_hf": str(round(hf, 4)) if hf != float("inf") else "inf",
                    "first_seen_rank": str(rank),
                    "first_seen_buffer": str(round(buffer_pct, 2)),
                    "first_seen_debt_usd": str(round(debt_usd, 2)),
                    "first_seen_coll_usd": str(round(coll_usd, 2)),
                    "times_in_ranking": "1",
                    "last_seen_ts": str(now),
                    "entered_liq_ts": "",
                    "entered_liq_hf": "",
                    "time_to_liq_s": "",
                    "exited_liq_ts": "",
                    "was_liquidated": "0",
                    "bot_would_submit": "0",
                    "est_profit_usd": "",
                    "outcome": "pending",
                })
                new_cards += 1
            else:
                # Update times_in_ranking + last_seen
                pipe = self.redis.pipeline()
                pipe.hincrby(key, "times_in_ranking", 1)
                pipe.hset(key, "last_seen_ts", str(now))
                await pipe.execute()

        # ── 2. Detect liquidatable entry/exit ─────────────────
        current_liq: Set[str] = set()
        try:
            users = await self.redis.zrangebyscore(
                "aave:liquidatable", 0.000001, 0.999999,
            )
            current_liq = set(users)
        except Exception as e:
            logger.warning("aave:liquidatable read: %s", e)

        previous_liq: Set[str] = set()
        try:
            prev = await self.redis.smembers(SC_LIQ_PREV)
            previous_liq = set(prev)
        except Exception:
            pass

        entered = current_liq - previous_liq   # newly liquidatable
        exited = previous_liq - current_liq     # no longer liquidatable

        entered_liq = 0
        for user in entered:
            await self._on_enter_liquidatable(user, now)
            entered_liq += 1

        exited_liq = 0
        for user in exited:
            await self._on_exit_liquidatable(user, now)
            exited_liq += 1

        # Update previous snapshot
        try:
            pipe = self.redis.pipeline()
            pipe.delete(SC_LIQ_PREV)
            if current_liq:
                pipe.sadd(SC_LIQ_PREV, *current_liq)
            await pipe.execute()
        except Exception as e:
            logger.debug("liq_set: %s", e)

        return {"new_cards": new_cards, "entered_liq": entered_liq, "exited_liq": exited_liq}

    # ── Lifecycle events ──────────────────────────────────────

    async def _on_enter_liquidatable(self, user: str, now: int):
        """User just appeared in aave:liquidatable."""
        key = f"{SC_PREFIX}{user}"
        exists = await self.redis.exists(key)
        if not exists:
            return  # never tracked — skip

        first_seen = float(await self.redis.hget(key, "first_seen_ts") or 0)
        time_to_liq = int(now - first_seen) if first_seen > 0 else 0
        hf_now = await self._get_hf(user)

        await self.redis.hset(key, mapping={
            "entered_liq_ts": str(now),
            "entered_liq_hf": str(round(hf_now, 4)),
            "time_to_liq_s": str(time_to_liq),
            "outcome": "liquidatable",
        })

    async def _on_exit_liquidatable(self, user: str, now: int):
        """User left aave:liquidatable — either rescued or liquidated."""
        key = f"{SC_PREFIX}{user}"
        exists = await self.redis.exists(key)
        if not exists:
            return

        # Check if user still exists and has zero positions (was liquidated)
        user_key = f"aave:user:{user}"
        user_exists = await self.redis.exists(user_key)
        was_liquidated = False

        if user_exists:
            pos_raw = await self.redis.hget(user_key, "positions")
            if pos_raw:
                try:
                    pos = json.loads(pos_raw)
                    has_coll = any(v.get("collateral", 0) > 0 for v in pos.values())
                    has_debt = any(v.get("debt", 0) > 0 for v in pos.values())
                    # If user had HF<1 but now has no debt and no collateral,
                    # they were likely liquidated
                    if not has_coll and not has_debt:
                        was_liquidated = True
                except (json.JSONDecodeError, TypeError):
                    pass
        else:
            # User key deleted — was pruned by reconciler (fully exited)
            was_liquidated = True

        mapping = {
            "exited_liq_ts": str(now),
        }
        if was_liquidated:
            mapping["was_liquidated"] = "1"
            mapping["outcome"] = "liquidated"

        await self.redis.hset(key, mapping=mapping)

    # ── Helpers ───────────────────────────────────────────────

    async def _get_hf(self, user: str) -> float:
        raw = await self.redis.hget(f"aave:user:{user}", "health_factor")
        if raw and raw != "inf":
            try:
                return float(raw)
            except (ValueError, TypeError):
                pass
        return float("inf")

    async def _get_exposure(self, user: str) -> tuple:
        """Return (debt_usd, coll_usd) from positions summing."""
        pos_raw = await self.redis.hget(f"aave:user:{user}", "positions")
        if not pos_raw:
            return 0.0, 0.0
        try:
            pos = json.loads(pos_raw)
        except (json.JSONDecodeError, TypeError):
            return 0.0, 0.0

        total_debt = 0.0
        total_coll = 0.0
        for reserve_addr, p in pos.items():
            cfg = await self.redis.hgetall(f"aave:reserve:{reserve_addr}")
            if not cfg or "decimals" not in cfg:
                continue
            dec = int(cfg["decimals"])
            price_raw = cfg.get("price", "0")
            if not price_raw or price_raw == "0":
                continue
            price = int(price_raw) / 1e8

            debt = p.get("debt", 0)
            coll = p.get("collateral", 0)
            if debt > 0:
                total_debt += (debt / (10 ** dec)) * price
            if coll > 0:
                total_coll += (coll / (10 ** dec)) * price

        return total_debt, total_coll

    # ── Aggregate metrics ─────────────────────────────────────

    async def _compute_aggregates(self):
        """Compute rolling precision, recall, lead time, conversion rates."""
        try:
            # Scan all scorecards for counts
            total_tracked = 0
            total_liq = 0
            total_liquidated = 0
            lead_times = []

            cursor = 0
            while True:
                cursor, keys = await self.redis.scan(
                    cursor, match=f"{SC_PREFIX}0x*", count=200,
                )
                for key in keys:
                    data = await self.redis.hgetall(key)
                    if not data or not data.get("first_seen_ts"):
                        continue
                    total_tracked += 1

                    if data.get("entered_liq_ts"):
                        total_liq += 1
                        ttl = data.get("time_to_liq_s")
                        if ttl:
                            lead_times.append(int(ttl))

                    if data.get("was_liquidated") == "1":
                        total_liquidated += 1

                if cursor == 0:
                    break

            avg_lead = sum(lead_times) / len(lead_times) if lead_times else 0.0
            conversion = total_liq / total_tracked if total_tracked > 0 else 0.0
            liq_rate = total_liquidated / total_liq if total_liq > 0 else 0.0

            # Precision = liquidated / tracked (how many predictions resulted in liquidation)
            precision = total_liquidated / total_tracked if total_tracked > 0 else 0.0
            # Recall: of all users who entered aave:liquidatable, what fraction
            # had a scorecard AND were liquidated? Use tracker's cumulative count
            # as denominator — it tracks ALL liquidatable entries independently.
            tracker_liq_total = int(await self.redis.get(
                "forecast:tracker:total_liquidated",
            ) or 0)
            recall = total_liquidated / tracker_liq_total if tracker_liq_total > 0 else 0.0

            pipe = self.redis.pipeline()
            pipe.set(SC_TOTAL, str(total_tracked))
            pipe.set(SC_LIQ_TOTAL, str(total_liq))
            pipe.set(SC_LIQ_DONE, str(total_liquidated))
            pipe.set(SC_PRECISION, str(round(precision, 4)))
            pipe.set(SC_RECALL, str(round(recall, 4)))
            pipe.set(SC_AVG_LEAD, str(round(avg_lead, 1)))
            pipe.set(SC_CONVERSION, str(round(conversion, 4)))
            pipe.set(SC_LIQ_RATE, str(round(liq_rate, 4)))
            pipe.set(SC_LAST_CYCLE, str(int(time.time())))
            await pipe.execute()

        except Exception as e:
            logger.warning("aggregates: %s", e)

    # ── Dashboard ─────────────────────────────────────────────

    async def print_dashboard(self, top_n: int = 20):
        """Print full scorecard dashboard."""
        await self.start()
        try:
            # Aggregates
            total = int(await self.redis.get(SC_TOTAL) or 0)
            total_liq = int(await self.redis.get(SC_LIQ_TOTAL) or 0)
            total_done = int(await self.redis.get(SC_LIQ_DONE) or 0)
            precision = float(await self.redis.get(SC_PRECISION) or 0)
            recall = float(await self.redis.get(SC_RECALL) or 0)
            avg_lead = float(await self.redis.get(SC_AVG_LEAD) or 0)
            conversion = float(await self.redis.get(SC_CONVERSION) or 0)
            liq_rate = float(await self.redis.get(SC_LIQ_RATE) or 0)

            print(f"\n{'='*70}")
            print(f"  FORECAST SCORECARD — {time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            print(f"{'='*70}")
            print(f"  Total Tracked:     {total:>8}")
            print(f"  Became Liquidatable: {total_liq:>6}  ({conversion:.1%} conversion)")
            print(f"  Actually Liquidated: {total_done:>6}  ({liq_rate:.1%} of liquidatable)")
            print(f"")
            print(f"  Precision:  {precision:.1%}  (liquidated / tracked)")
            print(f"  Recall:     {recall:.1%}  (caught / all liquidations)")
            print(f"  Avg Lead Time: {avg_lead:,.0f}s  (first_seen → liquidatable)")
            print(f"")

            # Top N scorecards by recency
            print(f"  {'TOP'} {top_n} RECENT SCORECARDS:")
            print(f"  {'User':<18} {'1st HF':>7} {'Rank':>5} {'Debt $':>9} "
                  f"{'→Liq?':>6} {'Lead':>7} {'Outcome':<14}")
            print(f"  {'-'*18} {'-'*7} {'-'*5} {'-'*9} {'-'*6} {'-'*7} {'-'*14}")

            # Get most recently updated scorecards
            keys = await self.redis.keys(f"{SC_PREFIX}0x*")
            entries: List[ScorecardEntry] = []
            for key in keys[:100]:  # limit for performance
                data = await self.redis.hgetall(key)
                if not data:
                    continue
                e = ScorecardEntry(
                    user=key.replace(SC_PREFIX, ""),
                    first_seen_ts=_safe_float(data.get("first_seen_ts")),
                    first_seen_hf=_safe_float(data.get("first_seen_hf")),
                    first_seen_rank=int(data.get("first_seen_rank", 0)),
                    first_seen_debt_usd=_safe_float(data.get("first_seen_debt_usd")),
                    entered_liq_ts=_safe_float(data.get("entered_liq_ts")),
                    time_to_liq_s=int(data.get("time_to_liq_s") or 0),
                    was_liquidated=data.get("was_liquidated") == "1",
                    outcome=data.get("outcome", "pending"),
                )
                entries.append(e)

            # Sort by last_seen desc
            entries.sort(key=lambda e: -e.first_seen_ts)

            for e in entries[:top_n]:
                liq_mark = "✓" if e.entered_liq_ts > 0 else "—"
                lead = f"{e.time_to_liq_s}s" if e.time_to_liq_s > 0 else "—"
                outcome_icon = {"pending": "⏳", "liquidatable": "🔴",
                                "liquidated": "💀", "expired_safe": "🟢"}.get(e.outcome, "?")
                print(f"  {e.user[:16]:<18} {e.first_seen_hf:>7.3f} {e.first_seen_rank:>5} "
                      f"${e.first_seen_debt_usd:>8,.0f} {liq_mark:>6} {lead:>7} "
                      f"{outcome_icon} {e.outcome:<11}")

            print(f"{'='*70}\n")
        finally:
            await self.stop()


# ── CLI ──────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(
        description="Forecast Scorecard — per-user liquidation tracking",
    )
    parser.add_argument("--redis", default="redis://localhost:6379")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--dashboard", action="store_true")
    parser.add_argument("--top", type=int, default=20)
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()

    sc = ForecastScorecard(redis_url=args.redis)

    if args.reset:
        await sc.start()
        # Delete all scorecard keys and metrics
        keys = await sc.redis.keys(f"{SC_PREFIX}0x*")
        pipe = sc.redis.pipeline()
        for k in keys:
            pipe.delete(k)
        for k in [SC_TOTAL, SC_LIQ_TOTAL, SC_LIQ_DONE, SC_PROFITABLE,
                  SC_PRECISION, SC_RECALL, SC_AVG_LEAD, SC_CONVERSION,
                  SC_LIQ_RATE, SC_LAST_CYCLE, SC_LIQ_PREV]:
            pipe.delete(k)
        await pipe.execute()
        print(f"Reset {len(keys)} scorecards + metrics.")
        await sc.stop()
        return

    if args.dashboard:
        await sc.print_dashboard(top_n=args.top)
    elif args.watch:
        await sc.run_forever()
    else:
        await sc.print_dashboard(top_n=args.top)


if __name__ == "__main__":
    asyncio.run(main())
