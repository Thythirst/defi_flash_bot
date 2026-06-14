#!/usr/bin/env python3
"""
metrics.py — Watchlist observability dashboard.

Produces:
  - watchlist size over time
  - HF distribution histogram
  - Active debt distribution
  - Oracle trigger frequency
  - New borrowers/day
  - Candidate count
  - Competitor activity
  - RPC health + failover events

Usage:
  python -m services.watchlist.metrics           # Print to stdout
  python -m services.watchlist.metrics --json    # JSON output
  python -m services.watchlist.metrics --loop 30 # Refresh every 30s
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import redis
from dotenv import load_dotenv
load_dotenv(dotenv_path=project_root / ".env")

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("watchlist.metrics")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")


def get_metrics() -> dict:
    """Gather all watchlist metrics from Redis."""
    r = redis.from_url(REDIS_URL, decode_responses=True)

    metrics = {}

    # Watchlist
    watchlist_size = r.zcard("arb:watchlist:active") or 0
    metrics["watchlist_size"] = watchlist_size

    # Top 10 lowest HF
    top = r.zrange("arb:watchlist:active", 0, 9, withscores=True)
    metrics["top10_lowest_hf"] = [{"address": a, "hf": round(s, 4)} for a, s in top]

    # HF distribution (buckets)
    all_scores = r.zrange("arb:watchlist:active", 0, -1, withscores=True)
    hf_buckets = {"<1.00": 0, "1.00-1.05": 0, "1.05-1.10": 0, "1.10-1.20": 0,
                  "1.20-1.50": 0, "1.50-2.00": 0, ">2.00": 0}
    total_debt = 0.0
    debt_buckets = {"<$1K": 0, "$1K-5K": 0, "$5K-50K": 0, ">$50K": 0}

    for addr, hf in all_scores:
        # HF buckets
        if hf < 1.0:
            hf_buckets["<1.00"] += 1
        elif hf < 1.05:
            hf_buckets["1.00-1.05"] += 1
        elif hf < 1.10:
            hf_buckets["1.05-1.10"] += 1
        elif hf < 1.20:
            hf_buckets["1.10-1.20"] += 1
        elif hf < 1.50:
            hf_buckets["1.20-1.50"] += 1
        elif hf < 2.00:
            hf_buckets["1.50-2.00"] += 1
        else:
            hf_buckets[">2.00"] += 1

        # Debt buckets (from user hash)
        user_data = r.hgetall(f"arb:watchlist:user:{addr}")
        debt = float(user_data.get("debt_usd", 0))
        total_debt += debt
        if debt < 1000:
            debt_buckets["<$1K"] += 1
        elif debt < 5000:
            debt_buckets["$1K-5K"] += 1
        elif debt < 50000:
            debt_buckets["$5K-50K"] += 1
        else:
            debt_buckets[">$50K"] += 1

    metrics["hf_distribution"] = hf_buckets
    metrics["debt_distribution"] = debt_buckets
    metrics["total_debt_usd"] = round(total_debt, 2)

    # Candidates (HF < 1.0)
    candidates_count = hf_buckets["<1.00"]
    metrics["candidates"] = candidates_count

    # Meta
    meta = r.hgetall("arb:watchlist:meta")
    metrics["meta"] = {
        "bootstrap_ts": meta.get("bootstrap_ts", "never"),
        "last_refresh_block": meta.get("last_refresh_block", "0"),
        "total_refreshed": meta.get("total_refreshed", "0"),
        "total_pruned": meta.get("total_pruned", "0"),
    }

    # Competitors
    top_competitors = r.zrevrange("arb:watchlist:competitors", 0, 9, withscores=True)
    metrics["top_competitors"] = [{"address": a, "liquidations": int(s)} for a, s in top_competitors]

    # Recent liquidations (last 10)
    recent = r.xrevrange("arb:watchlist:liquidations", count=10)
    metrics["recent_liquidations"] = [
        {"tx": e[1].get("tx_hash", "")[:16], "block": e[1].get("block", ""),
         "liquidator": e[1].get("liquidator", "")[:14]}
        for e in recent
    ]

    # Oracle events (last 10)
    oracle_events = r.xrevrange("arb:watchlist:oracle_events", count=10)
    metrics["recent_oracle_events"] = [
        {"symbol": e[1].get("symbol", ""), "deviation": e[1].get("deviation_pct", "")}
        for e in oracle_events
    ]

    # Metrics hash
    m = r.hgetall("arb:watchlist:metrics")
    metrics["refresh_latency_ms"] = m.get("refresh_latency_ms", "0")
    metrics["rpc_failures"] = m.get("rpc_failures_total", "0")

    r.close()
    return metrics


def print_dashboard(metrics: dict):
    """Pretty-print the metrics dashboard."""
    print(f"\n{'='*60}")
    print(f"  PROGRESSIVE WATCHLIST — DASHBOARD")
    print(f"  {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*60}")
    print(f"  Watchlist size:           {metrics['watchlist_size']:>6}")
    print(f"  Candidates (HF < 1.0):    {metrics['candidates']:>6}")
    print(f"  Total debt monitored:     ${metrics['total_debt_usd']:>10,.2f}")
    print(f"  Refresh latency:          {metrics['refresh_latency_ms']:>6}ms")
    print(f"  RPC failures:             {metrics['rpc_failures']:>6}")

    print(f"\n  ── HF Distribution ──")
    for bucket, count in metrics["hf_distribution"].items():
        bar = "█" * min(count, 40)
        print(f"  {bucket:>12}: {count:>5} {bar}")

    print(f"\n  ── Debt Distribution ──")
    for bucket, count in metrics["debt_distribution"].items():
        print(f"  {bucket:>12}: {count:>5}")

    print(f"\n  ── Top 5 Lowest HF ──")
    for entry in metrics["top10_lowest_hf"][:5]:
        print(f"  {entry['address'][:14]:<14} HF={entry['hf']:.4f}")

    print(f"\n  ── Top 5 Competitors ──")
    for entry in metrics["top_competitors"][:5]:
        print(f"  {entry['address'][:14]:<14} {entry['liquidations']} liquidations")

    print(f"\n  ── Recent Liquidations ──")
    for entry in metrics["recent_liquidations"][:5]:
        print(f"  blk={entry['block']} liq={entry['liquidator']} tx={entry['tx']}")

    print(f"\n  ── Meta ──")
    for k, v in metrics["meta"].items():
        print(f"  {k}: {v}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Watchlist Dashboard")
    parser.add_argument("--json", action="store_true", help="JSON output")
    parser.add_argument("--loop", type=int, default=0, help="Refresh interval in seconds")
    args = parser.parse_args()

    if args.loop:
        while True:
            metrics = get_metrics()
            if args.json:
                print(json.dumps(metrics))
            else:
                print_dashboard(metrics)
            time.sleep(args.loop)
    else:
        metrics = get_metrics()
        if args.json:
            print(json.dumps(metrics, indent=2))
        else:
            print_dashboard(metrics)
