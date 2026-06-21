#!/usr/bin/env python3
"""
competitor_summary.py — Weekly competitor intelligence report
Reads liquidations.db and Redis comp:stats to profile who's winning
races, at what gas price, and which positions they target.

Cron entry (Monday 10:00 UTC):
    0 10 * * 1 /home/ubuntu/defi_flash_bot/venv/bin/python3 \
        /home/ubuntu/defi_flash_bot/scripts/competitor_summary.py \
        >> /home/ubuntu/defi_flash_bot/logs/competitor_summary.log 2>&1

Requires: liquidations.db with at least some lost_race/lost_race_observed rows.
Output:   logs + /home/ubuntu/defi_flash_bot/logs/competitor_weekly.json
"""

import json
import logging
import os
import sqlite3
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

PROD_DIR    = Path("/home/ubuntu/defi_flash_bot")
OUTCOMES_DB = PROD_DIR / "liquidations.db"
REPORT_FILE = PROD_DIR / "logs" / "competitor_weekly.json"
REDIS_URL   = os.getenv("REDIS_URL", "redis://localhost:6379")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class CompetitorProfile:
    address:          str
    wins:             int
    avg_gas_gwei:     float
    first_seen_ts:    int
    last_seen_ts:     int
    top_collateral:   str = ""
    top_debt:         str = ""
    races_vs_us:      int = 0   # times they beat us when we also submitted


@dataclass
class WeeklyReport:
    generated_at:         str
    period_days:          int
    total_liquidations:   int   # all observed on-chain
    our_confirmed:        int
    our_reverted:         int
    races_lost:           int   # we submitted, they won
    observed_losses:      int   # they won, we never submitted
    our_win_rate:         float
    total_competitors:    int
    top_competitors:      list[CompetitorProfile] = field(default_factory=list)
    gas_analysis:         dict = field(default_factory=dict)
    timing_analysis:      dict = field(default_factory=dict)
    recommendations:      list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Queries
# ---------------------------------------------------------------------------

def get_period_stats(conn: sqlite3.Connection, days: int = 7) -> dict:
    """Overall stats for the reporting period."""
    cutoff = time.time() - (days * 86400)
    stats  = {}

    # Our outcomes
    rows = conn.execute("""
        SELECT result, COUNT(*) as n
        FROM outcomes
        WHERE submitted_at > ?
        GROUP BY result
    """, (cutoff,)).fetchall()

    result_map = {r: n for r, n in rows}
    stats["confirmed"]       = result_map.get("confirmed", 0)
    stats["reverted"]        = result_map.get("reverted", 0)
    stats["lost_race"]       = result_map.get("lost_race", 0)
    stats["lost_race_obs"]   = result_map.get("lost_race_observed", 0)
    stats["pending"]         = result_map.get("pending", 0)
    stats["total_observed"]  = sum(result_map.values())

    total_attempts = stats["confirmed"] + stats["reverted"] + stats["lost_race"]
    stats["win_rate"] = (
        stats["confirmed"] / total_attempts
        if total_attempts > 0 else 0.0
    )
    return stats


def get_top_competitors(
    conn:     sqlite3.Connection,
    limit:    int = 10,
    days:     int = 7,
) -> list[CompetitorProfile]:
    """Top competitors by win count in the period."""
    cutoff = time.time() - (days * 86400)

    rows = conn.execute("""
        SELECT
            c.address,
            c.wins,
            c.avg_gas_price,
            c.first_seen,
            c.last_seen,
            COUNT(CASE WHEN o.result='lost_race' THEN 1 END) as races_vs_us
        FROM competitors c
        LEFT JOIN outcomes o ON o.competitor_address = c.address
            AND o.submitted_at > ?
        WHERE c.last_seen > ?
        GROUP BY c.address
        ORDER BY c.wins DESC
        LIMIT ?
    """, (cutoff, cutoff, limit)).fetchall()

    profiles = []
    for addr, wins, avg_gas, first_seen, last_seen, races_vs_us in rows:
        # Find their most common collateral target
        top_row = conn.execute("""
            SELECT collateral_asset, COUNT(*) as n
            FROM outcomes
            WHERE competitor_address = ? AND submitted_at > ?
            GROUP BY collateral_asset ORDER BY n DESC LIMIT 1
        """, (addr, cutoff)).fetchone()

        profiles.append(CompetitorProfile(
            address       = addr,
            wins          = wins or 0,
            avg_gas_gwei  = round((avg_gas or 0) / 1e9, 6),
            first_seen_ts = first_seen or 0,
            last_seen_ts  = last_seen or 0,
            top_collateral= top_row[0][:10] if top_row else "unknown",
            races_vs_us   = races_vs_us or 0,
        ))
    return profiles


def get_gas_analysis(conn: sqlite3.Connection, days: int = 7) -> dict:
    """
    Gas price comparison: our bids vs competitor bids.
    Key question: by how much are we losing on gas?
    """
    cutoff = time.time() - (days * 86400)

    # Our gas on confirmed txs
    our_gas = conn.execute("""
        SELECT AVG(gas_cost_usd) as avg_cost_usd
        FROM outcomes
        WHERE result='confirmed' AND submitted_at > ? AND gas_cost_usd > 0
    """, (cutoff,)).fetchone()

    # Competitor avg gas (from running average in competitors table)
    comp_gas = conn.execute("""
        SELECT AVG(avg_gas_price) as avg_comp_gas
        FROM competitors
        WHERE last_seen > ? AND avg_gas_price > 0
    """, (cutoff,)).fetchone()

    # Races we lost — were we outbid?
    race_rows = conn.execute("""
        SELECT COUNT(*) as lost, AVG(c.avg_gas_price) as comp_avg
        FROM outcomes o
        JOIN competitors c ON c.address = o.competitor_address
        WHERE o.result = 'lost_race'
          AND o.submitted_at > ?
          AND c.avg_gas_price > 0
    """, (cutoff,)).fetchone()

    return {
        "our_avg_gas_cost_usd": round(our_gas[0] or 0, 4),
        "comp_avg_gas_gwei":    round((comp_gas[0] or 0) / 1e9, 6),
        "races_lost_to_gas":    race_rows[0] if race_rows else 0,
        "comp_avg_on_losses":   round((race_rows[1] or 0) / 1e9, 6) if race_rows else 0,
    }


def get_timing_analysis(conn: sqlite3.Connection, days: int = 7) -> dict:
    """When do liquidations happen? Peak hours for cascade events."""
    cutoff = time.time() - (days * 86400)

    rows = conn.execute("""
        SELECT
            CAST(strftime('%H', datetime(submitted_at, 'unixepoch')) AS INTEGER) as hour,
            COUNT(*) as n,
            SUM(CASE WHEN result='confirmed' THEN 1 ELSE 0 END) as ours
        FROM outcomes
        WHERE submitted_at > ?
        GROUP BY hour
        ORDER BY n DESC
    """, (cutoff,)).fetchall()

    peak_hours = [{"hour": h, "total": n, "ours": o} for h, n, o in rows[:6]]
    return {"peak_hours_utc": peak_hours}


def generate_recommendations(
    stats:       dict,
    competitors: list[CompetitorProfile],
    gas:         dict,
) -> list[str]:
    """Actionable recommendations from the data."""
    recs = []

    # Gas competitiveness
    comp_gas = gas.get("comp_avg_on_losses", 0)
    if comp_gas > 0:
        if comp_gas > 0.1:
            recs.append(
                f"Competitors paying {comp_gas:.4f}gwei on races you lose — "
                f"raise surge_buffer in gas_oracle.py to cover this"
            )
        else:
            recs.append(
                f"Gas competitive — competitors paying {comp_gas:.4f}gwei, "
                f"similar to your current strategy"
            )

    # Win rate
    wr = stats.get("win_rate", 0)
    if wr == 0 and stats["total_observed"] > 0:
        recs.append(
            "Win rate 0% — check if positions are reaching _execute_liquidation. "
            "Verify HFEngine trigger is firing on HF < 1.0"
        )
    elif 0 < wr < 0.3:
        recs.append(
            f"Win rate {wr:.1%} — losing most contested races. "
            f"Consider raising gas oracle percentile to P90"
        )
    elif wr >= 0.7:
        recs.append(f"Win rate {wr:.1%} — gas strategy competitive")

    # Competitor concentration
    if competitors:
        top = competitors[0]
        if top.wins > 10:
            recs.append(
                f"Top competitor {top.address[:10]}… won {top.wins}x "
                f"avg {top.avg_gas_gwei:.4f}gwei — "
                f"study their gas pattern to match"
            )

    # No data yet
    if stats["total_observed"] == 0:
        recs.append(
            "No liquidation events recorded yet — market quiet or "
            "liq_log_parser not receiving events. "
            "Check WSManager liq_monitor connection."
        )

    return recs


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def build_report(days: int = 7) -> WeeklyReport:
    if not OUTCOMES_DB.exists():
        logger.warning("[CompetitorSummary] liquidations.db not found")
        return WeeklyReport(
            generated_at       = datetime.now(timezone.utc).isoformat(),
            period_days        = days,
            total_liquidations = 0,
            our_confirmed      = 0,
            our_reverted       = 0,
            races_lost         = 0,
            observed_losses    = 0,
            our_win_rate       = 0.0,
            total_competitors  = 0,
            recommendations    = ["liquidations.db not found — outcome DB not initialised"],
        )

    conn = sqlite3.connect(str(OUTCOMES_DB))

    stats       = get_period_stats(conn, days)
    competitors = get_top_competitors(conn, limit=10, days=days)
    gas         = get_gas_analysis(conn, days)
    timing      = get_timing_analysis(conn, days)
    recs        = generate_recommendations(stats, competitors, gas)

    total_comp = conn.execute("SELECT COUNT(*) FROM competitors").fetchone()[0]
    conn.close()

    return WeeklyReport(
        generated_at       = datetime.now(timezone.utc).isoformat(),
        period_days        = days,
        total_liquidations = stats["total_observed"],
        our_confirmed      = stats["confirmed"],
        our_reverted       = stats["reverted"],
        races_lost         = stats["lost_race"],
        observed_losses    = stats["lost_race_obs"],
        our_win_rate       = stats["win_rate"],
        total_competitors  = total_comp,
        top_competitors    = competitors,
        gas_analysis       = gas,
        timing_analysis    = timing,
        recommendations    = recs,
    )


def print_report(r: WeeklyReport) -> None:
    print("\n" + "═" * 60)
    print(f"  COMPETITOR INTELLIGENCE — Last {r.period_days} days")
    print(f"  {r.generated_at}")
    print("═" * 60)

    print(f"\n── Our Performance ──────────────────────────────────────")
    print(f"  Confirmed:        {r.our_confirmed:>6}")
    print(f"  Reverted:         {r.our_reverted:>6}")
    print(f"  Races lost:       {r.races_lost:>6}  (we submitted, they won)")
    print(f"  Observed losses:  {r.observed_losses:>6}  (they won, we never submitted)")
    print(f"  Win rate:         {r.our_win_rate:>5.1%}")

    print(f"\n── Gas Analysis ─────────────────────────────────────────")
    g = r.gas_analysis
    print(f"  Competitor avg gas:          {g.get('comp_avg_gas_gwei', 0):.6f} gwei")
    print(f"  Comp gas on our losses:      {g.get('comp_avg_on_losses', 0):.6f} gwei")
    print(f"  Our avg gas cost:            {g.get('our_avg_gas_cost_usd', 0):.4f} USD")

    if r.top_competitors:
        print(f"\n── Top {len(r.top_competitors)} Competitors ─────────────────────────────────")
        print(f"  {'Address':14}  {'Wins':>6}  {'Avg Gas':>12}  {'vs Us':>8}  {'Last Seen'}")
        for c in r.top_competitors:
            last = datetime.fromtimestamp(c.last_seen_ts, tz=timezone.utc).strftime("%m/%d %H:%M") \
                   if c.last_seen_ts else "never"
            print(
                f"  {c.address[:12]}…  {c.wins:>6}  "
                f"{c.avg_gas_gwei:>10.6f}g  {c.races_vs_us:>6}x  {last}"
            )
    else:
        print(f"\n── Competitors ──────────────────────────────────────────")
        print(f"  No competitor data yet — waiting for first liquidation event")

    if r.timing_analysis.get("peak_hours_utc"):
        print(f"\n── Peak Hours (UTC) ─────────────────────────────────────")
        for h in r.timing_analysis["peak_hours_utc"][:5]:
            bar = "█" * min(h["total"], 30)
            print(f"  {h['hour']:02d}:00  {h['total']:>5} liq  {bar}")

    print(f"\n── Recommendations ──────────────────────────────────────")
    for rec in r.recommendations:
        print(f"  • {rec}")

    print("═" * 60 + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    logger.info("[CompetitorSummary] Generating weekly report...")

    report = build_report(days=7)
    print_report(report)

    # Save JSON for downstream tools
    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(json.dumps(asdict(report), indent=2))
    logger.info(f"[CompetitorSummary] Report saved to {REPORT_FILE}")

    # Also run a 30-day lookback for trend context
    if report.total_liquidations > 0:
        logger.info("[CompetitorSummary] Running 30-day lookback...")
        report_30 = build_report(days=30)
        out_30 = REPORT_FILE.parent / "competitor_monthly.json"
        out_30.write_text(json.dumps(asdict(report_30), indent=2))


if __name__ == "__main__":
    main()
