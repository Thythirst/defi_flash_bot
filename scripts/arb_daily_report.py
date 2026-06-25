#!/usr/bin/env python3
"""
arb_daily_report.py — Daily arbitrage report from SQLite data.

Reads arb_scanner.db, generates a summary of:
- Opportunities found (count, best, avg profit)
- Gas trends (min/max/avg base fee)
- Competitor activity (who, how many, method breakdown)
- Submissions (if any executed)

Outputs JSON + Markdown for Telegram delivery.

Usage:
    venv/bin/python3 scripts/arb_daily_report.py [--date YYYY-MM-DD]
"""
from __future__ import annotations

import json
import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

project_root = Path(__file__).parent.parent
DB_PATH = project_root / "data" / "arb_scanner.db"


def get_date_range(date_str: Optional[str] = None):
    """Return (start_iso, end_iso) for the given date (UTC)."""
    if date_str:
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    return dt.isoformat(), (dt + timedelta(days=1)).isoformat()


def query_opportunities(conn: sqlite3.Connection, start: str, end: str) -> dict:
    """Opportunity summary for the day."""
    row = conn.execute(
        """SELECT
             COUNT(*) as total,
             COALESCE(SUM(net_profit_usd), 0) as total_profit,
             COALESCE(AVG(net_profit_usd), 0) as avg_profit,
             COALESCE(MAX(net_profit_usd), 0) as best_profit,
             COALESCE(AVG(spread_pct), 0) as avg_spread
           FROM opportunities
           WHERE timestamp >= ? AND timestamp < ?""",
        (start, end),
    ).fetchone()

    # Top 5 opportunities
    top5 = conn.execute(
        """SELECT token_in, token_out, buy_dex, sell_dex, net_profit_usd, spread_pct, timestamp
           FROM opportunities
           WHERE timestamp >= ? AND timestamp < ?
           ORDER BY net_profit_usd DESC LIMIT 5""",
        (start, end),
    ).fetchall()

    # Breakdown by pair
    pairs = conn.execute(
        """SELECT
             SUBSTR(token_in, 1, 10) || '→' || SUBSTR(token_out, 1, 10) as pair,
             buy_dex || '→' || sell_dex as route,
             COUNT(*) as count,
             ROUND(AVG(net_profit_usd), 2) as avg_profit,
             ROUND(MAX(net_profit_usd), 2) as max_profit
           FROM opportunities
           WHERE timestamp >= ? AND timestamp < ?
           GROUP BY token_in, token_out, buy_dex, sell_dex
           ORDER BY count DESC LIMIT 10""",
        (start, end),
    ).fetchall()

    # Hourly distribution
    hourly = conn.execute(
        """SELECT
             CAST(SUBSTR(timestamp, 12, 2) AS INTEGER) as hour,
             COUNT(*) as count,
             ROUND(AVG(net_profit_usd), 2) as avg_profit
           FROM opportunities
           WHERE timestamp >= ? AND timestamp < ?
           GROUP BY hour ORDER BY hour""",
        (start, end),
    ).fetchall()

    return {
        "total": row[0],
        "total_profit_usd": round(row[1], 2),
        "avg_profit_usd": round(row[2], 2),
        "best_profit_usd": round(row[3], 2),
        "avg_spread_pct": round(row[4], 4),
        "top5": [
            {
                "token_in": t[0][:12], "token_out": t[1][:12],
                "buy_dex": t[2], "sell_dex": t[3],
                "net_profit": round(t[4], 2), "spread_pct": round(t[5], 4),
                "time": t[6][11:19] if t[6] else "",
            }
            for t in top5
        ],
        "by_pair": [
            {"pair": p[0], "route": p[1], "count": p[2],
             "avg_profit": p[3], "max_profit": p[4]}
            for p in pairs
        ],
        "by_hour": [
            {"hour": h[0], "count": h[1], "avg_profit": h[2]}
            for h in hourly
        ],
    }


def query_gas(conn: sqlite3.Connection, start: str, end: str) -> dict:
    """Gas summary for the day."""
    row = conn.execute(
        """SELECT
             COUNT(*) as samples,
             ROUND(AVG(base_fee_gwei), 4) as avg_base,
             ROUND(MIN(base_fee_gwei), 4) as min_base,
             ROUND(MAX(base_fee_gwei), 4) as max_base,
             ROUND(AVG(eth_price_usd), 2) as avg_eth_price
           FROM gas_samples
           WHERE timestamp >= ? AND timestamp < ?""",
        (start, end),
    ).fetchone()

    # Hourly gas trend
    hourly = conn.execute(
        """SELECT
             CAST(SUBSTR(timestamp, 12, 2) AS INTEGER) as hour,
             ROUND(AVG(base_fee_gwei), 4) as avg_base,
             ROUND(MAX(base_fee_gwei), 4) as max_base
           FROM gas_samples
           WHERE timestamp >= ? AND timestamp < ?
           GROUP BY hour ORDER BY hour""",
        (start, end),
    ).fetchall()

    return {
        "samples": row[0],
        "avg_base_gwei": row[1],
        "min_base_gwei": row[2],
        "max_base_gwei": row[3],
        "avg_eth_price": row[4] or 0,
        "trend": [
            {"hour": h[0], "avg_gwei": h[1], "max_gwei": h[2]}
            for h in hourly
        ],
    }


def query_competitors(conn: sqlite3.Connection, start: str, end: str) -> dict:
    """Competitor activity for the day."""
    total = conn.execute(
        "SELECT COUNT(*) FROM competitor_txns WHERE timestamp >= ? AND timestamp < ?",
        (start, end),
    ).fetchone()[0]

    by_bot = conn.execute(
        """SELECT method, COUNT(*) as count
           FROM competitor_txns
           WHERE timestamp >= ? AND timestamp < ?
           GROUP BY method ORDER BY count DESC LIMIT 10""",
        (start, end),
    ).fetchall()

    avg_gas = conn.execute(
        """SELECT ROUND(AVG(gas_price_gwei), 2)
           FROM competitor_txns
           WHERE timestamp >= ? AND timestamp < ? AND gas_price_gwei > 0""",
        (start, end),
    ).fetchone()

    return {
        "total": total,
        "avg_gas_gwei": round(avg_gas[0], 2) if avg_gas and avg_gas[0] else 0,
        "breakdown": [
            {"bot": b[0], "count": b[1]} for b in by_bot
        ],
    }


def query_submissions(conn: sqlite3.Connection, start: str, end: str) -> dict:
    """Submissions for the day."""
    total = conn.execute(
        "SELECT COUNT(*) FROM submissions WHERE timestamp >= ? AND timestamp < ?",
        (start, end),
    ).fetchone()[0]

    confirmed = conn.execute(
        "SELECT COUNT(*) FROM submissions WHERE timestamp >= ? AND timestamp < ? AND status='confirmed'",
        (start, end),
    ).fetchone()[0]

    profit = conn.execute(
        """SELECT COALESCE(SUM(actual_profit_usd), 0)
           FROM submissions WHERE timestamp >= ? AND timestamp < ? AND status='confirmed'""",
        (start, end),
    ).fetchone()

    return {
        "total": total,
        "confirmed": confirmed,
        "total_profit_usd": round(profit[0], 2),
    }


def generate_report(date_str: Optional[str] = None) -> dict:
    """Generate full daily report as a dict."""
    start, end = get_date_range(date_str)
    report_date = start[:10]

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    report = {
        "date": report_date,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "opportunities": query_opportunities(conn, start, end),
        "gas": query_gas(conn, start, end),
        "competitors": query_competitors(conn, start, end),
        "submissions": query_submissions(conn, start, end),
    }

    # Store report
    conn.execute(
        "INSERT OR REPLACE INTO daily_reports (date, report_json) VALUES (?, ?)",
        (report_date, json.dumps(report)),
    )
    conn.commit()
    conn.close()

    return report


def format_markdown(report: dict) -> str:
    """Format report as Telegram-friendly Markdown."""
    opps = report["opportunities"]
    gas = report["gas"]
    comp = report["competitors"]
    subs = report["submissions"]

    md = f"""## 📊 Arbitrum DEX Arb Report — {report['date']}

### Opportunities
| Metric | Value |
|--------|-------|
| Total found | **{opps['total']}** |
| Best profit | **${opps['best_profit_usd']:,.2f}** |
| Avg profit | ${opps['avg_profit_usd']:,.2f} |
| Total potential | ${opps['total_profit_usd']:,.2f} |
| Avg spread | {opps['avg_spread_pct']:.3f}% |
"""

    if opps["top5"]:
        md += "\n**Top 5:**\n"
        for t in opps["top5"]:
            md += f"- {t['time']} {t['token_in']}→{t['token_out']} {t['buy_dex']}→{t['sell_dex']} **${t['net_profit']:,.2f}**\n"

    if opps["by_pair"]:
        md += "\n**By pair:**\n"
        for p in opps["by_pair"][:6]:
            md += f"- {p['pair']} {p['route']}: {p['count']}× avg ${p['avg_profit']}\n"

    # Active hours heatmap
    active_hours = [(h["hour"], h["count"]) for h in opps.get("by_hour", []) if h["count"] > 0]
    if active_hours:
        md += "\n**Active hours (UTC):** "
        md += ", ".join(f"{h:02d}h({c})" for h, c in active_hours)

    md += f"""\n
### Gas
| Metric | Value |
|--------|-------|
| Samples | {gas['samples']} |
| Avg base fee | {gas['avg_base_gwei']:.4f} gwei |
| Range | {gas['min_base_gwei']:.4f} – {gas['max_base_gwei']:.4f} gwei |
| ETH price | ${gas['avg_eth_price']:,.0f} |
"""

    md += f"""\n### Competitors
| Metric | Value |
|--------|-------|
| MEV txns detected | {comp['total']} |
| Avg gas used | {comp['avg_gas_gwei']:.2f} gwei |
"""

    if comp["breakdown"]:
        for b in comp["breakdown"]:
            md += f"- {b['bot']}: {b['count']}\n"

    md += f"""\n### Execution
| Metric | Value |
|--------|-------|
| Submitted | {subs['total']} |
| Confirmed | {subs['confirmed']} |
| Total profit | ${subs['total_profit_usd']:,.2f} |
"""

    # Status line
    if opps["total"] > 0:
        md += "\n✅ Opportunities exist — arb market is active."
    else:
        md += "\n⚠️ Zero opportunities — check scanner health."

    md += f"\n\n_Report generated {report['generated_at'][:19]}Z_"

    return md


if __name__ == "__main__":
    date_arg = sys.argv[2] if len(sys.argv) > 2 and sys.argv[1] == "--date" else None
    report = generate_report(date_arg)
    md = format_markdown(report)
    print(md)

    # Also save JSON
    out_path = project_root / "data" / f"arb_report_{report['date']}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2))
    print(f"\nJSON saved: {out_path}")
