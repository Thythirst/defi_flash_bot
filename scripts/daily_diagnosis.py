#!/usr/bin/env python3
"""
daily_diagnosis.py — Autonomous daily pipeline diagnosis
Runs at 10:00 UTC via cron. Checks actual live state, compares to
previous findings, proposes file:line fixes for current top bottleneck.

Cron entry:
    0 10 * * * /root/defi_flash_bot/prod/venv/bin/python3 \
        /root/defi_flash_bot/prod/scripts/daily_diagnosis.py \
        >> /root/defi_flash_bot/prod/logs/daily_diagnosis.log 2>&1

State file: /root/defi_flash_bot/prod/logs/diagnosis_state.json
    Tracks previous findings so repeat issues are flagged vs new ones.
"""

import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROD_DIR     = Path("/root/defi_flash_bot/prod")
STATE_FILE   = PROD_DIR / "logs" / "diagnosis_state.json"
LOG_FILE     = PROD_DIR / "logs" / "daily_diagnosis.log"
PIPELINE_LOG = PROD_DIR / "logs" / "pipeline_v3.log"
SKIPS_DB     = PROD_DIR / "skips.db"
OUTCOMES_DB  = PROD_DIR / "liquidations.db"
BACKTEST_DB  = PROD_DIR / "backtest_full2.db"

logging.basicConfig(
    level  = logging.INFO,
    format = "%(asctime)s  %(message)s",
    datefmt= "%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Finding:
    severity:    str    # CRITICAL | HIGH | MEDIUM | LOW | OK
    category:    str    # submission | profit | gas | rpc | data | market
    description: str
    evidence:    str
    fix:         str    # file:line — what to change
    first_seen:  str = ""
    times_seen:  int = 1


@dataclass
class DiagnosisReport:
    timestamp:      str
    run_number:     int
    top_bottleneck: str
    findings:       list[Finding] = field(default_factory=list)
    metrics:        dict          = field(default_factory=dict)
    resolved:       list[str]     = field(default_factory=list)
    new_issues:     list[str]     = field(default_factory=list)


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"run_number": 0, "previous_findings": [], "previous_metrics": {}}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


# ---------------------------------------------------------------------------
# Metric collectors
# ---------------------------------------------------------------------------

def collect_process_metrics() -> dict:
    """Check if pipeline and execution engine are running."""
    metrics = {}
    for name, pattern in [
        ("pipeline_running",   "pipeline_v3.py"),
        ("engine_running",     "execution_engine"),
        ("backtest_running",   "backtest.py"),
    ]:
        result = subprocess.run(
            ["pgrep", "-f", pattern],
            capture_output=True, text=True
        )
        metrics[name] = result.returncode == 0
    return metrics


def collect_outcome_metrics() -> dict:
    """Check outcomes DB for submission/confirmation counts."""
    metrics = {
        "submissions":  0,
        "confirmed":    0,
        "reverted":     0,
        "pending":      0,
        "total_profit": 0.0,
        "last_confirmed_block": 0,
    }
    if not OUTCOMES_DB.exists():
        return metrics
    try:
        conn = sqlite3.connect(str(OUTCOMES_DB))

        # Count by status
        rows = conn.execute(
            "SELECT status, COUNT(*), SUM(COALESCE(actual_profit_usd,0)) "
            "FROM outcomes GROUP BY status"
        ).fetchall()
        for status, count, profit in rows:
            metrics["submissions"] += count
            if status == "confirmed":
                metrics["confirmed"]    = count
                metrics["total_profit"] = profit or 0.0
            elif status == "reverted":
                metrics["reverted"] = count
            elif status == "pending":
                metrics["pending"]  = count

        # Last confirmed block
        row = conn.execute(
            "SELECT MAX(block_number) FROM outcomes WHERE status='confirmed'"
        ).fetchone()
        if row and row[0]:
            metrics["last_confirmed_block"] = row[0]

        conn.close()
    except Exception as e:
        metrics["db_error"] = str(e)
    return metrics


def collect_skip_metrics() -> dict:
    """Check skip telemetry for top rejection reasons."""
    metrics = {"total_skips": 0, "top_reasons": [], "last_24h_skips": 0}
    if not SKIPS_DB.exists():
        return metrics
    try:
        conn = sqlite3.connect(str(SKIPS_DB))
        cutoff = time.time() - 86400

        total = conn.execute(
            "SELECT COUNT(*) FROM skip_events WHERE timestamp > ?", (cutoff,)
        ).fetchone()[0]
        metrics["last_24h_skips"] = total

        rows = conn.execute("""
            SELECT reason, COUNT(*) as n, AVG(profit_usd) as avg_profit
            FROM skip_events
            WHERE timestamp > ?
            GROUP BY reason ORDER BY n DESC LIMIT 5
        """, (cutoff,)).fetchall()
        metrics["top_reasons"] = [
            {"reason": r, "count": n, "avg_profit": round(p or 0, 2)}
            for r, n, p in rows
        ]
        conn.close()
    except Exception as e:
        metrics["db_error"] = str(e)
    return metrics


def collect_log_metrics() -> dict:
    """Parse recent pipeline log for errors and key events."""
    metrics = {
        "errors_24h":       0,
        "warnings_24h":     0,
        "candidates_seen":  0,
        "last_candidate_hf":0.0,
        "prices_fresh":     "unknown",
        "pre_warm_status":  "unknown",
        "gas_oracle_source":"unknown",
    }

    # Try pipeline log file first
    log_paths = [
        PIPELINE_LOG,
        PROD_DIR / "pipeline_v3.log",
        Path("/tmp/pipeline_v3.log"),
    ]
    log_text = ""
    for lp in log_paths:
        if lp.exists() and lp.stat().st_size > 0:
            # Read last 5000 lines
            result = subprocess.run(
                ["tail", "-n", "5000", str(lp)],
                capture_output=True, text=True
            )
            log_text = result.stdout
            break

    # Try journalctl if no file found
    if not log_text:
        result = subprocess.run(
            ["journalctl", "-u", "pipeline.service",
             "--since", "24 hours ago", "--no-pager"],
            capture_output=True, text=True
        )
        log_text = result.stdout

    if not log_text:
        metrics["log_source"] = "not found"
        return metrics

    lines = log_text.split("\n")
    for line in lines:
        ll = line.lower()
        if " error " in ll or "error:" in ll:
            metrics["errors_24h"] += 1
        elif " warning " in ll or "warning:" in ll:
            metrics["warnings_24h"] += 1
        if "candidates=" in line:
            try:
                val = line.split("candidates=")[1].split()[0]
                if int(val) > 0:
                    metrics["candidates_seen"] += int(val)
            except Exception:
                pass
        if "prices_fresh=" in line:
            try:
                metrics["prices_fresh"] = line.split("prices_fresh=")[1].split()[0]
            except Exception:
                pass
        if "CachePrewarm" in line and "warm=" in line:
            try:
                metrics["pre_warm_status"] = line.split("warm=")[1].split()[0]
            except Exception:
                pass
        if "GasOracle" in line and "source=" in line:
            try:
                metrics["gas_oracle_source"] = line.split("source=")[1].split()[0]
            except Exception:
                pass

    return metrics


def collect_backtest_metrics() -> dict:
    """Check backtest progress if running."""
    metrics = {"backtest_complete": False, "win_rate_current": None}
    if not BACKTEST_DB.exists():
        return metrics
    try:
        conn = sqlite3.connect(str(BACKTEST_DB))
        total    = conn.execute("SELECT COUNT(*) FROM liquidations").fetchone()[0]
        enriched = conn.execute(
            "SELECT COUNT(*) FROM liquidations WHERE base_fee > 0"
        ).fetchone()[0]
        if enriched > 0:
            won = conn.execute(
                "SELECT COUNT(*) FROM liquidations WHERE would_win_current=1"
            ).fetchone()[0]
            metrics["backtest_total"]    = total
            metrics["backtest_enriched"] = enriched
            metrics["win_rate_current"]  = round(won / enriched * 100, 1)
            metrics["backtest_complete"] = (enriched == total)
        conn.close()
    except Exception as e:
        metrics["db_error"] = str(e)
    return metrics


# ---------------------------------------------------------------------------
# Bottleneck analyzer
# ---------------------------------------------------------------------------

def analyze_bottleneck(
    process:  dict,
    outcomes: dict,
    skips:    dict,
    logs:     dict,
    backtest: dict,
    previous: dict,
) -> list[Finding]:
    """
    Core diagnosis logic.
    Returns ordered list of findings, most critical first.
    The question answered: what is between HF<1.0 and a confirmed profit?
    """
    findings = []

    # ── Pipeline not running ────────────────────────────────────────────
    if not process.get("pipeline_running"):
        findings.append(Finding(
            severity    = "CRITICAL",
            category    = "process",
            description = "Pipeline process not running",
            evidence    = "pgrep pipeline_v3.py returned no results",
            fix         = "systemctl restart pipeline.service  OR  "
                          "cd /root/defi_flash_bot/prod && "
                          "python3 -u services/rev2/pipeline_v3.py &",
        ))
        return findings  # Nothing else matters if pipeline is down

    # ── Execution engine not running ────────────────────────────────────
    if not process.get("engine_running"):
        findings.append(Finding(
            severity    = "HIGH",
            category    = "process",
            description = "Execution engine not running",
            evidence    = "pgrep execution_engine returned no results",
            fix         = "systemctl restart execution-engine.service",
        ))

    # ── Zero submissions ever ────────────────────────────────────────────
    if outcomes["submissions"] == 0 and logs.get("errors_24h", 0) == 0:
        findings.append(Finding(
            severity    = "HIGH",
            category    = "market",
            description = "Zero submissions — no positions crossed HF<1.0",
            evidence    = f"outcomes DB empty, errors=0 — market is genuinely quiet",
            fix         = "No fix needed — wait for market volatility. "
                          "Monitor 530+ positions below HF 1.2.",
        ))

    # ── Submissions but zero confirmations ──────────────────────────────
    if outcomes["submissions"] > 0 and outcomes["confirmed"] == 0:
        findings.append(Finding(
            severity    = "CRITICAL",
            category    = "submission",
            description = f"{outcomes['submissions']} txs submitted, 0 confirmed",
            evidence    = f"reverted={outcomes['reverted']} pending={outcomes['pending']}",
            fix         = "Check revert reasons: cast receipt <tx_hash> --rpc-url $RPC | grep revertReason. "
                          "Likely: competitor faster (HEALTH_FACTOR_NOT_BELOW_THRESHOLD), "
                          "or profit check failing (NotProfitable in executor).",
        ))

    # ── High skip rate ──────────────────────────────────────────────────
    top_reasons = skips.get("top_reasons", [])
    if top_reasons:
        top = top_reasons[0]
        if top["count"] > 10:
            findings.append(Finding(
                severity    = "HIGH",
                category    = "profit",
                description = f"Top skip reason: {top['reason']} ({top['count']}x in 24h)",
                evidence    = f"avg_profit=${top['avg_profit']:.2f} on skipped trades",
                fix         = _fix_for_skip_reason(top["reason"], top["avg_profit"]),
            ))

    # ── Price feeds stale ────────────────────────────────────────────────
    prices = logs.get("prices_fresh", "")
    if prices and "/" in prices:
        fresh, total = prices.split("/")
        if int(fresh) < int(total):
            findings.append(Finding(
                severity    = "MEDIUM",
                category    = "data",
                description = f"Price feeds: {prices} fresh",
                evidence    = f"Stale feeds cause HF miscalculation",
                fix         = "Check PricePoller logs for which feeds are stale. "
                              "wstETH: expected (24h heartbeat). "
                              "Others: check Chainlink feed addresses in price_poller.py",
            ))

    # ── Pre-warm below target ────────────────────────────────────────────
    prewarm = logs.get("pre_warm_status", "")
    if prewarm and "/" in prewarm:
        warm, total = prewarm.split("/")
        if int(warm) < int(total) * 0.8:
            findings.append(Finding(
                severity    = "MEDIUM",
                category    = "latency",
                description = f"Pre-warm {prewarm} — below 80% coverage",
                evidence    = "Cross-asset positions failing QuoterV2 timeout",
                fix         = "cache_prewarm.py: raise max_build_time_ms from 350 to 500. "
                              "Or check quote_cache.py KNOWN_SLOW_PAIRS coverage.",
            ))

    # ── Gas oracle on fallback ───────────────────────────────────────────
    if logs.get("gas_oracle_source") == "fallback":
        findings.append(Finding(
            severity    = "LOW",
            category    = "gas",
            description = "Gas oracle using fallback (insufficient block history)",
            evidence    = "source=fallback in logs — needs 5+ blocks of tip data",
            fix         = "pipeline_v3.py on_new_block(): confirm oracle.update() "
                          "is called with eth_maxPriorityFeePerGas every block.",
        ))

    # ── High error count ─────────────────────────────────────────────────
    errors = logs.get("errors_24h", 0)
    if errors > 50:
        findings.append(Finding(
            severity    = "HIGH",
            category    = "stability",
            description = f"{errors} errors in last 24h",
            evidence    = "High error rate may indicate RPC issues or code bugs",
            fix         = "grep ERROR /path/to/pipeline.log | sort | uniq -c | sort -rn | head -20",
        ))
    elif errors > 10:
        findings.append(Finding(
            severity    = "MEDIUM",
            category    = "stability",
            description = f"{errors} errors in last 24h",
            evidence    = "Moderate error rate — check for RPC 429s or timeouts",
            fix         = "grep ERROR /path/to/pipeline.log | tail -20",
        ))

    # ── Backtest complete — calibration needed ───────────────────────────
    if backtest.get("backtest_complete") and backtest.get("win_rate_current"):
        wr = backtest["win_rate_current"]
        if wr < 70:
            findings.append(Finding(
                severity    = "HIGH",
                category    = "gas",
                description = f"Backtest win rate {wr}% — gas strategy underperforming",
                evidence    = f"{backtest['backtest_enriched']:,} liquidations analyzed",
                fix         = "gas_oracle.py: raise surge_buffer from 2.0 to 3.0. "
                              "Run calibration SQL: SELECT AVG(winner_gas_price/base_fee) "
                              "FROM liquidations WHERE would_win_current=0",
            ))
        else:
            findings.append(Finding(
                severity    = "OK",
                category    = "gas",
                description = f"Backtest win rate {wr}% — gas strategy competitive",
                evidence    = f"{backtest['backtest_enriched']:,} liquidations analyzed",
                fix         = "No change needed. Consider raising percentile to P85 for cascades.",
            ))

    # ── All clear ────────────────────────────────────────────────────────
    if not findings:
        findings.append(Finding(
            severity    = "OK",
            category    = "market",
            description = "Pipeline healthy — waiting for market volatility",
            evidence    = "No errors, feeds fresh, pre-warm active",
            fix         = "No action needed.",
        ))

    return findings


def _fix_for_skip_reason(reason: str, avg_profit: float) -> str:
    fixes = {
        "profit_floor":        f"fix_min_profit.py: lower min_profit_usd from $2 to $1 "
                               f"(avg skipped profit=${avg_profit:.2f})",
        "no_eligible_collateral": "CollateralSelector: check liquidation_bonus_bps > 10000 "
                               "filter — eMode positions may have different bonus params",
        "stale_price":         "PricePoller: check FEED_MAX_AGE per asset in fix_wsteth_staleness.py",
        "gas_reserve":         "FastGasGuard: wallet ETH below reserve — top up wallet",
        "build_failed":        "flash_loan_route.py: check SwapCalldataBuilder for quote failures "
                               "— add more pairs to KNOWN_SLOW_PAIRS in quote_cache.py",
        "submit_failed":       "blast_submit.py: check endpoint health — "
                               "QuickNode may be rate-limited",
        "position_not_found":  "position_loader.py: watchlist may be stale — "
                               "run WatchlistManager.force_bootstrap()",
    }
    return fixes.get(reason, f"Investigate {reason} in skip_telemetry.py REASON_DESCRIPTIONS")


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def generate_report(
    findings:  list[Finding],
    metrics:   dict,
    state:     dict,
) -> DiagnosisReport:
    """Compare findings to previous run, identify new vs recurring issues."""
    prev_descriptions = {f.get("description", "") for f in state.get("previous_findings", [])}
    curr_descriptions = {f.description for f in findings}

    resolved   = [d for d in prev_descriptions if d not in curr_descriptions and d]
    new_issues = [d for d in curr_descriptions if d not in prev_descriptions]

    # Annotate first_seen and times_seen
    prev_map = {f.get("description", ""): f for f in state.get("previous_findings", [])}
    for f in findings:
        if f.description in prev_map:
            f.first_seen = prev_map[f.description].get("first_seen", datetime.now(timezone.utc).isoformat())
            f.times_seen = prev_map[f.description].get("times_seen", 1) + 1
        else:
            f.first_seen = datetime.now(timezone.utc).isoformat()
            f.times_seen = 1

    # Determine top bottleneck
    critical = [f for f in findings if f.severity == "CRITICAL"]
    high     = [f for f in findings if f.severity == "HIGH"]
    if critical:
        top = f"CRITICAL: {critical[0].description}"
    elif high:
        top = f"HIGH: {high[0].description}"
    else:
        top = "OK: No critical issues — market quiet"

    return DiagnosisReport(
        timestamp      = datetime.now(timezone.utc).isoformat(),
        run_number     = state.get("run_number", 0) + 1,
        top_bottleneck = top,
        findings       = findings,
        metrics        = metrics,
        resolved       = resolved,
        new_issues     = new_issues,
    )


def print_report(report: DiagnosisReport) -> None:
    print("\n" + "═" * 60)
    print(f"  DAILY DIAGNOSIS — Run #{report.run_number}")
    print(f"  {report.timestamp}")
    print("═" * 60)

    print(f"\n🎯 TOP BOTTLENECK: {report.top_bottleneck}")

    if report.resolved:
        print(f"\n✅ RESOLVED since last run:")
        for r in report.resolved:
            print(f"   • {r}")

    if report.new_issues:
        print(f"\n🆕 NEW since last run:")
        for n in report.new_issues:
            print(f"   • {n}")

    print(f"\n── Findings ─────────────────────────────────────────────")
    icons = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢", "OK": "✅"}
    for f in report.findings:
        icon = icons.get(f.severity, "⚪")
        repeat = f" (seen {f.times_seen}x)" if f.times_seen > 1 else " (NEW)"
        print(f"\n{icon} [{f.severity}] {f.category.upper()}{repeat}")
        print(f"   {f.description}")
        print(f"   Evidence: {f.evidence}")
        print(f"   Fix: {f.fix}")

    print(f"\n── Key Metrics ──────────────────────────────────────────")
    m = report.metrics
    print(f"   Pipeline running:  {m.get('process', {}).get('pipeline_running', '?')}")
    print(f"   Submissions:       {m.get('outcomes', {}).get('submissions', 0)}")
    print(f"   Confirmed:         {m.get('outcomes', {}).get('confirmed', 0)}")
    print(f"   Total profit:      ${m.get('outcomes', {}).get('total_profit', 0):.2f}")
    print(f"   Skips (24h):       {m.get('skips', {}).get('last_24h_skips', 0)}")
    print(f"   Errors (24h):      {m.get('logs', {}).get('errors_24h', 0)}")
    print(f"   Price feeds:       {m.get('logs', {}).get('prices_fresh', '?')}")
    print(f"   Pre-warm:          {m.get('logs', {}).get('pre_warm_status', '?')}")
    print("═" * 60 + "\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    logger.info("Daily diagnosis starting...")
    state = load_state()

    # Collect all metrics
    process  = collect_process_metrics()
    outcomes = collect_outcome_metrics()
    skips    = collect_skip_metrics()
    logs     = collect_log_metrics()
    backtest = collect_backtest_metrics()

    all_metrics = {
        "process":  process,
        "outcomes": outcomes,
        "skips":    skips,
        "logs":     logs,
        "backtest": backtest,
    }

    # Analyze
    findings = analyze_bottleneck(
        process, outcomes, skips, logs, backtest, state
    )

    # Generate report
    report = generate_report(findings, all_metrics, state)
    print_report(report)

    # Save state for next run
    state["run_number"]        = report.run_number
    state["previous_findings"] = [asdict(f) for f in findings]
    state["previous_metrics"]  = all_metrics
    state["last_run"]          = report.timestamp
    save_state(state)

    logger.info(f"Diagnosis complete — run #{report.run_number}, top: {report.top_bottleneck}")

    # Exit with non-zero if critical issues found
    has_critical = any(f.severity == "CRITICAL" for f in findings)
    sys.exit(1 if has_critical else 0)


if __name__ == "__main__":
    main()
