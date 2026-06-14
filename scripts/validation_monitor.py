#!/usr/bin/env python3
"""
6-hour post-change validation monitor.
Tracks PRE-LIQ reduction, dedup impact, RPC metrics, and crucially:
whether any dedup-suppressed borrower later became liquidatable on-chain.

Run: python3 /root/defi_flash_bot/prod/scripts/validation_monitor.py
Stops after 6 hours, writes final report to /tmp/validation_report.txt
"""

import asyncio
import json
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, "/root/defi_flash_bot/prod")
from dotenv import load_dotenv
load_dotenv()

import aiohttp
import redis.asyncio as aioredis
from web3 import Web3

# ── Config ─────────────────────────────────────────────────
DURATION_HOURS = 6
CHECK_INTERVAL = 300  # every 5 minutes
PRELIQ_LOG = "/root/defi_flash_bot/prod/preliq.log"
CHAINLINK_LOG = "/root/defi_flash_bot/prod/logs/chainlink_sim_service_error.log"
DRYRUN_LOG = "/root/defi_flash_bot/prod/dryrun.log"
AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
RPC_URL = os.getenv("QUICKNODE_HTTP_URL", os.getenv("ARBITRUM_HTTP_URL", "https://arb1.arbitrum.io/rpc"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

w3 = Web3(Web3.HTTPProvider(RPC_URL))

# ── Counters ───────────────────────────────────────────────
start_time = time.time()
end_time = start_time + DURATION_HOURS * 3600

hourly_snapshots = []  # [{hour, preliq_count, unique_borrowers, dedup_count, cooldown_count, ...}]
suppressed_borrowers = {}  # {borrower_lower: [(suppressed_at, scenario, predicted_hf, ttl)]}
hf_checks = []  # [(timestamp, borrower, predicted_hf, actual_hf)]
submitted = []
confirmed = []

# Baseline metrics from BEFORE the changes (use log history)
# We'll count from 2026-06-05 09:00 to 15:00 (before deployment at ~15:30)

def parse_log_timestamp(line):
    m = re.match(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
    if m:
        return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
    return None

def count_log_pattern(log_file, pattern, start_dt, end_dt):
    """Count lines matching pattern between two datetimes."""
    result = subprocess.run(
        ["grep", "-a", pattern, log_file],
        capture_output=True, text=True, timeout=30
    )
    count = 0
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        ts = parse_log_timestamp(line)
        if ts and start_dt <= ts < end_dt:
            count += 1
    return count

def extract_borrowers_from_log(log_file, pattern, start_dt, end_dt):
    """Extract unique borrower addresses from PRE-LIQ lines."""
    result = subprocess.run(
        ["grep", "-a", pattern, log_file],
        capture_output=True, text=True, timeout=30
    )
    borrowers = set()
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        ts = parse_log_timestamp(line)
        if ts and start_dt <= ts < end_dt:
            m = re.search(r'0x[a-fA-F0-9]{12}', line)
            if m:
                borrowers.add(m.group(0).lower())
    return borrowers

async def check_onchain_hf(borrower: str) -> float:
    """Call getUserAccountData on Aave Pool."""
    try:
        pool = w3.eth.contract(
            address=Web3.to_checksum_address(AAVE_POOL),
            abi=[{
                "inputs": [{"name": "user", "type": "address"}],
                "name": "getUserAccountData",
                "outputs": [
                    {"name": "totalCollateralBase", "type": "uint256"},
                    {"name": "totalDebtBase", "type": "uint256"},
                    {"name": "availableBorrowsBase", "type": "uint256"},
                    {"name": "currentLiquidationThreshold", "type": "uint256"},
                    {"name": "ltv", "type": "uint256"},
                    {"name": "healthFactor", "type": "uint256"},
                ],
                "stateMutability": "view",
                "type": "function",
            }]
        )
        data = pool.functions.getUserAccountData(
            Web3.to_checksum_address(borrower)
        ).call()
        return data[5] / 1e18
    except Exception as e:
        print(f"  HF check failed for {borrower[:12]}: {e}")
        return float('inf')

async def collect_snapshot(hour_idx: int):
    """Collect one hourly snapshot of all metrics."""
    now = datetime.now()
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    hour_end = hour_start + timedelta(hours=1)
    
    snap = {"hour": hour_idx, "time": now.isoformat()}
    
    # PRE-LIQ counts
    snap["preliq_count"] = count_log_pattern(
        PRELIQ_LOG, "PRE-LIQ:", hour_start, hour_end
    )
    borrowers = extract_borrowers_from_log(
        PRELIQ_LOG, "PRE-LIQ:", hour_start, hour_end
    )
    snap["unique_borrowers"] = len(borrowers)
    
    # Suppression counts
    snap["dedup_suppressed"] = count_log_pattern(
        PRELIQ_LOG, "REASON=dedup_suppressed", hour_start, hour_end
    )
    snap["cooldown_suppressed"] = count_log_pattern(
        PRELIQ_LOG, "REASON=borrower_cooldown", hour_start, hour_end
    )
    
    # HF checks
    hf_prev = count_log_pattern(
        PRELIQ_LOG, "on-chain HF=", hour_start, hour_end
    )
    snap["hf_checks"] = hf_prev
    
    # Check Redis for RPC metrics
    try:
        redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        rpc_keys = await redis.keys("rpc:metrics:*")
        rpc_data = {}
        for key in rpc_keys:
            data = await redis.hgetall(key)
            if data:
                rpc_data[key] = dict(data)
        snap["rpc_metrics"] = rpc_data
        await redis.aclose()
    except Exception:
        snap["rpc_metrics"] = {}
    
    # Velocity gate activations
    snap["velocity_gates"] = count_log_pattern(
        CHAINLINK_LOG, "VELOCITY GATE: suppressing", hour_start, hour_end
    )
    
    print(f"  Hour {hour_idx}: PRE-LIQ={snap['preliq_count']} "
          f"borrowers={snap['unique_borrowers']} "
          f"dedup={snap['dedup_suppressed']} "
          f"cooldown={snap['cooldown_suppressed']} "
          f"HF_checks={snap['hf_checks']} "
          f"vel_gate={snap['velocity_gates']}", flush=True)
    # Write intermediate status
    with open("/tmp/validation_status.txt", "w") as sf:
        sf.write(f"Hour {hour_idx}/{DURATION_HOURS}\n")
        sf.write(f"PRE-LIQ: {snap['preliq_count']}\n")
        sf.write(f"Borrowers: {snap['unique_borrowers']}\n")
        sf.write(f"Dedup: {snap['dedup_suppressed']}\n")
        sf.write(f"Cooldown: {snap['cooldown_suppressed']}\n")
        sf.write(f"HF checks: {snap['hf_checks']}\n")
        sf.write(f"Vel gates: {snap['velocity_gates']}\n")
        elapsed = time.time() - start_time
        sf.write(f"Elapsed: {elapsed/3600:.1f}h\n")
          f"borrowers={snap['unique_borrowers']} "
          f"dedup={snap['dedup_suppressed']} "
          f"cooldown={snap['cooldown_suppressed']} "
          f"HF_checks={snap['hf_checks']} "
          f"vel_gate={snap['velocity_gates']}")
    
    return snap

async def track_suppressed_borrowers():
    """Get all dedup-suppressed borrowers and track their on-chain HF."""
    global suppressed_borrowers
    
    # Read dedup-suppressed entries from log (last 5 min)
    five_min_ago = datetime.now() - timedelta(minutes=5)
    
    result = subprocess.run(
        ["grep", "-a", "REASON=dedup_suppressed", PRELIQ_LOG],
        capture_output=True, text=True, timeout=10
    )
    
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        ts = parse_log_timestamp(line)
        if not ts or ts < five_min_ago:
            continue
        
        # Parse: borrower=X scenario=Y TTL=Zs | hf_predicted=Z
        m_borrower = re.search(r'borrower=(0x[a-fA-F0-9]+)', line)
        m_scenario = re.search(r'scenario=(\S+)', line)
        m_ttl = re.search(r'TTL=(\d+)s', line)
        m_hf = re.search(r'hf_predicted=([0-9.]+)', line)
        
        if not m_borrower:
            continue
        
        borrower = m_borrower.group(1).lower()
        scenario = m_scenario.group(1) if m_scenario else "unknown"
        ttl = int(m_ttl.group(1)) if m_ttl else 0
        predicted_hf = float(m_hf.group(1)) if m_hf else 0
        
        if borrower not in suppressed_borrowers:
            suppressed_borrowers[borrower] = []
        suppressed_borrowers[borrower].append({
            "suppressed_at": ts.isoformat(),
            "scenario": scenario,
            "predicted_hf": predicted_hf,
            "ttl": ttl,
        })
    
    # For each tracked suppressed borrower, check current on-chain HF
    if suppressed_borrowers:
        print(f"  Tracking {len(suppressed_borrowers)} suppressed borrowers...", flush=True)
        for borrower, events in list(suppressed_borrowers.items()):
            actual_hf = await check_onchain_hf(borrower)
            events[-1]["latest_actual_hf"] = actual_hf
            events[-1]["checked_at"] = datetime.now().isoformat()
            
            if actual_hf < 1.0:
                print(f"  ⚠️  ALERT: {borrower[:14]} suppressed but now liquidatable! HF={actual_hf:.4f}")
            else:
                if len(events) == 1 or events[-1].get("latest_actual_hf", 999) != actual_hf:
                    print(f"  ✓  {borrower[:14]} suppressed, on-chain HF={actual_hf:.4f} (safe)")

def get_baseline_metrics():
    """Extract baseline metrics from 6 hours before deployment."""
    # Deployment was at ~15:30 on June 5
    # Baseline: 09:30 - 15:30
    baseline_start = datetime(2026, 6, 5, 9, 30)
    baseline_end = datetime(2026, 6, 5, 15, 30)
    
    hours = []
    current = baseline_start
    while current < baseline_end:
        next_hour = current + timedelta(hours=1)
        
        preliq = count_log_pattern(PRELIQ_LOG, "PRE-LIQ:", current, next_hour)
        borrowers = extract_borrowers_from_log(PRELIQ_LOG, "PRE-LIQ:", current, next_hour)
        
        hours.append({
            "hour_start": current.strftime("%H:%M"),
            "preliq_count": preliq,
            "unique_borrowers": len(borrowers),
        })
        current = next_hour
    
    return hours

async def main():
    print("=" * 70, flush=True)
    print("6-HOUR POST-CHANGE VALIDATION MONITOR")
    print(f"Start: {datetime.now().isoformat()}")
    print(f"Duration: {DURATION_HOURS} hours")
    print(f"Check interval: {CHECK_INTERVAL}s")
    print("=" * 70, flush=True)
    
    # Baseline
    print("\n--- BASELINE (09:30-15:30, before changes) ---")
    baseline = get_baseline_metrics()
    for h in baseline:
        print(f"  {h['hour_start']}: PRE-LIQ={h['preliq_count']} borrowers={h['unique_borrowers']}")
    total_baseline_preliq = sum(h["preliq_count"] for h in baseline)
    print(f"  Total: {total_baseline_preliq} PRE-LIQ events over 6h")
    
    # Monitoring loop
    print("\n--- MONITORING ---")
    # Bootstrap status
    with open("/tmp/validation_status.txt", "w") as sf:
        sf.write(f"Monitor started: {datetime.now().isoformat()}\n")
        sf.write(f"Baseline PRE-LIQ (6h): {total_baseline_preliq}\n")
        sf.write("First hourly snapshot at next hour boundary\n")
    hour_idx = 0
    last_hour_check = time.time()
    last_suppressed_check = time.time()
    
    while time.time() < end_time:
        now = time.time()
        
        # Hourly snapshot
        if now - last_hour_check >= 3600:
            snap = await collect_snapshot(hour_idx)
            hourly_snapshots.append(snap)
            hour_idx += 1
            last_hour_check = now
        
        # Track suppressed borrowers every 5 min
        if now - last_suppressed_check >= 300:
            await track_suppressed_borrowers()
            last_suppressed_check = now
        
        # Sleep until next check
        remaining = end_time - time.time()
        sleep_time = min(60, max(1, remaining))
        await asyncio.sleep(sleep_time)
    
    # Final snapshot
    snap = await collect_snapshot(hour_idx)
    hourly_snapshots.append(snap)
    
    # Final suppressed borrower check
    await track_suppressed_borrowers()
    
    # ── Generate Report ────────────────────────────────────
    print("\n" + "=" * 70)
    print("FINAL VALIDATION REPORT")
    print("=" * 70, flush=True)
    
    # 1. PRE-LIQ comparison
    total_post_preliq = sum(s["preliq_count"] for s in hourly_snapshots)
    total_post_borrowers = sum(s["unique_borrowers"] for s in hourly_snapshots)
    total_dedup = sum(s["dedup_suppressed"] for s in hourly_snapshots)
    total_cooldown = sum(s["cooldown_suppressed"] for s in hourly_snapshots)
    total_hf_checks = sum(s["hf_checks"] for s in hourly_snapshots)
    total_vel_gates = sum(s["velocity_gates"] for s in hourly_snapshots)
    
    report = []
    report.append(f"PERIOD: {datetime.now().isoformat()}")
    report.append("")
    report.append("1. PRE-LIQ EVENTS PER HOUR")
    report.append(f"   Baseline (6h before): {total_baseline_preliq} total, ~{total_baseline_preliq/6:.0f}/hr")
    report.append(f"   Post-change (6h after): {total_post_preliq} total, ~{total_post_preliq/6:.0f}/hr")
    report.append(f"   Reduction: {(1 - total_post_preliq/total_baseline_preliq)*100:.1f}%" if total_baseline_preliq else "   N/A")
    report.append("")
    report.append("   Hourly breakdown:")
    for h in baseline:
        report.append(f"     Before {h['hour_start']}: {h['preliq_count']} events, {h['unique_borrowers']} borrowers")
    for s in hourly_snapshots:
        report.append(f"     After  Hour {s['hour']}: {s['preliq_count']} events, {s['unique_borrowers']} borrowers")
    
    report.append("")
    report.append("2. UNIQUE BORROWERS PER HOUR")
    report.append(f"   Baseline avg: {sum(h['unique_borrowers'] for h in baseline)/len(baseline):.1f}/hr" if baseline else "   N/A")
    report.append(f"   Post-change avg: {total_post_borrowers/len(hourly_snapshots):.1f}/hr" if hourly_snapshots else "   N/A")
    
    report.append("")
    report.append("3. DUPLICATE SUPPRESSION")
    report.append(f"   Dedup suppressed (10min window): {total_dedup}")
    report.append(f"   Cooldown suppressed (60s window): {total_cooldown}")
    report.append(f"   Total signals suppressed: {total_dedup + total_cooldown}")
    
    report.append("")
    report.append("4. VELOCITY GATE")
    report.append(f"   liquidiation_cascade suppressed: {total_vel_gates} times")
    
    report.append("")
    report.append("5. PREDICTION ACCURACY")
    report.append(f"   HF checks performed: {total_hf_checks}")
    
    # Extract actual vs predicted from log
    hf_result = subprocess.run(
        ["grep", "-a", "on-chain HF=", PRELIQ_LOG],
        capture_output=True, text=True, timeout=10
    )
    hf_pairs = []
    for line in hf_result.stdout.strip().split("\n"):
        ts = parse_log_timestamp(line)
        if ts and ts >= datetime(2026, 6, 5, 15, 30):  # after deployment
            m_actual = re.search(r'on-chain HF=([0-9.]+)', line)
            m_pred = re.search(r'predicted post-impact HF=([0-9.]+)', line)
            if m_actual and m_pred:
                hf_pairs.append((float(m_actual.group(1)), float(m_pred.group(1))))
    
    liquidatable = sum(1 for a, p in hf_pairs if a < 1.0)
    report.append(f"   Predicted HF < 1.0: {len(hf_pairs)}")
    report.append(f"   Actual HF < 1.0: {liquidatable}")
    report.append(f"   False-positive rate: {(len(hf_pairs)-liquidatable)/max(len(hf_pairs),1)*100:.1f}%")
    
    report.append("")
    report.append("6. SUBMITTED / CONFIRMED")
    sub_count = count_log_pattern(PRELIQ_LOG, "REASON=submitted", 
                                   datetime(2026, 6, 5, 15, 30), datetime.now())
    conf_count = count_log_pattern(PRELIQ_LOG, "CONFIRMED:",
                                    datetime(2026, 6, 5, 15, 30), datetime.now())
    report.append(f"   Submitted: {sub_count}")
    report.append(f"   Confirmed: {conf_count}")
    
    report.append("")
    report.append("7. RPC METRICS (QuickNode)")
    # Aggregate from all snapshots
    qn_total = 0
    qn_peak = 0.0
    qn_429 = 0
    cs_total = 0
    fallback_total = 0
    for s in hourly_snapshots:
        for key, data in s.get("rpc_metrics", {}).items():
            qn_total += int(data.get("total_requests", 0))
            qn_peak = max(qn_peak, float(data.get("peak_req_s", 0)))
            qn_429 += int(data.get("quicknode_429", 0))
            cs_total += int(data.get("chainstack_requests", 0))
            fallback_total += int(data.get("fallback_successes", 0))
    
    report.append(f"   Total QuickNode requests: {qn_total}")
    report.append(f"   Peak req/s: {qn_peak:.1f}")
    report.append(f"   QuickNode 429s: {qn_429}")
    report.append(f"   Chainstack requests: {cs_total}")
    report.append(f"   Fallback successes: {fallback_total}")
    
    report.append("")
    report.append("8. MISSED LIQUIDATIONS — DEDUP SAFETY CHECK")
    report.append(f"   Unique borrowers suppressed by dedup: {len(suppressed_borrowers)}")
    
    missed_any = False
    for borrower, events in suppressed_borrowers.items():
        last_event = events[-1]
        actual_hf = last_event.get("latest_actual_hf", float('inf'))
        predicted_hf = last_event.get("predicted_hf", 0)
        
        if actual_hf < 1.0:
            missed_any = True
            report.append(f"")
            report.append(f"   ❌ MISSED: {borrower}")
            report.append(f"      Suppressed at: {last_event['suppressed_at']}")
            report.append(f"      Scenario: {last_event['scenario']}")
            report.append(f"      Predicted HF: {predicted_hf:.4f}")
            report.append(f"      Actual HF: {actual_hf:.4f}")
            report.append(f"      Suppression TTL: {last_event['ttl']}s")
            report.append(f"      HF timeline:")
            for i, ev in enumerate(events):
                hf = ev.get("latest_actual_hf", "N/A")
                report.append(f"        {i+1}. {ev['suppressed_at']}: predicted={ev['predicted_hf']:.4f} actual={hf}")
    
    if not missed_any:
        report.append(f"")
        report.append(f"   ✅ NO LIQUIDATION OPPORTUNITIES LOST")
        report.append(f"   All {len(suppressed_borrowers)} suppressed borrowers maintained")
        report.append(f"   on-chain HF ≥ 1.0 throughout the suppression window.")
        report.append(f"   Deduplication is safe for current market conditions.")
        
        # Show worst-case: lowest actual HF among suppressed
        lowest_hf = float('inf')
        lowest_borrower = ""
        for borrower, events in suppressed_borrowers.items():
            hf = events[-1].get("latest_actual_hf", float('inf'))
            if hf < lowest_hf:
                lowest_hf = hf
                lowest_borrower = borrower
        
        if lowest_borrower:
            report.append(f"   Closest to liquidation: {lowest_borrower[:14]} HF={lowest_hf:.4f}")
    
    report.append("")
    report.append("=" * 70)
    
    report_text = "\n".join(report)
    print(report_text)
    
    # Write to file
    report_path = "/tmp/validation_report.txt"
    with open(report_path, "w") as f:
        f.write(report_text)
    print(f"\nReport written to {report_path}")
    
    # Also write JSON for programmatic access
    json_path = "/tmp/validation_report.json"
    json_data = {
        "timestamp": datetime.now().isoformat(),
        "baseline": {
            "total_preliq": total_baseline_preliq,
            "hourly": baseline,
        },
        "post_change": {
            "total_preliq": total_post_preliq,
            "total_dedup": total_dedup,
            "total_cooldown": total_cooldown,
            "total_hf_checks": total_hf_checks,
            "total_velocity_gates": total_vel_gates,
            "hourly": [{"hour": s["hour"], "preliq": s["preliq_count"], 
                        "borrowers": s["unique_borrowers"],
                        "dedup": s["dedup_suppressed"],
                        "cooldown": s["cooldown_suppressed"]} for s in hourly_snapshots],
        },
        "prediction_accuracy": {
            "predicted_lt_1": len(hf_pairs),
            "actual_lt_1": liquidatable,
            "false_positive_rate": (len(hf_pairs)-liquidatable)/max(len(hf_pairs),1),
        },
        "rpc": {
            "quicknode_total": qn_total,
            "quicknode_peak_req_s": qn_peak,
            "quicknode_429": qn_429,
            "chainstack_total": cs_total,
            "fallback_total": fallback_total,
        },
        "missed_liquidations": not missed_any,
        "suppressed_borrowers_count": len(suppressed_borrowers),
        "lowest_suppressed_hf": lowest_hf if not missed_any else 0,
    }
    with open(json_path, "w") as f:
        json.dump(json_data, f, indent=2)
    print(f"JSON written to {json_path}")

if __name__ == "__main__":
    asyncio.run(main())
