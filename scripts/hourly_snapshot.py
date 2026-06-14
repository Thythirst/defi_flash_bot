#!/usr/bin/env python3
"""
Hourly validation data collector + dedup safety check.
Runs via cron every 60 minutes. Aggregated by the final report script.
"""

import json
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta

sys.path.insert(0, "/root/defi_flash_bot/prod")
from dotenv import load_dotenv
load_dotenv()
from web3 import Web3

PRELIQ_LOG = "/root/defi_flash_bot/prod/preliq.log"
AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
RPC_URL = os.getenv("QUICKNODE_HTTP_URL", os.getenv("ARBITRUM_HTTP_URL", "https://arb1.arbitrum.io/rpc"))
DATA_DIR = "/tmp/validation_data"
os.makedirs(DATA_DIR, exist_ok=True)

w3 = Web3(Web3.HTTPProvider(RPC_URL))

def parse_ts(line):
    m = re.match(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
    return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S") if m else None

def count_between(logfile, pattern, start, end):
    """Count grep matches in time window."""
    result = subprocess.run(
        ["grep", "-a", pattern, logfile],
        capture_output=True, text=True, timeout=30
    )
    count = 0
    for line in result.stdout.strip().split("\n"):
        if not line: continue
        ts = parse_ts(line)
        if ts and start <= ts < end:
            count += 1
    return count

def extract_borrowers(logfile, pattern, start, end):
    """Unique borrower addresses from log lines in time window."""
    result = subprocess.run(
        ["grep", "-a", pattern, logfile],
        capture_output=True, text=True, timeout=30
    )
    borrowers = set()
    for line in result.stdout.strip().split("\n"):
        if not line: continue
        ts = parse_ts(line)
        if ts and start <= ts < end:
            m = re.search(r'PRE-LIQ:\s+(0x[a-fA-F0-9]+)', line)
            if m:
                borrowers.add(m.group(1).lower())
    return borrowers

def get_dedup_suppressed_borrowers(hour_start, hour_end):
    """Get dedup-suppressed borrowers and their predicted HFs."""
    result = subprocess.run(
        ["grep", "-a", "REASON=dedup_suppressed", PRELIQ_LOG],
        capture_output=True, text=True, timeout=30
    )
    suppressed = []
    for line in result.stdout.strip().split("\n"):
        if not line: continue
        ts = parse_ts(line)
        if not ts or ts < hour_start or ts >= hour_end: continue
        
        m_b = re.search(r'borrower=(0x[a-fA-F0-9]+)', line)
        m_s = re.search(r'scenario=(\S+)', line)
        m_hf = re.search(r'hf_predicted=([0-9.]+)', line)
        m_ttl = re.search(r'TTL=(\d+)s', line)
        
        if m_b:
            suppressed.append({
                "borrower": m_b.group(1).lower(),
                "scenario": m_s.group(1) if m_s else "unknown",
                "predicted_hf": float(m_hf.group(1)) if m_hf else 0,
                "ttl": int(m_ttl.group(1)) if m_ttl else 0,
                "suppressed_at": ts.isoformat(),
            })
    return suppressed

def check_onchain_hf(borrower):
    """Call Aave getUserAccountData."""
    try:
        pool = w3.eth.contract(
            address=Web3.to_checksum_address(AAVE_POOL),
            abi=[{"inputs":[{"name":"user","type":"address"}],"name":"getUserAccountData",
                  "outputs":[{"name":"totalCollateralBase","type":"uint256"},
                             {"name":"totalDebtBase","type":"uint256"},
                             {"name":"availableBorrowsBase","type":"uint256"},
                             {"name":"currentLiquidationThreshold","type":"uint256"},
                             {"name":"ltv","type":"uint256"},
                             {"name":"healthFactor","type":"uint256"}],
                  "stateMutability":"view","type":"function"}]
        )
        data = pool.functions.getUserAccountData(Web3.to_checksum_address(borrower)).call()
        return data[5] / 1e18
    except Exception as e:
        return float('inf')

def main():
    now = datetime.now()
    hour_start = now.replace(minute=0, second=0, microsecond=0)
    hour_end = hour_start + timedelta(hours=1)
    
    snapshot = {
        "timestamp": now.isoformat(),
        "hour_start": hour_start.isoformat(),
        "hour_end": hour_end.isoformat(),
    }
    
    # PRE-LIQ
    snapshot["preliq_count"] = count_between(PRELIQ_LOG, "PRE-LIQ:", hour_start, hour_end)
    borrowers = extract_borrowers(PRELIQ_LOG, "PRE-LIQ:", hour_start, hour_end)
    snapshot["unique_borrowers"] = len(borrowers)
    
    # Suppression
    snapshot["dedup_suppressed"] = count_between(PRELIQ_LOG, "REASON=dedup_suppressed", hour_start, hour_end)
    snapshot["cooldown_suppressed"] = count_between(PRELIQ_LOG, "REASON=borrower_cooldown", hour_start, hour_end)
    
    # HF checks
    snapshot["hf_checks"] = count_between(PRELIQ_LOG, "on-chain HF=", hour_start, hour_end)
    
    # Velocity gates
    cl_log = "/root/defi_flash_bot/prod/logs/chainlink_sim_service_error.log"
    snapshot["velocity_gates"] = count_between(cl_log, "VELOCITY GATE: suppressing", hour_start, hour_end)
    
    # Submitted / confirmed
    snapshot["submitted"] = count_between(PRELIQ_LOG, "REASON=submitted", hour_start, hour_end)
    snapshot["confirmed"] = count_between(PRELIQ_LOG, "CONFIRMED:", hour_start, hour_end)
    
    # ── Dedup Safety Check ──────────────────────────────────
    # Get ALL dedup-suppressed borrowers (not just this hour — cumulative)
    # and verify their on-chain HF is safe
    all_suppressed = get_dedup_suppressed_borrowers(
        datetime(2026, 6, 5, 15, 30),  # deployment time
        now
    )
    
    # Deduplicate to unique borrowers
    unique_borrowers = {}
    for s in all_suppressed:
        b = s["borrower"]
        if b not in unique_borrowers or s["suppressed_at"] > unique_borrowers[b]["suppressed_at"]:
            unique_borrowers[b] = s
    
    # Check each one
    hf_results = []
    any_liquidatable = False
    for borrower, info in unique_borrowers.items():
        hf = check_onchain_hf(borrower)
        entry = {
            "borrower": borrower,
            "suppressed_at": info["suppressed_at"],
            "scenario": info["scenario"],
            "predicted_hf": info["predicted_hf"],
            "actual_hf": hf,
            "checked_at": now.isoformat(),
        }
        hf_results.append(entry)
        if hf < 1.0:
            any_liquidatable = True
            print(f"⚠️  LIQUIDATABLE: {borrower[:14]} HF={hf:.4f} (was suppressed at {info['suppressed_at']})")
    
    snapshot["suppressed_borrowers_count"] = len(unique_borrowers)
    snapshot["suppressed_hf_results"] = hf_results
    snapshot["any_liquidatable"] = any_liquidatable
    
    # ── RPC Metrics ─────────────────────────────────────────
    # Read from RPC METRICS log lines
    import subprocess as sp
    rpc_data = {}
    result = sp.run(
        ["grep", "-a", "RPC METRICS", "/root/defi_flash_bot/prod/dryrun.log"],
        capture_output=True, text=True, timeout=30
    )
    for line in result.stdout.strip().split("\n"):
        if not line: continue
        ts = parse_ts(line)
        if not ts or ts < hour_start or ts >= hour_end: continue
        
        m_svc = re.search(r'\[(\S+)\]', line)
        m_total = re.search(r'total=(\d+)', line)
        m_peak = re.search(r'peak=([0-9.]+)/s', line)
        m_sus = re.search(r'sustained=([0-9.]+)/s', line)
        m_qn = re.search(r'QuickNode_429=(\d+)', line)
        m_cs = re.search(r'Chainstack=(\d+)', line)
        m_pa = re.search(r'PublicArb=(\d+)', line)
        m_fb = re.search(r'fallback=([0-9.]+)%', line)
        
        svc = m_svc.group(1) if m_svc else "unknown"
        if svc not in rpc_data:
            rpc_data[svc] = {"total": 0, "peak": 0, "qn_429": 0, "cs": 0, "pa": 0, "fallback": 0}
        
        d = rpc_data[svc]
        d["total"] += int(m_total.group(1)) if m_total else 0
        d["peak"] = max(d["peak"], float(m_peak.group(1)) if m_peak else 0)
        d["qn_429"] += int(m_qn.group(1)) if m_qn else 0
        d["cs"] += int(m_cs.group(1)) if m_cs else 0
        d["pa"] += int(m_pa.group(1)) if m_pa else 0
        if m_fb:
            d["fallback"] += 1
    
    snapshot["rpc_metrics"] = rpc_data
    
    # Write
    outfile = os.path.join(DATA_DIR, f"snapshot_{hour_start.strftime('%H%M')}.json")
    with open(outfile, "w") as f:
        json.dump(snapshot, f, indent=2, default=str)
    
    print(f"Snapshot {hour_start.strftime('%H:%M')}: PRE-LIQ={snapshot['preliq_count']} "
          f"borrowers={snapshot['unique_borrowers']} dedup={snapshot['dedup_suppressed']} "
          f"cooldown={snapshot['cooldown_suppressed']} HF_checks={snapshot['hf_checks']} "
          f"vel_gate={snapshot['velocity_gates']} "
          f"suppressed_borrowers={snapshot['suppressed_borrowers_count']} "
          f"liquidatable={snapshot['any_liquidatable']}")
    print(f"  Saved to {outfile}")

if __name__ == "__main__":
    main()
