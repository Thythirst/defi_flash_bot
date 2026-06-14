#!/usr/bin/env python3
"""Alert watchdog for preliq-engine.

Runs every 2 minutes. Scans journalctl for alert conditions.
Outputs nothing when all clear. Only prints when an alert is triggered.

Alert conditions:
- 3+ consecutive reverts (no confirmation in between)
- RPC failover activation
- Kill switch activation
- _build_swap_calldata() exception
- FlashLoanFailed (replay revert reason on failed txs)
"""
import subprocess, re, os, json

ALERT_SINCE = "3 minutes ago"

def fetch_recent_logs():
    cmd = ["journalctl", "-u", "preliq-engine", "--since", ALERT_SINCE, 
           "--no-pager", "-o", "cat", "-p", "warning", "-p", "err"]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
    return r.stdout.split("\n")

def count_consecutive_reverts(lines):
    """Count recent reverts and check if 3+ consecutive (no confirmation between)."""
    reversions = []
    for line in lines:
        if "❌ REVERTED" in line:
            reversions.append(line)
        elif "✅ CONFIRMED" in line:
            reversions = []  # reset count on any confirmation
    return len(reversions)

def check_alerts():
    lines = fetch_recent_logs()
    alerts = []
    
    # 1. Consecutive reverts
    rev_count = count_consecutive_reverts(lines)
    if rev_count >= 3:
        alerts.append(f"🚨 {rev_count} CONSECUTIVE REVERTS — no confirmations in between")
        # Show the reverted tx hashes
        for line in lines:
            if "❌ REVERTED" in line:
                alerts.append(f"   {line.strip()[:200]}")
    
    # 2. RPC failover
    for line in lines:
        if "⚠️ RPC FAILOVER" in line:
            alerts.append(f"⚠️ RPC FAILOVER: {line.strip()[:200]}")
    
    # 3. Kill switch
    for line in lines:
        if "KILLSWITCH" in line.upper() and "blocked" in line.lower():
            alerts.append(f"🛑 KILL SWITCH ACTIVATED: {line.strip()[:200]}")
    
    # 4. Swap calldata failures
    for line in lines:
        if "swap calldata build failed" in line.lower():
            alerts.append(f"⚠️ SWAP CALLDATA FAILURE: {line.strip()[:200]}")
    
    # 5. FlashLoanFailed — check revert reasons
    # Look for REVERTED lines with FlashLoanFailed context
    for line in lines:
        if "REVERTED" in line:
            # Extract tx hash and try to get revert reason
            m = re.search(r'REVERTED: (0x[a-fA-F0-9]+)', line)
            if m:
                tx_hash_partial = m.group(1)
                # Try to find full tx hash in surrounding lines
                full_tx = None
                for l in lines:
                    if tx_hash_partial in l:
                        # Try to find 66-char hex
                        m2 = re.search(r'(0x[a-fA-F0-9]{64})', l)
                        if m2:
                            full_tx = m2.group(1)
                            break
                if full_tx:
                    try:
                        # Replay tx to get revert reason
                        r = subprocess.run([
                            "cast", "tx", full_tx,
                            "--rpc-url", os.getenv("QUICKNODE_ARBITRUM_HTTP_URL", 
                                "https://necessary-flashy-fog.arbitrum-mainnet.quiknode.pro/9359711fe3d68c27e68e33106299b588e43c96db/"),
                        ], capture_output=True, text=True, timeout=10)
                        # cast run to get trace
                        r2 = subprocess.run([
                            "cast", "run", full_tx,
                            "--rpc-url", "https://arb1.arbitrum.io/rpc",
                            "--quick",
                        ], capture_output=True, text=True, timeout=15)
                        output = (r2.stdout + r2.stderr).lower()
                        if "flashloan" in output and ("fail" in output or "revert" in output):
                            alerts.append(f"💥 FLASH LOAN FAILED: {full_tx}")
                    except Exception:
                        pass  # can't trace — skip
    
    # 6. Check Redis kill switch (belt + suspenders)
    try:
        r = subprocess.run(["redis-cli", "GET", "risk:killswitch"],
                          capture_output=True, text=True, timeout=3)
        if r.stdout.strip() == "1":
            alerts.append("🛑 Redis risk:killswitch=1 — submissions blocked")
    except Exception:
        pass
    
    if not alerts:
        return ""
    
    return "\n".join(alerts)

if __name__ == "__main__":
    result = check_alerts()
    if result:
        print(result)
