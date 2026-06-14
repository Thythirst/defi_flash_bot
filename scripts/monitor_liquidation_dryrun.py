#!/usr/bin/env python3
"""
Monitor liquidation-dryrun service health.
Modes:
  --summary   : print 15-min summary to stdout (for cron)
  --watch     : poll every 30s, print to stdout (for bg process with watch_patterns)
Both modes: exit code 1 if critical condition detected.
"""
import subprocess, json, sys, time, os
from datetime import datetime, timezone

SERVICE = "liquidation-dryrun"
LOG_SINCE_DEFAULT = "15 minutes ago"

def run(cmd):
    r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=15)
    return r.stdout.strip(), r.stderr.strip(), r.returncode

def get_systemd_state():
    out, _, _ = run(f"systemctl show {SERVICE} -p ActiveState -p SubState -p NRestarts -p StateChangeTimestamp")
    state = {}
    for line in out.splitlines():
        if '=' in line:
            k, v = line.split('=', 1)
            state[k] = v
    return {
        "active_state": state.get("ActiveState", "unknown"),
        "sub_state": state.get("SubState", "unknown"),
        "nrestarts": int(state.get("NRestarts", 0)),
        "state_since": state.get("StateChangeTimestamp", "unknown"),
    }

def get_rpc_metrics():
    """Parse last 15 min of RPC METRICS lines from journald."""
    out, _, _ = run(
        f"journalctl -u {SERVICE} --since '15 minutes ago' --no-pager 2>&1 | "
        f"grep 'RPC METRICS' | tail -1"
    )
    if not out:
        return {"total": 0, "quicknode_429": 0, "chainstack": 0, "publicarb": 0, "fallback_pct": 0, "all_failed": 0}

    # Format: 📊 RPC METRICS [liquidation-dryrun] total=N | QuickNode_429=N | Chainstack=N | PublicArb=N | fallback=X.X% | all_failed=N
    metrics = {}
    for part in out.split("|"):
        part = part.strip()
        if '=' in part:
            # Extract after last space or colon
            kv = part.split("=")
            if len(kv) >= 2:
                key = kv[0].strip().split()[-1] if kv[0].strip().split() else kv[0].strip()
                val = kv[-1].strip().rstrip('%')
                try:
                    if '.' in val:
                        metrics[key] = float(val)
                    else:
                        metrics[key] = int(val)
                except ValueError:
                    metrics[key] = val
    return metrics

def get_exceptions():
    """Count unhandled exceptions in last 15 min."""
    out, _, _ = run(
        f"journalctl -u {SERVICE} --since '15 minutes ago' --no-pager 2>&1 | "
        f"grep -c 'Exception:'"
    )
    return int(out) if out.isdigit() else 0

def get_rate_limit_details():
    """Extract all RATE_LIMIT lines for verification."""
    out, _, _ = run(
        f"journalctl -u {SERVICE} --since '15 minutes ago' --no-pager 2>&1 | "
        f"grep 'RATE_LIMIT'"
    )
    lines = [l.strip() for l in out.splitlines() if l.strip()]
    quicknode_429s = [l for l in lines if 'QuickNode' in l]
    chainstack = [l for l in lines if 'Chainstack' in l]
    return {
        "rate_limit_total": len(lines),
        "quicknode_429": len(quicknode_429s),
        "chainstack_fallback": len(chainstack),
        "lines": lines[-10:],  # last 10 for detail
    }

def check_critical(state, metrics, exceptions):
    """Return list of critical conditions."""
    alerts = []
    if state["active_state"] not in ("active",):
        alerts.append(f"CRITICAL: service state={state['active_state']}/{state['sub_state']}")
    if state["nrestarts"] > 0:
        alerts.append(f"CRITICAL: NRestarts={state['nrestarts']} (was 0)")
    if metrics.get("all_failed", 0) > 0:
        alerts.append(f"CRITICAL: all_failed={metrics.get('all_failed')}")
    return alerts

def print_summary():
    state = get_systemd_state()
    metrics = get_rpc_metrics()
    exceptions = get_exceptions()
    rate_limits = get_rate_limit_details()
    alerts = check_critical(state, metrics, exceptions)

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"=== liquidation-dryrun 15-min report [{now}] ===")
    print(f"State: {state['active_state']}/{state['sub_state']}")
    print(f"Uptime since: {state['state_since']}")
    print(f"NRestarts: {state['nrestarts']} (delta: {state['nrestarts']})")
    print(f"Crash count (exceptions): {exceptions}")
    print(f"")
    print(f"--- RPC Metrics ---")
    print(f"QuickNode_429: {metrics.get('quicknode_429', 0)}")
    print(f"Chainstack fallback: {metrics.get('chainstack', 0)}")
    print(f"PublicArb fallback: {metrics.get('publicarb', 0)}")
    print(f"Total requests: {metrics.get('total', 0)}")
    print(f"Fallback rate: {metrics.get('fallback', 0):.1f}%")
    print(f"all_failed: {metrics.get('all_failed', 0)}")
    print(f"")
    print(f"--- Rate Limit Detail ---")
    print(f"RATE_LIMIT events: {rate_limits['rate_limit_total']}")
    print(f"  QuickNode hits: {rate_limits['quicknode_429']}")
    print(f"  Chainstack rescues: {rate_limits['chainstack_fallback']}")
    match = "✓ MATCH" if rate_limits['quicknode_429'] == rate_limits['chainstack_fallback'] or rate_limits['chainstack_fallback'] >= rate_limits['quicknode_429'] else "⚠ MISMATCH"
    print(f"  429→fallback match: {match}")
    if rate_limits['lines']:
        print(f"  Last events:")
        for l in rate_limits['lines'][-5:]:
            print(f"    {l[:180]}")

    if alerts:
        print(f"\n🚨 ALERTS:")
        for a in alerts:
            print(f"  {a}")
    else:
        print(f"\n✓ No critical conditions detected.")

    return 1 if alerts else 0

def watch_loop():
    """Continuous watch mode — exits on critical condition."""
    prev_restarts = None
    streak = 0
    
    # Get initial NRestarts
    state = get_systemd_state()
    prev_restarts = state["nrestarts"]
    
    while True:
        state = get_systemd_state()
        metrics = get_rpc_metrics()
        exceptions = get_exceptions()
        
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        
        # Critical checks
        if state["nrestarts"] > prev_restarts:
            print(f"CRITICAL:NRestarts increased: {prev_restarts} → {state['nrestarts']} at {ts}")
            sys.exit(1)
        
        if state["active_state"] not in ("active",):
            print(f"CRITICAL:Service state changed to {state['active_state']}/{state['sub_state']} at {ts}")
            sys.exit(1)
        
        if metrics.get("all_failed", 0) > 0:
            print(f"CRITICAL:all_failed > 0: {metrics.get('all_failed')} at {ts}")
            sys.exit(1)
        
        # Periodic status (every 5 min)
        streak += 1
        if streak % 10 == 0:  # every 5 min (30s * 10)
            print(f"WATCH [{ts}]: OK — state={state['active_state']}/{state['sub_state']}, "
                  f"NRestarts={state['nrestarts']}, 429s={metrics.get('quicknode_429', 0)}, "
                  f"fallback={metrics.get('chainstack', 0)}, all_failed={metrics.get('all_failed', 0)}")
        
        prev_restarts = state["nrestarts"]
        time.sleep(30)

if __name__ == "__main__":
    if "--watch" in sys.argv:
        watch_loop()
    else:
        sys.exit(print_summary())
