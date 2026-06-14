#!/usr/bin/env python3
"""Hourly preliq-engine monitoring report.

Reads journalctl for the last hour, parses key events, reads Redis stats,
checks RPC health, and prints a formatted report.

Quiet when zero activity. Designed for cron delivery.
"""
import subprocess, json, re, time, os
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ── Journalctl parse ──────────────────────────────────────
def fetch_logs(since="1 hour ago"):
    cmd = ["journalctl", "-u", "preliq-engine", "--since", since, "--no-pager", "-o", "cat"]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
    return r.stdout.split("\n")

def parse_logs(lines):
    events = defaultdict(int)
    details = []
    re_submitted = re.compile(r"🚀 SUBMITTED.*profit=\$(?P<profit>[\d.]+).*EV=\$(?P<ev>[\d.]+)")
    re_confirmed = re.compile(r"✅ CONFIRMED.*gasCost=\$(?P<gas>[\d.]+).*profit=\$(?P<profit>[\d.]+)")
    re_reverted = re.compile(r"❌ REVERTED")
    re_lost = re.compile(r"🏁 LOST RACE")
    re_opp = re.compile(r"🎯 RISK POLL")
    re_competitor = re.compile(r"⚠️ Competitor liquidation")
    re_killswitch = re.compile(r"KILLSWITCH")
    re_failover = re.compile(r"⚠️ RPC FAILOVER")
    re_swap_fail = re.compile(r"swap calldata build failed|Skipping cross-asset")
    
    total_profit = 0.0
    total_gas = 0.0
    
    for line in lines:
        if re_submitted.search(line):
            events["submitted"] += 1
            m = re_submitted.search(line)
            if m:
                details.append(("SUBMITTED", float(m.group("profit")), float(m.group("ev"))))
        elif re_confirmed.search(line):
            events["confirmed"] += 1
            m = re_confirmed.search(line)
            if m:
                total_profit += float(m.group("profit"))
                total_gas += float(m.group("gas"))
        elif re_reverted.search(line):
            events["reverted"] += 1
        elif re_lost.search(line):
            events["lost_race"] += 1
        elif re_opp.search(line):
            events["opportunities"] += 1
        elif re_competitor.search(line):
            events["competitors_seen"] += 1
        elif re_killswitch.search(line):
            events["killswitch_triggered"] += 1
        elif re_failover.search(line):
            events["rpc_failover"] += 1
        elif re_swap_fail.search(line):
            events["swap_calldata_fail"] += 1
    
    return events, total_profit, total_gas

# ── Redis stats ───────────────────────────────────────────
def fetch_redis_stats():
    try:
        r = subprocess.run(["redis-cli", "KEYS", "preliq:stats:*"], 
                          capture_output=True, text=True, timeout=5)
        keys = [k for k in r.stdout.strip().split("\n") if k]
        if not keys:
            return {}
        # Get all stats for the latest key
        latest = sorted(keys)[-1]
        r2 = subprocess.run(["redis-cli", "HGETALL", latest],
                           capture_output=True, text=True, timeout=5)
        pairs = r2.stdout.strip().split("\n")
        stats = {}
        for i in range(0, len(pairs), 2):
            if i+1 < len(pairs):
                stats[pairs[i]] = pairs[i+1]
        return stats
    except Exception as e:
        return {"error": str(e)}

# ── RPC Health ─────────────────────────────────────────────
RPC_URLS = {
    "QuickNode": os.getenv("QUICKNODE_ARBITRUM_HTTP_URL", "https://necessary-flashy-fog.arbitrum-mainnet.quiknode.pro/9359711fe3d68c27e68e33106299b588e43c96db/"),
    "Chainstack": os.getenv("CHAINSTACK_ARBITRUM_HTTP_URL", "https://arbitrum-mainnet.core.chainstack.com/b718a2bff0d80347e0ce705841095295"),
    "Alchemy": os.getenv("ALCHEMY_HTTP_URL", "https://arb-mainnet.g.alchemy.com/v2/cDv5G5jCMZwj_s61SNeh_"),
    "Public arb1": "https://arb1.arbitrum.io/rpc",
}

def check_rpc_health():
    results = {}
    for name, url in RPC_URLS.items():
        try:
            r = subprocess.run([
                "curl", "-s", "-X", "POST", url,
                "-H", "Content-Type: application/json",
                "-d", '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}',
                "--max-time", "5",
            ], capture_output=True, text=True, timeout=10)
            d = json.loads(r.stdout)
            if "result" in d:
                block = int(d["result"], 16)
                results[name] = f"✓ block {block:,}"
            else:
                results[name] = f"✗ {d.get('error', {}).get('message', 'unknown')[:60]}"
        except Exception as e:
            results[name] = f"✗ {str(e)[:60]}"
    return results

# ── Main ──────────────────────────────────────────────────
if __name__ == "__main__":
    lines = fetch_logs()
    events, total_profit, total_gas = parse_logs(lines)
    redis_stats = fetch_redis_stats()
    rpc_health = check_rpc_health()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    
    has_activity = any(v > 0 for v in events.values())
    
    if not has_activity:
        print(f"## Hourly Report — {now}")
        print()
        print("No activity detected in the last hour.")
        print()
        print("### RPC Health")
        for name, status in rpc_health.items():
            print(f"- {name}: {status}")
        print()
        print("Bot is alive and waiting for opportunities.")
    else:
        print(f"## Hourly Report — {now}")
        print()
        print("### Activity")
        for key in ["opportunities", "submitted", "confirmed", "reverted", "lost_race", "competitors_seen"]:
            if events.get(key, 0) > 0:
                print(f"- {key.replace('_', ' ').title()}: {events[key]}")
        
        if events.get("submitted", 0) > 0:
            net = total_profit - total_gas
            print()
            print("### Financials")
            print(f"- Gross profit: ${total_profit:,.2f}")
            print(f"- Gas spent: ${total_gas:,.2f}")
            print(f"- Net profit: ${net:,.2f}")
        
        if events.get("rpc_failover", 0) > 0:
            print(f"\n⚠️ RPC Failover events: {events['rpc_failover']}")
        if events.get("killswitch_triggered", 0) > 0:
            print(f"\n🚨 KILL SWITCH triggered: {events['killswitch_triggered']}")
        if events.get("swap_calldata_fail", 0) > 0:
            print(f"\n⚠️ Swap calldata failures: {events['swap_calldata_fail']}")
        
        print()
        print("### RPC Health")
        for name, status in rpc_health.items():
            print(f"- {name}: {status}")
        
        if redis_stats:
            print()
            print("### Redis Stats (latest minute)")
            print(f"- Oracle signals: {redis_stats.get('oracle_signals', 'N/A')}")
            print(f"- Simulations: {redis_stats.get('simulations', 'N/A')}")
            print(f"- Opportunities: {redis_stats.get('opportunities', 'N/A')}")
            print(f"- Submitted: {redis_stats.get('submitted', 'N/A')}")
            print(f"- Confirmed: {redis_stats.get('confirmed', 'N/A')}")
            print(f"- Reverted: {redis_stats.get('reverted', 'N/A')}")
            print(f"- Lost race: {redis_stats.get('lost_race', 'N/A')}")
            print(f"- Total EV captured: ${redis_stats.get('total_ev', 'N/A')}")
