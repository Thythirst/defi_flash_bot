#!/usr/bin/env python3
"""Aave V3 Base liquidations via Alchemy — with aggressive rate-limiting."""
import os, sys, time
from collections import defaultdict
from datetime import datetime, timezone

os.chdir("/home/ubuntu/defi_flash_bot")
from dotenv import load_dotenv; load_dotenv()
from web3 import Web3

w3 = Web3(Web3.HTTPProvider(os.getenv("BASE_RPC_URL")))
current = w3.eth.block_number
TWO_WEEKS = 604_800
start_block = current - TWO_WEEKS

POOL = "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5"
SIG = "0xe413a321e8681d831f20dbcc4619fd8e1863a956cd3e4259bb01b5e315f7712c"

# Use 50K chunks, 0.5s delay — Alchemy allows ~330M CU/month for free tier
# eth_getLogs costs ~75 CU, so ~4.4M calls/month ≈ 1.7 calls/sec
CHUNK = 50_000
total_blocks = current - start_block
all_logs = []
daily = defaultdict(lambda: {"count": 0, "debt_usd": 0.0, "collat_usd": 0.0})

print(f"Base Aave V3 via Alchemy — {start_block:,} → {current:,} ({total_blocks:,} blocks)")

for i in range(0, total_blocks, CHUNK):
    fr = start_block + i
    to = min(fr + CHUNK, current)
    
    try:
        logs = w3.eth.get_logs({
            "address": POOL, "fromBlock": fr, "toBlock": to, "topics": [SIG]
        })
        all_logs.extend(logs)
        pct = min((i + CHUNK) / total_blocks * 100, 100)
        
        if logs:
            for log in logs:
                bn = log["blockNumber"]
                block = w3.eth.get_block(bn)
                day = datetime.fromtimestamp(block["timestamp"], tz=timezone.utc).strftime("%Y-%m-%d")
                data = log["data"]
                daily[day]["count"] += 1
                daily[day]["debt_usd"] += int.from_bytes(data[0:32], 'big') / 1e6
                daily[day]["collat_usd"] += int.from_bytes(data[32:64], 'big') / 1e18
        
        if logs or i % 200_000 == 0:
            print(f"  [{fr:,}→{to:,}] {len(logs):>4} evts | {pct:5.1f}% | total={len(all_logs)}", flush=True)
        
        time.sleep(0.5)  # Conservative rate limit
        
    except Exception as e:
        err = str(e)[:150]
        print(f"  [{fr:,}→{to:,}] ERR: {err}", flush=True)
        # On 429, wait longer
        if "429" in err or "Too Many" in err:
            print(f"  Rate limited, waiting 5s...", flush=True)
            time.sleep(5)
        else:
            time.sleep(1)

print(f"\n{'='*60}")
print(f"Base Aave V3 — TOTAL: {len(all_logs)} liquidations in 2 weeks")

if all_logs:
    for day in sorted(daily.keys()):
        d = daily[day]
        print(f"  {day}: {d['count']:>4} evts | ${d['debt_usd']:>12,.0f} debt | {d['collat_usd']:>12,.4f} collateral")
    print(f"\n  TOTAL: ${sum(d['debt_usd'] for d in daily.values()):,.0f} debt, {sum(d['collat_usd'] for d in daily.values()):.4f} collateral")
else:
    print("ZERO liquidations.")
