#!/usr/bin/env python3
"""Aave V3 Base liquidations via mainnet.base.org — 2 weeks."""
import time
from collections import defaultdict
from datetime import datetime, timezone
from web3 import Web3

w3 = Web3(Web3.HTTPProvider("https://mainnet.base.org"))
current = w3.eth.block_number
TWO_WEEKS = 604_800
start_block = current - TWO_WEEKS

POOL = "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5"
SIG = "0xe413a321e8681d831f20dbcc4619fd8e1863a956cd3e4259bb01b5e315f7712c"

CHUNK = 10_000
total_blocks = current - start_block
all_logs = []
daily = defaultdict(lambda: {"count": 0, "debt_usd": 0.0, "collat_usd": 0.0})
errors = 0

print(f"Base Aave V3 — {start_block:,} → {current:,} ({total_blocks:,} blocks)")

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
        
        if logs or i % 100_000 == 0:
            print(f"  [{fr:,}→{to:,}] {len(logs):>4} evts | {pct:5.1f}% | total={len(all_logs)}", flush=True)
        
        time.sleep(0.15)
        
    except Exception as e:
        errors += 1
        err = str(e)[:120]
        if errors <= 3:
            print(f"  [{fr:,}→{to:,}] ERR: {err}", flush=True)
        time.sleep(1)

print(f"\n{'='*60}")
print(f"Base Aave V3 — TOTAL: {len(all_logs)} liquidations in 2 weeks")
print(f"Errors: {errors}/{total_blocks//CHUNK} chunks")

if all_logs:
    for day in sorted(daily.keys()):
        d = daily[day]
        print(f"  {day}: {d['count']:>4} evts | ${d['debt_usd']:>12,.0f} debt | {d['collat_usd']:>12,.4f} collat")
    total_debt = sum(d["debt_usd"] for d in daily.values())
    total_collat = sum(d["collat_usd"] for d in daily.values())
    print(f"\n  TOTAL: ${total_debt:,.0f} debt, {total_collat:.4f} collateral")
else:
    print("ZERO liquidations.")
