#!/usr/bin/env python3
"""Aave V3 liquidation events on Base — 2 weeks via PublicNode."""
import os, sys, time
from collections import defaultdict
from datetime import datetime, timezone

os.chdir("/home/ubuntu/defi_flash_bot")
from dotenv import load_dotenv; load_dotenv()
from web3 import Web3

w3 = Web3(Web3.HTTPProvider("https://base.publicnode.com"))
current = w3.eth.block_number

# Base block time ~2s: 14 * 86400 / 2 = 604,800
TWO_WEEKS = 604_800
start_block = current - TWO_WEEKS

POOL = "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5"
SIG = "0xe413a321e8681d831f20dbcc4619fd8e1863a956cd3e4259bb01b5e315f7712c"

CHUNK = 10_000
total_blocks = current - start_block
n_chunks = total_blocks // CHUNK + 1

all_logs = []
daily = defaultdict(lambda: {"count": 0, "debt_usd": 0.0, "collat_usd": 0.0})

print(f"Base Aave V3 — block {start_block:,} → {current:,} ({total_blocks:,} blocks, {n_chunks} chunks)")

for i in range(0, total_blocks, CHUNK):
    fr = start_block + i
    to = min(fr + CHUNK, current)
    
    try:
        logs = w3.eth.get_logs({
            "address": POOL,
            "fromBlock": fr,
            "toBlock": to,
            "topics": [SIG]
        })
        all_logs.extend(logs)
        
        pct = min((i + CHUNK) / total_blocks * 100, 100)
        
        if logs:
            for log in logs:
                topics = log["topics"]
                data = log["data"]
                debt_wei = int.from_bytes(data[0:32], 'big')
                collat_wei = int.from_bytes(data[32:64], 'big')
                bn = log["blockNumber"]
                block = w3.eth.get_block(bn)
                day = datetime.fromtimestamp(block["timestamp"], tz=timezone.utc).strftime("%Y-%m-%d")
                daily[day]["count"] += 1
                daily[day]["debt_usd"] += debt_wei / 1e6
                daily[day]["collat_usd"] += collat_wei / 1e18
        
        if logs or i % 100_000 == 0:
            print(f"  [{fr:,}→{to:,}] {len(logs):>4} evts | {pct:5.1f}% | total={len(all_logs)}", flush=True)
        
        time.sleep(0.15)
        
    except Exception as e:
        err = str(e)[:120]
        print(f"  [{fr:,}→{to:,}] ERR: {err}", flush=True)
        time.sleep(1)

print(f"\n{'='*60}")
print(f"Base Aave V3 — TOTAL: {len(all_logs)} liquidations in 2 weeks")

if all_logs:
    print(f"\nDaily breakdown:")
    print(f"{'Date':>12} | {'Events':>6} | {'Debt (USDC)':>14} | {'Collateral (ETH)':>16}")
    print("-" * 58)
    for day in sorted(daily.keys()):
        d = daily[day]
        print(f"{day:>12} | {d['count']:>6} | ${d['debt_usd']:>12,.0f} | {d['collat_usd']:>14,.4f}")
    total_debt = sum(d["debt_usd"] for d in daily.values())
    total_collat = sum(d["collat_usd"] for d in daily.values())
    print(f"{'TOTAL':>12} | {len(all_logs):>6} | ${total_debt:>12,.0f} | {total_collat:>14,.4f}")
else:
    print("ZERO liquidations.")
