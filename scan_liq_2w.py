#!/usr/bin/env python3
"""Query Aave V3 liquidation events on Arbitrum via DRPC — 2 weeks, 10K-block chunks."""
import os, sys, time
from collections import defaultdict
from datetime import datetime, timezone

os.chdir("/home/ubuntu/defi_flash_bot")
from dotenv import load_dotenv; load_dotenv()
from web3 import Web3

w3 = Web3(Web3.HTTPProvider(os.getenv("DRPC_RPC_URL")))
current = w3.eth.block_number
TWO_WEEKS = 4_838_400
start_block = current - TWO_WEEKS

POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
SIG = "0xe413a321e8681d831f20dbcc4619fd8e1863a956cd3e4259bb01b5e315f7712c"

CHUNK = 10_000
total_blocks = current - start_block
n_chunks = total_blocks // CHUNK + 1

all_logs = []
daily = defaultdict(lambda: {"count": 0, "debt_usd": 0.0, "collat_usd": 0.0})
block_times = {}  # cache block→timestamp

print(f"Block {start_block:,} → {current:,} ({total_blocks:,} blocks, {n_chunks} chunks)")

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
        
        pct = (i + CHUNK) / total_blocks * 100
        if logs:
            # Decode events
            for log in logs:
                topics = log["topics"]
                data = log["data"]
                debt_wei = int.from_bytes(data[0:32], 'big')
                collat_wei = int.from_bytes(data[32:64], 'big')
                
                # Get block timestamp
                bn = log["blockNumber"]
                if bn not in block_times:
                    block_times[bn] = w3.eth.get_block(bn)["timestamp"]
                ts = block_times[bn]
                day = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                
                daily[day]["count"] += 1
                daily[day]["debt_usd"] += debt_wei / 1e6   # assume USDC debt
                daily[day]["collat_usd"] += collat_wei / 1e18  # assume ETH collateral
        
        print(f"  [{fr:,}→{to:,}] {len(logs):>4} evts | {min(pct,100):5.1f}% | total={len(all_logs)}", flush=True)
        time.sleep(0.08)
        
    except Exception as e:
        print(f"  [{fr:,}→{to:,}] ERROR: {str(e)[:150]}", flush=True)
        time.sleep(1)
        continue

print(f"\n{'='*60}")
print(f"TOTAL: {len(all_logs)} liquidation events in 2 weeks")
print(f"{'='*60}")

if all_logs:
    print(f"\nDaily breakdown:")
    print(f"{'Date':>12} | {'Events':>6} | {'Debt (USDC)':>14} | {'Collateral (ETH)':>16}")
    print("-" * 58)
    for day in sorted(daily.keys()):
        d = daily[day]
        print(f"{day:>12} | {d['count']:>6} | ${d['debt_usd']:>12,.0f} | {d['collat_usd']:>14,.4f}")
    
    # Totals
    total_debt = sum(d["debt_usd"] for d in daily.values())
    total_collat = sum(d["collat_usd"] for d in daily.values())
    print(f"{'TOTAL':>12} | {len(all_logs):>6} | ${total_debt:>12,.0f} | {total_collat:>14,.4f}")
    
    # Unique users
    users = set()
    for log in all_logs:
        users.add("0x" + log["topics"][3].hex()[-40:])
    print(f"\nUnique users liquidated: {len(users)}")
else:
    print("ZERO liquidations in 2 weeks.")
