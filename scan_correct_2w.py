#!/usr/bin/env python3
"""Re-scan Aave V3 Arbitrum 2-week liquidations with CORRECT event signature."""
import os, sys, time
from collections import defaultdict
from datetime import datetime, timezone

os.chdir("/home/ubuntu/defi_flash_bot")
from dotenv import load_dotenv; load_dotenv()
from web3 import Web3
from eth_hash.auto import keccak

w3 = Web3(Web3.HTTPProvider(os.getenv("DRPC_RPC_URL")))
current = w3.eth.block_number
TWO_WEEKS = 4_838_400
start_block = current - TWO_WEEKS

POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
# CORRECT signature
SIG = "0x" + keccak(b"LiquidationCall(address,address,address,uint256,uint256,address,bool)").hex()

CHUNK = 10_000
total_blocks = current - start_block
all_logs = []
daily = defaultdict(lambda: {"count": 0, "debt_usd": 0.0, "collat_usd": 0.0, "users": set()})

print(f"Aave V3 Arbitrum CORRECT scan — {start_block:,} → {current:,} ({total_blocks:,} blocks)")
print(f"Topic: {SIG}")

for i in range(0, total_blocks, CHUNK):
    fr = start_block + i
    to = min(fr + CHUNK, current)
    try:
        logs = w3.eth.get_logs({"address": POOL, "fromBlock": fr, "toBlock": to, "topics": [SIG]})
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
                user = "0x" + topics[3].hex()[-40:]
                daily[day]["count"] += 1
                daily[day]["debt_usd"] += debt_wei / 1e6
                daily[day]["collat_usd"] += collat_wei / 1e18
                daily[day]["users"].add(user)
        
        if logs or i % 500_000 == 0:
            print(f"  [{fr:,}→{to:,}] {len(logs):>4} evts | {pct:5.1f}% | total={len(all_logs)}", flush=True)
        time.sleep(0.08)
    except Exception as e:
        print(f"  [{fr:,}→{to:,}] ERR: {str(e)[:100]}", flush=True)
        time.sleep(1)

print(f"\n{'='*60}")
print(f"TOTAL: {len(all_logs)} liquidation events in 2 weeks (correct hash)")

if all_logs:
    print(f"\n{'Date':>12} | {'Events':>6} | {'Debt (USDC)':>14} | {'Coll (ETH)':>14} | {'Users':>6}")
    print("-" * 65)
    for day in sorted(daily.keys()):
        d = daily[day]
        print(f"{day:>12} | {d['count']:>6} | ${d['debt_usd']:>12,.0f} | {d['collat_usd']:>12,.4f} | {len(d['users']):>6}")
    total_debt = sum(d["debt_usd"] for d in daily.values())
    total_collat = sum(d["collat_usd"] for d in daily.values())
    all_users = set()
    for d in daily.values(): all_users.update(d["users"])
    print(f"{'TOTAL':>12} | {len(all_logs):>6} | ${total_debt:>12,.0f} | {total_collat:>12,.4f} | {len(all_users):>6}")
else:
    print("Still zero — even with correct hash?")
