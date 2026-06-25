#!/usr/bin/env python3
"""Query Compound V3 AbsorbCollateral events on Arbitrum — 2 weeks."""
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

# Compound V3 USDC Comet on Arbitrum
COMET = "0xA5EDBDD9646f8dFF606d7448e414884C7d905dCA"
# AbsorbCollateral(address,address,address,uint256,uint256)
ABSORB_SIG = "0xcc5fbb9e8de4afb50c44e0297eef78eec2bb58dc255d4d1336c6c8abfa20e45a"

CHUNK = 10_000
total_blocks = current - start_block

all_logs = []
daily = defaultdict(lambda: {"count": 0})

print(f"Compound V3 USDC Arbitrum — scanning {total_blocks:,} blocks...")

for i in range(0, total_blocks, CHUNK):
    fr = start_block + i
    to = min(fr + CHUNK, current)
    
    try:
        logs = w3.eth.get_logs({
            "address": COMET,
            "fromBlock": fr,
            "toBlock": to,
            "topics": [ABSORB_SIG]
        })
        all_logs.extend(logs)
        pct = (i + CHUNK) / total_blocks * 100
        
        if logs:
            for log in logs:
                bn = log["blockNumber"]
                block = w3.eth.get_block(bn)
                day = datetime.fromtimestamp(block["timestamp"], tz=timezone.utc).strftime("%Y-%m-%d")
                daily[day]["count"] += 1
        
        if logs or i % 500_000 == 0:
            print(f"  [{fr:,}→{to:,}] {len(logs):>4} absorbs | {min(pct,100):5.1f}% | total={len(all_logs)}", flush=True)
        
        time.sleep(0.08)
        
    except Exception as e:
        print(f"  [{fr:,}→{to:,}] ERR: {str(e)[:120]}", flush=True)
        time.sleep(1)

print(f"\n{'='*60}")
print(f"TOTAL Compound V3 AbsorbCollateral events: {len(all_logs)}")
if all_logs:
    print(f"\nDaily breakdown:")
    for day in sorted(daily.keys()):
        print(f"  {day}: {daily[day]['count']} events")
else:
    print("ZERO Compound V3 liquidations in 2 weeks.")
