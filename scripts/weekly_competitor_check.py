#!/usr/bin/env python3
"""Weekly Competitor Liquidation Monitor — Lightweight RPC Pulse Check.

Uses public Arbitrum RPC with small eth_getLogs windows to avoid rate limits.
Checks recent blocks for Aave V3 + Compound V3 liquidation activity.
If the market is quiet, reports quickly. If activity found, decodes competitors.

Usage:
    python3 scripts/weekly_competitor_check.py
    python3 scripts/weekly_competitor_check.py --hours 168  # full week
    python3 scripts/weekly_competitor_check.py --json
"""

import argparse
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

from dotenv import load_dotenv
from web3 import Web3

load_dotenv(os.path.expanduser("~/defi_flash_bot/.env"))

# ── Config ────────────────────────────────────────────────────
RPC_URL = "https://arb1.arbitrum.io/rpc"

AAVE_V3_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
COMPOUND_MARKETS = {
    "cUSDCv3": "0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf",
    "cUSDTv3": "0xd98Be00b5D27fc98112BdE293e487f8D4cA57d07",
    "cWETHv3": "0x6f7D514bbD4aFf3BcD1140B7344b32f063dEe486",
}

BOT_ADDRESS = os.getenv("BOT_ADDRESS", "0x1269800101780229B50919e1e27be62DC6279e9B").lower()

LIQUIDATION_CALL_TOPIC = Web3.keccak(
    text="LiquidationCall(address,address,address,uint256,uint256,address,bool)"
).hex()
ABSORB_COLLATERAL_TOPIC = Web3.keccak(
    text="AbsorbCollateral(address,address,address,uint256,uint256,uint256)"
).hex()

# Small windows to avoid RPC rate limits
WINDOW_SIZE = 2_000  # blocks per request
BATCH_PAUSE = 2.0    # seconds between batches
BLOCKS_PER_HOUR = 60 * 60 // 0.25  # ~14,400 blocks/hour


def connect():
    w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={"timeout": 20}))
    if not w3.is_connected():
        sys.exit(f"ERROR: Cannot connect to {RPC_URL}")
    return w3


def scan_events(w3, contract, topic, from_block, to_block, max_hours):
    """Scan blocks in small windows. Returns list of log dicts."""
    all_logs = []
    current = from_block
    batches = 0
    
    while current < to_block:
        end = min(current + WINDOW_SIZE - 1, to_block)
        try:
            logs = w3.eth.get_logs({
                "address": contract,
                "topics": [[topic]],
                "fromBlock": current,
                "toBlock": end,
            })
            all_logs.extend(logs)
        except Exception:
            pass  # skip failed windows
        
        batches += 1
        current = end + 1
        
        # Rate-limit pause
        if batches % 3 == 0:
            time.sleep(BATCH_PAUSE)
        
        # Early exit: if no logs after scanning 6 hours, market is dead
        if len(all_logs) == 0 and (current - from_block) > 6 * BLOCKS_PER_HOUR:
            break
    
    return all_logs


def decode_aave_liquidation(log):
    topics = log["topics"]
    data = log["data"]
    decoded = Web3().codec.decode(
        ["uint256", "uint256", "address", "bool"],
        bytes.fromhex(data[2:])
    )
    return {
        "tx_hash": log["transactionHash"].hex(),
        "block": log["blockNumber"],
        "collateral_asset": "0x" + topics[1].hex()[-40:],
        "debt_asset": "0x" + topics[2].hex()[-40:],
        "user": "0x" + topics[3].hex()[-40:],
        "debt_to_cover": decoded[0],
        "collateral_amount": decoded[1],
        "liquidator": decoded[2].lower(),
    }


def decode_compound_absorb(log):
    topics = log["topics"]
    data = log["data"]
    decoded = Web3().codec.decode(
        ["uint256", "uint256", "uint256"],
        bytes.fromhex(data[2:])
    )
    return {
        "tx_hash": log["transactionHash"].hex(),
        "block": log["blockNumber"],
        "liquidator": ("0x" + topics[1].hex()[-40:]).lower(),
        "user": "0x" + topics[2].hex()[-40:],
        "asset": "0x" + topics[3].hex()[-40:],
        "collateral_amount": decoded[0],
        "usd_value": decoded[1],
    }


def estimate_profit(debt_to_cover, debt_asset):
    """Conservative 5% liquidation bonus estimate."""
    asset = debt_asset.lower()
    if asset == "0x82af49447d8a07e3bd95bd0d56f35241523fbab1":  # WETH
        return round((debt_to_cover / 1e18) * 2000 * 0.05, 2)
    elif asset in ("0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                   "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8",
                   "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9"):
        return round((debt_to_cover / 1e6) * 0.05, 2)
    elif asset == "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f":
        return round((debt_to_cover / 1e8) * 80000 * 0.05, 2)
    return 0


def main():
    parser = argparse.ArgumentParser(description="Weekly competitor liquidation check")
    parser.add_argument("--hours", type=int, default=6,
                        help="Hours to scan (default: 6, for weekly use 168)")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    
    w3 = connect()
    to_block = w3.eth.block_number
    from_block = max(0, to_block - int(args.hours * BLOCKS_PER_HOUR))
    
    if not args.json:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        print(f"🔍 Liquidation Pulse Check — {now}")
        print(f"   Scanning past {args.hours}h ({from_block:,} → {to_block:,})")
        print()
    
    # ── Aave V3 ────────────────────────────────────────────────
    aave_raw = scan_events(w3, AAVE_V3_POOL, LIQUIDATION_CALL_TOPIC,
                           from_block, to_block, args.hours)
    aave_liqs = [decode_aave_liquidation(log) for log in aave_raw]
    
    # ── Compound V3 ────────────────────────────────────────────
    compound_liqs = []
    for market, comet in COMPOUND_MARKETS.items():
        raw = scan_events(w3, comet, ABSORB_COLLATERAL_TOPIC,
                         from_block, to_block, args.hours)
        decoded = [decode_compound_absorb(log) for log in raw]
        for d in decoded:
            d["market"] = market
        compound_liqs.extend(decoded)
    
    total = len(aave_liqs) + len(compound_liqs)
    
    # ── Report ─────────────────────────────────────────────────
    if args.json:
        output = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "block_range": {"from": from_block, "to": to_block},
            "hours_scanned": args.hours,
            "total_liquidations": total,
            "aave_count": len(aave_liqs),
            "compound_count": len(compound_liqs),
        }
        if total == 0:
            output["status"] = "quiet"
        else:
            output["status"] = "active"
            comps = defaultdict(lambda: {"aave": 0, "compound": 0})
            for liq in aave_liqs:
                comps[liq["liquidator"]]["aave"] += 1
            for liq in compound_liqs:
                comps[liq["liquidator"]]["compound"] += 1
            output["competitors"] = {
                addr: data for addr, data in
                sorted(comps.items(), key=lambda x: -(x[1]["aave"] + x[1]["compound"]))
            }
        print(json.dumps(output, indent=2))
        return
    
    print("=" * 55)
    print(f"  Aave V3 liquidations:       {len(aave_liqs):>5}")
    print(f"  Compound V3 liquidations:   {len(compound_liqs):>5}")
    print(f"  ────────────────────────────────")
    print(f"  Total:                      {total:>5}")
    print()
    
    if total == 0:
        print("  ✅ Market is quiet — zero liquidation activity detected.")
        print(f"     (Scanned {args.hours}h / {to_block - from_block:,} blocks)")
        return
    
    # ── Active market — show competitors ───────────────────────
    competitors = defaultdict(lambda: {"aave": 0, "compound": 0, "profit_usd": 0})
    for liq in aave_liqs:
        addr = liq["liquidator"]
        competitors[addr]["aave"] += 1
        competitors[addr]["profit_usd"] += estimate_profit(liq["debt_to_cover"], liq["debt_asset"])
    for liq in compound_liqs:
        addr = liq["liquidator"]
        competitors[addr]["compound"] += 1
    
    print("🏦 Top Liquidators")
    print(f"  {'Address':<44} {'Aave':>5} {'Comp':>5} {'Est.$':>8}")
    print("  " + "-" * 64)
    for addr, data in sorted(competitors.items(),
                             key=lambda x: -(x[1]["aave"] + x[1]["compound"]))[:10]:
        marker = " ⬅️ YOU" if addr == BOT_ADDRESS else ""
        print(f"  {addr:<44} {data['aave']:>5} {data['compound']:>5} "
              f"${data['profit_usd']:>7,.2f}{marker}")
    
    if BOT_ADDRESS not in competitors:
        print()
        print("  ⚠️  Your bot did NOT execute any liquidations this period.")
    
    our_count = competitors.get(BOT_ADDRESS, {}).get("aave", 0) + \
                competitors.get(BOT_ADDRESS, {}).get("compound", 0)
    share = our_count / total * 100 if total > 0 else 0
    print(f"\n  📊 Our share: {our_count}/{total} ({share:.1f}%)")


if __name__ == "__main__":
    main()
