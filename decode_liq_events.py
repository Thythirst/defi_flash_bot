#!/usr/bin/env python3
"""Decode the 43 liquidation events with correct token decimals."""
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
SIG = "0x" + keccak(b"LiquidationCall(address,address,address,uint256,uint256,address,bool)").hex()

# Known token decimals (from pipeline_v3.py)
DECIMALS = {
    "0xaf88d065e77c8cC2239327C5EDb3A432268e5831": 6,   # USDC native
    "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8": 6,   # USDC.e
    "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9": 6,   # USDT
    "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1": 18,  # WETH
    "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f": 8,   # WBTC
    "0x912CE59144191C1204E64559FE8253a0e49E6548": 18,  # ARB
    "0x5979D7b546E38E414F7E9822514be443A4800529": 18,  # wstETH
    "0xda10009cBd5D07dd0CeCc66161FC93D7c9000da1": 18,  # DAI
}

# Approximate USD prices (from current market)
PRICES = {
    "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1": 1675,  # WETH
    "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f": 87000, # WBTC
    "0x912CE59144191C1204E64559FE8253a0e49E6548": 0.25,  # ARB
    "0x5979D7b546E38E414F7E9822514be443A4800529": 2075,  # wstETH (1.237 * 1675)
}

# ERC20 decimals ABI
ERC20_ABI = '[{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"}]'

all_logs = []
decimals_cache = {}

def get_decimals(asset):
    asset = Web3.to_checksum_address(asset)
    if asset in DECIMALS:
        return DECIMALS[asset]
    if asset in decimals_cache:
        return decimals_cache[asset]
    try:
        contract = w3.eth.contract(address=asset, abi=json.loads(ERC20_ABI))
        d = contract.functions.decimals().call()
        decimals_cache[asset] = d
        return d
    except:
        decimals_cache[asset] = 18
        return 18

def get_price(asset):
    asset = Web3.to_checksum_address(asset)
    # USDC/USDT = $1
    if DECIMALS.get(asset) == 6:
        return 1.0
    return PRICES.get(asset, 0)

import json

print("Collecting events...")
for i in range(0, TWO_WEEKS, 10_000):
    fr = start_block + i
    to = min(fr + 10_000, current)
    try:
        logs = w3.eth.get_logs({"address": POOL, "fromBlock": fr, "toBlock": to, "topics": [SIG]})
        all_logs.extend(logs)
        if i % 500_000 == 0:
            print(f"  {i//10_000}/{TWO_WEEKS//10_000} chunks, {len(all_logs)} events...", flush=True)
        time.sleep(0.08)
    except:
        time.sleep(1)

print(f"\nTotal: {len(all_logs)} events\n")

daily = defaultdict(lambda: {"count": 0, "debt_usd": 0.0, "collat_usd": 0.0, "assets": set()})
all_users = set()

for log in all_logs:
    topics = log["topics"]
    data = log["data"]
    collateral_asset = Web3.to_checksum_address("0x" + topics[1].hex()[-40:])
    debt_asset = Web3.to_checksum_address("0x" + topics[2].hex()[-40:])
    user = "0x" + topics[3].hex()[-40:]
    debt_raw = int.from_bytes(data[0:32], 'big')
    collat_raw = int.from_bytes(data[32:64], 'big')
    
    debt_dec = get_decimals(debt_asset)
    collat_dec = get_decimals(collateral_asset)
    debt_usd = (debt_raw / 10**debt_dec) * get_price(debt_asset)
    collat_usd = (collat_raw / 10**collat_dec) * get_price(collateral_asset)
    
    bn = log["blockNumber"]
    block = w3.eth.get_block(bn)
    day = datetime.fromtimestamp(block["timestamp"], tz=timezone.utc).strftime("%Y-%m-%d")
    
    daily[day]["count"] += 1
    daily[day]["debt_usd"] += debt_usd
    daily[day]["collat_usd"] += collat_usd
    daily[day]["assets"].add(debt_asset)
    all_users.add(user)

print(f"{'Date':>12} | {'Evts':>4} | {'Debt USD':>14} | {'Coll USD':>14} | {'Users':>5} | Assets")
print("-" * 85)
for day in sorted(daily.keys()):
    d = daily[day]
    asset_list = ','.join(a[:8] for a in sorted(d["assets"]))
    print(f"{day:>12} | {d['count']:>4} | ${d['debt_usd']:>12,.0f} | ${d['collat_usd']:>12,.0f} | {len(set()) if False else '':>5} | {asset_list[:40]}")

total_debt = sum(d["debt_usd"] for d in daily.values())
total_collat = sum(d["collat_usd"] for d in daily.values())
print(f"{'TOTAL':>12} | {len(all_logs):>4} | ${total_debt:>12,.0f} | ${total_collat:>12,.0f} | {len(all_users):>5}")

# Show a few sample events
print(f"\nSample events (first 5):")
for log in all_logs[:5]:
    topics = log["topics"]
    data = log["data"]
    coll = "0x" + topics[1].hex()[-40:]
    debt = "0x" + topics[2].hex()[-40:]
    user = "0x" + topics[3].hex()[-40:]
    debt_raw = int.from_bytes(data[0:32], 'big')
    collat_raw = int.from_bytes(data[32:64], 'big')
    d = get_decimals(debt)
    debt_usd = (debt_raw / 10**d) * get_price(debt)
    print(f"  Block {log['blockNumber']:,}: user={user[:10]}... debt={debt_usd:,.0f} USD coll={collat_raw / 10**get_decimals(coll):.4f}")
