#!/usr/bin/env python3
"""Scrape Aave V3 Arbitrum borrowers. Key from file, never in stdout."""
import json, time, os, sys

# Read key from .env — never printed
with open("/home/ubuntu/defi_flash_bot/.env") as f:
    for line in f:
        if line.startswith("GRAPH_API_KEY="):
            key = line.split("=", 1)[1].strip()
            break

sys.stderr.write(f"Key: loaded ({len(key)} chars)\n")
sys.stderr.flush()

url = f"https://gateway.thegraph.com/api/{key}/subgraphs/id/GQFbb95cE6d8mV989mL5figjaGaKCQB3xqYrr1bRyXqF"
del key  # wipe from local scope

import urllib.request
import urllib.error

def query(query_str):
    data = json.dumps({"query": query_str}).encode()
    req = urllib.request.Request(url, data=data, headers={
        "Content-Type": "application/json",
        "User-Agent": "curl/8.0",
    })
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)

# Quick count via first page
result = query('{ userReserves(first: 1000, where: {currentTotalDebt_gt: "0"}) { id user { id } reserve { symbol decimals } currentTotalDebt } }')
items = result["data"]["userReserves"]
sys.stderr.write(f"First page: {len(items)} items\n")
sys.stderr.write(f"Sample: {items[0]['user']['id']} | {items[0]['reserve']['symbol']} | debt={items[0]['currentTotalDebt']}\n")
sys.stderr.flush()

# Collect with pagination
borrowers = {}  # addr -> positions
last_id = ""
page = 0

while True:
    id_f = f', id_gt: "{last_id}"' if last_id else ''
    q = f'{{ userReserves(first: 1000, where: {{currentTotalDebt_gt: "0"{id_f}}}) {{ id user {{ id }} reserve {{ symbol decimals }} currentTotalDebt currentATokenBalance usageAsCollateralEnabledOnUser }} }}'
    
    try:
        data = query(q)
        items = data.get("data", {}).get("userReserves", [])
    except Exception as e:
        sys.stderr.write(f"ERROR: {e}\n")
        break
    
    if not items:
        break
    
    for item in items:
        addr = item["user"]["id"]
        sym = item["reserve"]["symbol"]
        dec = int(item["reserve"]["decimals"])
        debt = int(item["currentTotalDebt"])
        human = debt / (10**dec)
        if human < 0.01:
            continue
        if addr not in borrowers:
            borrowers[addr] = {}
        borrowers[addr][sym] = {
            "debt_raw": str(debt), "debt_human": round(human, 4),
            "collateral_enabled": item.get("usageAsCollateralEnabledOnUser", False),
        }
    
    last_id = items[-1]["id"]
    page += 1
    
    if page % 5 == 0:
        sys.stderr.write(f"Page {page}: {len(borrowers)} borrowers\n")
        sys.stderr.flush()
    time.sleep(0.2)

sys.stderr.write(f"\nTotal: {len(borrowers)} borrowers, {page} pages\n")
sys.stderr.flush()

# Sort by rough USD value
for addr in borrowers:
    total = 0
    for sym, p in borrowers[addr].items():
        total += p["debt_human"] * (1 if "USDC" in sym or "USDT" in sym or "DAI" in sym else 2000)
    borrowers[addr]["_total_usd"] = round(total, 2)

sorted_b = sorted(borrowers.items(), key=lambda x: x[1]["_total_usd"], reverse=True)

# Save
with open("/home/ubuntu/defi_flash_bot/data/aave_borrowers.json", "w") as f:
    json.dump({"total": len(borrowers), "borrowers": dict(sorted_b)}, f, indent=2)

with open("/home/ubuntu/defi_flash_bot/data/aave_borrower_addresses.txt", "w") as f:
    for addr, _ in sorted_b:
        f.write(addr + "\n")

sys.stderr.write(f"Saved {len(borrowers)} borrowers\n")
sys.stderr.flush()
