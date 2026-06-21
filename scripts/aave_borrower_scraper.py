#!/usr/bin/env python3
"""Scrape all Aave V3 Arbitrum borrowers. API key read from file, never printed."""
import json, subprocess, time, sys

# Ensure unbuffered output for background mode
sys.stdout.reconfigure(line_buffering=True) if hasattr(sys.stdout, 'reconfigure') else None

def load_key():
    """Load API key — value never printed, only used in subprocess calls."""
    # Read from .env directly into variable (never printed)
    result = subprocess.run(
        "grep GRAPH_API_KEY /home/ubuntu/defi_flash_bot/.env | cut -d= -f2",
        shell=True, capture_output=True, text=True, timeout=5
    )
    return result.stdout.strip()

API_KEY = load_key()
print(f"API key: loaded ({len(API_KEY)} chars)")

SUBGRAPH_ID = "GQFbb95cE6d8mV989mL5figjaGaKCQB3xqYrr1bRyXqF"
URL = f"https://gateway.thegraph.com/api/{API_KEY}/subgraphs/id/{SUBGRAPH_ID}"

# Wipe the key variable from memory so it can't leak in tracebacks
del API_KEY

def query(query_str, timeout=30):
    """Query the subgraph via curl. Key passes through subprocess only."""
    payload = json.dumps({"query": query_str})
    for attempt in range(3):
        try:
            result = subprocess.run(
                ["curl", "-s", "-X", "POST", URL, "-H", "Content-Type: application/json", "-d", payload],
                capture_output=True, text=True, timeout=timeout
            )
            data = json.loads(result.stdout)
            if "errors" in data:
                raise Exception(json.dumps(data["errors"]))
            return data
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)

# Paginate all userReserves
borrowers = {}
last_id = ""
page = 0
total_items = 0

while True:
    id_filter = f', id_gt: "{last_id}"' if last_id else ''
    q = f'{{ userReserves(first: 1000, where: {{currentTotalDebt_gt: "0"}}{id_filter}) {{ id user {{ id }} reserve {{ symbol decimals }} currentTotalDebt currentATokenBalance usageAsCollateralEnabledOnUser }} }}'

    try:
        result = query(q)
        items = result.get("data", {}).get("userReserves", [])
    except Exception as e:
        print(f"ERROR page {page}: {e}")
        break

    if not items:
        print(f"Done at page {page}")
        break

    for item in items:
        addr = item["user"]["id"]
        symbol = item["reserve"]["symbol"]
        decimals = int(item["reserve"]["decimals"])
        raw_debt = int(item["currentTotalDebt"])
        human_debt = raw_debt / (10 ** decimals)
        if human_debt < 0.01:
            continue

        if addr not in borrowers:
            borrowers[addr] = {"positions": {}}
        borrowers[addr]["positions"][symbol] = {
            "debt_raw": str(raw_debt),
            "debt_human": round(human_debt, 4),
            "collateral_raw": item["currentATokenBalance"],
            "collateral_enabled": item["usageAsCollateralEnabledOnUser"],
            "decimals": decimals,
        }

    last_id = items[-1]["id"]
    total_items += len(items)
    page += 1

    if page % 10 == 0:
        print(f"Page {page}: {len(borrowers)} borrowers, {total_items} total positions")
    time.sleep(0.25)

# Compute rough USD totals
print(f"\nTotal: {page} pages, {len(borrowers)} borrowers, {total_items} positions")

for addr in borrowers:
    total = 0
    for p in borrowers[addr]["positions"].values():
        if p["decimals"] <= 6:
            total += p["debt_human"]
        else:
            total += p["debt_human"] * 2000
    borrowers[addr]["total_usd_approx"] = round(total, 2)

sorted_borrowers = sorted(borrowers.items(), key=lambda x: x[1]["total_usd_approx"], reverse=True)

# Top 30
print(f"\nTop 30 borrowers:")
for i, (addr, data) in enumerate(sorted_borrowers[:30]):
    top_positions = sorted(data["positions"].values(), key=lambda x: x["debt_human"], reverse=True)[:3]
    positions_str = ", ".join(f"{p['debt_human']:.2f}" for p in top_positions)
    has_collateral = any(p["collateral_enabled"] for p in data["positions"].values())
    collat_tag = " [COLLATERAL]" if has_collateral else ""
    print(f"  {i+1:2d}. {addr}: ~${data['total_usd_approx']:>12,.0f} | {positions_str}{collat_tag}")

# Save
out = {"total_borrowers": len(borrowers), "borrowers": dict(sorted_borrowers)}
with open("/home/ubuntu/defi_flash_bot/data/aave_borrowers.json", "w") as f:
    json.dump(out, f, indent=2)

with open("/home/ubuntu/defi_flash_bot/data/aave_borrower_addresses.txt", "w") as f:
    for addr, _ in sorted_borrowers:
        f.write(addr + "\n")

print(f"\nSaved to data/aave_borrowers.json and data/aave_borrower_addresses.txt")
