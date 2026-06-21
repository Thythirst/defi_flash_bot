#!/usr/bin/env python3
import json, subprocess, time, os

ENV = "/home/ubuntu/defi_flash_bot/.env"
with open(ENV) as f:
    for line in f:
        if line.startswith("GRAPH_API_KEY=***            KEY = line.split("=", 1)[1].strip()
            break
URL = f"https://gateway.thegraph.com/api/{KEY}/subgraphs/id/GQFbb95cE6d8mV989mL5figjaGaKCQB3xqYrr1bRyXqF"
del KEY

def q(s):
    p = json.dumps({"query": s})
    for _ in range(3):
        r = subprocess.run(["curl","-s","-X","POST",URL,"-H","Content-Type: application/json","-d",p], capture_output=True, text=True, timeout=30)
        if r.returncode==0 and r.stdout.strip():
            d = json.loads(r.stdout)
            if "errors" not in d: return d
        time.sleep(2**_)
    raise Exception("fail")

b, lid, pg = {}, "", 0
while True:
    f = f', id_gt: "{lid}"' if lid else ''
    g = f'{{ userReserves(first: 1000, where: {{currentTotalDebt_gt: "0"{f}}}) {{ id user {{ id }} reserve {{ symbol decimals }} currentTotalDebt currentATokenBalance usageAsCollateralEnabledOnUser }} }}'
    d = q(g); items = d.get("data",{}).get("userReserves",[])
    if not items: break
    for x in items:
        a = x["user"]["id"]; s = x["reserve"]["symbol"]; dec = int(x["reserve"]["decimals"])
        h = int(x["currentTotalDebt"])/(10**dec)
        if h<0.01: continue
        b.setdefault(a,{})[s] = {"debt_raw": x["currentTotalDebt"], "debt_human": round(h,4), "col": x.get("usageAsCollateralEnabledOnUser",False)}
    lid = items[-1]["id"]; pg += 1
    if pg%10==0: print(f"Page {pg}: {len(b)} borrowers", flush=True)
    time.sleep(0.15)

print(f"\nDone: {pg} pages, {len(b)} borrowers", flush=True)
for a in b:
    t = sum(p["debt_human"]*(1 if any(x in s for x in ["USDC","USDT","DAI","EURC","GHO"]) else 2000) for s,p in b[a].items())
    b[a]["_usd"] = round(t,2)

sb = sorted(b.items(), key=lambda x: x[1]["_usd"], reverse=True)
for i,(a,d) in enumerate(sb[:20]):
    r = {k:v for k,v in d.items() if k!="_usd"}
    tp = sorted(r.items(), key=lambda x: x[1]["debt_human"], reverse=True)[:3]
    ts = ", ".join(f"{s}:{p['debt_human']:.2f}" for s,p in tp)
    co = " [COL]" if any(p.get("col") for p in r.values()) else ""
    print(f"  {i+1}. {a}: ~${d['_usd']:,.0f} | {ts}{co}", flush=True)

os.makedirs("/home/ubuntu/defi_flash_bot/data", exist_ok=True)
json.dump({"total":len(b),"borrowers":dict(sb)}, open("/home/ubuntu/defi_flash_bot/data/aave_borrowers.json","w"), indent=2)
open("/home/ubuntu/defi_flash_bot/data/aave_borrower_addresses.txt","w").write("\n".join(a for a,_ in sb)+"\n")
print(f"Saved {len(b)} borrowers", flush=True)
