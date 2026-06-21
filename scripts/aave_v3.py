import json, subprocess, time, os
with open("/home/ubuntu/defi_flash_bot/.env") as f:
    for line in f:
        if line.startswith("GRAPH_API_KEY="):
            KEY = line.split("=",1)[1].strip(); break
URL = f"https://gateway.thegraph.com/api/{KEY}/subgraphs/id/GQFbb95cE6d8mV989mL5figjaGaKCQB3xqYrr1bRyXqF"
del KEY
def gql(s):
    p=json.dumps({"query":s})
    for i in range(3):
        r=subprocess.run(["curl","-s","-X","POST",URL,"-H","Content-Type: application/json","-d",p],capture_output=True,text=True,timeout=30)
        if r.returncode==0 and r.stdout.strip():
            d=json.loads(r.stdout)
            if "errors" not in d: return d
        time.sleep(2**i)
b,lid,pg={},"",0
while True:
    f=f', id_gt: "{lid}"' if lid else ''
    q=f'{{ userReserves(first: 1000, where: {{currentTotalDebt_gt: "0"{f}}}) {{ id user {{ id }} reserve {{ symbol decimals }} currentTotalDebt currentATokenBalance usageAsCollateralEnabledOnUser }} }}'
    d=gql(q); it=d.get("data",{}).get("userReserves",[])
    if not it: break
    for x in it:
        a=x["user"]["id"];s=x["reserve"]["symbol"];dec=int(x["reserve"]["decimals"]);h=int(x["currentTotalDebt"])/(10**dec)
        if h<0.01: continue
        b.setdefault(a,{})[s]={"d":x["currentTotalDebt"],"h":round(h,4),"c":x.get("usageAsCollateralEnabledOnUser",False)}
    lid=it[-1]["id"];pg+=1
    if pg%10==0: print(f"P{pg}: {len(b)} addr",flush=True)
    time.sleep(0.15)
print(f"Done: {pg}p, {len(b)} borrowers",flush=True)
for a in b: b[a]["$"]=round(sum(p["h"]*(1 if any(t in s for t in["USDC","USDT","DAI","EURC","GHO"])else 2000)for s,p in b[a].items()),2)
sb=sorted(b.items(),key=lambda x:x[1]["$"],reverse=True)
print("Top 20:"); [print(f"  {i+1}. {a}: ~${d['$']:,.0f} | "+", ".join(f"{s}:{p['h']:.2f}" for s,p in sorted({k:v for k,v in d.items()if k!="$"}.items(),key=lambda x:x[1]["h"],reverse=True)[:3])+(" [COL]" if any(p.get("c")for p in d.values()if isinstance(p,dict)and"c"in p)else""),flush=True) for i,(a,d)in enumerate(sb[:20])]
os.makedirs("/home/ubuntu/defi_flash_bot/data",exist_ok=True)
json.dump({"total":len(b),"borrowers":dict(sb)},open("/home/ubuntu/defi_flash_bot/data/aave_borrowers.json","w"),indent=2)
open("/home/ubuntu/defi_flash_bot/data/aave_borrower_addresses.txt","w").write("\n".join(a for a,_ in sb)+"\n")
print(f"Saved {len(b)}",flush=True)
