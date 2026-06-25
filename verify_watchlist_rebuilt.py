#!/usr/bin/env python3
"""Independently verify the rebuilt arb:watchlist:active against on-chain.
Uses DRPC (different RPC than the rebuild, which used publicnode/blastapi).
Confirms: stored HF ~= live on-chain HF, ordering correct, debt real, no 0.1 garbage."""
import os, json, time, urllib.request, redis
from dotenv import load_dotenv
load_dotenv("/home/ubuntu/defi_flash_bot/.env")
POOL="0x794a61358D6845594F94dc1DB02A252b5b4814aD"; SEL="0xbf92857c"; WAD=10**18
def _c(u): return u.strip().strip('"').strip("'") if u else u
# Independent RPC set: DRPC primary (NOT used in rebuild)
RPCS=[_c(u) for u in [os.getenv("DRPC_RPC_URL"), os.getenv("READ_RPC_SECONDARY")] if u]
def rpc(addr, tries=6):
    data=SEL+addr[2:].lower().rjust(64,"0")
    for i in range(tries):
        url=RPCS[i%len(RPCS)]
        try:
            body=json.dumps({"jsonrpc":"2.0","id":1,"method":"eth_call",
                "params":[{"to":POOL,"data":data},"latest"]}).encode()
            req=urllib.request.Request(url,body,{"Content-Type":"application/json","User-Agent":"vw/1.0"})
            h=json.load(urllib.request.urlopen(req,timeout=25)).get("result","")
            if len(h)>=386:
                return {"coll":int(h[2:66],16)/1e8,"debt":int(h[66:130],16)/1e8,
                        "hf":int(h[322:386],16)/WAD}
        except Exception: time.sleep(0.4)
    return None

r=redis.Redis(decode_responses=True)
n=r.zcard("arb:watchlist:active")
print(f"rebuilt watchlist size: {n}")
print(f"score range: min={r.zrange('arb:watchlist:active',0,0,withscores=True)} "
      f"max={r.zrange('arb:watchlist:active',-1,-1,withscores=True)}")
print(f"garbage check — entries with score<=0.9 (impossible if all healthy-band): "
      f"{r.zcount('arb:watchlist:active','-inf',0.9)}")
print(f"entries HF<1.0 (genuinely liquidatable right now): {r.zcount('arb:watchlist:active','-inf','(1.0')}\n")

# Sample: 8 most urgent (lowest HF) + 6 middle + 6 highest
sample=[]
sample+= [("urgent",a,s) for a,s in r.zrange("arb:watchlist:active",0,7,withscores=True)]
sample+= [("middle",a,s) for a,s in r.zrange("arb:watchlist:active",n//2,n//2+5,withscores=True)]
sample+= [("top",a,s) for a,s in r.zrange("arb:watchlist:active",-6,-1,withscores=True)]

print(f"{'bucket':<7} {'address':<12} {'stored':>8} {'on-chain':>9} {'Δ%':>7} {'debt$':>13} {'verdict'}")
print("-"*72)
ok=0; bad=0; checked=0
for bucket,addr,stored in sample:
    d=rpc(addr); checked+=1
    if d is None:
        print(f"{bucket:<7} {addr[:10]:<12} {stored:>8.4f} {'RPC-FAIL':>9}"); continue
    real=d["hf"]; delta=(real-stored)/stored*100 if stored else 0
    if d["debt"]<1.0:
        verdict="!! debt gone (closed since rebuild)"; bad+=1
    elif abs(delta)<3.0:
        verdict="match"; ok+=1
    elif abs(delta)<10.0:
        verdict="drift (ok, time passed)"; ok+=1
    else:
        verdict="<<< MISMATCH"; bad+=1
    print(f"{bucket:<7} {addr[:10]:<12} {stored:>8.4f} {real:>9.4f} {delta:>6.1f}% {d['debt']:>13,.0f}  {verdict}")
print("-"*72)
print(f"checked={checked}  match/drift={ok}  problem={bad}")
