#!/usr/bin/env python3
"""Flush + rebuild arb:watchlist:active from FRESH on-chain HFs via Multicall3.
Builds into a temp key, verifies, then atomically renames over the live key.
Matches bootstrap.py conventions: keep debt>=$1000 AND HF<2.0."""
import os, json, time, sys
from web3 import Web3
import redis
from dotenv import load_dotenv
load_dotenv("/home/ubuntu/defi_flash_bot/.env")

AAVE_POOL  = Web3.to_checksum_address("0x794a61358D6845594F94dc1DB02A252b5b4814aD")
MULTICALL3 = Web3.to_checksum_address("0xcA11bde05977b3631167028862bE2a173976CA11")
SELECTOR   = "0xbf92857c"
WAD = 10**18
MIN_DEBT_USD = 1000.0
MAX_HF = 2.0
BATCH = 300
LIVE_KEY = "arb:watchlist:active"
TMP_KEY  = "arb:watchlist:active:rebuild"

def _c(u): return u.strip().strip('"').strip("'") if u else u
# Only the two RPCs proven fast + reliable for size-300 multicalls.
RPCS = [_c(u) for u in [os.getenv("RPC_PUBLICNODE"), os.getenv("RPC_BLASTAPI")] if u]
MC_ABI = [{"inputs":[{"components":[{"name":"target","type":"address"},
    {"name":"allowFailure","type":"bool"},{"name":"callData","type":"bytes"}],
    "name":"calls","type":"tuple[]"}],"name":"aggregate3","outputs":[{"components":[
    {"name":"success","type":"bool"},{"name":"returnData","type":"bytes"}],
    "name":"returnData","type":"tuple[]"}],"stateMutability":"payable","type":"function"}]

def w3_for(url):
    return Web3(Web3.HTTPProvider(url, request_kwargs={"timeout":30,"headers":{"User-Agent":"rebuild/1.0"}}))
clients = [w3_for(u) for u in RPCS]
mcs = [c.eth.contract(address=MULTICALL3, abi=MC_ABI) for c in clients]
_i=[0]

def call_data_for(addr):
    return SELECTOR + addr[2:].lower().rjust(64,"0")

def multicall(addrs, tries=5):
    calls = [(AAVE_POOL, True, bytes.fromhex(call_data_for(a)[2:])) for a in addrs]
    for _ in range(tries):
        idx=_i[0]%len(mcs); _i[0]+=1
        try:
            return mcs[idx].functions.aggregate3(calls).call()
        except Exception as e:
            time.sleep(0.3)
    return None

# ── universe: current watchlist ∪ borrower file ──
r = redis.from_url("redis://localhost:6379", decode_responses=True)
universe = set(r.zrange(LIVE_KEY, 0, -1))
with open("/home/ubuntu/defi_flash_bot/data/aave_borrower_addresses.txt") as f:
    for line in f:
        a=line.strip().lower()
        if a.startswith("0x") and len(a)==42: universe.add(a)
universe = sorted(universe)
print(f"universe: {len(universe)} addresses  |  RPCs: {len(RPCS)}  |  batch={BATCH}")

active=[]; checked=0; ok=0; fail_batches=0
t0=time.time()
batches=[universe[i:i+BATCH] for i in range(0,len(universe),BATCH)]
for bi,batch in enumerate(batches):
    res=multicall(batch)
    if res is None:
        fail_batches+=1; continue
    for addr,(success,raw) in zip(batch,res):
        checked+=1
        if not success or len(raw)<192: continue
        h=raw.hex()
        debt = int(h[64:128],16)/1e8
        hf_raw = int(h[320:384],16)
        hf = hf_raw/WAD
        ok+=1
        if debt>=MIN_DEBT_USD and hf<MAX_HF:
            coll=int(h[0:64],16)/1e8
            lt=int(h[192:256],16)/1e4
            active.append({"addr":addr,"hf":round(hf,6),"debt":round(debt,2),
                           "coll":round(coll,2),"lt":lt})
    if (bi+1)%20==0:
        print(f"  batch {bi+1}/{len(batches)} | checked={checked} ok={ok} qualifying={len(active)} | {time.time()-t0:.0f}s",flush=True)

print(f"\nDONE scan: checked={checked} ok_reads={ok} fail_batches={fail_batches} qualifying(debt>=${MIN_DEBT_USD:.0f},HF<{MAX_HF})={len(active)}")
if not active:
    print("!! no qualifying positions — NOT touching live key"); sys.exit(1)

# ── write temp key ──
r.delete(TMP_KEY)
pipe=r.pipeline()
for e in active:
    pipe.zadd(TMP_KEY, {e["addr"]: e["hf"]})
    pipe.hset(f"arb:watchlist:user:{e['addr']}", mapping={
        "health_factor":str(e["hf"]),"debt_usd":str(e["debt"]),
        "collateral_usd":str(e["coll"]),"liq_threshold":str(e["lt"]),
        "last_refresh_ts":str(time.time()),"source":"rebuild_onchain"})
pipe.execute()
print(f"temp key {TMP_KEY}: {r.zcard(TMP_KEY)} members")
print("lowest-HF (most urgent) entries:")
for a,s in r.zrange(TMP_KEY,0,9,withscores=True):
    u=r.hgetall(f"arb:watchlist:user:{a}")
    print(f"  {a[:12]} HF={s:.4f} debt=${float(u.get('debt_usd',0)):,.0f}")

# ── atomic swap ──
r.rename(TMP_KEY, LIVE_KEY)
print(f"\nSWAPPED -> {LIVE_KEY}: {r.zcard(LIVE_KEY)} members (was 2244)")
print(f"backup retained at arb:watchlist:active:bak_prerebuild")
# write the fresh set to disk too
with open("/home/ubuntu/defi_flash_bot/data/watchlist_rebuilt.json","w") as f:
    json.dump(sorted(active,key=lambda x:x["hf"]), f, indent=2)
print("wrote data/watchlist_rebuilt.json")
