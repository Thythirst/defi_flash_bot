#!/usr/bin/env python3
"""Re-derive real Aave V3 liquidations from scratch using the VERIFIED topic hash.
Bounded recent-window scan with RPC rotation. Decodes each event and estimates
the liquidator's gross profit (collateral seized - debt repaid)."""
import os, json, sys, time, urllib.request
from dotenv import load_dotenv
load_dotenv("/home/ubuntu/defi_flash_bot/.env")

POOL  = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
# VERIFIED topic (matches liq_log_parser.py + cast sig-event)
TOPIC = "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286"

def _c(u): return u.strip().strip('"').strip("'") if u else u
RPCS = [_c(u) for u in [os.getenv("RPC_PUBLICNODE"), os.getenv("RPC_BLASTAPI"),
        os.getenv("READ_RPC_SECONDARY"), os.getenv("RPC_PUBLIC_ARB1"),
        os.getenv("DRPC_RPC_URL")] if u]
_i = [0]
def rpc(method, params, tries=6):
    last=None
    for _ in range(tries):
        url = RPCS[_i[0] % len(RPCS)]; _i[0]+=1
        try:
            body=json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":params}).encode()
            req=urllib.request.Request(url, body, {"Content-Type":"application/json","User-Agent":"liq/1.0"})
            r=json.load(urllib.request.urlopen(req,timeout=25))
            if "result" in r: return r["result"]
            last=r.get("error")
        except Exception as e:
            last=str(e)[:60]; time.sleep(0.3)
    return None

# 8-decimal USD price feed (rough, for profit sizing); raw token decimals
DECIMALS={"0xaf88d065e77c8cc2239327c5edb3a432268e5831":6,"0xff970a61a04b1ca14834a43f5de4533ebddb5cc8":6,
"0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9":6,"0x82af49447d8a07e3bd95bd0d56f35241523fbab1":18,
"0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f":8,"0x5979d7b546e38e414f7e9822514be443a4800529":18,
"0x912ce59144191c1204e64559fe8253a0e49e6548":18,"0xda10009cbd5d07dd0cecc66161fc93d7c9000da1":18,
"0x35751007a407ca6feffe80b3cb397736d2cf4dbe":18}
PRICES={"0xaf88d065e77c8cc2239327c5edb3a432268e5831":1.0,"0xff970a61a04b1ca14834a43f5de4533ebddb5cc8":1.0,
"0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9":1.0,"0x82af49447d8a07e3bd95bd0d56f35241523fbab1":3300.0,
"0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f":95000.0,"0x5979d7b546e38e414f7e9822514be443a4800529":3950.0,
"0x912ce59144191c1204e64559fe8253a0e49e6548":0.40,"0xda10009cbd5d07dd0cecc66161fc93d7c9000da1":1.0,
"0x35751007a407ca6feffe80b3cb397736d2cf4dbe":3600.0}

cur = int(rpc("eth_blockNumber",[]),16)
WINDOW = int(sys.argv[1]) if len(sys.argv)>1 else 700_000   # ~2.5 days
STEP = 9000
start = cur - WINDOW
print(f"current block {cur:,}  scanning {WINDOW:,} blocks ({start:,}..{cur:,}) for LiquidationCall\n")

events=[]
fr=start; chunks=0; failed=0
while fr<=cur:
    to=min(fr+STEP, cur)
    logs=rpc("eth_getLogs",[{"address":POOL,"fromBlock":hex(fr),"toBlock":hex(to),"topics":[TOPIC]}])
    chunks+=1
    if logs is None: failed+=1
    elif logs:
        for lg in logs: events.append(lg)
        print(f"  block {fr:,}: +{len(logs)} (total {len(events)})", flush=True)
    if chunks % 20 == 0: print(f"  ...{chunks} chunks, {len(events)} events, {failed} failed-chunks", flush=True)
    fr=to+1

print(f"\n=== {len(events)} LiquidationCall events found ({failed} chunk failures) ===\n")
def dec_addr(t): return "0x"+t[-40:]
def to_usd(asset, raw):
    a=asset.lower(); d=DECIMALS.get(a); p=PRICES.get(a)
    if d is None or p is None: return None
    return raw/10**d*p

total_gross=0.0; sized=0
print(f"{'block':>10} {'user':<12} {'debt_repaid$':>12} {'coll_seized$':>12} {'bonus$':>9} {'liquidator':<12}")
print("-"*72)
for lg in sorted(events,key=lambda x:int(x['blockNumber'],16)):
    blk=int(lg['blockNumber'],16); tp=lg['topics']
    coll=dec_addr(tp[1]); debt=dec_addr(tp[2]); user=dec_addr(tp[3])
    data=lg['data'][2:]
    debtToCover=int(data[0:64],16); collSeized=int(data[64:128],16)
    liquidator="0x"+data[128:192][-40:]
    debt_usd=to_usd(debt,debtToCover); coll_usd=to_usd(coll,collSeized)
    if debt_usd is not None and coll_usd is not None:
        bonus=coll_usd-debt_usd; total_gross+=bonus; sized+=1
        print(f"{blk:>10} {user[:10]:<12} {debt_usd:>12.2f} {coll_usd:>12.2f} {bonus:>9.2f} {liquidator[:10]}")
    else:
        print(f"{blk:>10} {user[:10]:<12} {'?':>12} {'?':>12} {'?':>9} {liquidator[:10]}  (coll={coll[:8]} debt={debt[:8]})")
print("-"*72)
print(f"events={len(events)}  priced={sized}  total liquidation bonus (gross spread) on priced=${total_gross:,.2f}")
