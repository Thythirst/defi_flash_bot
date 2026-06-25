#!/usr/bin/env python3
"""Bypass Redis/local engine entirely. Call getUserAccountData() directly
and compare real on-chain HF to the score stored in arb:watchlist:active."""
import os, json, sys, urllib.request, redis
from dotenv import load_dotenv
load_dotenv()

AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
SELECTOR  = "0xbf92857c"  # getUserAccountData(address)
WAD = 10**18

def _clean(u): return u.strip().strip('"').strip("'") if u else u
RPCS = [_clean(u) for u in [
    os.getenv("DRPC_RPC_URL"), os.getenv("READ_RPC_SECONDARY"),
    os.getenv("RPC_PUBLICNODE"), os.getenv("RPC_BLASTAPI"),
    os.getenv("RPC_PUBLIC_ARB1"),
] if u]

def rpc(url, method, params):
    body = json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":params}).encode()
    req = urllib.request.Request(url, body, {"Content-Type":"application/json",
                                             "User-Agent":"hf-verify/1.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)

def get_account_data(addr, block="latest"):
    data = SELECTOR + addr[2:].lower().rjust(64, "0")
    for url in RPCS:
        try:
            resp = rpc(url, "eth_call", [{"to":AAVE_POOL,"data":data}, block])
            if "result" in resp and len(resp["result"]) >= 386:
                h = resp["result"]
                return {
                    "collateral_usd": int(h[2:66],16)/1e8,
                    "debt_usd":       int(h[66:130],16)/1e8,
                    "liq_threshold":  int(h[194:258],16)/1e4,
                    "hf_raw":         int(h[322:386],16),
                    "rpc":            url.split("//")[1].split(".")[0],
                }
        except Exception as e:
            continue
    return None

r = redis.Redis(decode_responses=True)
# Sample across buckets
buckets = {
    "floor=0.1":   r.zrangebyscore("arb:watchlist:active", 0.1, 0.1, start=0, num=6, withscores=True),
    "0.1-0.9":     r.zrangebyscore("arb:watchlist:active", "(0.1", "(0.9", start=0, num=6, withscores=True),
    "0.9-1.0":     r.zrangebyscore("arb:watchlist:active", 0.9, "(1.0", start=0, num=6, withscores=True),
    ">=1.0":       r.zrangebyscore("arb:watchlist:active", 1.0, "+inf", start=0, num=6, withscores=True),
}

print(f"RPCs available: {[u.split('//')[1].split('.')[0] for u in RPCS]}\n")
print(f"{'bucket':<10} {'address':<14} {'stored':>8} {'real_HF':>10} {'debt$':>12} {'verdict'}")
print("-"*78)
liquidatable_real = 0
checked = 0
for name, members in buckets.items():
    for addr, stored in members:
        d = get_account_data(addr)
        checked += 1
        if d is None:
            print(f"{name:<10} {addr[:12]:<14} {stored:>8.3f} {'RPC-FAIL':>10}")
            continue
        if d["debt_usd"] == 0:
            real = "NO-DEBT"
            verdict = "ghost (no debt on-chain)"
            real_s = -1
        else:
            real_s = d["hf_raw"]/WAD
            real = f"{real_s:.4f}" if real_s < 1e6 else "inf(no-debt)"
            if real_s < 1.0:
                liquidatable_real += 1
                verdict = "<<< REALLY LIQUIDATABLE"
            else:
                ratio = real_s/stored if stored else 0
                verdict = f"stored off by {ratio:.0f}x" if ratio>1.5 else "ok-ish"
        print(f"{name:<10} {addr[:12]:<14} {stored:>8.3f} {real:>10} {d['debt_usd']:>12.2f}  {verdict}")
print("-"*78)
print(f"checked={checked}  really_liquidatable_now={liquidatable_real}")
