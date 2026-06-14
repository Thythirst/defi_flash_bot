#!/usr/bin/env python3
"""
watchlist_audit.py — Every 6 hours: verify watchlist health
Spot-checks random addresses for active debt, reports ghost ratio.

Cron entry:
    0 */6 * * * /root/defi_flash_bot/prod/venv/bin/python3 \\
        /root/defi_flash_bot/prod/scripts/watchlist_audit.py \\
        >> /root/defi_flash_bot/prod/logs/watchlist_audit.log 2>&1
"""

import asyncio, json, logging, os, random, time
from pathlib import Path
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

PROD_DIR   = Path("/root/defi_flash_bot/prod")
STATE_FILE = PROD_DIR / "logs" / "watchlist_audit_state.json"
AAVE_POOL  = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
MC3        = "0xcA11bde05977b3631167028862bE2a173976CA11"
SAMPLE_SIZE= 50   # spot-check N random addresses per audit

POOL_ABI = [{"inputs":[{"name":"user","type":"address"}],"name":"getUserAccountData",
  "outputs":[{"name":"totalCollateralBase","type":"uint256"},{"name":"totalDebtBase","type":"uint256"},
  {"name":"availableBorrowsBase","type":"uint256"},{"name":"currentLiquidationThreshold","type":"uint256"},
  {"name":"ltv","type":"uint256"},{"name":"healthFactor","type":"uint256"}],
  "stateMutability":"view","type":"function"}]

MC3_ABI = [{"inputs":[{"components":[{"name":"target","type":"address"},
  {"name":"allowFailure","type":"bool"},{"name":"callData","type":"bytes"}],
  "name":"calls","type":"tuple[]"}],"name":"aggregate3",
  "outputs":[{"components":[{"name":"success","type":"bool"},
  {"name":"returnData","type":"bytes"}],"name":"returnData","type":"tuple[]"}],
  "stateMutability":"payable","type":"function"}]

def load_state():
    if STATE_FILE.exists():
        try: return json.loads(STATE_FILE.read_text())
        except: pass
    return {"audits": [], "last_cardinality": {}}

def save_state(s): STATE_FILE.write_text(json.dumps(s, indent=2))

async def audit_watchlist(redis_url, rpc_url, redis_key="watchlist"):
    import redis.asyncio as aioredis
    r  = aioredis.from_url(redis_url)
    w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url, request_kwargs={"timeout":15}))
    pool = w3.eth.contract(address=AsyncWeb3.to_checksum_address(AAVE_POOL), abi=POOL_ABI)
    mc3  = w3.eth.contract(address=AsyncWeb3.to_checksum_address(MC3), abi=MC3_ABI)

    total   = await r.zcard(redis_key)
    members = await r.zrange(redis_key, 0, -1)
    addrs   = [m.decode() if isinstance(m, bytes) else m for m in members]

    sample = random.sample(addrs, min(SAMPLE_SIZE, len(addrs)))
    calls  = [{"target": AsyncWeb3.to_checksum_address(AAVE_POOL), "allowFailure": True,
               "callData": pool.functions.getUserAccountData(
                   AsyncWeb3.to_checksum_address(a)).build_transaction({"gas":0})["data"]}
              for a in sample]

    results = await mc3.functions.aggregate3(calls).call()
    active  = sum(1 for _, (ok, raw) in enumerate(results)
                  if ok and len(raw) >= 64 and int(raw.hex()[64:128], 16) > 0)
    ghosts  = len(sample) - active
    ghost_pct = ghosts / len(sample) * 100 if sample else 0

    logger.info(
        f"[WatchlistAudit] {redis_key}: total={total:,} "
        f"sample={len(sample)} active={active} ghosts={ghosts} "
        f"ghost_rate={ghost_pct:.1f}%"
    )

    await r.aclose()
    return {"key": redis_key, "total": total, "ghost_pct": ghost_pct, "ts": time.time()}

async def main():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
    rpc_url   = os.getenv("QUICKNODE_HTTP_URL", "https://arb1.arbitrum.io/rpc")
    state     = load_state()

    results = []
    for key in ["watchlist", "watchlist:compound:usdc", "watchlist:compound:usdt",
                "watchlist:compound:eth", "watchlist:base:aave"]:
        try:
            r = await audit_watchlist(redis_url, rpc_url, key)
            results.append(r)
        except Exception as e:
            logger.error(f"[WatchlistAudit] {key} failed: {e}")

    state["audits"].append({"ts": time.time(), "results": results})
    state["audits"] = state["audits"][-28:]  # 7 days of 6h audits
    save_state(state)

if __name__ == "__main__":
    asyncio.run(main())
