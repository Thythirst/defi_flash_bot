#!/usr/bin/env python3
import asyncio, re, sys, time
sys.path.insert(0, "/root/defi_flash_bot/prod/services/rev2")

alchemy_url = None
with open("/root/defi_flash_bot/prod/.env") as f:
    for line in f:
        m = re.match(r"^ALCHEMY_HTTP_URL=(.+)", line.strip())
        if m:
            alchemy_url = m.group(1).strip().strip('"').strip("'")
            break

print(f"Start: {time.strftime('%H:%M:%S')}")
print(f"Alchemy: {alchemy_url[:55]}...")

import redis.asyncio as aioredis
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider
from compound_v3 import CompoundWatchlistBuilder, COMPOUND_MARKETS

async def main():
    w3 = AsyncWeb3(AsyncHTTPProvider(alchemy_url, request_kwargs={"timeout": 30}))
    redis = aioredis.from_url("redis://localhost:6379", decode_responses=False)
    cfg = {**COMPOUND_MARKETS["USDC"], "market": "USDC"}
    builder = CompoundWatchlistBuilder(w3=w3, redis=redis, market_config=cfg, blocks_per_chunk=2000)

    latest = await w3.eth.block_number
    print(f"Latest block: {latest}")
    print(f"Backfilling from 72,000,000 to {latest}...")
    count = await builder.backfill(start_block=72_000_000)
    print(f"Done - {count} borrowers added")
    size = await redis.zcard("watchlist:compound:usdc")
    print(f"Redis ZSET size: {size}")
    print(f"End: {time.strftime('%H:%M:%S')}")
    await redis.aclose()

asyncio.run(main())
