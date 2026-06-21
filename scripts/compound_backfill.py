#!/usr/bin/env python3
"""Backfill Compound V3 watchlists for all markets."""
import asyncio, re, sys, time
sys.path.insert(0, "/home/ubuntu/defi_flash_bot/services/rev2")

chainstack_url = None
with open("/home/ubuntu/defi_flash_bot/.env") as f:
    for line in f:
        m = re.match(r"^CHAINSTACK_ARBITRUM_HTTP_URL=(.+)", line.strip())
        if m:
            chainstack_url = m.group(1).strip().strip('"').strip("'")
            break

print(f"Start: {time.strftime('%H:%M:%S')}")
print(f"RPC: {chainstack_url[:55]}...")

import redis.asyncio as aioredis
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider
from compound_v3 import CompoundWatchlistBuilder, COMPOUND_MARKETS

async def backfill_market(w3, redis, market_name, start_block):
    cfg = {**COMPOUND_MARKETS[market_name], "market": market_name}
    builder = CompoundWatchlistBuilder(w3=w3, redis=redis, market_config=cfg, blocks_per_chunk=2000)
    print(f"  {market_name}: scanning from block {start_block}...")
    count = await builder.backfill(start_block=start_block)
    size = await redis.zcard(f"watchlist:compound:{market_name.lower()}")
    print(f"  {market_name}: {count} new borrowers, {size} total")
    return count

async def main():
    w3 = AsyncWeb3(AsyncHTTPProvider(chainstack_url, request_kwargs={"timeout": 30}))
    redis = aioredis.from_url("redis://localhost:6379", decode_responses=False)

    latest = await w3.eth.block_number
    print(f"Latest block: {latest}")

    # Compound V3 markets on Arbitrum deploy blocks:
    # USDC: ~72M, USDT: ~133M, ETH: ~262M
    markets = [
        ("USDC", 72_000_000),
        ("USDT", 133_000_000),
        ("ETH", 262_000_000),
    ]

    total = 0
    for name, start in markets:
        count = await backfill_market(w3, redis, name, start)
        total += count

    print(f"\nTotal: {total} borrowers across {len(markets)} markets")
    print(f"End: {time.strftime('%H:%M:%S')}")
    await redis.aclose()

asyncio.run(main())
