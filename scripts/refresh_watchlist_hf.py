#!/usr/bin/env python3
"""
refresh_watchlist_hf.py — Re-verify HF for all existing Redis watchlist entries.

expand_watchlist_graph.py only adds NEW addresses; this refreshes existing scores
by calling getUserAccountData via Multicall3 for every address already in Redis.

Usage: python3 scripts/refresh_watchlist_hf.py
"""
import asyncio
import logging
import os
import sys
from pathlib import Path

import redis
from dotenv import load_dotenv
from eth_abi import decode as abi_decode
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("refresh_watchlist")

ARBITRUM_HTTP = os.getenv("ARBITRUM_HTTP_URL", "https://arb1.arbitrum.io/rpc")
REDIS_URL     = "redis://localhost:6379"
REDIS_KEY     = "arb:watchlist:active"
AAVE_POOL     = AsyncWeb3.to_checksum_address("0x794a61358D6845594F94dc1DB02A252b5b4814aD")
MULTICALL3    = AsyncWeb3.to_checksum_address("0xcA11bde05977b3631167028862bE2a173976CA11")
MAX_HF        = 3.0
BATCH_SIZE    = 300
CONCURRENCY   = 5

MULTICALL_ABI = [{"inputs":[{"components":[{"name":"target","type":"address"},
    {"name":"allowFailure","type":"bool"},{"name":"callData","type":"bytes"}],
    "name":"calls","type":"tuple[]"}],"name":"aggregate3","outputs":[{"components":[
    {"name":"success","type":"bool"},{"name":"returnData","type":"bytes"}],
    "name":"returnData","type":"tuple[]"}],"stateMutability":"payable","type":"function"}]

GUD_SELECTOR = bytes.fromhex("bf92857c")


def _calldata(addr: str) -> bytes:
    return GUD_SELECTOR + bytes.fromhex(addr[2:].lower().zfill(64))


async def verify_batch(w3: AsyncWeb3, addrs: list[str]) -> list[tuple[str, float]]:
    mc = w3.eth.contract(address=MULTICALL3, abi=MULTICALL_ABI)
    calls = [(AAVE_POOL, True, _calldata(a)) for a in addrs]
    try:
        results = await mc.functions.aggregate3(calls).call()
    except Exception as e:
        log.warning(f"Multicall failed (batch of {len(addrs)}): {e}")
        return []
    out = []
    for addr, (success, data) in zip(addrs, results):
        if not success or len(data) < 192:
            continue
        try:
            decoded = abi_decode(
                ["uint256","uint256","uint256","uint256","uint256","uint256"], data
            )
            debt_base = decoded[1]
            hf_raw    = decoded[5]
            if debt_base == 0:
                continue
            hf = hf_raw / 1e18 if hf_raw < (2**96) else float("inf")
            out.append((addr, hf))
        except Exception:
            continue
    return out


async def main() -> None:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    existing = r.zrange(REDIS_KEY, 0, -1, withscores=True)
    total = len(existing)
    log.info(f"Refreshing {total} positions via getUserAccountData …")
    log.info(f"RPC: {ARBITRUM_HTTP[:55]}…")

    w3 = AsyncWeb3(AsyncHTTPProvider(ARBITRUM_HTTP))
    addrs = [addr for addr, _ in existing]
    batches = [addrs[i:i + BATCH_SIZE] for i in range(0, len(addrs), BATCH_SIZE)]
    log.info(f"{len(batches)} batches × {BATCH_SIZE} = ~{len(batches)} Multicall3 calls, concurrency={CONCURRENCY}")

    sem = asyncio.Semaphore(CONCURRENCY)
    updated = removed = unchanged = errors = 0
    done_batches = 0
    log_every = max(1, len(batches) // 20)

    async def _process(batch: list[str]) -> None:
        nonlocal updated, removed, unchanged, errors, done_batches
        async with sem:
            results = await verify_batch(w3, batch)

        result_map = {addr: hf for addr, hf in results}
        zero_debt_addrs = set(batch) - set(result_map.keys())

        pipe = r.pipeline()
        for addr in batch:
            if addr in zero_debt_addrs:
                # debt fully repaid — remove from watchlist
                pipe.zrem(REDIS_KEY, addr)
                removed += 1
            else:
                hf = result_map[addr]
                if hf > MAX_HF:
                    pipe.zrem(REDIS_KEY, addr)
                    removed += 1
                else:
                    pipe.zadd(REDIS_KEY, {addr: hf})
                    updated += 1
        pipe.execute()

        errors += len(batch) - len(results) - len(zero_debt_addrs)
        done_batches += 1
        if done_batches % log_every == 0 or done_batches == len(batches):
            pct = done_batches / len(batches) * 100
            log.info(f"  [{pct:5.1f}%] {done_batches}/{len(batches)} batches  "
                     f"updated={updated} removed={removed} errors={errors}")

    tasks = [asyncio.create_task(_process(b)) for b in batches]
    await asyncio.gather(*tasks)

    final = r.zcard(REDIS_KEY)
    log.info(f"\nDone. {total} → {final} positions  "
             f"(updated={updated} removed={removed} errors={errors})")
    log.info(f"Underwater (<1.0):  {r.zcount(REDIS_KEY, 0, 1.0)}")
    log.info(f"Danger (0.97-1.0):  {r.zcount(REDIS_KEY, 0.97, 1.0)}")
    log.info(f"Near (1.0-1.05):    {r.zcount(REDIS_KEY, 1.0, 1.05)}")


if __name__ == "__main__":
    asyncio.run(main())
