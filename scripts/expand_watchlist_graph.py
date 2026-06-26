#!/usr/bin/env python3
"""
expand_watchlist_graph.py — Expand Redis watchlist using The Graph API.

Pulls ALL Aave V3 Arbitrum users with active debt from the subgraph,
verifies HF on-chain via Multicall3, and upserts into arb:watchlist:active.

Run: python3 scripts/expand_watchlist_graph.py
"""
import asyncio
import logging
import os
import sys
import time
from pathlib import Path

import aiohttp
import redis
from dotenv import load_dotenv
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("expand_watchlist")

# ── Config ──────────────────────────────────────────────────────────────────
GRAPH_KEY     = os.getenv("GRAPH_API_KEY", "")
ARBITRUM_HTTP = os.getenv("ARBITRUM_HTTP_URL", "https://arb1.arbitrum.io/rpc")
READ_RPC      = os.getenv("READ_RPC_PRIMARY") or os.getenv("DRPC_RPC_URL") or ARBITRUM_HTTP

SUBGRAPH_URL  = f"https://gateway.thegraph.com/api/{GRAPH_KEY}/subgraphs/id/DLuE98kEb5pQNXAcKFQGQgfSQ57Xdou4jnVbAEqMfy3B"

AAVE_POOL     = AsyncWeb3.to_checksum_address("0x794a61358D6845594F94dc1DB02A252b5b4814aD")
MULTICALL3    = AsyncWeb3.to_checksum_address("0xcA11bde05977b3631167028862bE2a173976CA11")
REDIS_URL     = "redis://localhost:6379"
REDIS_KEY     = "arb:watchlist:active"

# Only add positions where HF is meaningful (< 3.0) and debt > 0
MAX_HF        = 3.0
BATCH_SIZE    = 300   # addresses per Multicall3 call
PAGE_SIZE     = 1000  # users per Graph query page

MULTICALL_ABI = [{"inputs":[{"components":[{"name":"target","type":"address"},
    {"name":"allowFailure","type":"bool"},{"name":"callData","type":"bytes"}],
    "name":"calls","type":"tuple[]"}],"name":"aggregate3","outputs":[{"components":[
    {"name":"success","type":"bool"},{"name":"returnData","type":"bytes"}],
    "name":"returnData","type":"tuple[]"}],"stateMutability":"payable","type":"function"}]

GUD_SELECTOR = bytes.fromhex("bf92857c")  # getUserAccountData(address)


def _calldata(addr: str) -> bytes:
    return GUD_SELECTOR + bytes.fromhex(addr[2:].lower().zfill(64))


async def fetch_all_borrowers(session: aiohttp.ClientSession) -> list[str]:
    """Paginate through The Graph to get all users with borrowedReservesCount > 0."""
    all_users: list[str] = []
    skip = 0
    log.info("Fetching borrowers from The Graph...")

    while True:
        query = """
        {
          users(
            first: %d
            skip: %d
            where: { borrowedReservesCount_gt: 0 }
            orderBy: id
            orderDirection: asc
          ) {
            id
          }
        }
        """ % (PAGE_SIZE, skip)

        async with session.post(SUBGRAPH_URL, json={"query": query},
                                timeout=aiohttp.ClientTimeout(total=30)) as r:
            data = await r.json()

        if "errors" in data:
            log.error(f"Graph error: {data['errors']}")
            break

        users = data.get("data", {}).get("users", [])
        if not users:
            break

        batch = [u["id"] for u in users]
        all_users.extend(batch)
        log.info(f"  Page {skip // PAGE_SIZE + 1}: got {len(batch)} users (total={len(all_users)})")

        if len(users) < PAGE_SIZE:
            break
        skip += PAGE_SIZE

    log.info(f"Graph returned {len(all_users)} total borrowers")
    return all_users


async def verify_hf_batch(w3, addrs: list[str]) -> list[tuple[str, float]]:
    """Run getUserAccountData via Multicall3 for a batch of addresses."""
    from eth_abi import decode as abi_decode

    mc = w3.eth.contract(address=MULTICALL3, abi=MULTICALL_ABI)
    calls = [(AAVE_POOL, True, _calldata(a)) for a in addrs]

    try:
        results = await mc.functions.aggregate3(calls).call()
    except Exception as e:
        log.warning(f"Multicall failed: {e}")
        return []

    out = []
    for addr, (success, data) in zip(addrs, results):
        if not success or len(data) < 192:
            continue
        try:
            decoded = abi_decode(
                ["uint256", "uint256", "uint256", "uint256", "uint256", "uint256"],
                data
            )
            debt_base = decoded[1]  # totalDebtBase (USD, 8 decimals)
            hf_raw    = decoded[5]  # healthFactor WAD
            if debt_base == 0:
                continue
            hf = hf_raw / 1e18 if hf_raw < (2**96) else float("inf")
            out.append((addr, hf))
        except Exception:
            continue
    return out


async def main() -> None:
    if not GRAPH_KEY:
        log.error("GRAPH_API_KEY not set in .env")
        sys.exit(1)

    r = redis.from_url(REDIS_URL, decode_responses=True)
    existing = set(r.zrange(REDIS_KEY, 0, -1))
    log.info(f"Current watchlist: {len(existing)} addresses")

    # Use free public RPC for bulk reads to avoid rate-limiting Chainstack
    READ_RPC_FOR_EXPAND = (
        os.getenv("RPC_PUBLICNODE")
        or os.getenv("RPC_BLASTAPI")
        or ARBITRUM_HTTP
    )
    w3 = AsyncWeb3(AsyncHTTPProvider(READ_RPC_FOR_EXPAND))

    async with aiohttp.ClientSession() as session:
        borrowers = await fetch_all_borrowers(session)

    if not borrowers:
        log.error("No borrowers returned from Graph — check API key and subgraph ID")
        sys.exit(1)

    # Filter to only new addresses not already in watchlist
    new_addrs = [a for a in borrowers if a.lower() not in {e.lower() for e in existing}]
    log.info(f"New addresses to verify: {len(new_addrs)} (skipping {len(borrowers) - len(new_addrs)} already known)")

    added = 0
    skipped_healed = 0
    skipped_zero   = 0
    CONCURRENCY = 5  # parallel Multicall3 calls

    sem = asyncio.Semaphore(CONCURRENCY)

    async def _verify_and_add(batch: list[str], batch_idx: int) -> tuple[int, int, int]:
        async with sem:
            results = await verify_hf_batch(w3, batch)
        pipe = r.pipeline()
        _added = _healed = 0
        for addr, hf in results:
            if hf > MAX_HF:
                _healed += 1
                continue
            pipe.zadd(REDIS_KEY, {addr: hf})
            _added += 1
        pipe.execute()
        _zero = len(batch) - len(results)
        return _added, _healed, _zero

    batches = [new_addrs[i:i + BATCH_SIZE] for i in range(0, len(new_addrs), BATCH_SIZE)]
    tasks = [asyncio.create_task(_verify_and_add(b, idx)) for idx, b in enumerate(batches)]

    log_every = max(1, len(batches) // 20)
    for idx, fut in enumerate(asyncio.as_completed(tasks)):
        a, h, z = await fut
        added          += a
        skipped_healed += h
        skipped_zero   += z
        if idx % log_every == 0 or idx == len(tasks) - 1:
            pct = (idx + 1) / len(tasks) * 100
            log.info(f"  [{pct:5.1f}%] done={idx+1}/{len(tasks)} added={added} healed={skipped_healed} zero_debt={skipped_zero}")

    total_now = r.zcard(REDIS_KEY)
    log.info(
        f"\nDone. Added {added} new addresses. "
        f"Watchlist: {len(existing)} → {total_now} (+{total_now - len(existing)})"
    )
    log.info(f"Skipped: {skipped_healed} healed (HF>{MAX_HF}), {skipped_zero} zero-debt")


if __name__ == "__main__":
    asyncio.run(main())
