#!/usr/bin/env python3
"""
spot_check_hf.py — Sample-verify AccountData.health_factor against fresh getUserAccountData().

Picks N positions spread across HF bands from Redis and cross-checks:
  Redis score  vs  fresh getUserAccountData() via Multicall3

Usage: python3 scripts/spot_check_hf.py [--n 25]
"""
import asyncio
import argparse
import logging
import os
import struct
import sys
from pathlib import Path

import redis
from dotenv import load_dotenv
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("spot_check")

ARBITRUM_HTTP = os.getenv("ARBITRUM_HTTP_URL", "https://arb1.arbitrum.io/rpc")
READ_RPC      = ARBITRUM_HTTP  # prefer Chainstack; DRPC is flaky
REDIS_URL     = "redis://localhost:6379"
REDIS_KEY     = "arb:watchlist:active"
MULTICALL3    = AsyncWeb3.to_checksum_address("0xcA11bde05977b3631167028862bE2a173976CA11")
AAVE_POOL     = AsyncWeb3.to_checksum_address("0x794a61358D6845594F94dc1DB02A252b5b4814aD")
WAD           = 10**18

GUD_SELECTOR = bytes.fromhex("bf92857c")

MULTICALL_ABI = [{"inputs":[{"components":[{"name":"target","type":"address"},
    {"name":"allowFailure","type":"bool"},{"name":"callData","type":"bytes"}],
    "name":"calls","type":"tuple[]"}],"name":"aggregate3","outputs":[{"components":[
    {"name":"success","type":"bool"},{"name":"returnData","type":"bytes"}],
    "name":"returnData","type":"tuple[]"}],"stateMutability":"payable","type":"function"}]


def _calldata(addr: str) -> bytes:
    return GUD_SELECTOR + bytes.fromhex(addr[2:].lower().zfill(64))


async def fetch_fresh_hf(w3: AsyncWeb3, addresses: list[str]) -> dict[str, float]:
    from eth_abi import decode as abi_decode
    mc = w3.eth.contract(address=MULTICALL3, abi=MULTICALL_ABI)
    # target is the Aave Pool — getUserAccountData(user) is called on the pool, not the user
    calls = [(AAVE_POOL, True, _calldata(a)) for a in addresses]
    results = await mc.functions.aggregate3(calls).call()
    out = {}
    for addr, (success, data) in zip(addresses, results):
        if not success or len(data) < 192:
            out[addr] = None
            continue
        try:
            decoded = abi_decode(
                ["uint256","uint256","uint256","uint256","uint256","uint256"], data
            )
            hf_raw = decoded[5]
            out[addr] = hf_raw / WAD if hf_raw < (2**96) else float("inf")
        except Exception:
            out[addr] = None
    return out


def sample_positions(r: redis.Redis, n: int) -> list[tuple[str, float]]:
    """Pick n positions spread across HF bands: <0.97, 0.97-1.0, 1.0-1.05, 1.05-1.2, >1.2."""
    bands = [
        (0.0,  0.97,  max(1, n // 6)),
        (0.97, 1.0,   max(1, n // 6)),
        (1.0,  1.02,  max(2, n // 5)),
        (1.02, 1.05,  max(2, n // 5)),
        (1.05, 1.20,  max(2, n // 5)),
        (1.20, 5.0,   max(1, n // 8)),
    ]
    sampled = []
    for lo, hi, count in bands:
        entries = r.zrangebyscore(REDIS_KEY, lo, hi, withscores=True, start=0, num=count * 3)
        # pick evenly spaced within the band
        step = max(1, len(entries) // count)
        picked = entries[::step][:count]
        sampled.extend(picked)
    return [(addr.decode() if isinstance(addr, bytes) else addr, score)
            for addr, score in sampled]


async def main(n: int):
    r = redis.Redis.from_url(REDIS_URL)
    total = r.zcard(REDIS_KEY)
    log.info(f"Redis watchlist: {total} positions. Sampling {n} across HF bands …")

    samples = sample_positions(r, n)
    log.info(f"Selected {len(samples)} positions. Fetching fresh getUserAccountData …")

    w3 = AsyncWeb3(AsyncHTTPProvider(READ_RPC))
    addrs = [addr for addr, _ in samples]
    fresh = await fetch_fresh_hf(w3, addrs)

    log.info(f"\n{'Address':12s}  {'Redis HF':>10s}  {'Fresh HF':>10s}  {'Delta':>10s}  {'Status':}")
    log.info("-" * 72)

    max_delta = 0.0
    mismatches = 0
    for addr, redis_hf in samples:
        f = fresh.get(addr)
        if f is None:
            log.info(f"{addr[:10]}…  {'N/A':>10s}  {'RPC fail':>10s}  {'—':>10s}  SKIP")
            continue
        delta = abs(redis_hf - f)
        max_delta = max(max_delta, delta)
        status = "OK" if delta < 0.01 else "STALE"
        if delta >= 0.01:
            mismatches += 1
        flag = "  <<<" if delta >= 0.01 else ""
        log.info(f"{addr[:10]}…  {redis_hf:>10.6f}  {f:>10.6f}  {delta:>10.6f}  {status}{flag}")

    log.info("-" * 72)
    log.info(f"Max delta: {max_delta:.6f}  |  Stale (>0.01): {mismatches}/{len(samples)}")
    log.info("PASS — all Redis scores within 0.01 of chain." if mismatches == 0
             else f"WARN — {mismatches} positions have stale Redis scores (>0.01 delta); re-expand recommended.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=25, help="positions to sample")
    args = ap.parse_args()
    asyncio.run(main(args.n))
