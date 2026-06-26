#!/usr/bin/env python3
"""
verify_hf_consistency.py — Confirm AccountData.health_factor matches getUserAccountData().

Samples 20 positions (plus specific failure cases) and compares:
  - AccountData.health_factor  (from loader._batch_account_data, the value driving decisions)
  - Fresh getUserAccountData()  (independent call)

These should be essentially identical since both call the same Aave contract method.
Any delta > 0.001 indicates a staleness or race condition.

Usage:
    cd ~/defi_flash_bot
    python3 scripts/verify_hf_consistency.py
"""

import asyncio
import sys
import os
import time
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "services" / "rev2"))

from dotenv import load_dotenv
load_dotenv(dotenv_path=project_root / ".env")

from web3 import AsyncWeb3, Web3
from position_loader import PositionLoader, MULTICALL3_ADDRESS, SELECTOR_GET_USER_ACCOUNT_DATA

AAVE_POOL     = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
WAD           = 10 ** 18

# Specific failure cases from 2026-06-26
# CB658687 = 0xcb6586874cc04b01cc4fdb777de502cea7b3d6c1 (HF ~1.01 in Redis last night)
FAILURE_CASES = [
    "0xcb6586874cc04b01cc4fdb777de502cea7b3d6c1",  # CB658687 wstETH whale
]

MULTICALL3_ABI = [
    {
        "inputs": [{"components": [
            {"internalType": "address", "name": "target", "type": "address"},
            {"internalType": "bool",    "name": "allowFailure", "type": "bool"},
            {"internalType": "bytes",   "name": "callData", "type": "bytes"},
        ], "internalType": "struct Multicall3.Call3[]", "name": "calls", "type": "tuple[]"}],
        "name": "aggregate3",
        "outputs": [{"components": [
            {"internalType": "bool",  "name": "success", "type": "bool"},
            {"internalType": "bytes", "name": "returnData", "type": "bytes"},
        ], "internalType": "struct Multicall3.Result[]", "name": "returnData", "type": "tuple[]"}],
        "stateMutability": "payable", "type": "function",
    }
]


async def fresh_getUserAccountData(w3: AsyncWeb3, pool_address: str, addresses: list[str]) -> dict[str, dict]:
    """Independent getUserAccountData call — NOT going through PositionLoader."""
    mc = w3.eth.contract(
        address=AsyncWeb3.to_checksum_address(MULTICALL3_ADDRESS),
        abi=MULTICALL3_ABI,
    )
    pool = AsyncWeb3.to_checksum_address(pool_address)
    calls = [
        {
            "target":       pool,
            "allowFailure": True,
            "callData":     "0x" + SELECTOR_GET_USER_ACCOUNT_DATA.hex()
                            + w3.codec.encode(["address"], [AsyncWeb3.to_checksum_address(a)]).hex(),
        }
        for a in addresses
    ]
    results = await mc.functions.aggregate3(calls).call()
    out = {}
    for addr, (success, raw) in zip(addresses, results):
        if not success or not raw:
            continue
        try:
            decoded = w3.codec.decode(
                ["uint256","uint256","uint256","uint256","uint256","uint256"], raw
            )
            out[addr.lower()] = {
                "total_collateral_base": decoded[0],
                "total_debt_base":       decoded[1],
                "liquidation_threshold": decoded[3],
                "health_factor":         decoded[5],
                "hf_float":              decoded[5] / WAD,
            }
        except Exception as e:
            print(f"  decode error {addr[:10]}: {e}")
    return out


async def main():
    rpc_url = os.getenv("ARBITRUM_HTTP_URL") or os.getenv("RPC_HTTP_URL", "https://arbitrum-one.publicnode.com")
    print(f"RPC: {rpc_url[:60]}...")

    w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(rpc_url))

    block = await w3.eth.block_number
    print(f"Block: {block}\n")

    # ── Step 1: Bootstrap a PositionLoader with the failure cases + 20 random positions ──
    import redis
    try:
        r = redis.from_url("redis://localhost:6379", decode_responses=True)
        # Get 20 near-liquidatable positions for a representative sample
        near_liq = r.zrangebyscore("arb:watchlist:active", 0, 1.05, start=0, num=30)
        r.close()
        print(f"Redis: {len(near_liq)} near-liquidatable positions (HF<1.05) for sample")
    except Exception as e:
        print(f"Redis unavailable: {e}")
        near_liq = []

    # Combine failure cases + sample (deduplicated)
    all_addrs = list({a.lower() for a in FAILURE_CASES + near_liq[:20]})
    print(f"Testing {len(all_addrs)} addresses\n")

    # ── Step 2: Load via PositionLoader (the path decisions are driven by) ──
    loader = PositionLoader(w3, AAVE_POOL)
    t0 = time.monotonic()
    loaded = await loader.bootstrap(all_addrs)
    load_ms = (time.monotonic() - t0) * 1000
    print(f"PositionLoader.bootstrap: {loaded} loaded in {load_ms:.0f}ms\n")

    # ── Step 3: Independent fresh getUserAccountData for the same addresses ──
    t1 = time.monotonic()
    fresh = await fresh_getUserAccountData(w3, AAVE_POOL, all_addrs)
    fresh_ms = (time.monotonic() - t1) * 1000
    print(f"Independent getUserAccountData: {len(fresh)} results in {fresh_ms:.0f}ms\n")

    # ── Step 4: Compare ──
    print("=" * 76)
    print(f"{'Address':>12}  {'Loader HF':>10}  {'Fresh HF':>10}  {'Delta':>8}  Status")
    print("=" * 76)

    max_delta = 0.0
    mismatches = 0

    for addr in all_addrs:
        key = addr.lower()
        loader_acc  = loader.get(addr)
        fresh_entry = fresh.get(key)

        is_failure = addr.lower() in [f.lower() for f in FAILURE_CASES]
        tag = " ← FAILURE CASE" if is_failure else ""

        if loader_acc is None and fresh_entry is None:
            print(f"  {addr[:10]}…  {'no debt':>10}  {'no debt':>10}  {'—':>8}  zero-debt / no position{tag}")
            continue

        loader_hf = loader_acc.hf_float if loader_acc else None
        fresh_hf  = fresh_entry["hf_float"] if fresh_entry else None

        if loader_hf is None:
            print(f"  {addr[:10]}…  {'MISSING':>10}  {fresh_hf:>10.6f}  {'—':>8}  NOT IN LOADER{tag}")
            continue
        if fresh_hf is None:
            print(f"  {addr[:10]}…  {loader_hf:>10.6f}  {'MISSING':>10}  {'—':>8}  NOT IN FRESH{tag}")
            continue

        delta = abs(loader_hf - fresh_hf)
        max_delta = max(max_delta, delta)
        status = "✓ MATCH" if delta < 0.001 else "⚠ DELTA"
        if delta >= 0.001:
            mismatches += 1

        print(f"  {addr[:10]}…  {loader_hf:>10.6f}  {fresh_hf:>10.6f}  {delta:>8.6f}  {status}{tag}")

    print("=" * 76)
    print(f"\nMax delta: {max_delta:.6f}  Mismatches (>0.001): {mismatches}/{len(all_addrs)}")

    if mismatches == 0:
        print("\n✓ AccountData.health_factor matches getUserAccountData() for all sampled positions.")
        print("  The loader and direct on-chain calls are consistent — same source, same block.")
    else:
        print(f"\n⚠ {mismatches} positions show HF delta > 0.001 (likely staleness — different blocks).")

    # ── Step 5: Specific failure case detail ──
    print("\n── Failure case detail ──")
    for addr in FAILURE_CASES:
        key = addr.lower()
        acc = loader.get(addr)
        fr  = fresh.get(key)
        print(f"\nAddress: {addr}")
        if acc:
            print(f"  Loader  → HF={acc.hf_float:.6f}  debt={acc.total_debt_base/1e8:.2f} USD"
                  f"  collateral={acc.total_collateral_base/1e8:.2f} USD"
                  f"  liquidatable={acc.is_liquidatable}")
        else:
            print("  Loader  → NOT FOUND (zero debt or not in watchlist)")
        if fr:
            print(f"  Fresh   → HF={fr['hf_float']:.6f}  debt={fr['total_debt_base']/1e8:.2f} USD"
                  f"  collateral={fr['total_collateral_base']/1e8:.2f} USD"
                  f"  liquidatable={fr['health_factor'] < WAD and fr['total_debt_base'] > 0}")
        else:
            print("  Fresh   → NOT FOUND (zero debt)")

        # Also check Redis score for comparison
        try:
            import redis as _redis
            rc = _redis.from_url("redis://localhost:6379", decode_responses=True)
            redis_score = rc.zscore("arb:watchlist:active", addr.lower())
            rc.close()
            if redis_score is not None:
                print(f"  Redis   → score (HF) = {redis_score:.6f}")
                if acc:
                    print(f"  Redis vs Loader delta = {abs(redis_score - acc.hf_float):.6f}")
            else:
                print("  Redis   → not in watchlist")
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
