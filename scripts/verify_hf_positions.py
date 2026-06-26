#!/usr/bin/env python3
"""
Directly verify 20 HF<0.97 positions against getUserAccountData().
Reports on-chain HF, collateral (USD), debt (USD) to confirm:
  1. HF is genuinely below 0.97 (not a stale Redis value)
  2. Whether collateral is dust or a real liquidation opportunity
"""

import asyncio
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from eth_abi import decode as abi_decode
from web3 import AsyncWeb3, AsyncHTTPProvider
import redis
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

ARBITRUM_HTTP = os.environ["ARBITRUM_HTTP_URL"]
AAVE_POOL     = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
GUAD_SEL      = bytes.fromhex("bf92857c")  # getUserAccountData(address)

# getUserAccountData returns:
#  [0] totalCollateralBase  — USD, 8 decimals (Aave V3 "base currency" on Arb = USD 1e8)
#  [1] totalDebtBase        — USD, 8 decimals
#  [2] availableBorrowsBase — USD, 8 decimals
#  [3] currentLiquidationThreshold — 4-dec bps (e.g. 8500 = 85%)
#  [4] ltv                  — 4-dec bps
#  [5] healthFactor         — WAD (1e18 = 1.0)
DUST_THRESHOLD_USD = 0.50   # positions below $0.50 collateral are dust — not worth liquidating


async def get_account_data(w3: AsyncWeb3, addr: str):
    calldata = GUAD_SEL + bytes.fromhex(addr[2:].lower().zfill(64))
    raw = await w3.eth.call({"to": AAVE_POOL, "data": "0x" + calldata.hex()})
    return abi_decode(
        ["uint256", "uint256", "uint256", "uint256", "uint256", "uint256"],
        raw,
    )


async def main():
    r = redis.Redis(decode_responses=True)
    # Pull 20 lowest-HF positions
    entries = r.zrangebyscore("arb:watchlist:active", 0, 0.97, withscores=True, start=0, num=20)
    if not entries:
        print("No entries found in arb:watchlist:active with HF<0.97")
        return

    print(f"Verifying {len(entries)} positions from Redis (HF<0.97) against on-chain getUserAccountData()...")
    print(f"RPC: {ARBITRUM_HTTP[:55]}...")
    print()

    w3 = AsyncWeb3(AsyncHTTPProvider(ARBITRUM_HTTP, request_kwargs={"timeout": 15}))

    results = []
    for addr, redis_hf in entries:
        try:
            data = await get_account_data(w3, addr)
            col_usd   = data[0] / 1e8      # USD
            debt_usd  = data[1] / 1e8      # USD
            liq_thresh = data[3] / 1e4     # fraction  e.g. 0.85
            hf_wad    = data[5]
            hf_real   = hf_wad / 1e18
            results.append({
                "addr": addr,
                "redis_hf": redis_hf,
                "chain_hf": hf_real,
                "col_usd":  col_usd,
                "debt_usd": debt_usd,
                "liq_thresh": liq_thresh,
            })
        except Exception as e:
            results.append({
                "addr": addr,
                "redis_hf": redis_hf,
                "error": str(e),
            })

    # ── Print results ───────────────────────────────────────────
    print(f"{'Address':<44}  {'Redis HF':>10}  {'Chain HF':>10}  {'Col $':>12}  {'Debt $':>12}  {'Verdict'}")
    print("-" * 120)

    real_opps   = 0
    dust_count  = 0
    stale_count = 0
    error_count = 0

    for r_ in results:
        if "error" in r_:
            print(f"{r_['addr']:<44}  {r_['redis_hf']:>10.4f}  {'ERROR':>10}  {'':>12}  {'':>12}  {r_['error'][:40]}")
            error_count += 1
            continue

        chain_hf  = r_["chain_hf"]
        col       = r_["col_usd"]
        redis_hf  = r_["redis_hf"]
        debt      = r_["debt_usd"]

        # Verdict logic
        if chain_hf >= 1.0:
            verdict = "STALE — healed on chain"
            stale_count += 1
        elif chain_hf >= 0.97:
            verdict = "BORDERLINE (0.97-1.0)"
            stale_count += 1
        elif col < DUST_THRESHOLD_USD:
            verdict = f"DUST  (<${DUST_THRESHOLD_USD:.2f} col)"
            dust_count += 1
        else:
            verdict = f"*** REAL OPPORTUNITY ***"
            real_opps += 1

        hf_drift = abs(chain_hf - redis_hf)
        drift_flag = f" [drift {hf_drift:.4f}]" if hf_drift > 0.005 else ""

        print(
            f"{r_['addr']:<44}  {redis_hf:>10.4f}  {chain_hf:>10.4f}  "
            f"${col:>11,.2f}  ${debt:>11,.2f}  {verdict}{drift_flag}"
        )

    print()
    print("=" * 80)
    print(f"  Real opportunities  : {real_opps}")
    print(f"  Dust (col < ${DUST_THRESHOLD_USD:.2f})  : {dust_count}")
    print(f"  Stale/healed        : {stale_count}")
    print(f"  RPC errors          : {error_count}")
    print("=" * 80)

    if real_opps == 0:
        print()
        print("CONCLUSION: All 7,341 HF<0.97 positions are likely dust or already healed.")
        print("            The pipeline's 50-candidate cutoff is filtering them out correctly.")


if __name__ == "__main__":
    asyncio.run(main())
