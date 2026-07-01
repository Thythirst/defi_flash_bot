"""
pendle_aave_overlap.py — Find every chain/asset pair where a Pendle
active market's underlying exactly matches an Aave V3 reserve, across
every chain both protocols share.

This is the survey step of the Pendle-vs-Aave basis-trade investigation
(implied fixed yield vs floating rate on the same underlying). Matches
by exact on-chain address, not name-guessing — a first pass using
name-matching on Ethereum missed USDG entirely.

Findings as of 2026-07-01 (see project memory for full writeup):
  - Only 9 of Pendle's 11 chains also have Aave V3 live (Sonic/Mantle
    are in Aave's address-book but have zero bytecode at the listed
    Pool address on-chain — not actually deployed yet).
  - Ethereum: 4 overlaps (wstETH, USDe, sUSDe, USDG) out of 61 active
    markets. Two illusory (0% Aave utilization on wstETH/sUSDe), one
    wrong-direction (USDG: Aave floating rates exceed Pendle's fixed
    rate), one real (USDe) but fails depth check (see
    pendle_market_depth.py).
  - Plasma: 1 overlap (sUSDe), illusory (0% utilization).
  - Arbitrum/BNB/Base/Optimism/Monad: zero overlap.
  - GHO and USDC/USDT: zero overlap, active or historical, on any
    chain, ever — Pendle's PT/YT mechanism requires a yield-bearing
    wrapper to tokenize; raw reserve stablecoins have no native yield
    to split, so Pendle always wraps a yield-bearing derivative
    instead (aUSDC, dUSDC, syrupUSDC, stkGHO, etc.), never the bare
    Aave-listed token.

Usage:
    python scripts/pendle_aave_overlap.py --chain-id 1 \
        --aave-pool 0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2 \
        --rpc https://ethereum-rpc.publicnode.com
"""

from __future__ import annotations

import argparse
import asyncio

import requests
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

POOL_ABI = [
    {"inputs": [], "name": "getReservesList", "outputs": [{"name": "", "type": "address[]"}],
     "stateMutability": "view", "type": "function"},
]
ERC20_ABI = [{"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}],
              "type": "function"}]


async def get_aave_reserves(rpc: str, pool: str) -> dict[str, str]:
    w3 = AsyncWeb3(AsyncHTTPProvider(rpc))
    c = w3.eth.contract(address=AsyncWeb3.to_checksum_address(pool), abi=POOL_ABI)
    reserves = await c.functions.getReservesList().call()
    out = {}
    for r in reserves:
        try:
            ec = w3.eth.contract(address=r, abi=ERC20_ABI)
            sym = await ec.functions.symbol().call()
        except Exception:
            sym = "ERR"
        out[r.lower()] = sym
    return out


def get_pendle_active_markets(chain_id: int) -> list[dict]:
    url = f"https://api-v2.pendle.finance/core/v1/{chain_id}/markets/active"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json().get("markets", [])


async def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--chain-id", type=int, required=True)
    ap.add_argument("--aave-pool", required=True)
    ap.add_argument("--rpc", required=True)
    args = ap.parse_args()

    print(f"Fetching Aave reserves (chain {args.chain_id})...")
    aave = await get_aave_reserves(args.rpc, args.aave_pool)
    print(f"  {len(aave)} reserves")

    print("Fetching Pendle active markets...")
    markets = get_pendle_active_markets(args.chain_id)
    print(f"  {len(markets)} active markets\n")

    hits = []
    for m in markets:
        ua = m["underlyingAsset"].split("-")[1].lower()
        if ua in aave:
            hits.append((m, aave[ua]))

    if not hits:
        print("NO OVERLAP")
        return

    print(f"{'Pendle market':>20} | {'Aave symbol':>12} | {'TVL':>14} | {'implied APY':>12} | expiry")
    for m, sym in hits:
        d = m["details"]
        print(f"{m['name']:>20} | {sym:>12} | ${d['liquidity']:>12,.0f} | "
              f"{d['impliedApy']*100:>10.2f}% | {m['expiry'][:10]}")


if __name__ == "__main__":
    asyncio.run(main())
