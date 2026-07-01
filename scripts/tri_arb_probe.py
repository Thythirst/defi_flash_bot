"""
tri_arb_probe.py — Live triangular arbitrage probe (verify-before-build).

Checks real QuoterV2/Algebra quotes RIGHT NOW for 3-hop cycles among
WETH/WBTC/ARB/USDC/USDT on Camelot + Uni V3 (Arbitrum). Not a backtest —
every number here is a live on-chain call.

Cycles tested (only pairs with confirmed real pools, per swap_monitor.py
MONITORED_PAIRS):
    Triangle A: WETH -> USDC -> ARB -> WETH  (and reverse)
    Triangle B: WETH -> WBTC -> USDC -> WETH (and reverse)

USDT is excluded from triangles — no monitored USDT/ARB or USDT/WBTC pool,
so USDT only connects back to WETH (dead end for a 3-hop cycle).

For each cycle, tries every combination of venues per leg (UniV3 fee tiers +
Camelot) and reports the best round trip, at multiple trade sizes to see
where slippage kills it (1, 5, 20, 50, 100 units of the starting token).
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "services" / "rev2"))
load_dotenv(ROOT / ".env")

WETH = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
USDT = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"
WBTC = "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f"
ARB  = "0x912CE59144191C1204E64559FE8253a0e49E6548"

DECIMALS = {WETH: 18, USDC: 6, USDT: 6, WBTC: 8, ARB: 18}
SYMBOLS  = {WETH: "WETH", USDC: "USDC", USDT: "USDT", WBTC: "WBTC", ARB: "ARB"}

UNIV3_QUOTER   = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"
CAMELOT_QUOTER = "0x0Fc73040b26E9bC8514fA028D998E73A254Fa76E"

UNIV3_QUOTER_ABI = [{
    "inputs": [{
        "components": [
            {"name": "tokenIn",           "type": "address"},
            {"name": "tokenOut",          "type": "address"},
            {"name": "amountIn",          "type": "uint256"},
            {"name": "fee",               "type": "uint24"},
            {"name": "sqrtPriceLimitX96", "type": "uint160"},
        ],
        "name": "params", "type": "tuple",
    }],
    "name": "quoteExactInputSingle",
    "outputs": [
        {"name": "amountOut", "type": "uint256"},
        {"name": "sqrtPriceX96After", "type": "uint160"},
        {"name": "initializedTicksCrossed", "type": "uint32"},
        {"name": "gasEstimate", "type": "uint256"},
    ],
    "stateMutability": "nonpayable", "type": "function",
}]

CAMELOT_QUOTER_ABI = [{
    "inputs": [
        {"name": "tokenIn", "type": "address"},
        {"name": "tokenOut", "type": "address"},
        {"name": "amountIn", "type": "uint256"},
        {"name": "limitSqrtPrice", "type": "uint160"},
    ],
    "name": "quoteExactInputSingle",
    "outputs": [
        {"name": "amountOut", "type": "uint256"},
        {"name": "fee", "type": "uint16"},
    ],
    "stateMutability": "nonpayable", "type": "function",
}]

# Only pairs with a confirmed real pool (per swap_monitor.py's 15 discovered
# pools). Each entry: venues available for that pair.
PAIR_VENUES = {
    frozenset((WETH, USDC)): [("univ3", 500), ("univ3", 3000), ("camelot", 0)],
    frozenset((WETH, USDT)): [("univ3", 500), ("univ3", 3000), ("camelot", 0)],
    frozenset((WBTC, WETH)): [("univ3", 500), ("camelot", 0)],
    frozenset((WBTC, USDC)): [("univ3", 500), ("camelot", 0)],
    frozenset((ARB,  USDC)): [("univ3", 500), ("univ3", 3000), ("camelot", 0)],
    frozenset((ARB,  WETH)): [("univ3", 3000), ("camelot", 0)],
}

TRIANGLES = [
    ("A", [WETH, USDC, ARB]),   # WETH->USDC->ARB->WETH
    ("A-rev", [WETH, ARB, USDC]),
    ("B", [WETH, WBTC, USDC]),  # WETH->WBTC->USDC->WETH
    ("B-rev", [WETH, USDC, WBTC]),
]

SIZES_UNITS = [1, 5, 20, 50, 100]


class Quoter:
    def __init__(self, w3: AsyncWeb3):
        self.univ3 = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(UNIV3_QUOTER), abi=UNIV3_QUOTER_ABI)
        self.camelot = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(CAMELOT_QUOTER), abi=CAMELOT_QUOTER_ABI)

    async def quote(self, venue, token_in, token_out, amount_in) -> int | None:
        dex, fee = venue
        try:
            if dex == "univ3":
                r = await self.univ3.functions.quoteExactInputSingle({
                    "tokenIn": token_in, "tokenOut": token_out,
                    "amountIn": amount_in, "fee": fee, "sqrtPriceLimitX96": 0,
                }).call()
                return r[0]
            else:
                r = await self.camelot.functions.quoteExactInputSingle(
                    token_in, token_out, amount_in, 0).call()
                return r[0]
        except Exception as e:
            return None

    async def best_leg(self, token_in, token_out, amount_in):
        """Return (best_amount_out, venue) across all venues for this pair."""
        key = frozenset((token_in, token_out))
        venues = PAIR_VENUES.get(key)
        if not venues:
            return None, None
        results = await asyncio.gather(
            *[self.quote(v, token_in, token_out, amount_in) for v in venues]
        )
        best = None
        best_v = None
        for v, out in zip(venues, results):
            if out and (best is None or out > best):
                best = out
                best_v = v
        return best, best_v


async def probe_cycle(q: Quoter, path: list[str], start_units: float):
    """path = [A, B, C] meaning A->B->C->A. Returns dict with leg detail."""
    a, b, c = path
    a_dec = DECIMALS[a]
    amount_in = int(start_units * 10**a_dec)

    out1, v1 = await q.best_leg(a, b, amount_in)
    if not out1:
        return None
    out2, v2 = await q.best_leg(b, c, out1)
    if not out2:
        return None
    out3, v3 = await q.best_leg(c, a, out2)
    if not out3:
        return None

    profit_units = (out3 - amount_in) / 10**a_dec
    profit_pct = (out3 - amount_in) / amount_in * 100

    return {
        "path": f"{SYMBOLS[a]}->{SYMBOLS[b]}->{SYMBOLS[c]}->{SYMBOLS[a]}",
        "start_units": start_units,
        "amount_in_raw": amount_in,
        "amount_out_raw": out3,
        "profit_units": profit_units,
        "profit_pct": profit_pct,
        "legs": [
            f"{SYMBOLS[a]}->{SYMBOLS[b]} via {v1[0]}{'' if v1[0]=='camelot' else '-'+str(v1[1])}",
            f"{SYMBOLS[b]}->{SYMBOLS[c]} via {v2[0]}{'' if v2[0]=='camelot' else '-'+str(v2[1])}",
            f"{SYMBOLS[c]}->{SYMBOLS[a]} via {v3[0]}{'' if v3[0]=='camelot' else '-'+str(v3[1])}",
        ],
    }


async def main():
    rpc = os.getenv("READ_RPC_PRIMARY") or os.getenv("RPC_PUBLICNODE") or os.getenv("ARBITRUM_HTTP_URL")
    if not rpc:
        print("No ARBITRUM_HTTP_URL/ALCHEMY_HTTP_URL in .env")
        sys.exit(1)

    w3 = AsyncWeb3(AsyncHTTPProvider(rpc))
    block = await w3.eth.block_number
    print(f"Connected. Block={block}\n")

    q = Quoter(w3)

    for name, path in TRIANGLES:
        print(f"=== Triangle {name}: {'->'.join(SYMBOLS[t] for t in path)}->{SYMBOLS[path[0]]} ===")
        for units in SIZES_UNITS:
            r = await probe_cycle(q, path, units)
            if r is None:
                print(f"  size={units:>4} {SYMBOLS[path[0]]:5} -> NO QUOTE (missing pool/liquidity)")
                continue
            flag = "PROFIT" if r["profit_pct"] > 0 else "loss"
            print(
                f"  size={units:>4} {SYMBOLS[path[0]]:5} -> "
                f"out={r['amount_out_raw']/10**DECIMALS[path[0]]:.6f} "
                f"profit={r['profit_units']:+.6f} {SYMBOLS[path[0]]} "
                f"({r['profit_pct']:+.4f}%) [{flag}]  "
                f"legs: {' | '.join(r['legs'])}"
            )
        print()


if __name__ == "__main__":
    asyncio.run(main())
