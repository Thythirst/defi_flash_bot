"""
pendle_market_depth.py — Real depth check for a Pendle PT market, via
Pendle's hosted swap-quote endpoint (same Newton's-method solver that
matches on-chain execution, not a spot-price estimate).

Origin: used to verify the Pendle PT-USDe/Aave-USDe basis trade
(implied fixed yield 5.11% vs Aave floating borrow 2.62%). Headline
Pendle market TVL was $446.9K; this script found real usable depth
tops out around $50K (11% of TVL) before the AMM curve breaks down
into economically nonsensical quotes, and $1M was flat-out rejected
by Pendle's own solver ("ApproxFail. Market liquidity is likely
insufficient"). See project memory for the full writeup.

Usage:
    python scripts/pendle_market_depth.py \
        --chain 1 --market 0x43c97094da0e894d3af2fda6f507d59a29888251 \
        --token-in 0x4c9edd5852cd905f086c759e8383e09bff1e68b3 \
        --pt 0x3ec43f4158d51df0928979c09eaf33cad287065c \
        --expiry 2026-08-13 --headline-apy 0.0511
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone

import requests

RECEIVER = "0x000000000000000000000000000000000000dEaD"
DEFAULT_SIZES_USD = [1_000, 5_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000]


def quote(chain_id: int, market: str, token_in: str, token_out: str, amount_in_wei: int):
    url = f"https://api-v2.pendle.finance/core/v2/sdk/{chain_id}/markets/{market}/swap"
    params = {
        "receiver": RECEIVER,
        "slippage": "0.01",
        "tokenIn": token_in,
        "tokenOut": token_out,
        "amountIn": str(amount_in_wei),
    }
    r = requests.get(url, params=params, timeout=20)
    if r.status_code != 200:
        return None, r.status_code, r.text[:200]
    d = r.json()
    guess = d["contractCallParams"][3]["guessOffchain"]
    return int(guess), 200, None


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--chain", type=int, required=True)
    ap.add_argument("--market", required=True)
    ap.add_argument("--token-in", required=True, help="underlying token address (e.g. USDe)")
    ap.add_argument("--pt", required=True, help="PT token address for this market")
    ap.add_argument("--expiry", required=True, help="YYYY-MM-DD")
    ap.add_argument("--headline-apy", type=float, required=True, help="Pendle's quoted implied APY, e.g. 0.0511")
    ap.add_argument("--sizes", type=int, nargs="*", default=DEFAULT_SIZES_USD)
    args = ap.parse_args()

    expiry = datetime.strptime(args.expiry, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    days = (expiry - datetime.now(timezone.utc)).days
    if days <= 0:
        print(f"Market already expired ({args.expiry})")
        sys.exit(1)

    print(f"Days to maturity: {days}\n")
    print(f"{'Size':>14} | {'PT received':>16} | {'exec price/PT':>14} | {'realized APY':>14} | {'vs headline':>14}")
    for size in args.sizes:
        amount_wei = int(size * 1e18)
        pt_out, code, err = quote(args.chain, args.market, args.token_in, args.pt, amount_wei)
        if pt_out is None:
            print(f"{size:>14,} | ERROR {code} {err}")
            continue
        pt_human = pt_out / 1e18
        price_per_pt = size / pt_human
        realized_apy = (1 / price_per_pt) ** (365 / days) - 1
        degradation = realized_apy - args.headline_apy
        print(
            f"{size:>14,} | {pt_human:>16,.2f} | {price_per_pt:>14.6f} | "
            f"{realized_apy*100:>13.3f}% | {degradation*100:>+13.3f}pp"
        )


if __name__ == "__main__":
    main()
