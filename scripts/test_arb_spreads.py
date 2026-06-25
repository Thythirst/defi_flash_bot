"""
test_arb_spreads.py — Raw spread diagnostic: logs per-pair DEX quotes without
profit filtering, so we can see how far we are from threshold.

Usage:
    cd ~/defi_flash_bot && venv/bin/python scripts/test_arb_spreads.py
"""

import asyncio
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "rev2"))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

HTTP_URL          = os.getenv("ARBITRUM_HTTP_URL") or os.getenv("ALCHEMY_HTTP_URL", "")
ARB_EXECUTOR_ADDR = os.getenv("ARB_EXECUTOR_ADDR", "0x52184ca20E848A2e219b03eCFC7Dc04e839F50aF")
ETH_PRICE_USD     = 3500.0   # rough — no oracle in this script

ARB_GAS_UNITS = 800_000
BASE_FEE_WEI  = 100_000_000   # 0.1 gwei


def gas_cost_usd() -> float:
    return (ARB_GAS_UNITS * BASE_FEE_WEI * 2) / 1e18 * ETH_PRICE_USD


async def main():
    import aiohttp
    connector = aiohttp.TCPConnector(limit=10, keepalive_timeout=30, enable_cleanup_closed=True)
    session   = aiohttp.ClientSession(connector=connector)
    provider  = AsyncHTTPProvider(HTTP_URL)
    await provider.cache_async_session(session)
    w3 = AsyncWeb3(provider)

    from multi_dex_router import MultiDexRouter, UNIV3_FEE_TIERS
    from dex_arbitrage import (
        WETH, USDC, USDT, ARB, WBTC, DECIMALS, MONITORED_PAIRS,
    )

    multi_dex = MultiDexRouter(w3, ARB_EXECUTOR_ADDR)
    gas_usd   = gas_cost_usd()

    logger.info(f"Block: {await w3.eth.block_number}")
    logger.info(f"Gas cost estimate: ${gas_usd:.4f}")
    logger.info(f"{'Pair':<18} {'Camelot':>14} {'UniV3':>14} {'winner':>8} "
                f"{'spread%':>8} {'gross$':>8} {'net$':>8} {'profitable':>10}")
    logger.info("-" * 100)

    STABLE_PRICE = {USDC: 1.0, USDT: 1.0}
    TOKEN_PRICE  = {
        WETH: ETH_PRICE_USD,
        WBTC: 105_000.0,
        ARB: 0.60,
    }
    TOKEN_PRICE.update(STABLE_PRICE)

    for token_a, token_b, amount_a in MONITORED_PAIRS:
        try:
            # Camelot quote
            c_q = await multi_dex._quote_camelot(
                AsyncWeb3.to_checksum_address(token_a),
                AsyncWeb3.to_checksum_address(token_b),
                amount_a,
            )
            # UniV3 best quote
            uv3_quotes = await asyncio.gather(*[
                multi_dex._quote_univ3(
                    AsyncWeb3.to_checksum_address(token_a),
                    AsyncWeb3.to_checksum_address(token_b),
                    amount_a, fee,
                )
                for fee in UNIV3_FEE_TIERS
            ], return_exceptions=True)
            valid_uv3 = [q for q in uv3_quotes if q and not isinstance(q, Exception)]
            u_q = max(valid_uv3, key=lambda q: q.amount_out) if valid_uv3 else None

            if not c_q or not u_q:
                dec_a = DECIMALS.get(token_a, 18)
                pair  = f"{token_a[-6:]}/{token_b[-6:]}"
                logger.info(f"{pair:<18} quote failed (c={c_q is not None} u={u_q is not None})")
                continue

            c_out = c_q.amount_out
            u_out = u_q.amount_out

            if c_out > u_out:
                winner = "camelot"
                b_amt  = c_out
                # Sell B on UniV3
                sell_quotes = await asyncio.gather(*[
                    multi_dex._quote_univ3(
                        AsyncWeb3.to_checksum_address(token_b),
                        AsyncWeb3.to_checksum_address(token_a),
                        b_amt, fee,
                    )
                    for fee in UNIV3_FEE_TIERS
                ], return_exceptions=True)
                valid_sell = [q for q in sell_quotes if q and not isinstance(q, Exception)]
                sell_q = max(valid_sell, key=lambda q: q.amount_out) if valid_sell else None
            else:
                winner = "univ3"
                b_amt  = u_out
                # Sell B on Camelot
                sell_q = await multi_dex._quote_camelot(
                    AsyncWeb3.to_checksum_address(token_b),
                    AsyncWeb3.to_checksum_address(token_a),
                    b_amt,
                )

            if not sell_q:
                logger.info(f"sell quote failed for {token_a[-6:]}/{token_b[-6:]}")
                continue

            a_back      = sell_q.amount_out
            dec_a       = DECIMALS.get(token_a, 18)
            price_a     = TOKEN_PRICE.get(token_a, 1.0)
            gross       = a_back - amount_a
            gross_usd   = (gross / 10**dec_a) * price_a
            spread_pct  = (gross / amount_a) * 100 if amount_a > 0 else 0
            net_usd     = gross_usd - gas_usd

            pair = f"{token_a[-6:]}/{token_b[-6:]}"
            logger.info(
                f"{pair:<18} {c_out:>14} {u_out:>14} {winner:>8} "
                f"{spread_pct:>7.4f}% {gross_usd:>7.4f}$ {net_usd:>7.4f}$ "
                f"{'YES' if net_usd > 0 else 'no':>10}"
            )

        except Exception as e:
            logger.warning(f"Pair {token_a[-6:]}/{token_b[-6:]} error: {e}")

    await session.close()


if __name__ == "__main__":
    asyncio.run(main())
