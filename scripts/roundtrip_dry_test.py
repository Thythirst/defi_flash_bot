"""Live REAL round-trip dry test on the exact pair/venues the Telegram
alerts fire on (WETH/USDC, buy UniV3-3000 -> sell UniV3-500, and reverse).

Both legs are real QuoterV2 quotes (price-impact + both pool fees included),
i.e. exactly what would happen on-chain. Nothing is broadcast.
"""
import asyncio, os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "rev2"))
from web3 import AsyncWeb3
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from multi_dex_router import MultiDexRouter

WETH = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
EXECUTOR = "0x52184ca20E848A2e219b03eCFC7Dc04e839F50aF"
WETH_USD = 3500.0   # approx; only used to size notional + value gas
GAS_USD = 0.15

async def quote(mdr, tin, tout, amt, fee):
    q = await mdr._quote_univ3(AsyncWeb3.to_checksum_address(tin),
                               AsyncWeb3.to_checksum_address(tout), amt, fee)
    return q.amount_out if q else None

async def round_trip(mdr, weth_in, buy_fee, sell_fee):
    # Leg 1: WETH -> USDC on buy_fee tier
    usdc = await quote(mdr, WETH, USDC, weth_in, buy_fee)
    if not usdc:
        return None
    # Leg 2: USDC -> WETH on sell_fee tier
    weth_back = await quote(mdr, USDC, WETH, usdc, sell_fee)
    if not weth_back:
        return None
    gross_weth = weth_back - weth_in
    gross_usd = (gross_weth / 1e18) * WETH_USD
    net_usd = gross_usd - GAS_USD
    return weth_back, gross_usd, net_usd

async def main():
    url = os.getenv("ARBITRUM_HTTP_URL")
    w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(url))
    mdr = MultiDexRouter(w3, EXECUTOR)
    print(f"RPC ok, chainId={await w3.eth.chain_id}\n")
    print(f"{'notional':>10} {'dir':>14} {'gross_usd':>10} {'net_usd':>9}")
    for notional_usd in (1000, 5000, 10000, 25000, 50000):
        weth_in = int((notional_usd / WETH_USD) * 1e18)
        for buy_fee, sell_fee, tag in ((3000, 500, "buy3000>sell500"),
                                       (500, 3000, "buy500>sell3000")):
            r = await round_trip(mdr, weth_in, buy_fee, sell_fee)
            if r is None:
                print(f"{notional_usd:>10} {tag:>14}   quote-failed")
                continue
            _, gross_usd, net_usd = r
            flag = "  <-- PROFIT" if net_usd > 0 else ""
            print(f"${notional_usd:>9} {tag:>14} {gross_usd:>+10.2f} {net_usd:>+9.2f}{flag}")

asyncio.run(main())
