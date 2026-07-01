"""
lst_lrt_cross_dex_depth.py — Depth-checked peg scan for LST/LRT tokens
(cbETH, rETH, weETH, ezETH, rsETH) against real DEX quotes on Arbitrum
(Camelot + UniV3) and Base (Aerodrome Slipstream), using live Chainlink
X/ETH feeds as the fair-value reference.

Same discipline as services/rev2/lst_depeg_scanner.py (which covers only
wstETH): escalating trade sizes [1, 5, 20, 50, 100] units, report where
each venue's liquidity actually runs out rather than trusting a 1-unit
quote. See project memory for the full writeup and conclusions —
summary: cbETH/weETH are healthy on Base (Aerodrome), thin-to-nonexistent
on Arbitrum; rETH has real but shallow Arbitrum liquidity (~20-25 units)
and none on Base; ezETH is thin on both; rsETH is the worst market found
anywhere in this investigation series (Arbitrum liquidity evacuated
following the April 2026 Kelp DAO/LayerZero bridge exploit — see
watchlist_rseth_exposure.py for the follow-up on why that matters).

stETH itself is deliberately excluded — Lido does not bridge the
rebasing token to L2s, only wstETH, which the existing scanner covers.

Usage:
    python scripts/lst_lrt_cross_dex_depth.py
"""

from __future__ import annotations

import asyncio
import time

from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

DEPTH_SIZES = [1, 5, 20, 50, 100]

CHAINLINK_ABI = [{
    "inputs": [], "name": "latestRoundData",
    "outputs": [
        {"name": "roundId", "type": "uint80"}, {"name": "answer", "type": "int256"},
        {"name": "startedAt", "type": "uint256"}, {"name": "updatedAt", "type": "uint256"},
        {"name": "answeredInRound", "type": "uint80"},
    ], "stateMutability": "view", "type": "function",
}]
UNIV3_QUOTER_ABI = [{
    "inputs": [{"components": [
        {"name": "tokenIn", "type": "address"}, {"name": "tokenOut", "type": "address"},
        {"name": "amountIn", "type": "uint256"}, {"name": "fee", "type": "uint24"},
        {"name": "sqrtPriceLimitX96", "type": "uint160"}], "name": "params", "type": "tuple"}],
    "name": "quoteExactInputSingle",
    "outputs": [{"name": "amountOut", "type": "uint256"}, {"name": "sqrtPriceX96After", "type": "uint160"},
                {"name": "initializedTicksCrossed", "type": "uint32"}, {"name": "gasEstimate", "type": "uint256"}],
    "stateMutability": "nonpayable", "type": "function",
}]
CAMELOT_QUOTER_ABI = [{
    "inputs": [{"name": "tokenIn", "type": "address"}, {"name": "tokenOut", "type": "address"},
               {"name": "amountIn", "type": "uint256"}, {"name": "limitSqrtPrice", "type": "uint160"}],
    "name": "quoteExactInputSingle",
    "outputs": [{"name": "amountOut", "type": "uint256"}, {"name": "fee", "type": "uint16"}],
    "stateMutability": "nonpayable", "type": "function",
}]
AERODROME_QUOTER_ABI = [{
    "inputs": [{"components": [
        {"name": "tokenIn", "type": "address"}, {"name": "tokenOut", "type": "address"},
        {"name": "amountIn", "type": "uint256"}, {"name": "tickSpacing", "type": "int24"},
        {"name": "sqrtPriceLimitX96", "type": "uint160"}], "name": "params", "type": "tuple"}],
    "name": "quoteExactInputSingle",
    "outputs": [{"name": "amountOut", "type": "uint256"}, {"name": "sqrtPriceX96After", "type": "uint160"},
                {"name": "initializedTicksCrossed", "type": "uint32"}, {"name": "gasEstimate", "type": "uint256"}],
    "stateMutability": "nonpayable", "type": "function",
}]

ARB_RPC = "https://arb1.arbitrum.io/rpc"
BASE_RPC = "https://base-rpc.publicnode.com"
ARB_WETH = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
ARB_CAMELOT_QUOTER = "0x0Fc73040b26E9bC8514fA028D998E73A254Fa76E"
ARB_UNIV3_QUOTER = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"
BASE_WETH = "0x4200000000000000000000000000000000000006"
BASE_AERODROME_QUOTER = "0x254cF9E1E6e233aa1AC962CB9B05b2cfeAaE15b0"

# (chain, symbol, token_address, chainlink_X/ETH_feed)
ARBITRUM_ASSETS = [
    ("cbETH", "0x1dEBd73E752beaf79865fd6446b0c970eae7732F", "0xa668682974E3f121185a3cD94f00322beC674275"),
    ("rETH",  "0xEC70Dcb4A1EFa46b8F2D97C310C9c4790ba5ffA8", "0xD6aB2298946840262FcC278fF31516D39fF611eF"),
    ("weETH", "0x35751007a407ca6FEFfE80b3cB397736D2cf4dbe", "0xE141425bc1594b8039De6390db1cDaf4397EA22b"),
    ("ezETH", "0x2416092f143378750bb29b79eD961ab195CcEea5", "0x11E1836bFF2ce9d6A5bec9cA79dc998210f3886d"),
    ("rsETH", "0x4186BFC76E2E237523CBC30FD220FE055156b41F", "0xb0EA543f9F8d4B818550365d13F66Da747e1476A"),
]
BASE_ASSETS = [
    ("cbETH", "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22", "0x806b4Ac04501c29769051e42783cF04dCE41440b"),
    ("rETH",  "0xB6fe221Fe9EeF5aBa221c348bA20A1Bf5e73624c", "0xf397bF97280B488cA19ee3093E81C0a77F02e9a5"),
    ("weETH", "0x04C0599Ae5A44757c0af6F9eC3b93da8976c150A", "0xFC1415403EbB0c693f9a7844b92aD2Ff24775C65"),
    ("ezETH", "0x2416092f143378750bb29b79eD961ab195CcEea5", "0x960BDD1dFD20d7c98fa482D793C3dedD73A113a3"),
]


async def get_fair_value(w3, feed_addr, label):
    c = w3.eth.contract(address=AsyncWeb3.to_checksum_address(feed_addr), abi=CHAINLINK_ABI)
    _, answer, _, updated_at, _ = await c.functions.latestRoundData().call()
    age_min = (time.time() - updated_at) / 60
    fair = answer / 1e18
    print(f"  [{label}] Chainlink fair value = {fair:.6f} ETH  (feed age = {age_min:.1f} min = {age_min/60:.1f}h)")
    return fair


async def univ3_quote(w3, quoter_addr, token_in, token_out, amount_in, fee):
    c = w3.eth.contract(address=AsyncWeb3.to_checksum_address(quoter_addr), abi=UNIV3_QUOTER_ABI)
    try:
        r = await c.functions.quoteExactInputSingle({
            "tokenIn": token_in, "tokenOut": token_out, "amountIn": amount_in,
            "fee": fee, "sqrtPriceLimitX96": 0}).call()
        return r[0], r[2]
    except Exception:
        return None, None


async def camelot_quote(w3, quoter_addr, token_in, token_out, amount_in):
    c = w3.eth.contract(address=AsyncWeb3.to_checksum_address(quoter_addr), abi=CAMELOT_QUOTER_ABI)
    try:
        r = await c.functions.quoteExactInputSingle(token_in, token_out, amount_in, 0).call()
        return r[0]
    except Exception:
        return None


async def aerodrome_quote(w3, quoter_addr, token_in, token_out, amount_in, tick_spacing):
    c = w3.eth.contract(address=AsyncWeb3.to_checksum_address(quoter_addr), abi=AERODROME_QUOTER_ABI)
    try:
        r = await c.functions.quoteExactInputSingle({
            "tokenIn": token_in, "tokenOut": token_out, "amountIn": amount_in,
            "tickSpacing": tick_spacing, "sqrtPriceLimitX96": 0}).call()
        return r[0], r[2]
    except Exception:
        return None, None


async def scan_arbitrum():
    print("=" * 70); print("ARBITRUM"); print("=" * 70)
    w3 = AsyncWeb3(AsyncHTTPProvider(ARB_RPC))
    for name, token, feed in ARBITRUM_ASSETS:
        print(f"\n--- Arbitrum {name} ---")
        fair = await get_fair_value(w3, feed, f"{name}/ETH")
        for size in DEPTH_SIZES:
            amt_in = int(size * 1e18)
            cam = await camelot_quote(w3, ARB_CAMELOT_QUOTER, token, ARB_WETH, amt_in)
            uni = {}
            for fee in (500, 3000, 10000):
                out, ticks = await univ3_quote(w3, ARB_UNIV3_QUOTER, token, ARB_WETH, amt_in, fee)
                uni[fee] = (out, ticks)
            parts = [f"camelot={cam/1e18:.6f}" if cam else "camelot=NONE"]
            for fee, (out, ticks) in uni.items():
                parts.append(f"univ3-{fee}={out/1e18:.6f}(t{ticks})" if out else f"univ3-{fee}=NONE")
            best = max([v for v in [cam] + [x[0] for x in uni.values()] if v], default=None)
            dev = (best / amt_in - fair) / fair * 100 if best else None
            dev_str = f"dev={dev:+.3f}%" if dev is not None else "NO LIQUIDITY"
            print(f"  size={size:>4} {name}: {' | '.join(parts)}  [{dev_str}]")


async def scan_base():
    print("\n" + "=" * 70); print("BASE"); print("=" * 70)
    w3 = AsyncWeb3(AsyncHTTPProvider(BASE_RPC))
    for name, token, feed in BASE_ASSETS:
        print(f"\n--- Base {name} (Aerodrome) ---")
        fair = await get_fair_value(w3, feed, f"{name}/ETH")
        for size in DEPTH_SIZES:
            amt_in = int(size * 1e18)
            results = {}
            for ts in (1, 50, 60, 100, 200):
                out, ticks = await aerodrome_quote(w3, BASE_AERODROME_QUOTER, token, BASE_WETH, amt_in, ts)
                results[ts] = (out, ticks)
            parts = [f"ts{ts}={out/1e18:.6f}(t{ticks})" if out else f"ts{ts}=NONE" for ts, (out, ticks) in results.items()]
            best = max([v[0] for v in results.values() if v[0]], default=None)
            dev = (best / amt_in - fair) / fair * 100 if best else None
            dev_str = f"dev={dev:+.3f}%" if dev is not None else "NO LIQUIDITY"
            print(f"  size={size:>4} {name}: {' | '.join(parts)}  [{dev_str}]")


async def main():
    await scan_arbitrum()
    await scan_base()


if __name__ == "__main__":
    asyncio.run(main())
