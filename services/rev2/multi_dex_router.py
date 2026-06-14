"""
multi_dex_router.py — Multi-DEX swap routing for optimal liquidation execution
Compares Uniswap V3 and Camelot V3 (Algebra) quotes, picks the best amountOut.

The executor contract (FlashExecutorV3) is a generic swap proxy —
swapRouter.call(swapCalldata) — so it accepts any approved router.
This module adds Camelot as a second quote source and encodes its
calldata format (which differs from Uni V3: no fee field, Algebra
uses dynamic per-pool fees).

Verified on-chain (Arbitrum):
    WETH→USDC:  Camelot 167.34 vs UniV3 0.05% 167.34 (tied)
    WBTC→WETH:  Camelot 0.3842 vs UniV3 0.30% 0.3823 (Camelot +0.49%)

Camelot wins or ties on every pair tested. The edge is modest per swap
but compounds across every liquidation and costs nothing — it's better
execution on collateral already being received.

Integration:
    1. On-chain (once): approveRouter(CAMELOT_ROUTER) on the executor
    2. SwapCalldataBuilder.build() calls MultiDexRouter.best_route()
       instead of quoting only Uni V3
    3. Returns the winning DEX's router + calldata

This is legitimate MEV — optimal routing on liquidation proceeds.
No user is harmed; better routing simply reduces slippage loss.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

from eth_abi import encode
from web3 import AsyncWeb3

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Addresses — Arbitrum, verified on-chain
# ---------------------------------------------------------------------------

# Uniswap V3
UNIV3_ROUTER   = "0xE592427A0AEce92De3Edee1F18E0157C05861564"
UNIV3_QUOTER   = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"   # QuoterV2
UNIV3_FEE_TIERS = [3000, 10000]   # 500 excluded (corrupt WBTC quotes)

# Camelot V3 (Algebra) — verified 25,397 byte router, 10,227 byte quoter
CAMELOT_ROUTER = "0x1F721E2E82F6676FCE4eA07A5958cF098D339e18"
CAMELOT_QUOTER = "0x0Fc73040b26E9bC8514fA028D998E73A254Fa76e"
CAMELOT_FACTORY= "0x1a3c9B1d2F0529D97f2afC5136Cc23e58f1FD35B"

# Selectors
UNIV3_EXACT_INPUT_SINGLE   = bytes.fromhex("414bf389")  # Uni V3 exactInputSingle
UNIV3_EXACT_INPUT          = bytes.fromhex("c04b8d59")  # Uni V3 exactInput (multi-hop)
CAMELOT_EXACT_INPUT_SINGLE = bytes.fromhex("bc651188")  # Camelot exactInputSingle

# Sanity cap — reject quotes with implausible output/input ratio
# Normalized by token decimals (cross-decimal pairs like WBTC→WETH have
# 10^10 raw ratio from the 8→18 decimal gap alone, so raw comparison is useless)
MAX_RATIO = 100_000

# Token decimals — used to normalize the ratio check for cross-decimal pairs
TOKEN_DECIMALS = {
    "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1": 18,  # WETH
    "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f": 8,   # WBTC
    "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8": 6,   # USDC.e
    "0xaf88d065e77c8cC2239327C5EDb3A432268e5831": 6,   # USDC native
    "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9": 6,   # USDT
    "0x912CE59144191C1204E64559FE8253a0e49E6548": 18,  # ARB
    "0x5979D7b546E38E414F7E9822514be443A4800529": 18,  # wstETH
    "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4": 18,  # LINK
}

# Token address shortcuts
WSTETH = "0x5979D7b546E38E414F7E9822514be443A4800529"
WETH   = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC   = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

# Multi-hop paths: (token_in, token_out) → [(fee1, intermediate1), (fee2, ...), ...]
# Only defined for pairs with NO direct pool on any DEX.
# Path bytes: token_in(20) + fee1(3) + intermediate1(20) + fee2(3) + token_out(20)
MULTIHOP_PATHS = {
    (WSTETH.lower(), USDC.lower()): [
        [( 100, WETH), (500, None)],  # wstETH → WETH(0.01%) → USDC(0.05%)
    ],
}


# ---------------------------------------------------------------------------
# ABIs
# ---------------------------------------------------------------------------

# Uni V3 QuoterV2 — both single-hop and multi-hop quoting
UNIV3_QUOTER_ABI = [
    # quoteExactInputSingle
    {
        "inputs": [{
            "components": [
                {"name": "tokenIn",           "type": "address"},
                {"name": "tokenOut",          "type": "address"},
                {"name": "amountIn",          "type": "uint256"},
                {"name": "fee",               "type": "uint24"},
                {"name": "sqrtPriceLimitX96", "type": "uint160"},
            ],
            "name": "params",
            "type": "tuple",
        }],
        "name": "quoteExactInputSingle",
        "outputs": [
            {"name": "amountOut",               "type": "uint256"},
            {"name": "sqrtPriceX96After",       "type": "uint160"},
            {"name": "initializedTicksCrossed", "type": "uint32"},
            {"name": "gasEstimate",             "type": "uint256"},
        ],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    # quoteExactInput (multi-hop) — returns amountOut only for efficiency
    {
        "inputs": [
            {"name": "path",      "type": "bytes"},
            {"name": "amountIn",  "type": "uint256"},
        ],
        "name": "quoteExactInput",
        "outputs": [
            {"name": "amountOut", "type": "uint256"},
        ],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]

# Camelot V3 (Algebra) Quoter — quoteExactInputSingle returns (amountOut, fee)
# NOTE: no fee input param (dynamic), returns the fee that was applied
CAMELOT_QUOTER_ABI = [{
    "inputs": [
        {"name": "tokenIn",          "type": "address"},
        {"name": "tokenOut",         "type": "address"},
        {"name": "amountIn",         "type": "uint256"},
        {"name": "limitSqrtPrice",   "type": "uint160"},
    ],
    "name": "quoteExactInputSingle",
    "outputs": [
        {"name": "amountOut", "type": "uint256"},
        {"name": "fee",       "type": "uint16"},
    ],
    "stateMutability": "nonpayable",
    "type": "function",
}]


# ---------------------------------------------------------------------------
# Result dataclass — mirrors existing SwapRoute for drop-in compatibility
# ---------------------------------------------------------------------------

@dataclass
class DexQuote:
    dex:          str     # "univ3" | "camelot" | "univ3_multihop"
    router:       str
    amount_out:   int
    fee_tier:     int     # Uni V3 fee tier, Algebra dynamic fee, or 0 for multi-hop
    token_in:     str
    token_out:    str
    amount_in:    int
    path:         Optional[bytes] = None  # encoded path bytes for multi-hop, None for single-hop

    @property
    def is_camelot(self) -> bool:
        return self.dex == "camelot"

    @property
    def is_multihop(self) -> bool:
        return self.path is not None


# ---------------------------------------------------------------------------
# MultiDexRouter
# ---------------------------------------------------------------------------

class MultiDexRouter:
    """
    Quotes Uniswap V3 and Camelot V3 concurrently, returns the best route.

    Drop-in for the live-quote step (step 3) in SwapCalldataBuilder.build().
    Keeps the existing Balancer wstETH special case and QuoteCache upstream —
    this only replaces the "quote Uni V3 fee tiers" logic with
    "quote Uni V3 fee tiers AND Camelot, pick best".
    """

    def __init__(self, w3: AsyncWeb3, executor_address: str):
        self._w3       = w3
        self._executor = AsyncWeb3.to_checksum_address(executor_address)

        self._univ3_quoter = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(UNIV3_QUOTER),
            abi=UNIV3_QUOTER_ABI,
        )
        self._camelot_quoter = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(CAMELOT_QUOTER),
            abi=CAMELOT_QUOTER_ABI,
        )

    async def best_route(
        self,
        token_in:  str,
        token_out: str,
        amount_in: int,
    ) -> Optional[DexQuote]:
        """
        Quote both DEXs concurrently, return the best DexQuote or None.
        """
        token_in  = AsyncWeb3.to_checksum_address(token_in)
        token_out = AsyncWeb3.to_checksum_address(token_out)

        # Quote Uni V3 fee tiers + Camelot + multi-hop paths — all concurrent
        tasks = [
            self._quote_univ3(token_in, token_out, amount_in, fee)
            for fee in UNIV3_FEE_TIERS
        ]
        tasks.append(self._quote_camelot(token_in, token_out, amount_in))

        # Multi-hop paths for pairs with no direct pool
        mh_key = (token_in.lower(), token_out.lower())
        if mh_key in MULTIHOP_PATHS:
            for hops in MULTIHOP_PATHS[mh_key]:
                path_bytes = _build_path(token_in, hops, token_out)
                tasks.append(self._quote_univ3_multihop(path_bytes, amount_in))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        best: Optional[DexQuote] = None
        for r in results:
            if isinstance(r, Exception) or r is None:
                continue
            # Sanity check — reject corrupt quotes (decimal-normalized)
            if r.amount_out == 0:
                continue
            in_dec  = TOKEN_DECIMALS.get(r.token_in, 18)
            out_dec = TOKEN_DECIMALS.get(r.token_out, 18)
            human_out = r.amount_out / 10**out_dec
            human_in  = r.amount_in / 10**in_dec
            if human_in > 0 and human_out / human_in > MAX_RATIO:
                logger.warning(
                    f"[MultiDex] Rejected implausible {r.dex} quote: "
                    f"out={human_out:.4f} in={human_in:.4f} "
                    f"ratio={human_out/human_in:.0f}x"
                )
                continue
            if best is None or r.amount_out > best.amount_out:
                best = r

        if best:
            logger.debug(
                f"[MultiDex] Best: {best.dex} "
                f"{token_in[:8]}→{token_out[:8]} "
                f"out={best.amount_out} fee={best.fee_tier}"
            )
        return best

    async def _quote_univ3(
        self, token_in: str, token_out: str, amount_in: int, fee: int,
    ) -> Optional[DexQuote]:
        try:
            result = await self._univ3_quoter.functions.quoteExactInputSingle({
                "tokenIn":           token_in,
                "tokenOut":          token_out,
                "amountIn":          amount_in,
                "fee":               fee,
                "sqrtPriceLimitX96": 0,
            }).call()
            return DexQuote(
                dex="univ3", router=UNIV3_ROUTER,
                amount_out=result[0], fee_tier=fee,
                token_in=token_in, token_out=token_out, amount_in=amount_in,
            )
        except Exception as e:
            logger.debug(f"[MultiDex] UniV3 fee={fee} failed: {e}")
            return None

    async def _quote_camelot(
        self, token_in: str, token_out: str, amount_in: int,
    ) -> Optional[DexQuote]:
        try:
            # Camelot Algebra quoter: no fee param, returns (amountOut, fee)
            result = await self._camelot_quoter.functions.quoteExactInputSingle(
                token_in, token_out, amount_in, 0,
            ).call()
            return DexQuote(
                dex="camelot", router=CAMELOT_ROUTER,
                amount_out=result[0], fee_tier=result[1],  # applied dynamic fee
                token_in=token_in, token_out=token_out, amount_in=amount_in,
            )
        except Exception as e:
            logger.debug(f"[MultiDex] Camelot failed: {e}")
            return None

    async def _quote_univ3_multihop(
        self, path_bytes: bytes, amount_in: int,
    ) -> Optional[DexQuote]:
        """Quote a multi-hop path via Uni V3 QuoterV2.quoteExactInput."""
        try:
            result = await self._univ3_quoter.functions.quoteExactInput(
                path_bytes, amount_in,
            ).call()
            # quoteExactInput returns just amountOut (uint256)
            amount_out = result if isinstance(result, int) else result[0]
            return DexQuote(
                dex="univ3_multihop", router=UNIV3_ROUTER,
                amount_out=amount_out, fee_tier=0,
                token_in="", token_out="", amount_in=amount_in,
                path=path_bytes,
            )
        except Exception as e:
            logger.debug(f"[MultiDex] UniV3 multihop failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Calldata encoding — differs per DEX
    # ------------------------------------------------------------------

    def encode_calldata(
        self,
        quote:           DexQuote,
        amount_out_min:  int,
        deadline_offset: int = 180,
    ) -> bytes:
        """Encode swap calldata for the winning DEX."""
        deadline = int(time.time()) + deadline_offset

        if quote.is_multihop:
            return self._encode_exact_input(
                quote.path, self._executor, deadline,
                quote.amount_in, amount_out_min,
            )
        elif quote.is_camelot:
            return self._encode_camelot(quote, amount_out_min, deadline)
        else:
            return self._encode_univ3(quote, amount_out_min, deadline)

    def _encode_univ3(
        self, quote: DexQuote, amount_out_min: int, deadline: int,
    ) -> bytes:
        """
        Uni V3 exactInputSingle:
        (tokenIn, tokenOut, fee, recipient, deadline,
         amountIn, amountOutMinimum, sqrtPriceLimitX96)
        """
        encoded = encode(
            ["(address,address,uint24,address,uint256,uint256,uint256,uint160)"],
            [(
                AsyncWeb3.to_checksum_address(quote.token_in),
                AsyncWeb3.to_checksum_address(quote.token_out),
                quote.fee_tier,
                self._executor,
                deadline,
                quote.amount_in,
                amount_out_min,
                0,
            )]
        )
        return UNIV3_EXACT_INPUT_SINGLE + encoded

    def _encode_camelot(
        self, quote: DexQuote, amount_out_min: int, deadline: int,
    ) -> bytes:
        """
        Camelot V3 (Algebra) exactInputSingle — NO fee field:
        (tokenIn, tokenOut, recipient, deadline,
         amountIn, amountOutMinimum, limitSqrtPrice)

        This is the critical difference from Uni V3 — Algebra pools use
        dynamic per-pool fees so there's no fee parameter in the struct.
        """
        encoded = encode(
            ["(address,address,address,uint256,uint256,uint256,uint160)"],
            [(
                AsyncWeb3.to_checksum_address(quote.token_in),
                AsyncWeb3.to_checksum_address(quote.token_out),
                self._executor,
                deadline,
                quote.amount_in,
                amount_out_min,
                0,  # limitSqrtPrice
            )]
        )
        return CAMELOT_EXACT_INPUT_SINGLE + encoded

    def _encode_exact_input(
        self,
        path_bytes:      bytes,
        recipient:       str,
        deadline:        int,
        amount_in:       int,
        amount_out_min:  int,
    ) -> bytes:
        """
        Uni V3 exactInput (multi-hop):
        (bytes path, address recipient, uint256 deadline,
         uint256 amountIn, uint256 amountOutMinimum)
        """
        encoded = encode(
            ["(bytes,address,uint256,uint256,uint256)"],
            [(
                path_bytes,
                AsyncWeb3.to_checksum_address(recipient),
                deadline,
                amount_in,
                amount_out_min,
            )]
        )
        return UNIV3_EXACT_INPUT + encoded


# ---------------------------------------------------------------------------
# Path builder — constructs multi-hop path bytes for Uni V3 exactInput
# ---------------------------------------------------------------------------

def _build_path(token_in: str, hops: list, token_out: str) -> bytes:
    """
    Build Uni V3 multi-hop path bytes.

    hops: [(fee, intermediate), (fee, intermediate), ...]
    Last hop's token is None → substituted with token_out.

    Path format: token(20) + fee(3) + token(20) + fee(3) + ... + token(20)

    Example:
        _build_path(WSTETH, [(100, WETH), (500, None)], USDC)
        → WSTETH|0x000064|WETH|0x0001f4|USDC = 66 bytes
    """
    parts = [bytes.fromhex(token_in[2:])]
    for fee, token in hops:
        parts.append(fee.to_bytes(3, "big"))
        tok = token_out if token is None else token
        parts.append(bytes.fromhex(tok[2:]))
    return b"".join(parts)


# ---------------------------------------------------------------------------
# Integration into SwapCalldataBuilder
# ---------------------------------------------------------------------------
#
# 1. On-chain (once) — approve Camelot router on the executor:
#
#    cast send $CONTRACT_ADDR \
#      "approveRouter(address)" 0x1F721E2E82F6676FCE4eA07A5958cF098D339e18 \
#      --rpc-url $QUICKNODE_HTTP_URL --private-key $PK
#
# 2. In flash_loan_route.py SwapCalldataBuilder.__init__():
#
#    from multi_dex_router import MultiDexRouter
#    self._multi_dex = MultiDexRouter(w3, executor_address)
#
# 3. In SwapCalldataBuilder.build(), REPLACE the Uni-V3-only live quote step
#    (the FEE_TIERS loop) with multi-DEX comparison:
#
#    # Existing upstream stays the same:
#    #   - Balancer wstETH special case
#    #   - QuoteCache hit check
#    #
#    # Replace the live QuoterV2 step with:
#
#    quote = await self._multi_dex.best_route(token_in, token_out, amount_in)
#    if quote is None:
#        logger.warning(f"[SwapBuilder] No route on any DEX: {token_in[:8]}→{token_out[:8]}")
#        return None
#
#    amount_out_min = int(quote.amount_out * (10_000 - slippage_bps) / 10_000)
#    calldata       = self._multi_dex.encode_calldata(quote, amount_out_min)
#
#    if quote.is_camelot:
#        logger.info(
#            f"[SwapBuilder] Camelot wins: {token_in[:8]}→{token_out[:8]} "
#            f"out={quote.amount_out} (vs Uni V3)"
#        )
#
#    return SwapRoute(
#        router       = quote.router,           # Camelot OR Uni V3 — executor accepts both
#        calldata     = calldata,
#        fee_tier     = quote.fee_tier,
#        amount_in    = quote.amount_in,
#        amount_out   = quote.amount_out,
#        slippage_pct = slippage_bps / 10_000,
#    )
#
# 4. Update QuoteCache to use MultiDexRouter too (optional — for pre-warm):
#    The cache pre-fetches quotes; point it at multi_dex.best_route() so
#    cached quotes also reflect the best DEX.
#
# ---------------------------------------------------------------------------
#
# Expected impact (from verified quotes):
#   WETH→USDC liquidations: tied (no change)
#   WBTC→WETH liquidations: +0.49% on collateral swap
#   On a 1 WBTC liquidation: ~0.005 WBTC extra (~$500 at current prices)
#
# The edge is modest per swap but:
#   - Costs nothing (same gas, just different router)
#   - Compounds across every liquidation
#   - Camelot never loses — worst case it ties Uni V3
#   - Adds resilience: if Uni V3 has no pool, Camelot may
#
# ---------------------------------------------------------------------------
#
# Next DEXs to add (same pattern, after Camelot proven):
#   - Sushiswap V3 (Arbitrum) — another Uni V3 fork, same ABI as Uni V3
#   - Ramses (Arbitrum) — Solidly fork, different ABI
#   - Uni V3 multi-hop (exactInput with path bytes) — for pairs with no direct pool
#
# Each is ~50 lines: add quoter, add encoder, add to best_route() gather.
# ---------------------------------------------------------------------------
