"""
flash_loan_route.py — Wire executeLiquidation() with Balancer flash loan + swap calldata
Fixes W8: pipeline was calling executeLiquidation() with wrong parameter count,
          missing swapRouter and swapCalldata arguments → every tx reverts.

Contract interface (IFlashExecutorV3):
    executeLiquidation(
        address collateralAsset,
        address debtAsset,
        address borrower,
        uint256 debtToCover,
        bool    receiveAToken,
        address swapRouter,        ← was missing
        bytes   swapCalldata       ← was missing
    )

Flow:
    1. Pipeline calls executeLiquidation() with encoded swap route
    2. Contract calls Balancer vault for flash loan of debtAsset
    3. Contract calls Aave liquidationCall() with flash-loaned funds
    4. Contract receives collateralAsset
    5. Contract executes swap: collateral → debt token (to repay flash loan)
    6. Contract repays Balancer (0% fee)
    7. Contract keeps profit (collateral - debt - gas)

This module:
    SwapCalldataBuilder  — builds swapCalldata for Uni V3 exactInputSingle
    FlashLoanTxBuilder   — assembles the full executeLiquidation() tx
    RouteValidator       — pre-validates swap route has liquidity before submitting

Usage:
    builder = FlashLoanTxBuilder(
        rpc       = rpc_client,
        executor  = EXECUTOR_ADDRESS,
        wallet    = WALLET_ADDR,
        private_key = PRIVATE_KEY,
    )

    tx_data = await builder.build(
        collateral_asset = "0x...",
        debt_asset       = "0x...",
        borrower         = "0x...",
        debt_to_cover    = 1000_000_000,   # raw uint256
        shared_state     = self.shared_state,
        nonce            = await nonce_mgr.next(),
    )

    if tx_data:
        tx_hash = await blast_submit(tx_data.raw_tx)
"""

import asyncio
import logging
from dataclasses import dataclass
from typing import Optional

from web3 import AsyncWeb3, Web3
from eth_abi import encode
from wsteth_fix    import BalancerSwapRoute, WSTETH_ADDR
from multi_dex_router import MultiDexRouter, DexQuote

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Addresses — Arbitrum mainnet
# ---------------------------------------------------------------------------

BALANCER_VAULT    = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"
UNI_V3_ROUTER     = "0xE592427A0AEce92De3Edee1F18E0157C05861564"  # SwapRouter01
UNI_V3_ROUTER02   = "0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45"  # SwapRouter02
EXECUTOR_ADDR     = "0x83d60B7DE4334Fd34492E18cA95B2b9e47F00D80"

# Common Arbitrum token addresses — for WETH wrapping on swap path
WETH_ARBITRUM     = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC_NATIVE       = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
USDC_BRIDGED      = "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8"

# Fee tiers to try in order (cheapest first)
FEE_TIERS = [3000, 10000]  # 0.3%, 1% — 0.05% pool returns garbage quotes for WBTC pairs

# ---------------------------------------------------------------------------
# ABIs
# ---------------------------------------------------------------------------

EXECUTOR_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "collateralAsset", "type": "address"},
            {"internalType": "address", "name": "debtAsset",       "type": "address"},
            {"internalType": "address", "name": "borrower",        "type": "address"},
            {"internalType": "uint256", "name": "debtToCover",     "type": "uint256"},
            {"internalType": "bool",    "name": "receiveAToken",   "type": "bool"},
            {"internalType": "address", "name": "swapRouter",      "type": "address"},
            {"internalType": "bytes",   "name": "swapCalldata",    "type": "bytes"},
        ],
        "name": "executeLiquidation",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address", "name": "collateralAsset", "type": "address"},
            {"internalType": "address", "name": "debtAsset",       "type": "address"},
            {"internalType": "address", "name": "borrower",        "type": "address"},
            {"internalType": "uint256", "name": "debtToCover",     "type": "uint256"},
            {"internalType": "bool",    "name": "receiveAToken",   "type": "bool"},
            {"internalType": "address", "name": "swapRouter",      "type": "address"},
            {"internalType": "bytes",   "name": "swapCalldata",    "type": "bytes"},
        ],
        "name": "executeLiquidationViaAave",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address", "name": "collateralAsset", "type": "address"},
            {"internalType": "address", "name": "debtAsset",       "type": "address"},
            {"internalType": "address", "name": "borrower",        "type": "address"},
            {"internalType": "uint256", "name": "debtToCover",     "type": "uint256"},
            {"internalType": "bool",    "name": "receiveAToken",   "type": "bool"},
            {"internalType": "address", "name": "swapRouter",      "type": "address"},
            {"internalType": "bytes",   "name": "swapCalldata",    "type": "bytes"},
        ],
        "name": "executeLiquidationDirect",
        "outputs": [{"internalType": "uint256", "name": "profit", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address", "name": "debtAsset", "type": "address"},
            {
                "name": "items",
                "type": "tuple[]",
                "internalType": "struct BatchItem[]",
                "components": [
                    {"name": "collateralAsset", "type": "address", "internalType": "address"},
                    {"name": "borrower",        "type": "address", "internalType": "address"},
                    {"name": "debtToCover",     "type": "uint256", "internalType": "uint256"},
                    {"name": "receiveAToken",   "type": "bool",    "internalType": "bool"},
                    {"name": "swapRouter",      "type": "address", "internalType": "address"},
                    {"name": "swapCalldata",    "type": "bytes",   "internalType": "bytes"},
                ],
            },
        ],
        "name": "executeLiquidationBatch",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "BALANCER_VAULT",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "paused",
        "outputs": [{"internalType": "bool", "name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "address", "name": "router", "type": "address"}],
        "name": "approvedRouters",
        "outputs": [{"internalType": "bool", "name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    },
]

QUOTER_V2_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"internalType": "address", "name": "tokenIn",           "type": "address"},
                    {"internalType": "address", "name": "tokenOut",          "type": "address"},
                    {"internalType": "uint256", "name": "amountIn",          "type": "uint256"},
                    {"internalType": "uint24",  "name": "fee",               "type": "uint24"},
                    {"internalType": "uint160", "name": "sqrtPriceLimitX96", "type": "uint160"},
                ],
                "internalType": "struct IQuoterV2.QuoteExactInputSingleParams",
                "name": "params",
                "type": "tuple",
            }
        ],
        "name": "quoteExactInputSingle",
        "outputs": [
            {"internalType": "uint256", "name": "amountOut",               "type": "uint256"},
            {"internalType": "uint160", "name": "sqrtPriceX96After",       "type": "uint160"},
            {"internalType": "uint32",  "name": "initializedTicksCrossed", "type": "uint32"},
            {"internalType": "uint256", "name": "gasEstimate",             "type": "uint256"},
        ],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

QUOTER_V2_ADDR = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"

# P5: Per-asset slippage constants
_MIN_PROFIT_MARGIN_PCT = 0.005   # 0.5% min net profit after swap cost
_ABSOLUTE_MAX_SLIPPAGE = 0.10    # 10% hard ceiling — above this the oracle quote is corrupt


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class SwapRoute:
    router:       str     # approved router address
    calldata:     bytes   # encoded swap calldata
    fee_tier:     int     # best fee tier found
    amount_in:    int     # collateral amount in
    amount_out:   int     # expected debt token out
    slippage_pct: float   # implied slippage


@dataclass
class FlashLoanTxData:
    raw_tx:          bytes   # signed raw transaction — pass to blast_submit
    collateral_asset:str
    debt_asset:      str
    borrower:        str
    debt_to_cover:   int
    swap_route:      SwapRoute
    gas_limit:       int
    gas_price:       int
    nonce:           int
    estimated_profit_usd: float
    flash_source:    str = 'balancer'  # 'balancer' or 'aave'


# ---------------------------------------------------------------------------
# SwapCalldataBuilder — encodes Uni V3 exactInputSingle calldata
# ---------------------------------------------------------------------------

class SwapCalldataBuilder:
    """
    Builds swapCalldata for the executeLiquidation() call.

    The contract receives collateralAsset after liquidating and needs to
    swap it back to debtAsset to repay the Balancer flash loan.
    We encode exactInputSingle calldata for the best Uni V3 fee tier.

    Uni V3 SwapRouter exactInputSingle selector: 0x414bf389
    Params struct:
        address tokenIn
        address tokenOut
        uint24  fee
        address recipient      ← executor contract address
        uint256 deadline
        uint256 amountIn
        uint256 amountOutMinimum
        uint160 sqrtPriceLimitX96
    """

    # exactInputSingle function selector
    EXACT_INPUT_SINGLE_SELECTOR = bytes.fromhex("414bf389")

    # Token decimals for sanity-check normalization
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

    def __init__(self, w3: AsyncWeb3, executor_address: str, quoter=None, quote_cache=None):
        """
        Args:
            w3:             AsyncWeb3 instance
            executor_address: FlashExecutorV3 address
            quoter:         Optional QuoterAsync instance (from async_web3.py).
                            If provided, reuses shared quoter instead of creating
                            a new contract instance. Recommended — avoids duplicate
                            connections and enables shared quote caching later.
            quote_cache:    Optional QuoteCache instance. If provided, checks cache
                            before calling QuoterV2. ~0ms hit, QuoterV2 on miss.
        """
        self._w3        = w3
        self._executor  = AsyncWeb3.to_checksum_address(executor_address)
        self._quoter_async = quoter  # shared QuoterAsync if provided
        self._quote_cache  = quote_cache  # QuoteCache for cross-asset pairs
        self._multi_dex    = MultiDexRouter(w3, executor_address)  # Camelot + Uni V3

        # Balancer wstETH/WETH swap route (better liquidity than Uni V3)
        self._balancer_route = BalancerSwapRoute(w3, executor_address)

        # Only create raw contract quoter if no shared instance provided
        self._quoter    = None if quoter else w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(QUOTER_V2_ADDR),
            abi=QUOTER_V2_ABI,
        )

    async def build(
        self,
        token_in:    str,   # collateral asset (received from liquidation)
        token_out:   str,   # debt asset (needed to repay flash loan)
        amount_in:   int,   # collateral amount to swap
        slippage_bps:int = 50,  # 50bps = 0.5% slippage tolerance
        deadline_offset: int = 180,  # seconds from now
    ) -> Optional[SwapRoute]:
        """
        Find best fee tier via QuoterV2, encode exactInputSingle calldata.
        Returns SwapRoute or None if no liquid pool found.
        """
        token_in  = AsyncWeb3.to_checksum_address(token_in)
        token_out = AsyncWeb3.to_checksum_address(token_out)

        # ── wstETH → WETH: route through Balancer pool (better liquidity) ──
        # Only when output IS WETH — otherwise fall through to multi-DEX
        # which has the multi-hop wstETH→WETH→USDC path for non-WETH debt.
        if token_in.lower() == WSTETH_ADDR.lower() and token_out.lower() == WETH_ARBITRUM.lower():
            result = await self._balancer_route.build(amount_in, slippage_bps)
            if result:
                calldata, weth_out = result
                slippage = slippage_bps / 10_000
                logger.info(
                    f"[SwapBuilder] wstETH→WETH via Balancer: "
                    f"{amount_in/1e18:.4f} → {weth_out/1e18:.4f} WETH"
                )
                return SwapRoute(
                    router      = BALANCER_VAULT,
                    calldata    = calldata,
                    fee_tier    = 0,         # Balancer, not Uni V3
                    amount_in   = amount_in,
                    amount_out  = weth_out,
                    slippage_pct= slippage,
                )
            logger.warning("[SwapBuilder] Balancer route failed — falling through to Uni V3")
            # Fall through to existing Uni V3 fee tier logic as backup

        # ── Check quote cache before hitting QuoterV2 ─────────────────
        if self._quote_cache:
            cached = self._quote_cache.get(token_in, token_out, amount_in)
            if cached:
                amount_out, best_fee = cached
                amount_out_min = int(amount_out * (10_000 - slippage_bps) / 10_000)
                import time as _t
                deadline  = int(_t.time()) + deadline_offset
                calldata  = self._encode_exact_input_single(
                    token_in       = token_in,
                    token_out      = token_out,
                    fee            = best_fee,
                    recipient      = self._executor,
                    deadline       = deadline,
                    amount_in      = amount_in,
                    amount_out_min = amount_out_min,
                )
                slippage_pct = max(0.0, (amount_in - amount_out) / amount_in) if amount_in else 0.0
                logger.info(
                    f"[SwapBuilder] CACHE HIT: {token_in[:10]}→{token_out[:10]} "
                    f"fee={best_fee} out={amount_out}"
                )
                return SwapRoute(
                    router      = UNI_V3_ROUTER,
                    calldata    = calldata,
                    fee_tier    = best_fee,
                    amount_in   = amount_in,
                    amount_out  = amount_out,
                    slippage_pct= slippage_pct,
                )
            # Cache miss — log pair so we know what to add
            logger.info(
                f"[SwapBuilder] CACHE MISS: {token_in[:10]}→{token_out[:10]} "
                f"amount={amount_in} — falling through to QuoterV2"
            )
            # Fall through to live QuoterV2

        # ── Multi-DEX routing: quote Camelot V3 + Uni V3 in parallel ────
        # Returns DexQuote with .dex, .router, .amount_out, .fee_tier, etc.
        # Falls back to legacy Uni V3 if self._multi_dex is None or no route found
        if self._multi_dex is not None:
            quote = await self._multi_dex.best_route(
                token_in  = token_in,
                token_out = token_out,
                amount_in = amount_in,
            )
            if quote is not None:
                amount_out_min = int(quote.amount_out * (10_000 - slippage_bps) / 10_000)
                calldata = self._multi_dex.encode_calldata(
                    quote, amount_out_min, deadline_offset,
                )
                slippage_pct = max(0.0, (amount_in - quote.amount_out) / amount_in) if amount_in else 0.0

                logger.info(
                    f"[SwapBuilder] {quote.dex} wins: {token_in[:10]}→{token_out[:10]} "
                    f"out={quote.amount_out} fee={quote.fee_tier}"
                )
                return SwapRoute(
                    router       = quote.router,
                    calldata     = calldata,
                    fee_tier     = quote.fee_tier,
                    amount_in    = quote.amount_in,
                    amount_out   = quote.amount_out,
                    slippage_pct = slippage_pct,
                )
            # Multi-DEX returned None — fall through to legacy Uni V3 logic
            logger.warning(
                f"[SwapBuilder] Multi-DEX found no route for "
                f"{token_in[:10]}→{token_out[:10]} — falling back to Uni V3"
            )

        # Legacy Uni V3 fallback: quote fee tiers concurrently
        tasks = [
            asyncio.create_task(
                self._quote_fee_tier(token_in, token_out, amount_in, fee),
                name=f"quote_{fee}",
            )
            for fee in FEE_TIERS
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)
        best_out  = 0
        best_fee  = FEE_TIERS[0]

        for fee, result in zip(FEE_TIERS, results):
            if isinstance(result, Exception):
                logger.debug(f"[SwapBuilder] fee={fee} quote failed: {result}")
                continue
            amount_out, fee_used = result
            if amount_out > best_out:
                best_out = amount_out
                best_fee = fee_used

        if best_out == 0:
            logger.warning(
                f"[SwapBuilder] No liquid pool found for "
                f"{token_in[:10]}→{token_out[:10]}"
            )
            return None

        # Sanity check: reject quotes where output vastly exceeds input.
        in_dec  = self.TOKEN_DECIMALS.get(token_in, 18)
        out_dec = self.TOKEN_DECIMALS.get(token_out, 18)
        human_out = best_out / 10**out_dec
        human_in  = amount_in / 10**in_dec
        if human_in > 0 and human_out / human_in > 100_000:
            logger.warning(
                f"[SwapBuilder] Implausible quote rejected: "
                f"{token_in[:10]}→{token_out[:10]} "
                f"out={best_out} in={amount_in} ratio={best_out/amount_in:.0f}x"
            )
            return None

        # Apply slippage tolerance
        amount_out_min = int(best_out * (10_000 - slippage_bps) / 10_000)

        # Implied slippage vs no-slippage ideal
        slippage_pct = max(0.0, (amount_in - best_out) / amount_in) if amount_in else 0.0

        # Encode exactInputSingle calldata
        import time as _time
        deadline  = int(_time.time()) + deadline_offset
        calldata  = self._encode_exact_input_single(
            token_in       = token_in,
            token_out      = token_out,
            fee            = best_fee,
            recipient      = self._executor,
            deadline       = deadline,
            amount_in      = amount_in,
            amount_out_min = amount_out_min,
        )

        logger.debug(
            f"[SwapBuilder] Route: {token_in[:10]}→{token_out[:10]} "
            f"fee={best_fee} amountIn={amount_in} "
            f"amountOutMin={amount_out_min} slippage={slippage_pct:.2%}"
        )

        return SwapRoute(
            router      = UNI_V3_ROUTER,
            calldata    = calldata,
            fee_tier    = best_fee,
            amount_in   = amount_in,
            amount_out  = best_out,
            slippage_pct= slippage_pct,
        )

    async def _quote_fee_tier(
        self,
        token_in:  str,
        token_out: str,
        amount_in: int,
        fee:       int,
    ) -> tuple[int, int]:
        """
        Returns (amountOut, fee) or raises on failure.
        Uses shared QuoterAsync if available, else raw contract call.
        """
        if self._quoter_async is not None:
            # Use shared QuoterAsync — single fee tier call
            result = await self._quoter_async._quote_fee_tier(
                token_in, token_out, amount_in, fee
            )
            return result  # already (amountOut, fee)
        else:
            # Fallback: raw contract call
            result = await self._quoter.functions.quoteExactInputSingle({
                "tokenIn":           token_in,
                "tokenOut":          token_out,
                "amountIn":          amount_in,
                "fee":               fee,
                "sqrtPriceLimitX96": 0,
            }).call()
            return result[0], fee  # amountOut, fee

    def _encode_exact_input_single(
        self,
        token_in:       str,
        token_out:      str,
        fee:            int,
        recipient:      str,
        deadline:       int,
        amount_in:      int,
        amount_out_min: int,
    ) -> bytes:
        """
        ABI-encode exactInputSingle params.
        Layout: selector (4 bytes) + ABI-encoded tuple (256 bytes)
        """
        encoded_params = encode(
            ["(address,address,uint24,address,uint256,uint256,uint256,uint160)"],
            [(
                AsyncWeb3.to_checksum_address(token_in),
                AsyncWeb3.to_checksum_address(token_out),
                fee,
                AsyncWeb3.to_checksum_address(recipient),
                deadline,
                amount_in,
                amount_out_min,
                0,  # sqrtPriceLimitX96 = 0 (no limit)
            )]
        )
        return self.EXACT_INPUT_SINGLE_SELECTOR + encoded_params


# ---------------------------------------------------------------------------
# RouteValidator — pre-flight checks before building tx
# ---------------------------------------------------------------------------

class RouteValidator:
    """
    Pre-submission validation for swap routes. Caches paused() and
    approvedRouters() results — these change extremely rarely (paused
    only on contract upgrade, approvedRouters is static at deployment).
    Without caching, each cross-asset pre-warm build pays 200ms for
    these two calls, pushing total build time over the 350ms deadline.
    """
    def __init__(self, w3: AsyncWeb3, executor_address: str):
        self._executor = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(executor_address),
            abi=EXECUTOR_ABI,
        )
        self._w3 = w3
        self._paused: Optional[bool] = None
        self._paused_ts: float = 0.0
        self._approved: dict[str, bool] = {}

    async def validate(
        self,
        swap_router: str,
        collateral_asset: str,
        debt_asset: str,
        swap_route: SwapRoute,
        max_slippage_pct: float = 0.02,   # fallback only — overridden by bonus below
        liquidation_bonus_bps: int = 0,   # per-reserve bonus; 0 → use max_slippage_pct
    ) -> tuple[bool, str]:
        """
        Pre-submission validation.
        Returns (ok, reason).
        """
        # Check contract not paused (cached — changes only on upgrade)
        import time as _vt
        if self._paused is None or (_vt.time() - self._paused_ts) > 300:
            try:
                self._paused = await self._executor.functions.paused().call()
                self._paused_ts = _vt.time()
            except Exception as e:
                return False, f"paused() call failed: {e}"
        if self._paused:
            return False, "executor contract is paused"

        # Check router is approved (cached — approvedRouters is static)
        swap_router_key = swap_router.lower()
        if swap_router_key not in self._approved:
            try:
                self._approved[swap_router_key] = await self._executor.functions.approvedRouters(
                    AsyncWeb3.to_checksum_address(swap_router)
                ).call()
            except Exception as e:
                return False, f"approvedRouters() call failed: {e}"
        if not self._approved[swap_router_key]:
            return False, f"router {swap_router[:10]}… not approved in executor"

        # P5: Per-asset slippage cap = min(bonus_pct - min_profit_margin, ABSOLUTE_MAX).
        # The 2% global cap blocks wstETH (7% bonus, ~5.56% real swap cost = 1.44% net).
        # For wstETH: min(0.07 - 0.005, 0.10) = 6.5% — allows the liquidation through.
        # For WETH  : min(0.05 - 0.005, 0.10) = 4.5%   For USDC: same = 4.5%.
        # This is consistent with the oracle-normalised slippage already computed in
        # FlashLoanTxBuilder.build (lines ~794-816) — both use the same slippage_pct field.
        if liquidation_bonus_bps > 0:
            bonus_pct             = liquidation_bonus_bps / 10_000 - 1.0
            effective_max_slippage = min(
                max(bonus_pct - _MIN_PROFIT_MARGIN_PCT, 0.0),
                _ABSOLUTE_MAX_SLIPPAGE,
            )
        else:
            effective_max_slippage = max_slippage_pct

        # Check slippage acceptable
        if swap_route.slippage_pct > effective_max_slippage:
            return False, (
                f"slippage {swap_route.slippage_pct:.2%} > "
                f"per-asset max {effective_max_slippage:.2%} "
                f"(bonus={liquidation_bonus_bps}bps, margin={_MIN_PROFIT_MARGIN_PCT:.1%})"
            )

        # Check swap produces enough to cover flash loan
        # amount_out must be >= debt_to_cover (repay flash loan)
        # A small buffer handles fee rounding
        if swap_route.amount_out == 0:
            return False, "swap quote returned 0 amountOut"

        return True, "ok"


# ---------------------------------------------------------------------------
# FlashLoanTxBuilder — assembles the full executeLiquidation() tx
# ---------------------------------------------------------------------------

class FlashLoanTxBuilder:
    """
    Builds signed executeLiquidation() transactions with correct
    swapRouter and swapCalldata arguments.

    Replaces the broken _build_and_submit() path in pipeline_v3.py
    that was calling executeLiquidation() without swap arguments.

    Usage:
        builder = FlashLoanTxBuilder(rpc, EXECUTOR_ADDR, WALLET_ADDR, PRIVATE_KEY)
        tx_data = await builder.build(
            collateral_asset, debt_asset, borrower,
            debt_to_cover, shared_state, nonce
        )
        if tx_data:
            tx_hash = await blast_submit(tx_data.raw_tx)
    """

    GAS_LIMIT_FLASH  = 700_000   # flash loan path — more complex than direct
    GAS_LIMIT_DIRECT = 400_000   # direct path fallback
    GAS_LIMIT_BATCH  = 2_500_000 # batch: ~325k/item measured + flash overhead + headroom
    GAS_PREMIUM      = 2.0       # maxFeePerGas = base_fee * GAS_PREMIUM (EIP-1559)

    def __init__(
        self,
        rpc,                    # AsyncRPCClient
        executor_address: str,
        wallet_address:   str,
        private_key:      str,
        slippage_bps:     int = 50,    # 0.5% default swap slippage
        shared_state=None,             # SharedState — zero-RPC gas price reads
        quoter=None,                   # QuoterAsync — shared across Aave + Compound
        quote_cache=None,              # QuoteCache — pre-fetched swap quotes
        gas_oracle=None,               # GasOracle — trailing percentile gas pricing
    ):
        self._rpc           = rpc
        self._executor_addr = AsyncWeb3.to_checksum_address(executor_address)
        self._wallet        = AsyncWeb3.to_checksum_address(wallet_address)
        self._pk            = private_key
        self._slippage      = slippage_bps
        self._shared_state  = shared_state  # used by rebuild_with_nonce()
        self._gas_oracle    = gas_oracle    # trailing percentile — replaces static GAS_PREMIUM

        self._executor   = rpc.w3.eth.contract(
            address=self._executor_addr,
            abi=EXECUTOR_ABI,
        )
        self._swap_builder = SwapCalldataBuilder(rpc.w3, executor_address, quoter=quoter, quote_cache=quote_cache)
        self._validator    = RouteValidator(rpc.w3, executor_address)

        # Sync Web3 for signing + ABI encoding (local only — no RPC)
        self._sync_w3 = Web3()
        self._sync_executor = self._sync_w3.eth.contract(
            address=self._executor_addr,
            abi=EXECUTOR_ABI,
        )

        # Flash source cache: {token_lower: (source_str, expiry_monotonic)}
        # Balancer vault balances shift slowly; 30s TTL eliminates the per-build eth_call.
        self._flash_source_cache: dict[str, tuple[str, float]] = {}

    async def _erc20_balance(self, token: str, holder: str) -> int:
        """Read ERC-20 balanceOf via async eth_call."""
        token  = AsyncWeb3.to_checksum_address(token)
        holder = AsyncWeb3.to_checksum_address(holder)
        addr_padded = holder[2:].lower().rjust(64, '0')
        data = '0x70a08231' + addr_padded
        try:
            result = await self._rpc.w3.eth.call({'to': token, 'data': data})
            return int(result.hex(), 16) if result and result != b'' else 0
        except Exception:
            return 0

    async def choose_flash_source(self, flash_token: str, amount: int) -> str:
        """
        Decide which flash loan source to use based on available liquidity.
        Returns 'balancer' (0% fee, preferred) or 'aave' (0.09% fee, deep liquidity).
        Cache TTL=30s — Balancer vault balances shift slowly and this eliminates
        one eth_call from the hot path on every cache-hit rebuild.
        """
        import time as _time
        key = flash_token.lower()
        cached = self._flash_source_cache.get(key)
        if cached and _time.monotonic() < cached[1]:
            return cached[0]

        balancer_balance = await self._erc20_balance(flash_token, BALANCER_VAULT)

        if balancer_balance >= amount:
            result = 'balancer'
            # Cache for 30s — vault balance is stable unless a large swap drains it
            self._flash_source_cache[key] = (result, _time.monotonic() + 30.0)
            return result

        logger.info(
            f"[FlashSource] Balancer short for {flash_token[:10]}…: "
            f"has {balancer_balance}, need {amount} → routing to Aave"
        )
        return 'aave'            # 0.09% fee (9 bps) — deep liquidity for pool reserves

    async def build(
        self,
        collateral_asset: str,
        debt_asset:       str,
        borrower:         str,
        debt_to_cover:    int,
        shared_state,               # SharedState from hot_path_fix.py
        nonce:            int,
        collateral_amount:int = 0,  # if 0, uses debt_to_cover as proxy
        asset_prices_usd: dict = None,
        asset_decimals:   dict = None,
        liquidation_bonus_bps: int = 10500,  # per-reserve bonus from collateral_selector
    ) -> Optional[FlashLoanTxData]:
        """
        Build a signed executeLiquidation() tx with flash loan + swap route.

        Returns FlashLoanTxData on success, None on any validation failure.
        Caller should fall back to executeLiquidationDirect() if None returned.
        """
        collateral_asset = AsyncWeb3.to_checksum_address(collateral_asset)
        debt_asset       = AsyncWeb3.to_checksum_address(debt_asset)
        borrower         = AsyncWeb3.to_checksum_address(borrower)

        # Store per-reserve bonus for _estimate_profit
        self._liquidation_bonus_bps = liquidation_bonus_bps

        # ── Same-asset fast path: no swap needed ──────────────────
        # When collateral == debt, the executor repays flash loan directly
        # from received collateral. swapRouter=address(0), calldata=0x.
        if collateral_asset.lower() == debt_asset.lower():
            logger.debug(
                f"[FlashLoanBuilder] Same-asset {collateral_asset[:10]}… "
                f"— skipping swap, using address(0) router"
            )
            swap_route = SwapRoute(
                router       = "0x0000000000000000000000000000000000000000",
                calldata     = b"",
                fee_tier     = 0,
                amount_in    = debt_to_cover,
                amount_out   = debt_to_cover,  # 1:1 since same asset
                slippage_pct = 0.0,
            )
        else:
            # Amount to swap: collateral received ≈ debt_to_cover × (1 + liq_bonus)
            # Use collateral_amount if provided, else estimate from debt_to_cover
            # with a 5% bonus assumption as conservative floor
            if collateral_amount > 0:
                amount_in = collateral_amount
            else:
                amount_in = int(debt_to_cover * 1.05)

            # Build swap route: collateral → debt token
            swap_route = await self._swap_builder.build(
                token_in    = collateral_asset,
                token_out   = debt_asset,
                amount_in   = amount_in,
                slippage_bps= self._slippage,
            )

            if swap_route is None:
                logger.warning(
                    f"[FlashLoanBuilder] No swap route for "
                    f"{collateral_asset[:10]}→{debt_asset[:10]} — "
                    f"cannot build flash loan tx"
                )
                return None

        # ── Recompute slippage in oracle-price terms before validation ──────────
        # SwapCalldataBuilder computes slippage as (amount_in - amount_out)/amount_in
        # in raw token units, which is meaningless cross-asset (e.g. WETH→USDC always
        # reads ~100% because 1e18 >> 3000e6). Override here using oracle prices so
        # RouteValidator's 2% gate sees actual DEX market-impact slippage.
        if (
            swap_route is not None
            and swap_route.router != "0x0000000000000000000000000000000000000000"
            and asset_prices_usd
            and asset_decimals
            and swap_route.amount_in > 0
        ):
            col_key   = collateral_asset.lower()
            debt_key  = debt_asset.lower()
            col_price = next((p for k, p in asset_prices_usd.items() if k.lower() == col_key), 0)
            dbt_price = next((p for k, p in asset_prices_usd.items() if k.lower() == debt_key), 0)
            col_dec   = asset_decimals.get(collateral_asset, asset_decimals.get(col_key, 18))
            dbt_dec   = asset_decimals.get(debt_asset,       asset_decimals.get(debt_key, 18))
            if col_price > 0 and dbt_price > 0:
                oracle_out = swap_route.amount_in * col_price * (10 ** dbt_dec) // (dbt_price * (10 ** col_dec))
                if oracle_out > 0:
                    swap_route.slippage_pct = max(0.0, (oracle_out - swap_route.amount_out) / oracle_out)
                    logger.debug(
                        f"[FlashLoanBuilder] Oracle-normalised slippage for "
                        f"{collateral_asset[:10]}→{debt_asset[:10]}: "
                        f"{swap_route.slippage_pct:.3%} "
                        f"(oracle_out={oracle_out} actual={swap_route.amount_out})"
                    )

        # ── Flash loan liquidity check (ALL paths — Balancer vault must hold enough) ──
        flash_source = await self.choose_flash_source(debt_asset, debt_to_cover)
        self._flash_source = flash_source  # used in _estimate_profit for fee adjustment

        # Validate before building (skip for same-asset — no router needed)
        if swap_route.router != "0x0000000000000000000000000000000000000000":
            ok, reason = await self._validator.validate(
                swap_router           = swap_route.router,
                collateral_asset      = collateral_asset,
                debt_asset            = debt_asset,
                swap_route            = swap_route,
                liquidation_bonus_bps = liquidation_bonus_bps,  # P5: per-asset cap
            )
            if not ok:
                logger.warning(f"[FlashLoanBuilder] Validation failed: {reason}")
                return None

        # Get gas price — oracle (trailing percentile) or static fallback
        if self._gas_oracle:
            rec = self._gas_oracle.recommend()
            max_fee      = rec.max_fee_per_gas
            priority_fee = rec.max_priority_fee_per_gas
        else:
            base_fee     = shared_state.base_fee_wei or 100_000_000
            max_fee      = int(base_fee * self.GAS_PREMIUM * 2)
            priority_fee = max(int(base_fee * 0.5), 1_000_000)  # min 0.001 gwei tip

        # Build transaction — select contract function based on flash source
        try:
            if flash_source == 'aave':
                contract_fn = self._executor.functions.executeLiquidationViaAave
                logger.debug(f"[FlashLoanBuilder] Using Aave flash loan for {debt_asset[:10]}…")
            else:
                contract_fn = self._executor.functions.executeLiquidation

            tx_dict = await contract_fn(
                collateral_asset,
                debt_asset,
                borrower,
                debt_to_cover,
                False,              # receiveAToken = False (receive underlying)
                swap_route.router,
                swap_route.calldata,
            ).build_transaction({
                "from":                  self._wallet,
                "nonce":                 nonce,
                "gas":                   self.GAS_LIMIT_FLASH,
                "maxFeePerGas":          max_fee,
                "maxPriorityFeePerGas":  priority_fee,
                "chainId":               42161,  # Arbitrum One
            })

            # Sign locally (no RPC)
            signed = self._sync_w3.eth.account.sign_transaction(tx_dict, self._pk)
            raw_tx = signed.raw_transaction

        except Exception as e:
            logger.error(f"[FlashLoanBuilder] TX build failed: {e}")
            return None

        # Estimate profit for telemetry — subtract Aave fee if applicable
        estimated_profit_usd = self._estimate_profit(
            swap_route, debt_asset, debt_to_cover, max_fee, asset_prices_usd, asset_decimals
        )
        if flash_source == 'aave':
            # Aave V3 flashLoanSimple fee: 0.09% (9 bps) — deducted from profit estimate
            dec = asset_decimals.get(debt_asset, 18) if asset_decimals else 18
            debt_amount = debt_to_cover / (10 ** dec)
            debt_price_usd = 1.0  # default: stablecoin
            if asset_prices_usd:
                debt_key = debt_asset.lower()
                debt_price_usd = next(
                    (p / 1e8 for k, p in asset_prices_usd.items() if k.lower() == debt_key),
                    1.0,
                )
            aave_fee_usd = debt_amount * debt_price_usd * 0.0009
            estimated_profit_usd = max(0, estimated_profit_usd - aave_fee_usd)

        logger.info(
            f"[FlashLoanBuilder] Built flash loan tx — "
            f"borrower={borrower[:10]}… "
            f"collateral={collateral_asset[:10]}… "
            f"debt={debt_asset[:10]}… "
            f"debtToCover={debt_to_cover} "
            f"fee_tier={swap_route.fee_tier} "
            f"slippage={swap_route.slippage_pct:.2%} "
            f"source={flash_source} "
            f"est_profit=${estimated_profit_usd:.2f}"
        )

        return FlashLoanTxData(
            raw_tx           = raw_tx,
            collateral_asset = collateral_asset,
            debt_asset       = debt_asset,
            borrower         = borrower,
            debt_to_cover    = debt_to_cover,
            swap_route       = swap_route,
            gas_limit        = self.GAS_LIMIT_FLASH,
            gas_price        = max_fee,
            nonce            = nonce,
            estimated_profit_usd = estimated_profit_usd,
            flash_source     = flash_source,
        )

    def _estimate_profit(
        self,
        swap_route:       SwapRoute,
        debt_asset:       str,        # token we're repaying — match against price keys
        debt_to_cover:    int,
        gas_price:        int,
        asset_prices_usd: Optional[dict],
        asset_decimals:   Optional[dict],
    ) -> float:
        """
        Rough profit estimate for telemetry and pre-flight gate.
        profit ≈ collateral_received_usd - debt_repaid_usd - gas_usd
        """
        try:
            gas_cost_eth = (self.GAS_LIMIT_FLASH * gas_price) / 1e18
            # Rough ETH price — use from prices if available
            eth_price = 3500.0
            if asset_prices_usd and WETH_ARBITRUM in asset_prices_usd:
                eth_price = asset_prices_usd[WETH_ARBITRUM] / 1e8
            gas_cost_usd = gas_cost_eth * eth_price

            # Swap produces amount_out of debt token — we keep the surplus
            # Convert to USD using debt asset price if available
            if swap_route.fee_tier == 0:
                # Same-asset liquidation — profit is the liquidation bonus on collateral
                bonus_pct = (self._liquidation_bonus_bps / 10_000 - 1.0) if hasattr(self, '_liquidation_bonus_bps') else 0.05
                surplus   = int(debt_to_cover * bonus_pct)
            else:
                surplus = max(0, swap_route.amount_out - debt_to_cover)
            if asset_prices_usd and asset_decimals:
                # Match debt_asset, NOT swap_route.router (which is SwapRouter addr)
                debt_key = debt_asset.lower()
                debt_price = next(
                    (p / 1e8 for k, p in asset_prices_usd.items()
                     if k.lower() == debt_key), 1.0
                )
                dec = asset_decimals.get(debt_asset, 6)
                surplus_usd = (surplus / 10 ** dec) * debt_price
            else:
                surplus_usd = surplus / 1e6  # assume USDC-like 6 decimals

            return max(0.0, surplus_usd - gas_cost_usd)

        except Exception:
            return 0.0

    def rebuild_with_nonce(
        self,
        cached: "FlashLoanTxData",
        nonce: int,
    ) -> Optional["FlashLoanTxData"]:
        """
        Re-sign a cached FlashLoanTxData with a fresh nonce.
        Fully synchronous — zero RPC calls, zero asyncio overhead, ~0.5ms total.

        Uses sync ABI encoding via self._sync_executor.encodeABI() to bypass
        the AsyncContract.build_transaction() async machinery entirely.
        """
        try:
            # Gas price — from oracle or shared_state (both in-memory, no RPC)
            if self._gas_oracle:
                rec = self._gas_oracle.recommend()
                max_fee      = rec.max_fee_per_gas
                priority_fee = rec.max_priority_fee_per_gas
            elif self._shared_state and self._shared_state.base_fee_wei > 0:
                base_fee     = self._shared_state.base_fee_wei
                max_fee      = int(base_fee * self.GAS_PREMIUM * 2)
                priority_fee = max(int(base_fee * 0.5), 1_000_000)
            else:
                base_fee     = 100_000_000
                max_fee      = int(base_fee * self.GAS_PREMIUM * 2)
                priority_fee = max(int(base_fee * 0.5), 1_000_000)

            # Synchronous ABI encoding — no RPC, no async scheduler overhead
            fn_name = 'executeLiquidationViaAave' if cached.flash_source == 'aave' else 'executeLiquidation'
            calldata = self._sync_executor.encodeABI(
                fn_name=fn_name,
                args=[
                    cached.collateral_asset,
                    cached.debt_asset,
                    cached.borrower,
                    cached.debt_to_cover,
                    False,                       # receiveAToken
                    cached.swap_route.router,
                    cached.swap_route.calldata,
                ],
            )
            tx_dict = {
                "to":                    self._executor_addr,
                "data":                  calldata,
                "from":                  self._wallet,
                "nonce":                 nonce,
                "gas":                   self.GAS_LIMIT_FLASH,
                "maxFeePerGas":          max_fee,
                "maxPriorityFeePerGas":  priority_fee,
                "chainId":               42161,
                "type":                  2,
                "value":                 0,
            }

            # Sign locally — no RPC
            signed = self._sync_w3.eth.account.sign_transaction(tx_dict, self._pk)

            logger.debug(
                f"[FlashBuilder] rebuild_with_nonce — "
                f"borrower={cached.borrower[:10]}... "
                f"nonce={nonce} fee_tier={cached.swap_route.fee_tier}"
            )

            return FlashLoanTxData(
                raw_tx           = signed.raw_transaction,
                collateral_asset = cached.collateral_asset,
                debt_asset       = cached.debt_asset,
                borrower         = cached.borrower,
                debt_to_cover    = cached.debt_to_cover,
                swap_route       = cached.swap_route,        # fully reused
                gas_limit        = self.GAS_LIMIT_FLASH,
                gas_price        = max_fee,
                nonce            = nonce,
                estimated_profit_usd = cached.estimated_profit_usd,
            )

        except Exception as e:
            logger.error(f"[FlashBuilder] rebuild_with_nonce failed: {e}")
            return None

    async def build_batch(
        self,
        debt_asset: str,
        items: list,       # list of dicts: {borrower, collateral_asset, debt_to_cover, collateral_amount, asset_prices_usd, asset_decimals}
        shared_state,
        nonce: int,
    ) -> Optional[tuple]:  # (raw_tx: bytes, total_debt: int, profit_usd: float, n_built: int)
        """
        Build a signed executeLiquidationBatch() tx covering multiple borrowers
        in one flash loan. All items share debt_asset.

        Per-item swap routes are built independently; items that fail to get a
        route are skipped (logged) so a bad pair cannot kill the whole batch.
        Returns None if fewer than 2 items survive route-building.
        """
        debt_asset = AsyncWeb3.to_checksum_address(debt_asset)

        batch_tuples = []
        total_debt   = 0
        profit_usd   = 0.0

        for item in items:
            borrower         = AsyncWeb3.to_checksum_address(item['borrower'])
            collateral_asset = AsyncWeb3.to_checksum_address(item['collateral_asset'])
            debt_to_cover    = item['debt_to_cover']
            collateral_amt   = item.get('collateral_amount', 0)
            prices           = item.get('asset_prices_usd', {})
            decimals         = item.get('asset_decimals', {})

            if collateral_asset.lower() == debt_asset.lower():
                swap_router   = "0x0000000000000000000000000000000000000000"
                swap_calldata = b""
                item_profit   = 0.0
            else:
                amount_in  = collateral_amt if collateral_amt > 0 else int(debt_to_cover * 1.05)
                swap_route = await self._swap_builder.build(
                    token_in    = collateral_asset,
                    token_out   = debt_asset,
                    amount_in   = amount_in,
                    slippage_bps= self._slippage,
                )
                if swap_route is None:
                    logger.warning(
                        f"[BatchBuilder] No swap route {collateral_asset[:10]}→{debt_asset[:10]} "
                        f"borrower={borrower[:10]}… — skipping item"
                    )
                    continue
                swap_router   = swap_route.router
                swap_calldata = swap_route.calldata
                # Rough per-item profit: collateral USD out minus debt USD in
                col_key   = collateral_asset.lower()
                dbt_key   = debt_asset.lower()
                col_price = next((p for k, p in prices.items() if k.lower() == col_key), 0)
                dbt_price = next((p for k, p in prices.items() if k.lower() == dbt_key), 0)
                col_dec   = decimals.get(collateral_asset, decimals.get(col_key, 18))
                dbt_dec   = decimals.get(debt_asset,       decimals.get(dbt_key, 18))
                if col_price > 0 and dbt_price > 0:
                    col_usd = (amount_in / 10**col_dec) * (col_price / 1e8)
                    dbt_usd = (debt_to_cover / 10**dbt_dec) * (dbt_price / 1e8)
                    item_profit = max(0.0, col_usd - dbt_usd)
                else:
                    item_profit = 0.0

            batch_tuples.append((
                collateral_asset,
                borrower,
                debt_to_cover,
                False,          # receiveAToken — False so swap self-funds flash repay
                AsyncWeb3.to_checksum_address(swap_router),
                swap_calldata,
            ))
            total_debt += debt_to_cover
            profit_usd += item_profit

        if len(batch_tuples) < 2:
            logger.warning(
                f"[BatchBuilder] Only {len(batch_tuples)} item(s) survived route-building "
                f"for debt={debt_asset[:10]}… — falling back to single path"
            )
            return None

        if self._gas_oracle:
            rec          = self._gas_oracle.recommend()
            max_fee      = rec.max_fee_per_gas
            priority_fee = rec.max_priority_fee_per_gas
        else:
            base_fee     = shared_state.base_fee_wei or 100_000_000
            max_fee      = int(base_fee * self.GAS_PREMIUM * 2)
            priority_fee = max(int(base_fee * 0.5), 1_000_000)

        try:
            tx_dict = await self._executor.functions.executeLiquidationBatch(
                debt_asset,
                batch_tuples,
            ).build_transaction({
                "from":                 self._wallet,
                "nonce":                nonce,
                "gas":                  self.GAS_LIMIT_BATCH,
                "maxFeePerGas":         max_fee,
                "maxPriorityFeePerGas": priority_fee,
                "chainId":              42161,
            })
            signed = self._sync_w3.eth.account.sign_transaction(tx_dict, self._pk)
        except Exception as e:
            logger.error(f"[BatchBuilder] TX build failed: {e}")
            return None

        logger.info(
            f"[BatchBuilder] Built batch N={len(batch_tuples)} "
            f"debt={debt_asset[:10]}… totalDebt={total_debt} "
            f"est_profit=${profit_usd:.2f}"
        )
        return (signed.raw_transaction, total_debt, profit_usd, len(batch_tuples))


# ---------------------------------------------------------------------------
# pipeline_v3.py integration
# ---------------------------------------------------------------------------
#
# 1. Import:
#       from flash_loan_route import FlashLoanTxBuilder, FlashLoanTxData
#       from skip_telemetry import SkipReason, SkipEvent
#
# 2. In setup():
#       self.flash_builder = FlashLoanTxBuilder(
#           rpc              = self.rpc,
#           executor_address = CONTRACT_ADDR,
#           wallet_address   = WALLET_ADDR,
#           private_key      = PRIVATE_KEY,
#           slippage_bps     = 50,
#       )
#
# 3. Replace _build_and_submit() with flash loan path:
#
#       async def _build_and_submit(self, borrower, collateral_asset,
#                                   debt_to_cover, collateral_amount=0):
#
#           # ── Try flash loan path first ─────────────────────────────
#           nonce   = await self.nonce_mgr.next()
#           tx_data = await self.flash_builder.build(
#               collateral_asset  = collateral_asset,
#               debt_asset        = best_debt_asset,
#               borrower          = borrower,
#               debt_to_cover     = debt_to_cover,
#               shared_state      = self.shared_state,
#               nonce             = nonce,
#               collateral_amount = collateral_amount,
#               asset_prices_usd  = {a: self.prices.get_price(a) for a in ASSET_ADDRESSES
#                                    if self.prices.get_price(a)},
#               asset_decimals    = DECIMALS,
#           )
#
#           if tx_data is None:
#               # ── Flash loan failed — fall back to direct path ──────
#               logger.info(f"[Submit] Flash loan unavailable — trying direct path")
#               # ... existing _build_and_submit direct path logic ...
#               # (keep existing code as fallback)
#
#           raw_tx = tx_data.raw_tx
#
#           # ── Submit ───────────────────────────────────────────────
#           tx_hash = await blast_submit(raw_tx)
#           if tx_hash:
#               await self.tracker.add(borrower, tx_hash, nonce)
#               logger.info(
#                   f"[Submit] Flash loan tx submitted — "
#                   f"hash={tx_hash[:12]}… "
#                   f"est_profit=${tx_data.estimated_profit_usd:.2f}"
#               )
#           else:
#               await self.nonce_mgr.rewind()
#               self.skip_tel.record(SkipEvent(
#                   borrower = borrower,
#                   reason   = SkipReason.SUBMIT_FAILED,
#                   detail   = "blast_submit returned None on flash loan tx",
#               ))
#           return tx_hash
#
# 4. Update _build_and_cache_one() to use flash loan path for pre-warming:
#
#       async def _build_and_cache_one(self, borrower: str) -> bool:
#           # ... existing collateral/debt selection ...
#           tx_data = await self.flash_builder.build(
#               collateral_asset = best_c,
#               debt_asset       = best_d,
#               borrower         = borrower,
#               debt_to_cover    = debt_to_cover,
#               shared_state     = self.shared_state,
#               nonce            = 0,   # placeholder — replaced at fire time
#               collateral_amount= collateral_amount,
#           )
#           if tx_data is None:
#               return False
#           # Store raw unsigned dict for nonce replacement at fire time
#           self._presigned_cache[borrower]    = tx_data
#           self._presigned_snapshots[borrower]= PresignedSnapshot(
#               borrower         = borrower,
#               base_fee_wei     = self.shared_state.base_fee_wei,
#               debt_to_cover    = debt_to_cover,
#               collateral_asset = best_c,
#               debt_asset       = best_d,
#           )
#           return True
#
# 5. First run — verify router is approved:
#       python -c "
#       import asyncio
#       from web3 import AsyncWeb3
#       from web3.providers import AsyncHTTPProvider
#       async def check():
#           w3 = AsyncWeb3(AsyncHTTPProvider('YOUR_ALCHEMY_URL'))
#           from flash_loan_route import EXECUTOR_ABI, UNI_V3_ROUTER, EXECUTOR_ADDR
#           c = w3.eth.contract(address=EXECUTOR_ADDR, abi=EXECUTOR_ABI)
#           approved = await c.functions.approvedRouters(UNI_V3_ROUTER).call()
#           print(f'Uni V3 Router approved: {approved}')
#       asyncio.run(check())
#       "
#
#       If False: run approveRouter() as contract owner before going live.
#
# ---------------------------------------------------------------------------
