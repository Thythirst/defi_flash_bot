"""
aave_base.py — Aave V3 Base chain liquidation module
Extends the existing Arbitrum pipeline to Base chain.

Key differences from Arbitrum:
    - Uni V3 QuoterV2 not deployed on Base → use Aerodrome Quoter
    - Aerodrome uses int24 tickSpacing (not uint24 fee) in quote params
    - Aerodrome SwapRouter calldata differs from Uni V3 SwapRouter
    - tickSpacing fetched once per pool and cached
    - All other components identical: Balancer flash loans, Aave V3 ABI,
      position loading, HF engine, pre-warm, skip telemetry

Architecture:
    AerodromeQuoter      — replaces QuoterAsync for Base chain
    AerodromeSwapBuilder — replaces SwapCalldataBuilder for Base chain
    BaseChainConfig      — all Base addresses in one place
    AaveBaseModule       — drop-in alongside existing Arbitrum pipeline

Usage:
    base = AaveBaseModule(
        rpc_http     = os.getenv("BASE_RPC_URL"),
        rpc_wss      = os.getenv("BASE_WSS_URL"),
        redis        = redis_client,
        shared_state = shared_state,   # separate instance from Arbitrum
        nonce_mgr    = base_nonce_mgr, # separate nonce manager per chain
        executor_addr= os.getenv("BASE_EXECUTOR_ADDR"),
        private_key  = PRIVATE_KEY,
        wallet       = WALLET_ADDR,
    )
    await base.start()
    # base.on_new_block(block_number) in Base block handler
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from eth_abi import encode
from web3 import AsyncWeb3, Web3
from web3.providers import AsyncHTTPProvider

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Base chain addresses — all verified on-chain
# ---------------------------------------------------------------------------

class BaseChainConfig:
    """All verified Base mainnet addresses."""

    CHAIN_ID    = 8453
    CHAIN_NAME  = "base"

    # Aave V3 — verified 15 reserves
    AAVE_POOL         = "0xA238Dd80C259a72e81d7e4664a9801593F98d1c5"
    AAVE_DEPLOY_BLOCK = 2_357_000   # approx — refine with first Borrow event

    # Flash loans — same vault address as Arbitrum
    BALANCER_VAULT    = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"

    # DEX — Aerodrome Slipstream (Uni V3 QuoterV2 not deployed on Base)
    AERODROME_QUOTER  = "0x254cF9E1E6e233aa1AC962CB9B05b2cfeAaE15b0"
    AERODROME_ROUTER  = "0xBE6D8f0d05cC4be24d5167a3eF062215bE6D18a5"

    # Uni V3 SwapRouter (for execution — still works even without QuoterV2)
    UNI_V3_ROUTER     = "0x2626664c2603336E57B271c5C0b26F421741e481"
    UNI_V3_FACTORY    = "0x33128a8fC17869897dcE68Ed026d694621f6FDfD"

    # Multicall3 — same address on all EVM chains
    MULTICALL3        = "0xcA11bde05977b3631167028862bE2a173976CA11"

    # Base native tokens
    WETH              = "0x4200000000000000000000000000000000000006"
    USDC              = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
    USDbC             = "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA"  # bridged USDC
    cbETH             = "0x2Ae3F1Ec7F1F5012CFEab0185bfc7aa3cf0DEc22"
    wstETH            = "0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452"
    cbBTC             = "0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf"

    # Known Aerodrome pools — tickSpacing cached per pool
    # tickSpacing replaces fee tier in Aerodrome's Slipstream
    AERODROME_POOLS = {
        ("WETH", "USDC"): {
            "address":     "0xb2cc224c1c9feE385f8ad6a55b4d94E92359DC59",
            "tickSpacing": 100,
            "tvl_usd":     8_300_000,
        },
        ("WETH", "USDbC"): {
            "address":     None,   # populate if needed
            "tickSpacing": 100,
        },
        ("cbETH", "WETH"): {
            "address":     None,
            "tickSpacing": 1,
        },
        ("wstETH", "WETH"): {
            "address":     None,
            "tickSpacing": 1,
        },
    }

    # Public RPC fallback
    PUBLIC_RPC = "https://mainnet.base.org"

    @classmethod
    def get_pool(cls, token_in_symbol: str, token_out_symbol: str) -> Optional[dict]:
        """Look up known Aerodrome pool config."""
        key = (token_in_symbol, token_out_symbol)
        rkey = (token_out_symbol, token_in_symbol)
        return cls.AERODROME_POOLS.get(key) or cls.AERODROME_POOLS.get(rkey)


# ---------------------------------------------------------------------------
# ABIs
# ---------------------------------------------------------------------------

AERODROME_QUOTER_ABI = [
    {
        # quoteExactInputSingle((address tokenIn, address tokenOut,
        #                        uint256 amountIn, int24 tickSpacing,
        #                        uint160 sqrtPriceLimitX96))
        # → (uint256 amountOut, uint160 sqrtPriceX96After,
        #    uint32 initializedTicksCrossed, uint256 gasEstimate)
        "inputs": [
            {
                "components": [
                    {"internalType": "address", "name": "tokenIn",           "type": "address"},
                    {"internalType": "address", "name": "tokenOut",          "type": "address"},
                    {"internalType": "uint256", "name": "amountIn",          "type": "uint256"},
                    {"internalType": "int24",   "name": "tickSpacing",       "type": "int24"},
                    {"internalType": "uint160", "name": "sqrtPriceLimitX96", "type": "uint160"},
                ],
                "internalType": "struct IQuoterV2.QuoteExactInputSingleParams",
                "name":         "params",
                "type":         "tuple",
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

# Aerodrome Slipstream pool ABI (subset needed)
AERODROME_POOL_ABI = [
    {
        "inputs": [],
        "name": "tickSpacing",
        "outputs": [{"internalType": "int24", "name": "", "type": "int24"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "slot0",
        "outputs": [
            {"internalType": "uint160", "name": "sqrtPriceX96",        "type": "uint160"},
            {"internalType": "int24",   "name": "tick",                "type": "int24"},
            {"internalType": "uint16",  "name": "observationIndex",    "type": "uint16"},
            {"internalType": "uint16",  "name": "observationCardinality","type": "uint16"},
            {"internalType": "uint16",  "name": "observationCardinalityNext","type": "uint16"},
            {"internalType": "bool",    "name": "unlocked",            "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token0",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "token1",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# Aerodrome SwapRouter — exactInputSingle
AERODROME_ROUTER_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"internalType": "address", "name": "tokenIn",           "type": "address"},
                    {"internalType": "address", "name": "tokenOut",          "type": "address"},
                    {"internalType": "int24",   "name": "tickSpacing",       "type": "int24"},
                    {"internalType": "address", "name": "recipient",         "type": "address"},
                    {"internalType": "uint256", "name": "deadline",          "type": "uint256"},
                    {"internalType": "uint256", "name": "amountIn",          "type": "uint256"},
                    {"internalType": "uint256", "name": "amountOutMinimum",  "type": "uint256"},
                    {"internalType": "uint160", "name": "sqrtPriceLimitX96", "type": "uint160"},
                ],
                "internalType": "struct ICLRouter.ExactInputSingleParams",
                "name":         "params",
                "type":         "tuple",
            }
        ],
        "name": "exactInputSingle",
        "outputs": [{"internalType": "uint256", "name": "amountOut", "type": "uint256"}],
        "stateMutability": "payable",
        "type": "function",
    }
]

# exactInputSingle selector for Aerodrome router calldata encoding
AERODROME_EXACT_INPUT_SINGLE_SELECTOR = bytes.fromhex("a026383e")

MULTICALL3_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"internalType": "address", "name": "target",       "type": "address"},
                    {"internalType": "bool",    "name": "allowFailure", "type": "bool"},
                    {"internalType": "bytes",   "name": "callData",     "type": "bytes"},
                ],
                "internalType": "struct Multicall3.Call3[]",
                "name": "calls",
                "type": "tuple[]",
            }
        ],
        "name": "aggregate3",
        "outputs": [
            {
                "components": [
                    {"internalType": "bool",  "name": "success",    "type": "bool"},
                    {"internalType": "bytes", "name": "returnData", "type": "bytes"},
                ],
                "internalType": "struct Multicall3.Result[]",
                "name": "returnData",
                "type": "tuple[]",
            }
        ],
        "stateMutability": "payable",
        "type": "function",
    }
]


# ---------------------------------------------------------------------------
# AerodromeQuoter — replaces QuoterAsync for Base chain
# ---------------------------------------------------------------------------

@dataclass
class AerodromeQuoteResult:
    amount_out:    int
    tick_spacing:  int
    token_in:      str
    token_out:     str
    amount_in:     int
    slippage_pct:  float = 0.0


class AerodromeQuoter:
    """
    Quotes swaps on Aerodrome Slipstream pools.
    Drop-in replacement for QuoterAsync on Base chain.

    Key difference from Uni V3:
        - Uses int24 tickSpacing instead of uint24 fee
        - tickSpacing is fetched from pool and cached per (token_in, token_out)
        - Same return format: (amountOut, sqrtPriceX96After, ticksCrossed, gasEstimate)
    """

    def __init__(self, w3: AsyncWeb3):
        self._w3      = w3
        self._quoter  = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(BaseChainConfig.AERODROME_QUOTER),
            abi=AERODROME_QUOTER_ABI,
        )
        # Cache: (token_in, token_out) → [tickSpacing, ...]
        # Multiple pools may exist per pair with different tickSpacings
        self._tick_spacing_cache: dict[tuple, list[int]] = {}
        self._pool_cache:         dict[tuple, str]       = {}

        # Known tick spacings on Aerodrome (covers most liquid pools)
        # 1 = stable/correlated pairs (cbETH/WETH, wstETH/WETH)
        # 100 = volatile pairs (WETH/USDC, etc.)
        # 200 = high-volatility pairs
        self._default_tick_spacings = [1, 100, 200]

    async def best_quote(
        self,
        token_in:  str,
        token_out: str,
        amount_in: int,
    ) -> tuple[int, int]:
        """
        Quote across known tick spacings, return (best_amount_out, best_tick_spacing).
        Mirrors QuoterAsync.best_quote() interface for drop-in compatibility.
        """
        token_in  = AsyncWeb3.to_checksum_address(token_in)
        token_out = AsyncWeb3.to_checksum_address(token_out)

        # Get tick spacings to try for this pair
        cache_key      = (token_in.lower(), token_out.lower())
        tick_spacings  = self._tick_spacing_cache.get(
            cache_key, self._default_tick_spacings
        )

        tasks = [
            asyncio.create_task(
                self._quote_tick_spacing(token_in, token_out, amount_in, ts),
                name=f"quote_ts{ts}",
            )
            for ts in tick_spacings
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        best_out = 0
        best_ts  = tick_spacings[0]

        for ts, result in zip(tick_spacings, results):
            if isinstance(result, Exception):
                logger.debug(f"[AerodromeQuoter] tickSpacing={ts} failed: {result}")
                continue
            amount_out, tick_spacing = result
            if amount_out > best_out:
                best_out = amount_out
                best_ts  = tick_spacing

        logger.debug(
            f"[AerodromeQuoter] {token_in[:10]}→{token_out[:10]} "
            f"best={best_out} ts={best_ts}"
        )
        return best_out, best_ts

    async def _quote_tick_spacing(
        self,
        token_in:     str,
        token_out:    str,
        amount_in:    int,
        tick_spacing: int,
    ) -> tuple[int, int]:
        """Quote one tick spacing. Returns (amountOut, tickSpacing) or raises."""
        result = await self._quoter.functions.quoteExactInputSingle({
            "tokenIn":           token_in,
            "tokenOut":          token_out,
            "amountIn":          amount_in,
            "tickSpacing":       tick_spacing,
            "sqrtPriceLimitX96": 0,
        }).call()
        return result[0], tick_spacing  # amountOut, tickSpacing

    async def fetch_pool_tick_spacing(
        self,
        pool_address: str,
    ) -> int:
        """
        Fetch tickSpacing directly from a pool contract.
        Call once per pool and cache the result.
        """
        pool = self._w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(pool_address),
            abi=AERODROME_POOL_ABI,
        )
        ts = await pool.functions.tickSpacing().call()
        logger.debug(f"[AerodromeQuoter] Pool {pool_address[:10]} tickSpacing={ts}")
        return ts

    def cache_tick_spacing(
        self,
        token_in:     str,
        token_out:    str,
        tick_spacing: int,
    ) -> None:
        """Manually cache a known tick spacing for a pair."""
        key = (token_in.lower(), token_out.lower())
        existing = self._tick_spacing_cache.get(key, [])
        if tick_spacing not in existing:
            existing.append(tick_spacing)
        self._tick_spacing_cache[key] = existing


# ---------------------------------------------------------------------------
# AerodromeSwapBuilder — builds swap calldata for Base chain
# ---------------------------------------------------------------------------

@dataclass
class AerodromeSwapRoute:
    """Result from AerodromeSwapBuilder.build() — mirrors flash_loan_route.SwapRoute."""
    router:       str    # Aerodrome router address
    calldata:     bytes  # encoded exactInputSingle calldata
    tick_spacing: int    # used instead of fee_tier
    amount_in:    int
    amount_out:   int
    slippage_pct: float

    # Compatibility alias — flash_loan_route reads fee_tier
    @property
    def fee_tier(self) -> int:
        return self.tick_spacing


class AerodromeSwapBuilder:
    """
    Builds Aerodrome exactInputSingle swap calldata.
    Replaces SwapCalldataBuilder for Base chain.

    The key difference: Aerodrome uses tickSpacing (int24) not fee (uint24).
    The executor contract receives this calldata and calls Aerodrome router
    to swap collateral → debt token after liquidation.
    """

    def __init__(self, w3: AsyncWeb3, executor_address: str):
        self._w3       = w3
        self._executor = AsyncWeb3.to_checksum_address(executor_address)
        self._quoter   = AerodromeQuoter(w3)

        # Pre-seed known pairs from BaseChainConfig
        self._quoter.cache_tick_spacing(
            BaseChainConfig.WETH, BaseChainConfig.USDC, 100
        )
        self._quoter.cache_tick_spacing(
            BaseChainConfig.WETH, BaseChainConfig.USDbC, 100
        )
        self._quoter.cache_tick_spacing(
            BaseChainConfig.cbETH, BaseChainConfig.WETH, 1
        )
        self._quoter.cache_tick_spacing(
            BaseChainConfig.wstETH, BaseChainConfig.WETH, 1
        )

    async def build(
        self,
        token_in:        str,
        token_out:       str,
        amount_in:       int,
        slippage_bps:    int = 50,
        deadline_offset: int = 180,
    ) -> Optional[AerodromeSwapRoute]:
        """
        Find best Aerodrome route and encode swap calldata.
        Returns AerodromeSwapRoute or None if no liquid pool found.

        Direct replacement for SwapCalldataBuilder.build() on Base chain.
        """
        token_in  = AsyncWeb3.to_checksum_address(token_in)
        token_out = AsyncWeb3.to_checksum_address(token_out)

        amount_out, tick_spacing = await self._quoter.best_quote(
            token_in, token_out, amount_in
        )

        if amount_out == 0:
            logger.warning(
                f"[AerodromeSwapBuilder] No liquidity: "
                f"{token_in[:10]}→{token_out[:10]}"
            )
            return None

        amount_out_min = int(amount_out * (10_000 - slippage_bps) / 10_000)
        slippage_pct   = slippage_bps / 10_000

        deadline = int(time.time()) + deadline_offset
        calldata = self._encode_exact_input_single(
            token_in       = token_in,
            token_out      = token_out,
            tick_spacing   = tick_spacing,
            recipient      = self._executor,
            deadline       = deadline,
            amount_in      = amount_in,
            amount_out_min = amount_out_min,
        )

        logger.debug(
            f"[AerodromeSwapBuilder] Route: {token_in[:10]}→{token_out[:10]} "
            f"ts={tick_spacing} in={amount_in} outMin={amount_out_min} "
            f"slippage={slippage_pct:.2%}"
        )

        return AerodromeSwapRoute(
            router       = BaseChainConfig.AERODROME_ROUTER,
            calldata     = calldata,
            tick_spacing = tick_spacing,
            amount_in    = amount_in,
            amount_out   = amount_out,
            slippage_pct = slippage_pct,
        )

    def _encode_exact_input_single(
        self,
        token_in:       str,
        token_out:      str,
        tick_spacing:   int,
        recipient:      str,
        deadline:       int,
        amount_in:      int,
        amount_out_min: int,
    ) -> bytes:
        """
        ABI-encode Aerodrome exactInputSingle calldata.

        Struct layout (differs from Uni V3):
            address tokenIn
            address tokenOut
            int24   tickSpacing      ← int24 not uint24 fee
            address recipient
            uint256 deadline
            uint256 amountIn
            uint256 amountOutMinimum
            uint160 sqrtPriceLimitX96
        """
        encoded = encode(
            ["(address,address,int24,address,uint256,uint256,uint256,uint160)"],
            [(
                AsyncWeb3.to_checksum_address(token_in),
                AsyncWeb3.to_checksum_address(token_out),
                tick_spacing,
                AsyncWeb3.to_checksum_address(recipient),
                deadline,
                amount_in,
                amount_out_min,
                0,   # sqrtPriceLimitX96 = 0 (no limit)
            )]
        )
        return AERODROME_EXACT_INPUT_SINGLE_SELECTOR + encoded


# ---------------------------------------------------------------------------
# BaseFlashLoanTxBuilder — builds executeLiquidation txs for Base chain
# ---------------------------------------------------------------------------

class BaseFlashLoanTxBuilder:
    """
    Builds flash loan liquidation txs for Aave V3 on Base.
    Identical to Arbitrum's FlashLoanTxBuilder but uses AerodromeSwapBuilder
    instead of SwapCalldataBuilder (no Uni V3 QuoterV2 on Base).

    The executor contract is the same FlashExecutorV3.sol — it only needs
    a valid swapRouter and swapCalldata. Aerodrome router is approved
    separately (call approveRouter(AERODROME_ROUTER) on the Base executor).
    """

    GAS_LIMIT    = 700_000
    GAS_PREMIUM  = 2.0       # maxFeePerGas = base_fee * GAS_PREMIUM (EIP-1559)

    def __init__(
        self,
        w3:               AsyncWeb3,
        executor_address: str,
        wallet_address:   str,
        private_key:      str,
        executor_abi:     list,
        slippage_bps:     int = 50,
        shared_state      = None,
    ):
        self._w3           = w3
        self._executor_addr= AsyncWeb3.to_checksum_address(executor_address)
        self._wallet       = AsyncWeb3.to_checksum_address(wallet_address)
        self._pk           = private_key
        self._slippage     = slippage_bps
        self._shared_state = shared_state
        self._sync_w3      = Web3()

        self._executor     = w3.eth.contract(
            address=self._executor_addr,
            abi=executor_abi,
        )
        self._swap_builder = AerodromeSwapBuilder(w3, executor_address)

    async def build(
        self,
        collateral_asset: str,
        debt_asset:       str,
        borrower:         str,
        debt_to_cover:    int,
        nonce:            int,
        collateral_amount:int = 0,
    ) -> Optional[dict]:
        """
        Build signed executeLiquidation tx for Base chain.
        Returns dict with raw_tx, swap_route, estimated_profit.
        """
        collateral_asset = AsyncWeb3.to_checksum_address(collateral_asset)
        debt_asset       = AsyncWeb3.to_checksum_address(debt_asset)
        borrower         = AsyncWeb3.to_checksum_address(borrower)

        amount_in = collateral_amount or int(debt_to_cover * 1.05)

        # Build Aerodrome swap route
        swap_route = await self._swap_builder.build(
            token_in   = collateral_asset,
            token_out  = debt_asset,
            amount_in  = amount_in,
            slippage_bps = self._slippage,
        )

        if swap_route is None:
            logger.warning(
                f"[BaseFlashBuilder] No Aerodrome route: "
                f"{collateral_asset[:10]}→{debt_asset[:10]}"
            )
            return None

        base_fee  = (
            self._shared_state.base_fee_wei
            if self._shared_state and self._shared_state.base_fee_wei > 0
            else 100_000_000
        )
        max_fee      = int(base_fee * self.GAS_PREMIUM * 2)
        priority_fee = max(int(base_fee * 0.5), 1_000_000)  # min 0.001 gwei tip

        try:
            tx_dict = self._executor.functions.executeLiquidation(
                collateral_asset,
                debt_asset,
                borrower,
                debt_to_cover,
                False,                  # receiveAToken
                swap_route.router,      # Aerodrome router
                swap_route.calldata,    # Aerodrome exactInputSingle calldata
            ).build_transaction({
                "from":                  self._wallet,
                "nonce":                 nonce,
                "gas":                   self.GAS_LIMIT,
                "maxFeePerGas":          max_fee,
                "maxPriorityFeePerGas":  priority_fee,
                "chainId":               BaseChainConfig.CHAIN_ID,
            })

            signed = self._sync_w3.eth.account.sign_transaction(tx_dict, self._pk)

            logger.info(
                f"[BaseFlashBuilder] Built tx — "
                f"borrower={borrower[:10]}… "
                f"collateral={collateral_asset[:10]}… "
                f"debt={debt_asset[:10]}… "
                f"tickSpacing={swap_route.tick_spacing} "
                f"slippage={swap_route.slippage_pct:.2%}"
            )

            return {
                "raw_tx":          signed.raw_transaction,
                "swap_route":      swap_route,
                "collateral_asset":collateral_asset,
                "debt_asset":      debt_asset,
                "borrower":        borrower,
                "debt_to_cover":   debt_to_cover,
                "gas_price":       max_fee,
                "nonce":           nonce,
            }

        except Exception as e:
            logger.error(f"[BaseFlashBuilder] TX build failed: {e}")
            return None


# ---------------------------------------------------------------------------
# BaseWatchlistBuilder — Borrow event scraper for Base chain
# ---------------------------------------------------------------------------

# getUserAccountData ABI — used for debt validation via Multicall3
POOL_ACCOUNT_DATA_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "user", "type": "address"}],
        "name": "getUserAccountData",
        "outputs": [
            {"internalType": "uint256", "name": "totalCollateralBase",          "type": "uint256"},
            {"internalType": "uint256", "name": "totalDebtBase",                "type": "uint256"},
            {"internalType": "uint256", "name": "availableBorrowsBase",         "type": "uint256"},
            {"internalType": "uint256", "name": "currentLiquidationThreshold", "type": "uint256"},
            {"internalType": "uint256", "name": "ltv",                          "type": "uint256"},
            {"internalType": "uint256", "name": "healthFactor",                 "type": "uint256"},
        ],
        "stateMutability": "view",
        "type": "function",
    }
]

WAD = 10 ** 18


class BaseWatchlistBuilder:
    """
    Scrapes Aave V3 Base Borrow events to build borrower watchlist.
    Validates on-chain debt via Multicall3 BEFORE writing to Redis —
    prevents phantom positions (closed/zero-debt borrowers) from
    polluting the watchlist.

    Flow:
        1. Scan Borrow events → collect all addresses
        2. Batch-verify via Multicall3 → filter totalDebtBase > 0
        3. Write only verified-active addresses to Redis with real HF
    """

    BORROW_TOPIC = "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0"
    VERIFY_BATCH_SIZE = 200   # matches PositionLoader.BATCH_SIZE

    def __init__(
        self,
        w3:               AsyncWeb3,
        redis,
        redis_key:        str = "watchlist:base:aave",
        blocks_per_chunk: int = 2000,
    ):
        self._w3      = w3
        self._redis   = redis
        self._key     = redis_key
        self._chunk   = blocks_per_chunk
        self._ckpt    = f"{redis_key}:backfill_checkpoint"

        # Multicall3 contract — same address on all EVM chains
        self._mc = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(BaseChainConfig.MULTICALL3),
            abi=MULTICALL3_ABI,
        )
        # Pool contract (for calldata encoding)
        self._pool = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(BaseChainConfig.AAVE_POOL),
            abi=POOL_ACCOUNT_DATA_ABI,
        )

    async def _verify_active_debt(self, addresses: set[str]) -> dict[str, float]:
        """
        Batch-verify on-chain debt via Multicall3 aggregate3.
        Returns {address: health_factor} ONLY for addresses with totalDebtBase > 0.
        Ghosts (zero debt) are silently dropped.
        """
        active: dict[str, float] = {}
        addr_list = list(addresses)
        chunks = [
            addr_list[i:i + self.VERIFY_BATCH_SIZE]
            for i in range(0, len(addr_list), self.VERIFY_BATCH_SIZE)
        ]

        for chunk in chunks:
            calls = []
            valid_addrs = []
            for addr in chunk:
                try:
                    ca = AsyncWeb3.to_checksum_address(addr)
                    call_data = self._pool.functions.getUserAccountData(ca)._encode_transaction_data()
                    calls.append({
                        "target":       AsyncWeb3.to_checksum_address(BaseChainConfig.AAVE_POOL),
                        "allowFailure": True,
                        "callData":     call_data,
                    })
                    valid_addrs.append(addr.lower())
                except Exception:
                    continue

            if not calls:
                continue

            try:
                results = await asyncio.wait_for(
                    self._mc.functions.aggregate3(calls).call(),
                    timeout=30.0,
                )
            except asyncio.TimeoutError:
                logger.warning("[BaseWatchlist] Multicall3 timed out during debt verify")
                await asyncio.sleep(2)
                continue
            except Exception as e:
                logger.warning(f"[BaseWatchlist] Multicall3 failed: {e}")
                await asyncio.sleep(2)
                continue

            for addr, (success, raw) in zip(valid_addrs, results):
                if not success or len(raw) < 192:
                    continue
                try:
                    h          = raw.hex()
                    total_debt = int(h[64:128], 16)   # slot 1: totalDebtBase
                    hf_raw     = int(h[320:384], 16)  # slot 5: healthFactor
                    if total_debt == 0:
                        continue
                    hf = min(hf_raw / WAD, 10.0)
                    active[addr] = hf
                except Exception:
                    continue

            await asyncio.sleep(0)

        return active

    async def backfill(
        self,
        start_block: Optional[int] = None,
        end_block:   Optional[int] = None,
    ) -> int:
        """Scrape Borrow events, verify debt, and populate Redis ZSET."""
        if end_block is None:
            end_block = await self._w3.eth.block_number
        if start_block is None:
            ckpt = await self._redis.get(self._ckpt)
            start_block = int(ckpt) if ckpt else BaseChainConfig.AAVE_DEPLOY_BLOCK

        addresses: set[str] = set()
        block = start_block

        logger.info(
            f"[BaseWatchlist] Backfill {start_block:,}→{end_block:,} "
            f"({end_block - start_block:,} blocks)"
        )

        while block < end_block:
            to_block = min(block + self._chunk - 1, end_block)
            try:
                logs = await self._w3.eth.get_logs({
                    "address":   BaseChainConfig.AAVE_POOL,
                    "fromBlock": block,
                    "toBlock":   to_block,
                    "topics":    [self.BORROW_TOPIC],
                })
                for log in logs:
                    topics = log.get("topics", [])
                    if len(topics) >= 3:
                        t2   = topics[2] if isinstance(topics[2], str) else topics[2].hex()
                        addr = "0x" + t2[-40:]
                        addresses.add(addr.lower())

                chunks_done = (block - start_block) // self._chunk
                if chunks_done % 50 == 0 and chunks_done > 0:
                    pct = (block - start_block) / (end_block - start_block) * 100
                    logger.info(
                        f"[BaseWatchlist] {pct:.1f}% — "
                        f"block {to_block:,} — "
                        f"{len(addresses):,} addresses"
                    )

                if chunks_done % 100 == 0 and chunks_done > 0:
                    await self._redis.set(self._ckpt, str(to_block), ex=86400 * 7)

                block = to_block + 1
                await asyncio.sleep(0.05)

            except Exception as e:
                logger.warning(f"[BaseWatchlist] getLogs failed {block}→{to_block}: {e}")
                await asyncio.sleep(5)
                if any(k in str(e).lower() for k in ("limit", "rate", "too many")):
                    self._chunk = max(500, self._chunk // 2)
                continue

        n_raw = len(addresses)
        logger.info(f"[BaseWatchlist] Scan done — {n_raw:,} addresses found")

        # ─── VERIFY: only keep addresses with active on-chain debt ───
        logger.info(f"[BaseWatchlist] Verifying on-chain debt for {n_raw:,} addresses...")
        active = await self._verify_active_debt(addresses)
        n_active = len(active)
        n_ghosts = n_raw - n_active
        ghost_pct = n_ghosts / n_raw * 100 if n_raw > 0 else 0

        logger.info(
            f"[BaseWatchlist] Debt verification complete — "
            f"active={n_active:,} ghosts={n_ghosts:,} ({ghost_pct:.1f}% phantom)"
        )

        # ─── WRITE: only active addresses with real HF, minus excluded ───
        excluded = await self._redis.smembers(f"{self._key}:excluded")
        excluded_set = {
            (e.decode() if isinstance(e, bytes) else e).lower()
            for e in excluded
        }
        n_excluded = 0
        pipe = self._redis.pipeline()
        for addr, hf in active.items():
            if addr.lower() in excluded_set:
                n_excluded += 1
                continue
            pipe.zadd(self._key, {addr: hf})
        await pipe.execute()
        await self._redis.delete(self._ckpt)

        written = n_active - n_excluded
        if n_excluded:
            logger.info(
                f"[BaseWatchlist] {n_excluded} excluded addresses skipped "
                f"(watchlist:base:aave:excluded)"
            )
        logger.info(f"[BaseWatchlist] {written:,} verified-active borrowers written to {self._key}")
        return written


# ---------------------------------------------------------------------------
# AaveBaseModule — top-level coordinator
# ---------------------------------------------------------------------------

class AaveBaseModule:
    """
    Runs Aave V3 Base chain liquidations alongside the Arbitrum pipeline.
    Shares wallet/private key but uses separate:
        - AsyncRPCClient (Base RPC endpoints)
        - NonceManager (Base chain nonces)
        - SharedState (Base chain base fee)
        - PositionLoader (Base Aave pool)
        - Flash builder (Aerodrome swap routes)

    Usage in pipeline_v3.py:
        self.base = AaveBaseModule(...)
        await self.base.start()
        # In Base block handler:
        await self.base.on_new_block(block_number)
    """

    def __init__(
        self,
        rpc_http:       str,
        rpc_wss:        str,
        redis,
        wallet:         str,
        private_key:    str,
        executor_addr:  str,
        executor_abi:   list,
        skip_tel        = None,
        min_profit_usd: float = 2.0,
        refresh_interval:int  = 5,    # blocks between HF checks
        price_registry  = None,       # PriceRegistry from parent pipeline
    ):
        self._rpc_http   = rpc_http
        self._rpc_wss    = rpc_wss
        self._redis      = redis
        self._wallet     = AsyncWeb3.to_checksum_address(wallet)
        self._pk         = private_key
        self._exec_addr  = executor_addr
        self._exec_abi   = executor_abi
        self._skip       = skip_tel
        self._min_profit = min_profit_usd
        self._refresh_n  = refresh_interval
        self._prices     = price_registry  # shared PriceRegistry

        # Initialised in start()
        self._w3:           Optional[AsyncWeb3]          = None
        self._loader        = None   # PositionLoader
        self._hf_engine     = None   # LocalHFEngine
        self._nonce_mgr     = None   # NonceManager
        self._shared_state  = None   # SharedState
        self._flash_builder: Optional[BaseFlashLoanTxBuilder] = None
        self._in_flight: set[str] = set()
        self._running = False

    async def start(self) -> None:
        """Initialise Base chain components and load watchlist."""
        from web3.providers import AsyncHTTPProvider
        from async_web3 import AsyncRPCClient, NonceManager
        from hot_path_fix import SharedState
        from position_loader import PositionLoader

        # Base chain RPC
        self._w3 = AsyncWeb3(AsyncHTTPProvider(
            self._rpc_http,
            request_kwargs={"timeout": 15},
        ))

        rpc = AsyncRPCClient(self._rpc_http)
        await rpc.connect()

        # Chain-specific state
        self._shared_state = SharedState()
        self._nonce_mgr    = NonceManager(rpc.w3, self._wallet)
        await self._nonce_mgr.init()

        # Position loader for Base Aave pool
        self._loader = PositionLoader(
            w3           = rpc.w3,
            pool_address = BaseChainConfig.AAVE_POOL,
        )

        # Load watchlist — filter out known-excluded addresses
        #   watchlist:base:excluded — addresses we've confirmed are
        #   unliquidatable (e.g. weETH collateral with no DEX liquidity)
        members = await self._redis.zrange("watchlist:base:aave", 0, -1)
        excluded = await self._redis.smembers("watchlist:base:excluded")
        excluded_set = {
            (e.decode() if isinstance(e, bytes) else e).lower()
            for e in excluded
        }
        if members:
            watchlist = []
            skipped = 0
            for m in members:
                addr = m.decode() if isinstance(m, bytes) else m
                if addr.lower() in excluded_set:
                    skipped += 1
                    continue
                watchlist.append(addr)
            if skipped:
                logger.info(
                    f"[AaveBase] Skipped {skipped} excluded addresses "
                    f"(watchlist:base:excluded)"
                )
            loaded = await self._loader.bootstrap(watchlist)
            logger.info(f"[AaveBase] {loaded} positions loaded from watchlist")
        else:
            logger.warning(
                "[AaveBase] Empty watchlist — run BaseWatchlistBuilder.backfill() first"
            )

        # Flash builder with Aerodrome swap routes
        self._flash_builder = BaseFlashLoanTxBuilder(
            w3               = rpc.w3,
            executor_address = self._exec_addr,
            wallet_address   = self._wallet,
            private_key      = self._pk,
            executor_abi     = self._exec_abi,
            shared_state     = self._shared_state,
        )

        self._rpc     = rpc
        self._running = True
        logger.info(
            f"[AaveBase] Started — "
            f"{self._loader.position_count} positions, "
            f"executor={self._exec_addr[:10]}…"
        )

    async def stop(self) -> None:
        self._running = False

    async def on_new_block(self, block_number: int, base_fee_wei: int = 0) -> None:
        """
        Call from Base chain block handler every block.
        Refreshes hot positions and checks for liquidatable candidates.
        """
        if not self._running:
            return

        # Update shared state with Base chain base fee
        if base_fee_wei > 0:
            self._shared_state.on_new_block(block_number, base_fee_wei)

        # Refresh near-liquidatable positions
        if block_number % self._refresh_n == 0:
            try:
                if self._loader.position_count > 0:
                    await self._loader.refresh_hot(hf_threshold=1.2)
            except RuntimeError as e:
                # not bootstrapped yet — backfill still running, log once
                logger.info(f"[BaseBlock] Waiting for watchlist backfill... (block={block_number})")

        # Check for liquidatable positions
        if self._loader.position_count > 0:
            liquidatable = self._loader.liquidatable
            for pos in liquidatable:
                if pos.address in self._in_flight:
                    continue
                asyncio.create_task(
                    self._execute(pos),
                    name=f"base_liq_{pos.address[:8]}",
                )

    async def _execute(self, pos) -> None:
        """Attempt liquidation of one Base chain position."""
        self._in_flight.add(pos.address)
        try:
            from collateral_selector import CollateralSelector
            from blast_submit import blast_submit

            # Fetch reserve data if missing
            if not pos.reserves:
                await self._loader.refresh_hot(hf_threshold=1.05)
                pos = self._loader.get(pos.address)
                if not pos or not pos.reserves:
                    return

            # Select collateral
            selector = CollateralSelector(
                position_loader = self._loader,
            )
            if self._prices is not None:
                snap = self._prices.snapshot()
                prices_snap = {k: v / 1e8 for k, v in snap.items()}
            else:
                prices_snap = {}
            result = selector.select(
                account_data     = pos,
                total_debt_usd   = pos.total_debt_base / 1e8,
                asset_prices_usd = prices_snap,
                asset_decimals   = {},
            )
            if result is None:
                return

            # Profit gate
            if result.expected_profit_usd < self._min_profit:
                logger.debug(
                    f"[AaveBase] Below profit floor "
                    f"(${result.expected_profit_usd:.2f}) — skip"
                )
                return

            nonce   = await self._nonce_mgr.next()
            tx_data = await self._flash_builder.build(
                collateral_asset  = result.asset,
                debt_asset        = self._select_debt_asset(pos),
                borrower          = pos.address,
                debt_to_cover     = result.debt_to_cover,
                nonce             = nonce,
                collateral_amount = int(result.debt_to_cover * 1.05),
            )

            if tx_data is None:
                await self._nonce_mgr.rewind()
                return

            tx_hash = await blast_submit(tx_data["raw_tx"])
            if tx_hash:
                logger.info(
                    f"[AaveBase] Submitted — hash={tx_hash[:12]}… "
                    f"borrower={pos.address[:10]}… "
                    f"HF={pos.hf_float:.4f}"
                )
            else:
                await self._nonce_mgr.rewind()

        except Exception as e:
            logger.error(f"[AaveBase] Execute error: {e}")
        finally:
            self._in_flight.discard(pos.address)

    def _select_debt_asset(self, pos) -> str:
        """Select highest-USD-value debt asset from position reserves."""
        best_asset = None
        best_debt  = 0
        for reserve in pos.reserves:
            if reserve.total_debt > best_debt:
                best_debt  = reserve.total_debt
                best_asset = reserve.asset
        return best_asset or BaseChainConfig.WETH

    def status(self) -> str:
        return (
            f"[AaveBase] positions={self._loader.position_count if self._loader else 0} "
            f"in_flight={len(self._in_flight)}"
        )


# ---------------------------------------------------------------------------
# Deployment guide
# ---------------------------------------------------------------------------
#
# 1. Deploy FlashExecutorV3.sol for Base chain:
#
#    forge create contracts/FlashExecutorV3.sol:FlashExecutorV3 \
#      --constructor-args \
#        0xBA12222222228d8Ba445958a75a0704d566BF2C8 \
#        0xA238Dd80C259a72e81d7e4664a9801593F98d1c5 \
#        1000000 \
#      --rpc-url $BASE_RPC_URL \
#      --private-key $PK \
#      --verify
#
# 2. Approve Aerodrome router on Base executor:
#
#    cast send $BASE_EXECUTOR_ADDR \
#      "approveRouter(address)" 0xBE6D8f0d05cC4be24d5167a3eF062215bE6D18a5 \
#      --rpc-url $BASE_RPC_URL --private-key $PK
#
#    NOTE: Also approve Uni V3 SwapRouter as fallback:
#    cast send $BASE_EXECUTOR_ADDR \
#      "approveRouter(address)" 0x2626664c2603336E57B271c5C0b26F421741e481 \
#      --rpc-url $BASE_RPC_URL --private-key $PK
#
# 3. Build watchlist:
#
#    python3 - <<'EOF'
#    import asyncio, redis.asyncio as aioredis
#    from web3 import AsyncWeb3
#    from web3.providers import AsyncHTTPProvider
#    from aave_base import BaseWatchlistBuilder
#
#    async def main():
#        w3    = AsyncWeb3(AsyncHTTPProvider("https://mainnet.base.org"))
#        redis = aioredis.from_url("redis://localhost:6379")
#        b     = BaseWatchlistBuilder(w3, redis)
#        count = await b.backfill(start_block=2_357_000)
#        print(f"Done — {count} borrowers")
#        await redis.aclose()
#
#    asyncio.run(main())
#    EOF
#
# 4. Add to .env:
#    BASE_RPC_URL=https://mainnet.base.org  (or paid endpoint)
#    BASE_WSS_URL=wss://...
#    BASE_EXECUTOR_ADDR=0x...
#
# 5. Wire into pipeline_v3.py:
#
#    from aave_base import AaveBaseModule
#
#    # In setup():
#    self.base = AaveBaseModule(
#        rpc_http      = os.getenv("BASE_RPC_URL"),
#        rpc_wss       = os.getenv("BASE_WSS_URL"),
#        redis         = redis_client,
#        wallet        = WALLET_ADDR,
#        private_key   = PRIVATE_KEY,
#        executor_addr = os.getenv("BASE_EXECUTOR_ADDR"),
#        executor_abi  = EXECUTOR_ABI,   # same ABI as Arbitrum executor
#        skip_tel      = self.skip_tel,
#    )
#    await self.base.start()
#
#    # In Base block handler (separate WS subscription):
#    await self.base.on_new_block(block_number, base_fee_wei)
#
#    # In shutdown():
#    await self.base.stop()
#
# ---------------------------------------------------------------------------
