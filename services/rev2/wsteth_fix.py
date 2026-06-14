"""
wsteth_fix.py — wstETH price composition + Balancer TWAP fallback
Fixes two issues discovered during feed audit:

Issue 1: wstETH Chainlink feed returns ratio (1.2364), not USD price
    The feed at 0xb523... is wstETH/ETH exchange rate, not wstETH/USD.
    Pipeline was storing 1.2364e8 as the USD price → HF computed at
    ~$0.00000001 per wstETH instead of ~$2,028.
    Fix: compose wstETH/USD = wstETH/ETH ratio × ETH/USD price.

Issue 2: wstETH Chainlink has 24h heartbeat / 2% deviation threshold
    8h stale is within spec but PriceRegistry rejects it at 60s max age.
    Fix: Balancer wstETH/WETH pool TWAP as fallback when Chainlink stale.
    Also: route wstETH→WETH swaps through Balancer pool (better liquidity
    than Uni V3 for this pair on Arbitrum).

Components:
    WstETHPriceComposer   — computes wstETH/USD from ratio × ETH/USD
    BalancerTWAP          — reads wstETH/ETH price from Balancer pool
    WstETHPriceManager    — orchestrates both, updates PriceRegistry
    BalancerSwapRoute     — builds swap calldata for wstETH→WETH via Balancer

Usage:
    manager = WstETHPriceManager(
        rpc          = rpc_read,
        price_reg    = self.prices,
        poll_interval= 30,
    )
    await manager.start()

    # For flash loan swap route — in SwapCalldataBuilder.build():
    if token_in == WSTETH_ADDR:
        route = await BalancerSwapRoute.build(amount_in, min_out)
        return route  # instead of Uni V3 route
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

from web3 import AsyncWeb3

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Addresses — Arbitrum mainnet
# ---------------------------------------------------------------------------

WSTETH_ADDR       = "0x5979D7b546E38E414F7E9822514be443A4800529"
WETH_ADDR         = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

# Chainlink feeds
WSTETH_ETH_FEED   = "0xb523AE262D20A936BC152e6023996e46FDC2A95D"  # ratio feed
ETH_USD_FEED      = "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"  # USD feed

# Balancer wstETH/WETH pool on Arbitrum
BALANCER_VAULT    = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"
BALANCER_POOL_ID  = "0x36bf227d6bac96e2ab1ebb5492ecec69c691943f000200000000000000000316"
BALANCER_POOL_ADDR= "0x36bf227d6BaC96e2aB1EbB5492ECec69C691943F"

# Chainlink heartbeat for wstETH/ETH — 24h + buffer
WSTETH_MAX_AGE    = 90_000   # 25 hours

# ---------------------------------------------------------------------------
# ABIs
# ---------------------------------------------------------------------------

CHAINLINK_ABI = [
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"name": "roundId",         "type": "uint80"},
            {"name": "answer",          "type": "int256"},
            {"name": "startedAt",       "type": "uint256"},
            {"name": "updatedAt",       "type": "uint256"},
            {"name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
]

BALANCER_VAULT_ABI = [
    {
        "inputs": [
            {"name": "poolId",   "type": "bytes32"},
            {"name": "tokens",   "type": "address[]"},
            {"name": "amounts",  "type": "uint256[]"},
            {"name": "userData", "type": "bytes"},
        ],
        "name": "joinPool",
        "outputs": [],
        "stateMutability": "payable",
        "type": "function",
    },
    {
        "inputs": [{"name": "poolId", "type": "bytes32"}],
        "name": "getPoolTokens",
        "outputs": [
            {"name": "tokens",          "type": "address[]"},
            {"name": "balances",        "type": "uint256[]"},
            {"name": "lastChangeBlock", "type": "uint256"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {
                "components": [
                    {"name": "poolId",            "type": "bytes32"},
                    {"name": "kind",              "type": "uint8"},
                    {"name": "assetIn",           "type": "address"},
                    {"name": "assetOut",          "type": "address"},
                    {"name": "amount",            "type": "uint256"},
                    {"name": "userData",          "type": "bytes"},
                ],
                "name": "singleSwap",
                "type": "tuple",
            },
            {
                "components": [
                    {"name": "sender",            "type": "address"},
                    {"name": "fromInternalBalance","type": "bool"},
                    {"name": "recipient",         "type": "address"},
                    {"name": "toInternalBalance", "type": "bool"},
                ],
                "name": "funds",
                "type": "tuple",
            },
            {"name": "limit",     "type": "uint256"},
            {"name": "deadline",  "type": "uint256"},
        ],
        "name": "swap",
        "outputs": [{"name": "amountCalculated", "type": "uint256"}],
        "stateMutability": "payable",
        "type": "function",
    },
    {
        "inputs": [
            {
                "components": [
                    {"name": "poolId",            "type": "bytes32"},
                    {"name": "kind",              "type": "uint8"},
                    {"name": "assetIn",           "type": "address"},
                    {"name": "assetOut",          "type": "address"},
                    {"name": "amount",            "type": "uint256"},
                    {"name": "userData",          "type": "bytes"},
                ],
                "name": "singleSwap",
                "type": "tuple",
            },
            {
                "components": [
                    {"name": "sender",            "type": "address"},
                    {"name": "fromInternalBalance","type": "bool"},
                    {"name": "recipient",         "type": "address"},
                    {"name": "toInternalBalance", "type": "bool"},
                ],
                "name": "funds",
                "type": "tuple",
            },
            {"name": "limit",     "type": "uint256"},
            {"name": "deadline",  "type": "uint256"},
        ],
        "name": "querySwap",
        "outputs": [{"name": "amountCalculated", "type": "uint256"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]

# Balancer SwapKind enum
SWAP_GIVEN_IN  = 0  # exact amount in, compute amount out
SWAP_GIVEN_OUT = 1  # exact amount out, compute amount in


# ---------------------------------------------------------------------------
# WstETHPriceComposer — Fix Issue 1
# ---------------------------------------------------------------------------

class WstETHPriceComposer:
    """
    Computes wstETH/USD from wstETH/ETH ratio × ETH/USD price.

    Chainlink wstETH/ETH feed (0xb523...) returns the exchange rate:
        answer = 1.2364e8  (wstETH per ETH, 8 decimals)

    Chainlink ETH/USD feed (0x639F...) returns:
        answer = 1640.75e8  (USD per ETH, 8 decimals)

    Composed price:
        wstETH/USD = (wstETH/ETH ratio) × (ETH/USD price)
                   = 1.2364 × 1640.75
                   = ~$2,028.14 per wstETH
                   = 202814000000 (8 decimals)

    This is what should be stored in PriceRegistry for wstETH.
    """

    def __init__(self, w3: AsyncWeb3):
        self._wsteth_feed = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(WSTETH_ETH_FEED),
            abi=CHAINLINK_ABI,
        )
        self._eth_feed = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(ETH_USD_FEED),
            abi=CHAINLINK_ABI,
        )

    async def get_wsteth_usd(self) -> tuple[Optional[int], float]:
        """
        Returns (wsteth_usd_price, age_seconds).
        Price is in 8-decimal Chainlink format (e.g. 202814000000 = $2028.14).
        Returns (None, inf) if either feed fails or ratio feed is stale.
        """
        try:
            # Fetch both feeds concurrently
            ratio_task = asyncio.create_task(
                self._wsteth_feed.functions.latestRoundData().call()
            )
            eth_task = asyncio.create_task(
                self._eth_feed.functions.latestRoundData().call()
            )

            ratio_result, eth_result = await asyncio.gather(
                ratio_task, eth_task, return_exceptions=True
            )

            if isinstance(ratio_result, Exception):
                logger.warning(f"[WstETHComposer] wstETH/ETH feed failed: {ratio_result}")
                return None, float("inf")

            if isinstance(eth_result, Exception):
                logger.warning(f"[WstETHComposer] ETH/USD feed failed: {eth_result}")
                return None, float("inf")

            _, ratio,   _, ratio_updated,  _ = ratio_result
            _, eth_usd, _, eth_updated,    _ = eth_result

            # Age is the older of the two feeds
            now     = time.time()
            age     = now - min(ratio_updated, eth_updated)
            ratio_age = now - ratio_updated

            if ratio <= 0 or eth_usd <= 0:
                return None, float("inf")

            # Compose: wstETH/USD = ratio × ETH/USD
            # ratio is 18 decimals, eth_usd is 8 decimals
            # product is 26 decimals → /1e18 yields 8-decimal result
            wsteth_usd = int(ratio * eth_usd // 10**18)

            logger.debug(
                f"[WstETHComposer] "
                f"ratio={ratio/1e8:.4f} "
                f"eth_usd={eth_usd/1e8:.2f} "
                f"wsteth_usd={wsteth_usd/1e8:.2f} "
                f"ratio_age={ratio_age:.0f}s"
            )

            return wsteth_usd, ratio_age

        except Exception as e:
            logger.error(f"[WstETHComposer] Composition failed: {e}")
            return None, float("inf")


# ---------------------------------------------------------------------------
# BalancerTWAP — Fix Issue 2 (price fallback when Chainlink stale)
# ---------------------------------------------------------------------------

class BalancerTWAP:
    """
    Derives wstETH/ETH price from Balancer pool spot price.
    Used as fallback when Chainlink wstETH/ETH feed exceeds staleness threshold.

    Method: spot price from pool token balances.
    For a weighted pool with equal weights (50/50):
        spot_price = balance_WETH / balance_wstETH

    This is not a true TWAP (no time-weighted accumulator on Balancer V2
    weighted pools) but pool balances change slowly for this pair and
    provide a reliable price when Chainlink is temporarily stale.

    Pool: 0x36bf227d... (wstETH/WETH, Weighted, Arbitrum)
    Reserves: ~9,530 wstETH + ~10,912 WETH
    """

    def __init__(self, w3: AsyncWeb3):
        self._vault = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(BALANCER_VAULT),
            abi=BALANCER_VAULT_ABI,
        )
        self._pool_id = bytes.fromhex(BALANCER_POOL_ID[2:])  # strip 0x

    async def get_wsteth_eth_ratio(self) -> Optional[float]:
        """
        Returns wstETH/ETH ratio from pool balances, or None on failure.
        ratio > 1.0 (wstETH accumulates staking rewards so 1 wstETH > 1 ETH)
        """
        try:
            tokens, balances, _ = await self._vault.functions.getPoolTokens(
                self._pool_id
            ).call()

            # Find wstETH and WETH positions in token array
            wsteth_idx = None
            weth_idx   = None
            for i, token in enumerate(tokens):
                if token.lower() == WSTETH_ADDR.lower():
                    wsteth_idx = i
                elif token.lower() == WETH_ADDR.lower():
                    weth_idx = i

            if wsteth_idx is None or weth_idx is None:
                logger.warning("[BalancerTWAP] wstETH or WETH not found in pool tokens")
                return None

            wsteth_bal = balances[wsteth_idx]
            weth_bal   = balances[weth_idx]

            if wsteth_bal == 0:
                return None

            # Pool must have meaningful liquidity (≥1 WETH)
            if weth_bal < 10**18:
                logger.debug(f"[BalancerTWAP] Pool too thin: {weth_bal/1e18:.6f} WETH — skip")
                return None

            # Both tokens have 18 decimals — ratio is dimensionless
            ratio = weth_bal / wsteth_bal

            logger.debug(
                f"[BalancerTWAP] "
                f"wstETH_bal={wsteth_bal/1e18:.2f} "
                f"WETH_bal={weth_bal/1e18:.2f} "
                f"ratio={ratio:.4f}"
            )

            return ratio

        except Exception as e:
            logger.error(f"[BalancerTWAP] getPoolTokens failed: {e}")
            return None

    async def get_wsteth_usd(self, eth_usd_price: int) -> Optional[int]:
        """
        Returns wstETH/USD price in 8-decimal Chainlink format.
        Composes Balancer ratio × ETH/USD from PriceRegistry.

        Args:
            eth_usd_price: ETH/USD price from PriceRegistry (8 decimals)
        """
        ratio = await self.get_wsteth_eth_ratio()
        if ratio is None:
            return None

        # Convert ratio to 8-decimal format then compose
        ratio_8dec = int(ratio * 1e8)
        wsteth_usd = int(ratio_8dec * eth_usd_price / 1e8)

        logger.info(
            f"[BalancerTWAP] wstETH/USD via pool: "
            f"{wsteth_usd/1e8:.2f} "
            f"(ratio={ratio:.4f} × eth={eth_usd_price/1e8:.2f})"
        )

        return wsteth_usd


# ---------------------------------------------------------------------------
# WstETHPriceManager — orchestrates both, updates PriceRegistry
# ---------------------------------------------------------------------------

class WstETHPriceManager:
    """
    Manages wstETH/USD price in PriceRegistry.
    Primary:  Chainlink wstETH/ETH ratio × Chainlink ETH/USD
    Fallback: Balancer pool spot price × Chainlink ETH/USD

    Runs as background task, updates every poll_interval seconds.
    Switches to Balancer fallback automatically when Chainlink ratio
    feed exceeds WSTETH_MAX_AGE (25 hours).

    Usage:
        manager = WstETHPriceManager(rpc=rpc_read, price_reg=self.prices)
        await manager.start()
        # ... runs forever, updates PriceRegistry automatically
        await manager.stop()
    """

    def __init__(
        self,
        rpc,                    # AsyncRPCClient
        price_reg,              # PriceRegistry from execution_guards.py
        poll_interval: float = 30.0,
    ):
        self._price_reg   = price_reg
        self._interval    = poll_interval
        self._composer    = WstETHPriceComposer(rpc.w3)
        self._twap        = BalancerTWAP(rpc.w3)
        self._running     = False
        self._task        = None
        self._source      = "none"      # "chainlink" | "balancer" | "none"
        self._last_price  = 0
        self._poll_count  = 0

    async def start(self) -> None:
        # Immediate first update before pipeline starts processing
        await self._update()
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="wsteth_price")
        logger.info(
            f"[WstETHManager] Started — "
            f"initial price={self._last_price/1e8:.2f} "
            f"source={self._source}"
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()

    @property
    def current_price(self) -> int:
        return self._last_price

    @property
    def source(self) -> str:
        return self._source

    def status(self) -> str:
        return (
            f"wstETH/USD={self._last_price/1e8:.2f} "
            f"source={self._source} "
            f"polls={self._poll_count}"
        )

    async def _update(self) -> None:
        """Single update cycle — try Chainlink first, fall back to Balancer."""
        self._poll_count += 1

        # Get ETH/USD from PriceRegistry (needed for both paths)
        eth_usd = self._price_reg.get_price(WETH_ADDR)
        if eth_usd is None:
            logger.debug("[WstETHManager] ETH/USD not in PriceRegistry yet — skip")
            return

        # ── Primary: Chainlink composition ────────────────────────────────
        price, age = await self._composer.get_wsteth_usd()

        if price is not None and age < WSTETH_MAX_AGE:
            self._price_reg.update_price(WSTETH_ADDR, price)
            self._last_price = price
            self._source     = "chainlink"
            logger.debug(
                f"[WstETHManager] Chainlink: "
                f"${price/1e8:.2f} (age={age:.0f}s)"
            )
            return

        # ── Fallback: Balancer pool spot price ────────────────────────────
        if age >= WSTETH_MAX_AGE:
            logger.info(
                f"[WstETHManager] Chainlink stale ({age:.0f}s) — "
                f"switching to Balancer fallback"
            )

        price = await self._twap.get_wsteth_usd(eth_usd)

        if price is not None:
            self._price_reg.update_price(WSTETH_ADDR, price)
            self._last_price = price
            self._source     = "balancer"
            return

        # ── Both failed ───────────────────────────────────────────────────
        logger.warning("[WstETHManager] Both Chainlink and Balancer failed — wstETH price stale")
        self._source = "none"

    async def _loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._interval)
                await self._update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[WstETHManager] Loop error: {e}")
                await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# BalancerSwapRoute — better swap route for wstETH collateral
# ---------------------------------------------------------------------------

class BalancerSwapRoute:
    """
    Builds swap calldata for wstETH → WETH via Balancer pool.

    NOTE: As of 2026-06-08, the Balancer wstETH/WETH pool on Arbitrum has
    drained to <$0.10 TVL. This module falls through to Uni V3 for actual
    swap routing. The Balancer calldata path is preserved for when the pool
    recapitalizes.

    Swap selector 0x52bbbe29 verified in Vault bytecode.
    querySwap selector 0x43c0e7be NOT in Vault bytecode on Arbitrum.
    We compute expected output from pool reserve math instead.
    """

    # Balancer swap function selector: swap((bytes32,uint8,address,address,uint256,bytes),(address,bool,address,bool),uint256,uint256)
    # We encode the full calldata for the Balancer vault swap() call
    SWAP_SELECTOR = bytes.fromhex("52bbbe29")  # swap(tuple,tuple,uint256,uint256)

    def __init__(self, w3: AsyncWeb3, executor_address: str):
        self._vault    = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(BALANCER_VAULT),
            abi=BALANCER_VAULT_ABI,
        )
        self._executor = AsyncWeb3.to_checksum_address(executor_address)
        self._pool_id  = bytes.fromhex(BALANCER_POOL_ID[2:])

    async def quote(self, wsteth_amount: int) -> Optional[int]:
        """
        Quote WETH output for wstETH_amount via Balancer pool reserve math.

        Uses pool balances from getPoolTokens() and the constant-product formula
        (weighted pool with equal weights = constant product).

        querySwap() is not available on Arbitrum Vault (selector absent).
        We compute expected output instead:
            amountOut = weth_bal * amountIn / (wstETH_bal + amountIn)
            net = amountOut * (1 - 0.001)  // 0.1% Balancer fee

        Returns WETH amount or None on failure.
        """
        try:
            tokens, balances, _ = await self._vault.functions.getPoolTokens(
                self._pool_id
            ).call()

            wsteth_idx = weth_idx = None
            for i, token in enumerate(tokens):
                if token.lower() == WSTETH_ADDR.lower():
                    wsteth_idx = i
                elif token.lower() == WETH_ADDR.lower():
                    weth_idx = i

            if wsteth_idx is None or weth_idx is None:
                logger.warning("[BalancerSwap] wstETH or WETH not found in pool")
                return None

            wsteth_bal = balances[wsteth_idx]
            weth_bal   = balances[weth_idx]

            # Pool must have meaningful liquidity (≥1 WETH) to be usable
            if weth_bal < 10**18:
                logger.debug(
                    f"[BalancerSwap] Pool too thin: {weth_bal/1e18:.6f} WETH — skip"
                )
                return None

            # Constant product: amountOut = weth_bal * amountIn / (wstETH_bal + amountIn)
            # Apply 0.1% swap fee
            numerator   = weth_bal * wsteth_amount
            denominator = wsteth_bal + wsteth_amount

            if denominator == 0:
                return None

            raw_out = numerator // denominator
            fee     = raw_out // 1000   # 0.1% fee
            weth_out = raw_out - fee

            if weth_out == 0:
                return None

            logger.debug(
                f"[BalancerSwap] Quote (pool reserves): "
                f"{wsteth_amount/1e18:.4f} wstETH → "
                f"{weth_out/1e18:.6f} WETH "
                f"(pool: {wsteth_bal/1e18:.2f} wstETH + {weth_bal/1e18:.2f} WETH)"
            )
            return weth_out

        except Exception as e:
            logger.warning(f"[BalancerSwap] Quote failed: {e}")
            return None

    def build_calldata(
        self,
        wsteth_amount:  int,
        min_weth_out:   int,
        deadline_offset:int = 180,
    ) -> bytes:
        """
        Encode Balancer vault swap() calldata for wstETH → WETH.
        This replaces the Uni V3 exactInputSingle calldata for wstETH pairs.

        Returns bytes to pass as swapCalldata in executeLiquidation().
        The executor contract must approve Balancer vault for wstETH.
        """
        import time as _time
        deadline = int(_time.time()) + deadline_offset

        from eth_abi import encode

        # Encode swap() call: (singleSwap tuple, funds tuple, limit, deadline)
        encoded = encode(
            [
                "(bytes32,uint8,address,address,uint256,bytes)",  # singleSwap
                "(address,bool,address,bool)",                    # funds
                "uint256",                                        # limit
                "uint256",                                        # deadline
            ],
            [
                (
                    self._pool_id,
                    SWAP_GIVEN_IN,
                    AsyncWeb3.to_checksum_address(WSTETH_ADDR),
                    AsyncWeb3.to_checksum_address(WETH_ADDR),
                    wsteth_amount,
                    b"",
                ),
                (
                    self._executor,   # sender
                    False,            # fromInternalBalance
                    self._executor,   # recipient
                    False,            # toInternalBalance
                ),
                min_weth_out,
                deadline,
            ]
        )

        return self.SWAP_SELECTOR + encoded

    async def build(
        self,
        wsteth_amount: int,
        slippage_bps:  int = 50,
    ) -> Optional[tuple[bytes, int]]:
        """
        Quote then build swap calldata.
        Returns (calldata, quoted_weth_out) or None if quote fails.

        Drop-in for SwapCalldataBuilder.build() when token_in == WSTETH_ADDR:

            if token_in.lower() == WSTETH_ADDR.lower():
                result = await self.balancer_route.build(amount_in)
                if result:
                    calldata, weth_out = result
                    return SwapRoute(
                        router   = BALANCER_VAULT,
                        calldata = calldata,
                        fee_tier = 0,
                        amount_in= wsteth_amount,
                        amount_out= weth_out,
                        slippage_pct = slippage_bps / 10_000,
                    )
        """
        weth_out = await self.quote(wsteth_amount)
        if weth_out is None or weth_out == 0:
            return None

        min_out  = int(weth_out * (10_000 - slippage_bps) / 10_000)
        calldata = self.build_calldata(wsteth_amount, min_out)

        return calldata, weth_out


# ---------------------------------------------------------------------------
# pipeline_v3.py integration
# ---------------------------------------------------------------------------
#
# 1. Import:
#       from wsteth_fix import WstETHPriceManager, BalancerSwapRoute
#
# 2. In setup(), after self.prices and self.rpc_read are ready:
#       self.wsteth_mgr = WstETHPriceManager(
#           rpc       = self.rpc_read,
#           price_reg = self.prices,
#           poll_interval = 30,
#       )
#       await self.wsteth_mgr.start()
#       logger.info(f"[Setup] {self.wsteth_mgr.status()}")
#
# 3. In stats/wallet loop:
#       logger.info(self.wsteth_mgr.status())
#
# 4. In shutdown():
#       await self.wsteth_mgr.stop()
#
# ── flash_loan_route.py integration ──────────────────────────────────────
#
# In SwapCalldataBuilder.__init__(), add:
#       self._balancer_route = BalancerSwapRoute(w3, executor_address)
#
# In SwapCalldataBuilder.build(), add wstETH detection before fee tier loop:
#
#       # Route wstETH → WETH through Balancer (better liquidity than Uni V3)
#       if token_in.lower() == WSTETH_ADDR.lower():
#           result = await self._balancer_route.build(amount_in, slippage_bps)
#           if result:
#               calldata, weth_out = result
#               slippage = slippage_bps / 10_000
#               logger.info(
#                   f"[SwapBuilder] wstETH→WETH via Balancer: "
#                   f"{amount_in/1e18:.4f} → {weth_out/1e18:.4f} WETH"
#               )
#               return SwapRoute(
#                   router      = BALANCER_VAULT,
#                   calldata    = calldata,
#                   fee_tier    = 0,       # Balancer, not Uni V3
#                   amount_in   = amount_in,
#                   amount_out  = weth_out,
#                   slippage_pct= slippage,
#               )
#           logger.warning("[SwapBuilder] Balancer route failed — falling through to Uni V3")
#           # Fall through to existing Uni V3 fee tier logic as backup
#
# ── price_poller.py fix ───────────────────────────────────────────────────
#
# WstETHPriceManager replaces the raw Chainlink wstETH feed polling.
# In price_poller.py ARBITRUM_CHAINLINK_FEEDS, remove the wstETH entry:
#
#       # REMOVE this line from ARBITRUM_CHAINLINK_FEEDS:
#       "0x5979D7b546E38E414F7E9822514be443A4800529": "0xb523AE262D20A936BC152e6023996e46FDC2A95D",
#
# WstETHPriceManager handles wstETH pricing directly via composition.
# Having both active would cause PricePoller to overwrite the correct
# composed price with the raw ratio value every 30s.
#
# ---------------------------------------------------------------------------
#
# Expected log output after integration:
#
#   [WstETHManager] Started — initial price=2028.14 source=chainlink
#   [WstETHManager] Chainlink stale (86400s) — switching to Balancer fallback
#   [BalancerTWAP] wstETH/USD via pool: 2031.20 (ratio=1.2380 × eth=1639.90)
#   [WstETHManager] wstETH/USD=2031.20 source=balancer polls=2881
#
#   # When wstETH collateral liquidation fires:
#   [SwapBuilder] wstETH→WETH via Balancer: 0.4920 → 0.3978 WETH
#
# ---------------------------------------------------------------------------
