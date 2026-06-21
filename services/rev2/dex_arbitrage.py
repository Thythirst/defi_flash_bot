"""
dex_arbitrage.py — Two-point DEX arbitrage (Camelot vs Uniswap V3)
Monitors price spreads between DEXs and executes flash-loan arbitrage
when the spread exceeds gas + flash fee + slippage.

This reuses the existing infrastructure:
    - MultiDexRouter for quoting both DEXs
    - Balancer flash loans (0% fee) for capital
    - FlashExecutorV3 generic swap proxy for execution
    - Gas oracle for competitive bidding

Strategy:
    For a monitored pair (e.g. WETH/USDC):
    1. Quote buying WETH on Camelot, selling on Uni V3 (and vice versa)
    2. If sell_price - buy_price > gas + flash_fee + slippage → profitable
    3. Flash loan the input token, buy on cheap DEX, sell on expensive DEX,
       repay flash loan, keep the spread

This is legitimate, non-predatory MEV:
    - No user is harmed — you're correcting a price imbalance between pools
    - Arbitrage makes markets more efficient (prices converge)
    - Same risk profile as liquidations: atomic, flash-backed, no directional exposure

Honest limitation:
    On Arbitrum's centralized sequencer, you react to price changes AFTER
    the block lands (limited mempool visibility). The edge is reaction speed,
    which is why co-location matters. This is not front-running — it's
    backrunning the price change that's already public.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

from web3 import AsyncWeb3

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Tokens (Arbitrum)
WETH   = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC   = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
USDT   = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"
ARB    = "0x912CE59144191C1204E64559FE8253a0e49E6548"
WBTC   = "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f"

DECIMALS = {
    WETH: 18, USDC: 6, USDT: 6, ARB: 18, WBTC: 8,
}

# Balancer flash loan (0% fee — the key advantage)
BALANCER_VAULT = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"

# Pairs to monitor for arbitrage
# Each entry: (token_a, token_b, test_size_a)
# test_size is the probe amount — actual size scales to available liquidity
MONITORED_PAIRS = [
    (WETH, USDC, int(1e18)),      # 1 WETH probe
    (WETH, USDT, int(1e18)),
    (ARB,  USDC, int(1000e18)),   # 1000 ARB probe
    (WBTC, USDC, int(int(0.05e8))), # 0.05 WBTC probe
    (WBTC, WETH, int(int(0.05e8))),
]

# Minimum profit threshold — must clear this after all costs
MIN_PROFIT_USD = 5.0

# Flash loan fee (Balancer = 0%, but keep for other providers)
FLASH_FEE_BPS = 0

# Estimated gas cost per arb tx (2 swaps + flash loan overhead)
ARB_GAS_UNITS = 450_000

# Scan interval — how often to check spreads (every block ideally)
SCAN_INTERVAL = 2.0


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ArbOpportunity:
    token_in:        str      # token we flash-borrow
    token_out:       str      # intermediate token
    amount_in:       int
    buy_dex:         str      # where we buy token_out (cheap)
    sell_dex:        str      # where we sell token_out (expensive)
    buy_router:      str
    sell_router:     str
    expected_out:    int      # token_in returned after round trip
    gross_profit:    int      # in token_in units
    gross_profit_usd:float
    gas_cost_usd:    float
    net_profit_usd:  float
    spread_pct:      float

    def is_profitable(self) -> bool:
        return self.net_profit_usd >= MIN_PROFIT_USD


# ---------------------------------------------------------------------------
# ArbitrageScanner
# ---------------------------------------------------------------------------

class ArbitrageScanner:
    """
    Scans monitored pairs for two-point arbitrage between Camelot and Uni V3.

    Reuses MultiDexRouter's per-DEX quoting. For each pair, checks both
    directions (buy A→B on Camelot/sell on UniV3, and reverse), finds the
    profitable cycle, and constructs the flash-loan arb if it clears costs.

    Usage:
        scanner = ArbitrageScanner(
            multi_dex    = self._multi_dex,    # existing MultiDexRouter
            shared_state = self.shared_state,  # for gas + ETH price
            price_reg    = self.prices,        # for USD valuation
        )
        # In block handler:
        opp = await scanner.scan_once()
        if opp and opp.is_profitable():
            await self.execute_arb(opp)
    """

    def __init__(
        self,
        multi_dex,                 # MultiDexRouter instance
        shared_state = None,       # SharedState — gas price
        price_reg    = None,       # PriceRegistry — USD valuation
        min_profit_usd: float = MIN_PROFIT_USD,
    ):
        self._multi_dex   = multi_dex
        self._state       = shared_state
        self._prices      = price_reg
        self._min_profit  = min_profit_usd
        self._scans       = 0
        self._opportunities_found = 0

    async def scan_once(self) -> Optional[ArbOpportunity]:
        """
        Scan all monitored pairs, return the most profitable opportunity or None.
        """
        self._scans += 1

        tasks = [
            self._check_pair(token_a, token_b, size)
            for token_a, token_b, size in MONITORED_PAIRS
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        best: Optional[ArbOpportunity] = None
        for r in results:
            if isinstance(r, Exception) or r is None:
                continue
            if best is None or r.net_profit_usd > best.net_profit_usd:
                best = r

        if best and best.is_profitable():
            self._opportunities_found += 1
            logger.info(
                f"[Arb] OPPORTUNITY: {best.token_in[:8]}→{best.token_out[:8]} "
                f"buy={best.buy_dex} sell={best.sell_dex} "
                f"spread={best.spread_pct:.3f}% "
                f"net=${best.net_profit_usd:.2f}"
            )

        return best

    async def _check_pair(
        self, token_a: str, token_b: str, amount_a: int,
    ) -> Optional[ArbOpportunity]:
        """
        Check both arbitrage directions for a pair.

        Direction 1: A → B on DEX1, B → A on DEX2 (round trip in A)
        We want: end with more A than we started.
        """
        # Quote A→B on both DEXs
        # Then for the winning B amount, quote B→A on the OTHER dex
        # If round trip yields more A than amount_a → arbitrage exists

        # Get individual DEX quotes (not "best" — we need per-DEX)
        camelot_ab = await self._quote_single_dex("camelot", token_a, token_b, amount_a)
        univ3_ab   = await self._quote_single_dex("univ3",   token_a, token_b, amount_a)

        if camelot_ab is None or univ3_ab is None:
            return None

        # Direction 1: buy B on Camelot (more B out), sell B on Uni V3
        # Direction 2: buy B on Uni V3, sell B on Camelot
        # Pick whichever DEX gives more B for buying, sell on the other

        opportunities = []

        # Try: buy B where it's cheaper (more B per A), sell where it's pricier
        if camelot_ab > univ3_ab:
            # Camelot gives more B — buy there, sell B back to A on Uni V3
            b_amount = camelot_ab
            a_back   = await self._quote_single_dex("univ3", token_b, token_a, b_amount)
            if a_back:
                opportunities.append(("camelot", "univ3", b_amount, a_back))
        else:
            # Uni V3 gives more B — buy there, sell B back to A on Camelot
            b_amount = univ3_ab
            a_back   = await self._quote_single_dex("camelot", token_b, token_a, b_amount)
            if a_back:
                opportunities.append(("univ3", "camelot", b_amount, a_back))

        if not opportunities:
            return None

        buy_dex, sell_dex, b_amount, a_back = opportunities[0]

        # Gross profit in token_a units
        gross_profit = a_back - amount_a
        if gross_profit <= 0:
            return None

        # Value the profit in USD
        a_price_usd = self._get_price_usd(token_a)
        if a_price_usd is None:
            return None
        a_dec            = DECIMALS.get(token_a, 18)
        gross_profit_usd = (gross_profit / 10**a_dec) * a_price_usd

        # Gas cost
        gas_cost_usd = self._estimate_gas_cost_usd()

        # Net
        net_profit_usd = gross_profit_usd - gas_cost_usd
        spread_pct     = (gross_profit / amount_a) * 100

        return ArbOpportunity(
            token_in         = token_a,
            token_out        = token_b,
            amount_in        = amount_a,
            buy_dex          = buy_dex,
            sell_dex         = sell_dex,
            buy_router       = self._router_for(buy_dex),
            sell_router      = self._router_for(sell_dex),
            expected_out     = a_back,
            gross_profit     = gross_profit,
            gross_profit_usd = gross_profit_usd,
            gas_cost_usd     = gas_cost_usd,
            net_profit_usd   = net_profit_usd,
            spread_pct       = spread_pct,
        )

    async def _quote_single_dex(
        self, dex: str, token_in: str, token_out: str, amount_in: int,
    ) -> Optional[int]:
        """Get a quote from one specific DEX. Returns amount_out or None."""
        try:
            if dex == "camelot":
                result = await self._multi_dex._quote_camelot(
                    AsyncWeb3.to_checksum_address(token_in),
                    AsyncWeb3.to_checksum_address(token_out),
                    amount_in,
                )
            else:  # univ3 — try both fee tiers, take best
                from multi_dex_router import UNIV3_FEE_TIERS
                quotes = await asyncio.gather(*[
                    self._multi_dex._quote_univ3(
                        AsyncWeb3.to_checksum_address(token_in),
                        AsyncWeb3.to_checksum_address(token_out),
                        amount_in, fee,
                    )
                    for fee in UNIV3_FEE_TIERS
                ], return_exceptions=True)
                valid = [q for q in quotes
                         if q is not None and not isinstance(q, Exception)]
                if not valid:
                    return None
                result = max(valid, key=lambda q: q.amount_out)

            return result.amount_out if result else None
        except Exception as e:
            logger.debug(f"[Arb] {dex} quote failed: {e}")
            return None

    def _router_for(self, dex: str) -> str:
        from multi_dex_router import CAMELOT_ROUTER, UNIV3_ROUTER
        return CAMELOT_ROUTER if dex == "camelot" else UNIV3_ROUTER

    def _get_price_usd(self, asset: str) -> Optional[float]:
        """Get asset USD price from PriceRegistry (8-decimal Chainlink format)."""
        if self._prices is None:
            return None
        try:
            price = self._prices.get_price(asset)
            if price is None or price == 0:
                # Stablecoins
                if asset in (USDC, USDT):
                    return 1.0
                return None
            return price / 1e8  # Chainlink 8-decimal → USD float
        except Exception:
            if asset in (USDC, USDT):
                return 1.0
            return None

    def _estimate_gas_cost_usd(self) -> float:
        """Estimate gas cost for the arb tx in USD."""
        base_fee = (
            self._state.base_fee_wei
            if self._state and self._state.base_fee_wei > 0
            else 100_000_000   # 0.1 gwei default
        )
        # Add priority fee estimate (use 2x base as rough total)
        gas_price_wei = base_fee * 2
        gas_cost_eth  = (ARB_GAS_UNITS * gas_price_wei) / 1e18
        eth_price     = self._get_price_usd(WETH) or 1640.0
        return gas_cost_eth * eth_price

    @property
    def stats(self) -> dict:
        return {
            "scans":               self._scans,
            "opportunities_found": self._opportunities_found,
        }

    def log_stats(self) -> None:
        s = self.stats
        logger.info(
            f"[Arb] scans={s['scans']} "
            f"opportunities={s['opportunities_found']}"
        )


# ---------------------------------------------------------------------------
# ArbExecutor — builds and submits the flash-loan arb transaction
# ---------------------------------------------------------------------------

class ArbExecutor:
    """
    Builds the flash-loan arbitrage transaction.

    Flow (atomic, single tx):
        1. Flash loan `amount_in` of token_in from Balancer (0% fee)
        2. Swap token_in → token_out on buy_dex (cheap)
        3. Swap token_out → token_in on sell_dex (expensive)
        4. Repay flash loan (amount_in)
        5. Keep the difference (profit)

    NOTE: This requires an arbitrage executor contract that:
        - Receives the Balancer flash loan
        - Executes two swaps via approved routers
        - Repays the loan
        - Sends profit to owner

    The existing FlashExecutorV3 does liquidation+swap. An arb executor
    is similar but does swap+swap instead of liquidate+swap. This may
    require a new contract method `executeArb()` or a separate contract.

    See deployment notes below.
    """

    def __init__(
        self,
        w3: AsyncWeb3,
        arb_executor_address: str,
        wallet: str,
        private_key: str,
        shared_state = None,
    ):
        self._w3       = w3
        self._executor = AsyncWeb3.to_checksum_address(arb_executor_address)
        self._wallet   = AsyncWeb3.to_checksum_address(wallet)
        self._pk       = private_key
        self._state    = shared_state

    async def build_arb_tx(
        self,
        opp:   ArbOpportunity,
        nonce: int,
        multi_dex,
        slippage_bps: int = 30,
    ) -> Optional[dict]:
        """
        Build the flash-loan arb transaction.

        The arb executor contract receives:
            - flashToken, flashAmount (Balancer flash loan params)
            - buyRouter, buyCalldata (first swap)
            - sellRouter, sellCalldata (second swap)
            - minProfit (revert if not met)
        """
        from web3 import Web3

        # Build buy swap calldata (token_in → token_out on buy_dex)
        buy_quote = await self._build_swap_quote(
            multi_dex, opp.buy_dex,
            opp.token_in, opp.token_out, opp.amount_in, slippage_bps,
        )
        if buy_quote is None:
            return None
        buy_calldata, expected_out = buy_quote

        # Build sell swap calldata (token_out → token_in on sell_dex)
        sell_quote = await self._build_swap_quote(
            multi_dex, opp.sell_dex,
            opp.token_out, opp.token_in, expected_out, slippage_bps,
        )
        if sell_quote is None:
            return None
        sell_calldata, _ = sell_quote

        # Minimum profit — revert if arb doesn't clear this
        min_profit = int(opp.amount_in * (1 + opp.spread_pct / 100 * 0.5))

        # Gas
        base_fee = self._state.base_fee_wei if self._state else 100_000_000
        max_fee  = int(base_fee * 5.0) + int(base_fee * 0.5)
        priority = int(base_fee * 0.5)

        # NOTE: This assumes an executeArb method on the arb executor.
        # Encode the call — adjust signature to match deployed contract.
        try:
            sync_w3 = Web3()
            # Placeholder ABI encoding — replace with actual contract method
            logger.info(
                f"[ArbExec] Built arb: {opp.token_in[:8]}→{opp.token_out[:8]} "
                f"buy={opp.buy_dex} sell={opp.sell_dex} "
                f"min_profit={min_profit} net=${opp.net_profit_usd:.2f}"
            )
            return {
                "opportunity":   opp,
                "buy_calldata":  buy_calldata,
                "sell_calldata": sell_calldata,
                "min_profit":    min_profit,
                "max_fee":       max_fee,
                "priority":      priority,
                "nonce":         nonce,
            }
        except Exception as e:
            logger.error(f"[ArbExec] Build failed: {e}")
            return None

    async def _build_swap_quote(
        self, multi_dex, dex, token_in, token_out, amount_in, slippage_bps,
    ):
        """Build swap calldata for a specific DEX."""
        from multi_dex_router import DexQuote, CAMELOT_ROUTER, UNIV3_ROUTER

        if dex == "camelot":
            q = await multi_dex._quote_camelot(token_in, token_out, amount_in)
        else:
            from multi_dex_router import UNIV3_FEE_TIERS
            quotes = await asyncio.gather(*[
                multi_dex._quote_univ3(token_in, token_out, amount_in, fee)
                for fee in UNIV3_FEE_TIERS
            ], return_exceptions=True)
            valid = [x for x in quotes if x and not isinstance(x, Exception)]
            q = max(valid, key=lambda x: x.amount_out) if valid else None

        if q is None or q.amount_out == 0:
            return None

        amount_out_min = int(q.amount_out * (10_000 - slippage_bps) / 10_000)
        calldata = multi_dex.encode_calldata(q, amount_out_min)
        return calldata, q.amount_out


# ---------------------------------------------------------------------------
# Arb executor contract — deployment notes
# ---------------------------------------------------------------------------
#
# The existing FlashExecutorV3 does liquidate+swap. Arbitrage needs swap+swap
# inside a flash loan. You need an ArbExecutor contract:
#
#   contract ArbExecutor {
#       function executeArb(
#           address flashToken,
#           uint256 flashAmount,
#           address buyRouter,
#           bytes   calldata buyCalldata,
#           address sellRouter,
#           bytes   calldata sellCalldata,
#           uint256 minProfit
#       ) external onlyOwner {
#           // 1. Request Balancer flash loan
#           // 2. In receiveFlashLoan callback:
#           //    a. approve buyRouter, buyRouter.call(buyCalldata)
#           //    b. approve sellRouter, sellRouter.call(sellCalldata)
#           //    c. require(balance >= flashAmount + minProfit)
#           //    d. repay flashAmount to Balancer
#           //    e. transfer profit to owner
#       }
#   }
#
# This is structurally similar to your existing executor — same flash loan
# pattern, same generic router.call() swaps, just two swaps instead of
# liquidate+swap. Whitelist the same routers (Camelot, Uni V3) already approved.
#
# ---------------------------------------------------------------------------
#
# pipeline integration:
#
# 1. Import:
#       from dex_arbitrage import ArbitrageScanner, ArbExecutor
#
# 2. In setup():
#       self.arb_scanner = ArbitrageScanner(
#           multi_dex    = self.flash_builder._swap_builder._multi_dex,
#           shared_state = self.shared_state,
#           price_reg    = self.prices,
#           min_profit_usd = 5.0,
#       )
#       self.arb_executor = ArbExecutor(
#           w3 = self.rpc.w3,
#           arb_executor_address = os.getenv("ARB_EXECUTOR_ADDR"),
#           wallet = WALLET_ADDR,
#           private_key = PRIVATE_KEY,
#           shared_state = self.shared_state,
#       )
#
# 3. In block handler (on_new_block), after liquidation checks:
#       opp = await self.arb_scanner.scan_once()
#       if opp and opp.is_profitable():
#           nonce = await self.nonce_mgr.next()
#           arb_tx = await self.arb_executor.build_arb_tx(
#               opp, nonce, self.arb_scanner._multi_dex
#           )
#           if arb_tx:
#               # submit via blast_submit
#               ...
#
# 4. In stats loop:
#       self.arb_scanner.log_stats()
#
# ---------------------------------------------------------------------------
#
# IMPORTANT — realistic expectations:
#
# Two-point arb between Camelot and Uni V3 on major pairs (WETH/USDC) is
# HIGHLY competitive. Spreads are usually arbed away within one block by
# bots with co-located infrastructure. You will mostly see:
#   - Tiny spreads (< gas cost) — not profitable
#   - Occasional real spreads after large trades — race to capture
#
# The profitable opportunities are:
#   - Less liquid pairs (ARB/USDC, WBTC/WETH) where fewer bots compete
#   - Moments right after a large swap unbalances a pool
#   - Times of high volatility when prices move fast
#
# This is why co-location matters MORE for arb than liquidations.
# Liquidations have a ~12s window (oracle heartbeat). Arb spreads close
# in 1-2 blocks. Without co-location, you'll lose most arb races.
#
# Recommendation: deploy this AFTER migrating to AWS/co-location.
# Running it from Kenya (~200ms latency) will lose nearly every arb race.
# ---------------------------------------------------------------------------
