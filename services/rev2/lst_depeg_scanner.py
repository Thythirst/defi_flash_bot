"""
lst_depeg_scanner.py — LST/LRT depeg detection (OBSERVE-ONLY MODE)

Scope, deliberately narrow for v1:
  - wstETH only (the LST already integrated elsewhere in this codebase)
  - Pure DEX-arb detection only — NEVER redemption. Redemption takes
    days to unstake and breaks transaction atomicity. We only look
    for: buy cheap on DEX A, sell on DEX B (or back to fair value).
  - OBSERVE ONLY. This module logs candidate depegs. It does NOT
    build transactions, does NOT call flash loan executors, does NOT
    submit anything on-chain. That comes later, only after real
    observed data justifies it.
  - FAST DEPEGS ONLY. Read this carefully — it materially limits what
    this module can responsibly claim to detect, see below.

The critical lesson baked into this design (from the Base weETH
whale incident): a depeg can be mathematically real — fair value
genuinely diverges from the DEX price — and still be completely
UNEXECUTABLE if the pool doesn't have enough depth to absorb the
trade size without slippage eating the entire spread. A 1-token test
quote can look perfectly clean while a $500K trade would face 90%+
slippage. This module ALWAYS checks depth at the REQUIRED TRADE SIZE
before flagging anything as a real opportunity, not a token-amount
sanity quote.

FAIR VALUE SOURCE — important limitation, read before trusting this:
Arbitrum's wstETH is a bridged ERC20 wrapper, NOT the mainnet Lido
contract. It does NOT expose stEthPerToken() / tokensPerStEth() /
getStETHByWstETH() — those all revert on this chain. Verified directly
on-chain before writing this version (a wrong assumption here would
have silently broken every downstream calculation).

The only viable on-chain fair-value source on Arbitrum is the
Chainlink WSTETH/ETH feed (0xb523AE262D20A936BC152e6023996e46FDC2A95).
This feed has a 24h heartbeat / 2% deviation trigger — meaning:

  FAST depegs (>2% move)  → Chainlink's deviation trigger fires within
                             minutes. Fair value stays fresh. This
                             module detects these reliably.

  SLOW depegs (<2%, over
  many hours)              → Chainlink does NOT update (no deviation
                             trigger, waiting on the 24h heartbeat).
                             Fair value can be up to 24h stale.
                             A DEX-derived moving-average fallback was
                             considered and DELIBERATELY REJECTED: it
                             would be circular (comparing the market
                             against a lagged version of itself), since
                             a slow drift gets partially absorbed into
                             any DEX-based moving average too. There is
                             no independent ground truth for slow drifts
                             on this chain with current tooling.

This module explicitly does NOT attempt to catch slow depegs. It gates
on feed freshness (MAX_FEED_AGE_SECONDS) and skips the cycle entirely,
loudly, rather than silently using a stale number. This is the same
staleness-gate discipline already used elsewhere in this codebase
(StalenessPoller) — degrade visibly, never silently.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from web3 import AsyncWeb3

logger = logging.getLogger("lst_depeg_scanner")

# ─── Arbitrum addresses ──────────────────────────────────────────────────
WSTETH = "0x5979D7b546E38E414F7E9822514be443A4800529"
WETH = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"

CAMELOT_QUOTER = "0x0Fc73040b26E9bC8514fA028D998E73A254Fa76E"  # EIP-55 checksum
UNIV3_QUOTER_V2 = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"  # Arbitrum QuoterV2

# Chainlink WSTETH/ETH feed — the ONLY viable on-chain fair-value source
# on Arbitrum. Verified directly on-chain: Arbitrum's wstETH is a bridged
# ERC20 wrapper and does NOT expose Lido's native exchange-rate methods
# (stEthPerToken / tokensPerStEth / getStETHByWstETH all revert here).
CHAINLINK_WSTETH_ETH = "0xb523AE262D20A936BC152e6023996e46FDC2A95D"

# Feed has a 24h heartbeat / 2% deviation trigger. If the last update is
# older than this, we DO NOT trust it — skip the cycle rather than use
# a degraded number. This caps what we can detect to FAST depegs only
# (>2% moves trigger Chainlink's deviation update within minutes).
MAX_FEED_AGE_SECONDS = 2 * 60 * 60  # 2 hours

CHAINLINK_FEED_ABI = [
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"name": "roundId", "type": "uint80"},
            {"name": "answer", "type": "int256"},
            {"name": "startedAt", "type": "uint256"},
            {"name": "updatedAt", "type": "uint256"},
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

UNIV3_QUOTER_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"name": "tokenIn", "type": "address"},
                    {"name": "tokenOut", "type": "address"},
                    {"name": "amountIn", "type": "uint256"},
                    {"name": "fee", "type": "uint24"},
                    {"name": "sqrtPriceLimitX96", "type": "uint160"},
                ],
                "name": "params",
                "type": "tuple",
            }
        ],
        "name": "quoteExactInputSingle",
        "outputs": [
            {"name": "amountOut", "type": "uint256"},
            {"name": "sqrtPriceX96After", "type": "uint160"},
            {"name": "initializedTicksCrossed", "type": "uint32"},
            {"name": "gasEstimate", "type": "uint256"},
        ],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

CAMELOT_QUOTER_ABI = [
    {
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
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

# ─── Config ──────────────────────────────────────────────────────────────
DEPEG_ALERT_THRESHOLD_PCT = 1.0  # log candidates above this
MIN_VIABLE_DEPEG_PCT = 1.5       # only call it a real OPPORTUNITY above this
                                   # (gives headroom over gas + fees)

# UniV3 fee tier for wstETH/WETH on Arbitrum. VERIFIED on-chain:
#   fee=100 (0.01%): real liquidity, usable quotes (~1.2376 WETH/wstETH)
#   fee=500 (0.05%): NEAR-EMPTY — a 1 wstETH test quote returned ~0.0145
#                     WETH, i.e. the pool cannot support real volume.
#                     Do NOT default to 500 the way generic examples do.
UNIV3_FEE_TIER = 100

# Trade sizes to check depth at — escalating, so we know the REAL
# executable size, not just whether SOME size works
DEPTH_CHECK_SIZES_ETH = [1, 5, 20, 50, 100]

# Max acceptable slippage at the trade size we'd actually want to run
MAX_ACCEPTABLE_SLIPPAGE_PCT = 2.0

SCAN_INTERVAL_SECONDS = 15


@dataclass
class DepegCandidate:
    timestamp: float
    fair_value_weth: float          # what 1 wstETH SHOULD be worth, per Chainlink
    camelot_price_weth: float       # what Camelot actually quotes for 1 wstETH
    univ3_price_weth: float         # what UniV3 actually quotes for 1 wstETH
    cheapest_venue: str
    deepest_venue: str
    raw_depeg_pct: float            # based on 1-unit quotes only — NOT executable size
    feed_age_seconds: float = 0.0   # how stale the Chainlink feed was at scan time
    max_executable_size_eth: float = 0.0   # largest size that clears slippage check
    executable_profit_usd: float = 0.0     # profit AT that max executable size
    is_real_opportunity: bool = False       # only True if depth check passes
    rejection_reason: str = ""


class LSTDepegScanner:
    """
    Observe-only scanner. Computes fair value vs DEX price for wstETH,
    and — critically — validates actual executable depth before ever
    calling something a real opportunity.
    """

    def __init__(self, w3: AsyncWeb3, eth_price_usd_fn):
        """
        w3: an AsyncWeb3 instance already connected to a working RPC
        eth_price_usd_fn: async callable returning current ETH/USD,
                          so we can express findings in USD (reuse the
                          pipeline's existing PriceRegistry — do not
                          build a second, separate price source)
        """
        self.w3 = w3
        self.get_eth_price_usd = eth_price_usd_fn

        self.chainlink_feed = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(CHAINLINK_WSTETH_ETH),
            abi=CHAINLINK_FEED_ABI,
        )
        self._feed_decimals: Optional[int] = None  # cached after first read

        self.univ3_quoter = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(UNIV3_QUOTER_V2),
            abi=UNIV3_QUOTER_ABI,
        )
        self.camelot_quoter = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(CAMELOT_QUOTER),
            abi=CAMELOT_QUOTER_ABI,
        )

        self._running = False
        self.candidates_log: list[DepegCandidate] = []

    # ─── Fair value (Chainlink, staleness-gated) ──────────────────────
    async def get_fair_value_weth(self) -> tuple[Optional[float], float]:
        """
        Reads the Chainlink WSTETH/ETH feed directly. Returns
        (fair_value_weth, age_seconds). fair_value_weth is None if the
        feed is stale beyond MAX_FEED_AGE_SECONDS — callers MUST check
        for None and skip the cycle rather than substitute anything.

        This is the ONLY fair-value source for this module. There is
        deliberately no fallback to a DEX-derived average — see the
        module docstring for why that would be circular and unsound
        for the slow-depeg case specifically.
        """
        if self._feed_decimals is None:
            self._feed_decimals = await self.chainlink_feed.functions.decimals().call()

        round_data = await self.chainlink_feed.functions.latestRoundData().call()
        answer = round_data[1]
        updated_at = round_data[3]

        now = time.time()
        age_seconds = now - updated_at

        if age_seconds > MAX_FEED_AGE_SECONDS:
            logger.warning(
                f"[LSTDepeg] Chainlink WSTETH/ETH feed is {age_seconds/3600:.1f}h "
                f"stale (max allowed {MAX_FEED_AGE_SECONDS/3600:.1f}h) — "
                f"SKIPPING cycle. This is expected during slow/no price "
                f"movement (feed only updates on 2% deviation or 24h "
                f"heartbeat). Not a bug — see module docstring."
            )
            return None, age_seconds

        fair_value = answer / (10 ** self._feed_decimals)
        return fair_value, age_seconds

    # ─── DEX quotes at a given size ───────────────────────────────────
    async def quote_univ3(self, amount_in_wsteth: float, fee_tier: int = UNIV3_FEE_TIER) -> Optional[float]:
        try:
            amount_in_wei = int(amount_in_wsteth * 1e18)
            result = await self.univ3_quoter.functions.quoteExactInputSingle(
                (
                    AsyncWeb3.to_checksum_address(WSTETH),
                    AsyncWeb3.to_checksum_address(WETH),
                    amount_in_wei,
                    fee_tier,
                    0,
                )
            ).call()
            amount_out_wei = result[0]
            return amount_out_wei / 1e18
        except Exception as e:
            logger.debug(f"[LSTDepeg] UniV3 quote failed @ {amount_in_wsteth}: {e}")
            return None

    async def quote_camelot(self, amount_in_wsteth: float) -> Optional[float]:
        try:
            amount_in_wei = int(amount_in_wsteth * 1e18)
            result = await self.camelot_quoter.functions.quoteExactInputSingle(
                AsyncWeb3.to_checksum_address(WSTETH),
                AsyncWeb3.to_checksum_address(WETH),
                amount_in_wei,
                0,
            ).call()
            amount_out_wei = result[0]
            return amount_out_wei / 1e18
        except Exception as e:
            logger.debug(f"[LSTDepeg] Camelot quote failed @ {amount_in_wsteth}: {e}")
            return None

    # ─── THE CRITICAL CHECK: real depth at escalating trade sizes ─────
    async def find_max_executable_size(
        self, venue_quote_fn, fair_value_weth: float
    ) -> tuple[float, float]:
        """
        Walk UP through trade sizes and find the largest one where
        slippage (actual quote vs fair value) stays under
        MAX_ACCEPTABLE_SLIPPAGE_PCT. This is the fix for the Base
        weETH whale problem: a 1-unit quote can look perfect while a
        real-size trade collapses. We test the sizes we'd ACTUALLY
        want to trade, not a token amount.

        Returns (max_executable_size_eth, slippage_pct_at_that_size).
        If even the smallest test size fails, returns (0.0, 100.0).
        """
        last_good_size = 0.0
        last_good_slippage = 100.0

        for size in DEPTH_CHECK_SIZES_ETH:
            quoted_out = await venue_quote_fn(size)
            if quoted_out is None:
                break  # quote failed outright — pool can't handle this size at all

            expected_out_at_fair_value = size * fair_value_weth
            if expected_out_at_fair_value <= 0:
                break

            slippage_pct = (
                (expected_out_at_fair_value - quoted_out) / expected_out_at_fair_value
            ) * 100

            if slippage_pct > MAX_ACCEPTABLE_SLIPPAGE_PCT:
                # This size is too big — the LAST size we recorded is our answer
                break

            last_good_size = size
            last_good_slippage = slippage_pct

        return last_good_size, last_good_slippage

    # ─── One scan cycle ─────────────────────────────────────────────────
    async def scan_once(self) -> Optional[DepegCandidate]:
        fair_value, feed_age = await self.get_fair_value_weth()

        if fair_value is None:
            # Feed too stale to trust — already logged inside
            # get_fair_value_weth(). Do not substitute anything.
            # Just skip this cycle entirely.
            return None

        # Small test quotes first, just to detect a raw price difference
        camelot_1 = await self.quote_camelot(1.0)
        univ3_1 = await self.quote_univ3(1.0)

        if camelot_1 is None or univ3_1 is None:
            logger.warning("[LSTDepeg] Quote failure on one or both venues — skipping cycle")
            return None

        # Which venue is cheaper to BUY wstETH-equivalent exposure on
        # (i.e. gives LESS weth out per wstETH in = wstETH undervalued there)
        cheapest_price = min(camelot_1, univ3_1)
        cheapest_venue = "camelot" if camelot_1 <= univ3_1 else "univ3"
        deepest_venue = "univ3" if cheapest_venue == "camelot" else "camelot"

        raw_depeg_pct = ((fair_value - cheapest_price) / fair_value) * 100

        candidate = DepegCandidate(
            timestamp=time.time(),
            fair_value_weth=fair_value,
            camelot_price_weth=camelot_1,
            univ3_price_weth=univ3_1,
            cheapest_venue=cheapest_venue,
            deepest_venue=deepest_venue,
            raw_depeg_pct=raw_depeg_pct,
            feed_age_seconds=feed_age,
        )

        if abs(raw_depeg_pct) < DEPEG_ALERT_THRESHOLD_PCT:
            # Normal, healthy peg — nothing worth investigating further
            return candidate

        logger.info(
            f"[LSTDepeg] Raw depeg signal: {raw_depeg_pct:.3f}% "
            f"(fair={fair_value:.6f}, camelot={camelot_1:.6f}, univ3={univ3_1:.6f})"
        )

        if raw_depeg_pct < MIN_VIABLE_DEPEG_PCT:
            candidate.rejection_reason = (
                f"raw depeg {raw_depeg_pct:.3f}% below viable threshold "
                f"{MIN_VIABLE_DEPEG_PCT}% — likely just noise/rounding"
            )
            return candidate

        # ── THE MANDATORY DEPTH CHECK — do not skip this ──
        venue_fn = self.quote_camelot if cheapest_venue == "camelot" else self.quote_univ3
        max_size, slippage_at_max = await self.find_max_executable_size(
            venue_fn, fair_value
        )

        if max_size == 0.0:
            candidate.rejection_reason = (
                "depth check failed at SMALLEST test size — pool likely "
                "too illiquid to execute ANY meaningful trade (same failure "
                "mode as the Base weETH whale: real price gap, zero usable depth)"
            )
            logger.warning(
                f"[LSTDepeg] REJECTED — {candidate.rejection_reason}"
            )
            self.candidates_log.append(candidate)
            return candidate

        eth_price = await self.get_eth_price_usd()
        profit_per_eth = fair_value - cheapest_price
        executable_profit_usd = profit_per_eth * max_size * eth_price

        candidate.max_executable_size_eth = max_size
        candidate.executable_profit_usd = executable_profit_usd
        candidate.is_real_opportunity = True

        logger.warning(
            f"[LSTDepeg] *** REAL CANDIDATE *** depeg={raw_depeg_pct:.3f}% "
            f"max_executable_size={max_size} wstETH "
            f"slippage_at_max={slippage_at_max:.2f}% "
            f"profit_usd=${executable_profit_usd:.2f} "
            f"buy_venue={cheapest_venue} sell_venue={deepest_venue} "
            f"— OBSERVE ONLY, NOT EXECUTING"
        )

        self.candidates_log.append(candidate)
        return candidate

    # ─── Run loop ───────────────────────────────────────────────────────
    async def run(self):
        self._running = True
        logger.info(
            f"[LSTDepeg] Scanner started — OBSERVE-ONLY MODE, FAST DEPEGS ONLY. "
            f"wstETH/WETH, alert>{DEPEG_ALERT_THRESHOLD_PCT}%, "
            f"viable>{MIN_VIABLE_DEPEG_PCT}%, scan every {SCAN_INTERVAL_SECONDS}s. "
            f"Fair value: Chainlink WSTETH/ETH, max age {MAX_FEED_AGE_SECONDS/3600:.0f}h. "
            f"Slow depegs (<2%, gradual) are NOT reliably detected — see docstring."
        )
        cycle = 0
        while self._running:
            cycle += 1
            try:
                await self.scan_once()
            except Exception as e:
                logger.error(f"[LSTDepeg] Scan cycle {cycle} crashed: {e}", exc_info=True)
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

    def stop(self):
        self._running = False

    def summary(self) -> dict:
        """Quick stats for periodic reporting — how many cycles, how
        many raw signals, how many survived the depth check."""
        total = len(self.candidates_log)
        raw_signals = sum(1 for c in self.candidates_log if abs(c.raw_depeg_pct) >= DEPEG_ALERT_THRESHOLD_PCT)
        real_opportunities = sum(1 for c in self.candidates_log if c.is_real_opportunity)
        return {
            "total_logged": total,
            "raw_signals_above_threshold": raw_signals,
            "passed_depth_check": real_opportunities,
            "rejected_on_depth": raw_signals - real_opportunities,
        }
