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
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List

import aiohttp
from web3 import AsyncWeb3, Web3
from web3.providers import AsyncHTTPProvider

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

# Pairs to monitor for arbitrage.
# Each entry: (token_a, token_b, probe_size_a, univ3_fee_tiers, use_camelot)
#
# The real exploitable spreads on Arbitrum are BETWEEN Uniswap V3 fee tiers
# (e.g. 500 ↔ 3000), not Camelot↔UniV3 — confirmed by swap_monitor's live
# data (hundreds of UniV3-500↔UniV3-3000 spreads vs ~0 Camelot↔UniV3).
# So each fee tier is treated as a distinct venue, and Camelot is added only
# for pairs where it has a real, non-garbage pool.
#
# A pair needs >= 2 venues to arb. univ3_fee_tiers MUST include 500 where it
# exists — that is the deepest pool and the source of most spreads.
MONITORED_PAIRS = [
    (WETH, USDC, int(1e18),     [500, 3000], True),   # UniV3 500↔3000 + Camelot
    (WETH, USDT, int(1e18),     [500, 3000], False),  # Camelot pool too thin
    (WBTC, WETH, int(0.05e8),   [500],       True),   # UniV3-500 ↔ Camelot
    (WBTC, USDC, int(0.05e8),   [500, 3000], False),  # no real Camelot pool
    (ARB,  USDC, int(1000e18),  [500, 3000], True),
    (ARB,  WETH, int(1000e18),  [3000],      True),   # UniV3-3000 ↔ Camelot
]

# Uni V3 fee tiers we are willing to quote for arbitrage (includes 500 —
# the shared multi_dex_router.UNIV3_FEE_TIERS excludes it).
ARB_UNIV3_FEE_TIERS = [500, 3000, 10000]

# Reject any single-leg quote implying a cross-venue spread above this — a
# "too good" quote is almost always a thin/garbage pool that will not fill at
# size (the buy leg would revert on-chain, wasting gas). Real cross-venue
# arbs between these pools are well under 2%.
MAX_PLAUSIBLE_SPREAD_PCT = 5.0

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
    buy_amount_out:  int = 0  # pre-quoted buy output from spread check (skips re-quote)
    buy_fee:         int = 0  # buy pool fee tier (UniV3) or 0 (Camelot)
    sell_fee_tier:   int = 0  # sell pool fee tier (UniV3) or 0 (Camelot)

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
        self._raw_spreads_seen    = 0
        self._raw_spreads_rejected = 0

    async def scan_once(self) -> Optional[ArbOpportunity]:
        """
        Scan all monitored pairs, return the most profitable opportunity or None.
        """
        self._scans += 1

        tasks = [
            self._check_pair(token_a, token_b, size, univ3_fees, use_camelot)
            for token_a, token_b, size, univ3_fees, use_camelot in MONITORED_PAIRS
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
        univ3_fees: List[int], use_camelot: bool,
    ) -> Optional[ArbOpportunity]:
        """
        Find the best two-venue round-trip arbitrage for a pair.

        A "venue" is a single quotable pool: each Uni V3 fee tier is its own
        venue, plus Camelot. The real exploitable spread is between venues
        (most often UniV3-500 ↔ UniV3-3000), so they must be quoted separately
        — collapsing them with max() over fee tiers (the old behaviour) hides
        exactly the spread we trade.

        Round trip:
            1. Quote A→B on every venue → buy B where we get the most B (cheapest).
            2. With that B amount, quote B→A on every OTHER venue → sell where we
               get the most A back.
            3. If A_back > A_in after gas + on-chain profit guard → opportunity.

        Both legs are REAL QuoterV2 quotes (price-impact + fee included), so a
        positive round trip here corresponds to an executable on-chain arb —
        unlike a spot-price spread estimate, which ignores impact and reverts at
        the sell leg's amountOutMinimum guard.
        """
        # Build the venue set for this pair.
        venues: List[tuple] = [("univ3", f) for f in univ3_fees]
        if use_camelot:
            venues.append(("camelot", 0))
        if len(venues) < 2:
            return None

        # ── Leg 1: A→B on every venue, concurrently ──────────────────────
        ab = await asyncio.gather(
            *[self._quote_venue(v, token_a, token_b, amount_a) for v in venues],
            return_exceptions=True,
        )
        ab_valid = [(venues[i], r) for i, r in enumerate(ab)
                    if isinstance(r, int) and r > 0]
        if len(ab_valid) < 2:
            return None

        # Buy B where we receive the most B per A (the cheapest place to buy B).
        buy_venue, b_amount = max(ab_valid, key=lambda x: x[1])

        # ── Leg 2: B→A on every OTHER venue, concurrently ────────────────
        sell_venues = [v for v in venues if v != buy_venue]
        ba = await asyncio.gather(
            *[self._quote_venue(v, token_b, token_a, b_amount) for v in sell_venues],
            return_exceptions=True,
        )
        ba_valid = [(sell_venues[i], r) for i, r in enumerate(ba)
                    if isinstance(r, int) and r > 0]
        if not ba_valid:
            return None

        sell_venue, a_back = max(ba_valid, key=lambda x: x[1])

        # Quoted gross profit (QuoterV2 ideal — what the scanner would claim
        # but NOT what the on-chain tx actually delivers)
        quoted_gross = a_back - amount_a
        if quoted_gross <= 0:
            return None

        # Plausibility guard — a spread this large means a thin/garbage pool
        # whose buy leg would revert on-chain; skip rather than burn gas.
        if (quoted_gross / amount_a) * 100 > MAX_PLAUSIBLE_SPREAD_PCT:
            logger.debug(
                f"[Arb] implausible spread {(quoted_gross/amount_a)*100:.2f}% "
                f"{token_a[:8]}→{token_b[:8]} buy={buy_venue} sell={sell_venue} — skip"
            )
            return None

        buy_dex,  buy_fee  = buy_venue[0],  buy_venue[1]
        sell_dex, sell_fee = sell_venue[0], sell_venue[1]

        # Value the profit in USD (needed for gas cost and threshold check)
        a_price_usd = self._get_price_usd(token_a)
        if a_price_usd is None:
            return None
        a_dec = DECIMALS.get(token_a, 18)

        # ── Worst-case execution simulation ──────────────────────────
        # Do NOT use the ideal QuoterV2 round-trip as the profit.
        # Simulate both legs at the same slippage guards the on-chain tx
        # uses (ArbExecutor._build_tx: 0.3% buy, 0.3% sell). Only treat
        # as profitable if the worst-case outcome clears gas + threshold.
        #
        # Buy leg: pool moves 0.3% against us → buy_out_min is enforced
        # Sell leg: with reduced B, pool moves another 0.3% against us
        SLIPPAGE_BPS = 30
        worst_buy = int(b_amount * (10_000 - SLIPPAGE_BPS) / 10_000)
        worst_sell_rate = (a_back / b_amount) * (10_000 - SLIPPAGE_BPS) / 10_000
        worst_a_back = int(worst_buy * worst_sell_rate)
        worst_gross = worst_a_back - amount_a

        if worst_gross <= 0:
            self._raw_spreads_seen += 1
            self._raw_spreads_rejected += 1
            logger.debug(
                f"[Arb] {token_a[:8]}→{token_b[:8]} spread={quoted_gross/amount_a*100:.3f}% "
                f"but worst-case (2×0.3% slip) = {(worst_gross/amount_a)*100:.3f}% — unprofitable"
            )
            return None

        gross_profit_usd = (worst_gross / 10**a_dec) * a_price_usd
        gas_cost_usd = self._estimate_gas_cost_usd()
        net_profit_usd = gross_profit_usd - gas_cost_usd
        spread_pct = (quoted_gross / amount_a) * 100

        if net_profit_usd < self._min_profit:
            self._raw_spreads_seen += 1
            self._raw_spreads_rejected += 1
            return None

        self._raw_spreads_seen += 1

        return ArbOpportunity(
            token_in         = token_a,
            token_out        = token_b,
            amount_in        = amount_a,
            buy_dex          = buy_dex,
            sell_dex         = sell_dex,
            buy_router       = self._router_for(buy_dex),
            sell_router      = self._router_for(sell_dex),
            expected_out     = a_back,
            gross_profit     = quoted_gross,
            gross_profit_usd = gross_profit_usd,
            gas_cost_usd     = gas_cost_usd,
            net_profit_usd   = net_profit_usd,
            spread_pct       = spread_pct,
            buy_amount_out   = b_amount,
            buy_fee          = buy_fee,
            sell_fee_tier    = sell_fee,
        )

    async def _quote_venue(
        self, venue: tuple, token_in: str, token_out: str, amount_in: int,
    ) -> Optional[int]:
        """
        Quote a single venue. venue = ("univ3", fee_tier) or ("camelot", 0).
        Returns amount_out (int) or None. One venue = one pool — no max() over
        tiers, so each fee tier stays distinguishable.
        """
        dex, fee = venue
        try:
            tin  = AsyncWeb3.to_checksum_address(token_in)
            tout = AsyncWeb3.to_checksum_address(token_out)
            if dex == "camelot":
                result = await self._multi_dex._quote_camelot(tin, tout, amount_in)
            else:
                result = await self._multi_dex._quote_univ3(tin, tout, amount_in, fee)
            if result is None or result.amount_out == 0:
                return None
            return result.amount_out
        except Exception as e:
            logger.debug(f"[Arb] {dex} fee={fee} quote failed: {e}")
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
            "raw_spreads_seen":    self._raw_spreads_seen,
            "raw_spreads_rejected": self._raw_spreads_rejected,
        }

    def log_stats(self) -> None:
        s = self.stats
        logger.info(
            f"[Arb] scans={s['scans']} "
            f"opportunities={s['opportunities_found']} "
            f"raw_spreads={s['raw_spreads_seen']} "
            f"rejected={s['raw_spreads_rejected']}"
        )


# ---------------------------------------------------------------------------
# ArbExecutor — builds and submits arb txs to ArbExecutor.sol
# ---------------------------------------------------------------------------

_ARB_EXECUTOR_ABI = None

async def _resolved(value):
    return value


def _load_arb_abi() -> list:
    global _ARB_EXECUTOR_ABI
    if _ARB_EXECUTOR_ABI is None:
        artifact = (
            Path(__file__).parent.parent.parent
            / "out" / "ArbExecutor.sol" / "ArbExecutor.json"
        )
        _ARB_EXECUTOR_ABI = json.loads(artifact.read_text())["abi"]
    return _ARB_EXECUTOR_ABI


class ArbExecutor:
    """
    Encodes and submits ArbExecutor.sol's executeArbViaBalancer(ArbRoute).

    Flow (atomic, single tx):
        1. Fresh on-chain quotes for both legs (captures current pool state)
        2. Encode buy calldata (tokenIn → tokenOut, recipient = contract)
        3. Pack ArbRoute struct: buyCalldata pre-encoded, sell built on-chain
        4. Sign and broadcast EIP-1559 tx

    Deployed: 0x52184ca20E848A2e219b03eCFC7Dc04e839F50aF (Arbitrum, 2026-06-22)
    Approved routers: Camelot 0x1F72..., UniV3 0xE592...
    """

    ARB_GAS_LIMIT = 800_000
    CHAIN_ID      = 42161

    # Submit endpoints: Alchemy + direct Arbitrum sequencer + DRPC
    _SUBMIT_URLS: List[str] = []

    @classmethod
    def _build_submit_urls(cls) -> List[str]:
        urls = []
        for var in ("ARBITRUM_HTTP_URL", "ALCHEMY_HTTP_URL"):
            u = os.getenv(var, "")
            if u and u not in urls:
                urls.append(u)
        # Arbitrum sequencer direct endpoint (FCFS, lowest latency)
        urls.append("https://arb1-sequencer.arbitrum.io/rpc")
        for var in ("DRPC_RPC_URL", "READ_RPC_PRIMARY"):
            u = os.getenv(var, "")
            if u and u not in urls:
                urls.append(u)
        return urls

    def __init__(
        self,
        w3:                   AsyncWeb3,
        arb_executor_address: str,
        wallet:               str,
        private_key:          str,
        shared_state=None,
    ):
        from multi_dex_router import MultiDexRouter

        self._w3      = w3
        self._addr    = AsyncWeb3.to_checksum_address(arb_executor_address)
        self._wallet  = AsyncWeb3.to_checksum_address(wallet)
        self._pk      = private_key
        self._state   = shared_state

        # Sync Web3 contract for ABI encoding (AsyncContract lacks encode_abi)
        _sync_w3 = Web3()
        self._contract = _sync_w3.eth.contract(address=self._addr, abi=_load_arb_abi())

        # MultiDexRouter with arb executor as recipient — used only for
        # encoding buy calldata so tokens land in the contract, not the wallet
        self._enc_router = MultiDexRouter(w3, arb_executor_address)

        self._executed = 0
        self._failed   = 0

        # Raw aiohttp sessions for submission — pre-warmed by warmup()
        self._submit_sessions: Optional[List[aiohttp.ClientSession]] = None

        # Nonce cache — avoid one RPC round-trip per execution
        self._nonce: Optional[int] = None

    # ── Public API ──────────────────────────────────────────────────────────

    async def warmup(self) -> None:
        """Pre-warm TCP+TLS connections and fetch initial nonce. Call at startup."""
        await self._init_submit_sessions()
        self._nonce = await self._w3.eth.get_transaction_count(self._wallet, "pending")
        logger.info(f"[ArbExec] Warmed up — nonce={self._nonce}")
        asyncio.create_task(self._keepalive_loop())

    async def _keepalive_loop(self) -> None:
        """Ping submit endpoints every 20s to keep TCP connections alive."""
        urls = self._build_submit_urls()
        while True:
            await asyncio.sleep(20)
            if not self._submit_sessions:
                continue
            # Use sendRawTransaction with empty payload — only method sequencer supports
            body = {"id": 1, "jsonrpc": "2.0", "method": "eth_sendRawTransaction",
                    "params": ["0x"]}
            async def _ping(session: aiohttp.ClientSession, url: str) -> None:
                try:
                    async with session.post(url, json=body,
                                            timeout=aiohttp.ClientTimeout(total=3)) as r:
                        await r.read()
                except Exception:
                    pass
            await asyncio.gather(
                *[_ping(s, urls[i]) for i, s in enumerate(self._submit_sessions)],
                return_exceptions=True,
            )

    async def execute(
        self,
        opp:         ArbOpportunity,
        multi_dex,
        nonce:       Optional[int] = None,
        slippage_bps: int = 30,
        dry_run:     bool = False,
    ) -> Optional[str]:
        """
        Build, sign, and broadcast the arb transaction. Returns tx hash or None.
        """
        tx = await self._build_tx(opp, multi_dex, nonce, slippage_bps)
        if tx is None:
            return None

        if dry_run:
            logger.info(
                f"[ArbExec] DRY RUN: would send {opp.token_in[:10]}→"
                f"{opp.token_out[:10]} net=${opp.net_profit_usd:.2f}"
            )
            return None

        try:
            account = Web3().eth.account.from_key(self._pk)
            signed  = account.sign_transaction(tx)
            raw     = signed.raw_transaction

            tx_hash = await self._broadcast(raw)
            if tx_hash:
                self._executed += 1
                logger.info(f"[ArbExec] Broadcast: {tx_hash}")
                return tx_hash
            self._failed += 1
            return None
        except Exception as e:
            self._failed += 1
            logger.error(f"[ArbExec] Broadcast failed: {e}")
            return None

    async def _broadcast(self, raw: bytes) -> Optional[str]:
        """Fire signed tx to all submit endpoints simultaneously; return first hash."""
        if self._submit_sessions is None:
            await self._init_submit_sessions()

        raw_hex  = "0x" + raw.hex()
        body     = {"id": 1, "jsonrpc": "2.0", "method": "eth_sendRawTransaction",
                    "params": [raw_hex]}
        nonce_collision = False

        t_tasks_created = time.time()

        async def _post(session: aiohttp.ClientSession, url: str) -> Optional[str]:
            nonlocal nonce_collision
            try:
                t_start = time.time()
                lag_ms = int((t_start - t_tasks_created) * 1000)
                t0 = time.time()
                async with session.post(url, json=body, timeout=aiohttp.ClientTimeout(total=2)) as resp:
                    data = await resp.json(content_type=None)
                logger.info(f"[ArbExec] {url[8:30]} lag={lag_ms}ms rtt={int((time.time()-t0)*1000)}ms")
                if "error" in data:
                    err = data["error"].get("message", "")
                    if "already known" in err.lower() or "replacement" in err.lower():
                        logger.debug(f"[ArbExec] {url[:30]}: already known")
                    elif "nonce too low" in err.lower():
                        nonce_collision = True
                        logger.warning(f"[ArbExec] nonce too low — resetting cache")
                    else:
                        logger.warning(f"[ArbExec] {url[:30]} rpc error: {err[:80]}")
                    return None
                h = data.get("result", "")
                if h:
                    logger.info(f"[ArbExec] Confirmed via {url[:40]}: {h}")
                    return h
                return None
            except Exception as e:
                logger.warning(f"[ArbExec] {url[:30]} failed: {str(e)[:60]}")
                return None

        urls  = self._build_submit_urls()
        tasks = [asyncio.create_task(_post(self._submit_sessions[i], urls[i]))
                 for i in range(len(urls))]

        result: Optional[str] = None
        remaining = list(tasks)
        try:
            deadline = asyncio.get_event_loop().time() + 2.0
            while remaining and not result:
                wait_secs = deadline - asyncio.get_event_loop().time()
                if wait_secs <= 0:
                    break
                done, _ = await asyncio.wait(
                    remaining, timeout=wait_secs,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not done:
                    break
                for t in done:
                    remaining.remove(t)
                    if not t.cancelled() and t.exception() is None and t.result():
                        result = t.result()
                        break
        finally:
            for t in remaining:
                t.cancel()

        if result:
            if self._nonce is not None:
                self._nonce += 1
        elif nonce_collision:
            self._nonce = None

        return result

    async def _init_submit_sessions(self) -> None:
        urls  = self._build_submit_urls()
        sessions: List[aiohttp.ClientSession] = []
        for url in urls:
            connector = aiohttp.TCPConnector(limit=4, keepalive_timeout=300,
                                             force_close=False, enable_cleanup_closed=True)
            session   = aiohttp.ClientSession(connector=connector)
            # Warm TCP+TLS with a sendRawTransaction call using empty payload
            # (will fail with -32000 but the TCP handshake completes)
            try:
                async with session.post(url,
                                        json={"id":1,"jsonrpc":"2.0","method":"eth_sendRawTransaction","params":["0x"]},
                                        timeout=aiohttp.ClientTimeout(total=5)) as r:
                    await r.read()
            except Exception:
                pass
            sessions.append(session)
        self._submit_sessions = sessions
        logger.info(f"[ArbExec] Submit sessions ready: {urls}")

    # ── Internal ────────────────────────────────────────────────────────────

    async def _build_tx(
        self,
        opp:          ArbOpportunity,
        multi_dex,
        nonce:        Optional[int],
        slippage_bps: int,
    ) -> Optional[dict]:
        from multi_dex_router import UNIV3_FEE_TIERS

        from multi_dex_router import DexQuote

        token_in  = AsyncWeb3.to_checksum_address(opp.token_in)
        token_out = AsyncWeb3.to_checksum_address(opp.token_out)

        # ── Nonce — use cache to avoid one RPC round-trip ────────────────
        if nonce is None:
            if self._nonce is not None:
                nonce = self._nonce
            else:
                nonce = await self._w3.eth.get_transaction_count(self._wallet, "pending")
                self._nonce = nonce

        # ── Buy calldata from pre-quoted amount (no re-quote) ─────────────
        buy_amount_out = opp.buy_amount_out
        if not buy_amount_out:
            logger.info("[ArbExec] no pre-quoted buy amount, skipping")
            return None
        buy_out_min  = int(buy_amount_out * (10_000 - slippage_bps) / 10_000)
        buy_q = DexQuote(
            dex=opp.buy_dex, router=opp.buy_router,
            amount_out=buy_amount_out, fee_tier=opp.buy_fee,
            token_in=token_in, token_out=token_out, amount_in=opp.amount_in,
        )
        buy_calldata = self._enc_router.encode_calldata(buy_q, buy_out_min)

        # ── Sell parameters from opportunity (no re-quote) ────────────────
        sell_is_camelot = opp.sell_dex == "camelot"
        sell_fee        = 0 if sell_is_camelot else opp.sell_fee_tier

        # min_profit: 0.05% of principal in tokenIn units (on-chain guard enforces)
        min_profit   = max(1, opp.amount_in // 2000)
        sell_min_out = opp.amount_in + min_profit

        route = (
            token_in,           # tokenIn
            opp.amount_in,      # amountIn
            AsyncWeb3.to_checksum_address(opp.buy_router),   # buyRouter
            buy_calldata,       # buyCalldata (bytes, pre-encoded)
            AsyncWeb3.to_checksum_address(opp.sell_router),  # sellRouter
            token_out,          # tokenOut
            sell_is_camelot,    # sellIsCamelot
            sell_fee,           # sellFee (UniV3 fee tier or 0)
            sell_min_out,       # sellMinOut
            min_profit,         # minProfit
        )

        tx_data = self._contract.encode_abi(
            "executeArbViaBalancer", args=[route]
        )

        base_fee = (
            self._state.base_fee_wei
            if self._state and self._state.base_fee_wei > 0
            else 100_000_000
        )
        max_fee  = base_fee * 4
        priority = min(int(0.1e9), base_fee)

        logger.info(
            f"[ArbExec] Built: {opp.token_in[:10]} buy={opp.buy_dex} "
            f"sell={opp.sell_dex} buy_out={buy_amount_out} "
            f"min_profit={min_profit} net~${opp.net_profit_usd:.2f}"
        )

        return {
            "to":                 self._addr,
            "data":               tx_data,
            "nonce":              nonce,
            "gas":                self.ARB_GAS_LIMIT,
            "maxFeePerGas":       max_fee,
            "maxPriorityFeePerGas": priority,
            "chainId":            self.CHAIN_ID,
            "value":              0,
            "type":               2,
        }

    async def _quote_dex(self, multi_dex, dex: str, token_in, token_out, amount_in):
        """Fresh single-dex quote; for UniV3 returns the best fee tier."""
        from multi_dex_router import UNIV3_FEE_TIERS
        try:
            if dex == "camelot":
                return await multi_dex._quote_camelot(token_in, token_out, amount_in)
            else:
                quotes = await asyncio.gather(*[
                    multi_dex._quote_univ3(token_in, token_out, amount_in, fee)
                    for fee in UNIV3_FEE_TIERS
                ], return_exceptions=True)
                valid = [q for q in quotes if q and not isinstance(q, Exception)]
                return max(valid, key=lambda q: q.amount_out) if valid else None
        except Exception as e:
            logger.debug(f"[ArbExec] quote({dex}) failed: {e}")
            return None

    @property
    def stats(self) -> dict:
        return {"executed": self._executed, "failed": self._failed}
# ---------------------------------------------------------------------------
