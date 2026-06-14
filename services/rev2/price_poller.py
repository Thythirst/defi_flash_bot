"""
price_poller.py — HTTP polling fallback for Chainlink feeds not delivering via WSS
Fixes: prices_fresh=4/8 — ARB, LINK, wstETH, native USDC never update

Why this happens:
    Chainlink feeds only emit AnswerUpdated when price moves beyond their
    deviation threshold (e.g. 0.5% for majors, 1% for altcoins). Low-volatility
    periods can mean no WS event for hours. If the WSS subscription was also
    registered against a wrong address (proxy vs aggregator), it silently
    receives nothing. HTTP polling via latestRoundData() is immune to both.

Architecture:
    PricePoller runs a background asyncio task.
    Every poll_interval seconds it calls latestRoundData() for all configured
    feeds via Multicall3 (single RPC round trip for all 8 feeds).
    It skips feeds that already have fresh WS prices — WS always wins.
    It updates PriceRegistry with real on-chain prices for stale feeds.

Usage:
    poller = PricePoller(
        rpc           = rpc_client,        # AsyncRPCClient from async_web3.py
        price_registry= self.prices,       # PriceRegistry from execution_guards.py
        feeds         = ARBITRUM_CHAINLINK_FEEDS,
        poll_interval = 30,
    )
    await poller.start()
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from web3 import AsyncWeb3
from eth_abi import encode
from eth_utils import keccak

logger = logging.getLogger(__name__)

# Pre-compute selectors (avoid build_transaction which is async in web3 v7)
SELECTOR_DECIMALS = "0x" + keccak(b"decimals()").hex()[:8]
SELECTOR_LATEST_ROUND_DATA = "0x" + keccak(b"latestRoundData()").hex()[:8]

# ---------------------------------------------------------------------------
# Canonical Arbitrum Chainlink feed addresses
# asset address (Aave oracle key) → Chainlink aggregator address
# ---------------------------------------------------------------------------

ARBITRUM_CHAINLINK_FEEDS = {
    # ETH/USD
    "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1": "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",
    # WBTC/USD
    "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f": "0x6ce185860a4963106506C203335A2910413708e9",
    # USDC bridged/USD
    "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3",
    # USDT/USD
    "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9": "0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7",
    # ARB/USD  <- stale
    "0x912CE59144191C1204E64559FE8253a0e49E6548": "0xb2A824043730FE05F3DA2efaFa1CBbe83fa548D6",
    # LINK/USD
    "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4": "0x86E53CF1B870786351Da77A57575e79CB55812CB",
    # wstETH/USD — now handled by WstETHPriceManager (wsteth_fix.py)
    #   Composition: wstETH/ETH ratio × ETH/USD with Balancer pool TWAP fallback
    # native USDC/USD <- stale
    "0xaf88d065e77c8cC2239327C5EDb3A432268e5831": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3",
}

MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"

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

CHAINLINK_ABI = [
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"internalType": "uint80",  "name": "roundId",         "type": "uint80"},
            {"internalType": "int256",  "name": "answer",          "type": "int256"},
            {"internalType": "uint256", "name": "startedAt",       "type": "uint256"},
            {"internalType": "uint256", "name": "updatedAt",       "type": "uint256"},
            {"internalType": "uint80",  "name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# Chainlink heartbeat is 24h for most feeds — flag anything older than 1h
FEED_STALENESS_THRESHOLD = 3600


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class FeedStatus:
    asset_addr: str
    feed_addr: str
    description: str
    decimals: int
    last_price: int
    last_updated_on_chain: int
    last_polled: float
    source: str                    # "ws" | "http_poll" | "none"
    consecutive_failures: int = 0

    @property
    def on_chain_age_seconds(self) -> float:
        return time.time() - self.last_updated_on_chain if self.last_updated_on_chain else float("inf")

    @property
    def is_feed_healthy(self) -> bool:
        return self.on_chain_age_seconds < FEED_STALENESS_THRESHOLD

    @property
    def price_float(self) -> float:
        return self.last_price / (10 ** self.decimals) if self.decimals else 0.0


# ---------------------------------------------------------------------------
# PricePoller
# ---------------------------------------------------------------------------

class PricePoller:
    """
    Polls all Chainlink feeds via latestRoundData() using a single Multicall3
    call per cycle. Fills PriceRegistry for feeds that are stale or never
    received WS events — fixes prices_fresh=4/8 → 8/8.

    Key behaviours:
    - Single RPC round trip for all 8 feeds per poll
    - Immediate first poll at startup (fills gaps before WS warms up)
    - WS prices are NOT overwritten if they're fresh (WS wins on latency)
    - Warns if Chainlink itself hasn't updated a feed beyond heartbeat
    - Tracks consecutive failures per feed for diagnostics
    """

    def __init__(
        self,
        rpc,
        price_registry,
        feeds: dict[str, str] = None,
        poll_interval: float = 30.0,
        ws_freshness_threshold: float = 45.0,
    ):
        self._rpc      = rpc
        self._prices   = price_registry
        self._feeds    = feeds or ARBITRUM_CHAINLINK_FEEDS
        self._interval = poll_interval
        self._ws_fresh = ws_freshness_threshold

        self._mc = rpc.w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(MULTICALL3_ADDRESS),
            abi=MULTICALL3_ABI,
        )
        self._feed_contracts: dict[str, object] = {}
        self._feed_status: dict[str, FeedStatus] = {}

        self._running    = False
        self._task: Optional[asyncio.Task] = None
        self._poll_count = 0

    async def start(self) -> None:
        await self._init_feed_metadata()
        await self._poll_all()           # immediate fill before WS warms up
        self._running = True
        self._task = asyncio.create_task(self._poll_loop(), name="price_poller")
        logger.info(
            f"[PricePoller] Started — {len(self._feeds)} feeds, "
            f"poll_interval={self._interval}s"
        )
        self.log_status()

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
        logger.info(f"[PricePoller] Stopped after {self._poll_count} polls")

    def get_feed_status(self) -> list[FeedStatus]:
        return list(self._feed_status.values())

    def log_status(self) -> None:
        """One-line summary per feed — call at startup and periodically."""
        for s in self._feed_status.values():
            logger.info(
                f"[PricePoller]  {s.description:20s}  "
                f"price={s.price_float:>12.4f}  "
                f"on_chain_age={s.on_chain_age_seconds:>6.0f}s  "
                f"source={s.source:9s}  "
                f"{'⚠ CHAINLINK STALE' if not s.is_feed_healthy else 'OK'}"
            )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _init_feed_metadata(self) -> None:
        """Fetch decimals for each feed via Multicall3. Runs once at startup."""
        asset_addrs = list(self._feeds.keys())
        feed_addrs  = [self._feeds[a] for a in asset_addrs]

        for feed_addr in set(feed_addrs):
            ca = AsyncWeb3.to_checksum_address(feed_addr)
            if ca not in self._feed_contracts:
                self._feed_contracts[ca] = self._rpc.w3.eth.contract(
                    address=ca, abi=CHAINLINK_ABI
                )

        dec_calls = [
            {
                "target":       AsyncWeb3.to_checksum_address(f),
                "allowFailure": True,
                "callData":     SELECTOR_DECIMALS,
            }
            for f in feed_addrs
        ]

        try:
            dec_results = await self._mc.functions.aggregate3(dec_calls).call()
        except Exception as e:
            logger.warning(f"[PricePoller] decimals multicall failed: {e} — defaulting to 8")
            dec_results = [(False, b"")] * len(feed_addrs)

        for asset_addr, feed_addr, (success, raw) in zip(asset_addrs, feed_addrs, dec_results):
            decimals = 8
            if success and raw:
                try:
                    decimals = int(raw.hex(), 16) & 0xFF
                except Exception:
                    pass

            short = f"{asset_addr[-6:].upper()} feed"
            self._feed_status[asset_addr] = FeedStatus(
                asset_addr=asset_addr,
                feed_addr=feed_addr,
                description=short,
                decimals=decimals,
                last_price=0,
                last_updated_on_chain=0,
                last_polled=0.0,
                source="none",
            )

        logger.info(f"[PricePoller] Initialised {len(self._feed_status)} feed configs")

    async def _poll_all(self) -> int:
        """
        Single Multicall3 call to latestRoundData() for all feeds.
        Returns count of PriceRegistry entries updated this cycle.
        """
        asset_addrs = list(self._feeds.keys())
        feed_addrs  = [self._feeds[a] for a in asset_addrs]

        calls = [
            {
                "target":       AsyncWeb3.to_checksum_address(f),
                "allowFailure": True,
                "callData":     SELECTOR_LATEST_ROUND_DATA,
            }
            for f in feed_addrs
        ]

        try:
            results = await self._mc.functions.aggregate3(calls).call()
        except Exception as e:
            logger.error(f"[PricePoller] Multicall latestRoundData failed: {e}")
            return 0

        updated = 0
        now     = time.time()

        for asset_addr, (success, raw) in zip(asset_addrs, results):
            status = self._feed_status.get(asset_addr)
            if not status:
                continue

            if not success or not raw:
                status.consecutive_failures += 1
                if status.consecutive_failures % 5 == 1:
                    logger.warning(
                        f"[PricePoller] {status.description} failed "
                        f"({status.consecutive_failures} consecutive) — "
                        f"check feed address: {status.feed_addr}"
                    )
                continue

            decoded = self._decode_latest_round(raw)
            if decoded is None:
                status.consecutive_failures += 1
                continue

            _round_id, answer, _started_at, updated_at, _answered_in_round = decoded

            if answer <= 0:
                logger.warning(f"[PricePoller] {status.description} answer={answer} — skipping")
                continue

            status.last_price            = answer
            status.last_updated_on_chain = updated_at
            status.last_polled           = now
            status.consecutive_failures  = 0

            if not status.is_feed_healthy:
                logger.warning(
                    f"[PricePoller] ⚠ {status.description} Chainlink feed stale — "
                    f"last update {status.on_chain_age_seconds:.0f}s ago "
                    f"(their issue, not ours)"
                )

            # Only write to PriceRegistry if WS price is older than threshold
            ws_age = self._prices.age(asset_addr)
            if ws_age > self._ws_fresh:
                self._prices.update_price(asset_addr, answer)
                status.source = "http_poll"
                updated += 1
                logger.debug(
                    f"[PricePoller] {status.description}: "
                    f"{answer} written (WS was {ws_age:.0f}s stale)"
                )
            else:
                status.source = "ws"

        self._poll_count += 1

        if self._poll_count % 10 == 0:
            fresh = sum(1 for a in asset_addrs if self._prices.is_fresh(a))
            logger.info(
                f"[PricePoller] Poll #{self._poll_count} complete — "
                f"{fresh}/{len(asset_addrs)} fresh, {updated} updated this cycle"
            )

        return updated

    async def _poll_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._interval)
                await asyncio.wait_for(self._poll_all(), timeout=15.0)
            except asyncio.TimeoutError:
                logger.error("[PricePoller] _poll_all() timed out after 15s")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[PricePoller] Loop error: {e}")
                await asyncio.sleep(5)

    @staticmethod
    def _decode_latest_round(raw: bytes):
        """
        Decode latestRoundData ABI response.
        Returns (roundId, answer, startedAt, updatedAt, answeredInRound) or None.

        Layout: 5 × 32-byte slots
          [0]  uint80  roundId
          [1]  int256  answer
          [2]  uint256 startedAt
          [3]  uint256 updatedAt
          [4]  uint80  answeredInRound
        """
        if len(raw) < 160:
            return None
        try:
            h = raw.hex()
            round_id    = int(h[0:64],   16)
            answer      = int(h[64:128],  16)
            started_at  = int(h[128:192], 16)
            updated_at  = int(h[192:256], 16)
            ans_in_round= int(h[256:320], 16)
            if answer >= 2**255:     # signed int256
                answer -= 2**256
            return round_id, answer, started_at, updated_at, ans_in_round
        except Exception:
            return None


# ---------------------------------------------------------------------------
# Diagnostic utility — run once to verify feed addresses
# ---------------------------------------------------------------------------

async def verify_feed_addresses(rpc, feeds: dict[str, str] = None) -> None:
    """
    Confirms all feed addresses are correct and returning live data.
    Run this once on first deploy if any feeds remain at 0 after startup.

    Usage:
        import asyncio
        from price_poller import verify_feed_addresses
        from async_web3 import AsyncRPCClient

        async def main():
            rpc = AsyncRPCClient(http_url="https://your-rpc")
            await rpc.connect()
            await verify_feed_addresses(rpc)

        asyncio.run(main())
    """
    class _NullRegistry:
        def age(self, _):          return float("inf")
        def is_fresh(self, _):     return False
        def update_price(self, *_): pass

    poller = PricePoller(
        rpc=rpc,
        price_registry=_NullRegistry(),
        feeds=feeds or ARBITRUM_CHAINLINK_FEEDS,
        poll_interval=999999,
    )
    await poller._init_feed_metadata()
    await poller._poll_all()

    print("\n── Feed Verification ──────────────────────────────────────────")
    for s in poller.get_feed_status():
        status = "OK" if s.last_price > 0 and s.is_feed_healthy else "⚠ CHECK ADDRESS"
        print(
            f"  {s.asset_addr[:10]}…  "
            f"{s.description:20s}  "
            f"price={s.price_float:>12.4f}  "
            f"age={s.on_chain_age_seconds:>6.0f}s  "
            f"{status}"
        )
    print("───────────────────────────────────────────────────────────────\n")


# ---------------------------------------------------------------------------
# pipeline.py integration
# ---------------------------------------------------------------------------
#
# 1. Import:
#       from price_poller import PricePoller, ARBITRUM_CHAINLINK_FEEDS
#
# 2. Optional one-time address check (first deploy only):
#       await verify_feed_addresses(rpc_client)
#
# 3. In setup(), after self.rpc and self.prices are ready:
#       self.poller = PricePoller(
#           rpc            = self.rpc,
#           price_registry = self.prices,
#           feeds          = ARBITRUM_CHAINLINK_FEEDS,
#           poll_interval  = 30,
#       )
#       await self.poller.start()
#
# 4. In shutdown():
#       await self.poller.stop()
#
# 5. Expected wallet log after fix:
#       [Wallet] ETH=0.0830  candidates=0  prices_fresh=8/8
#
# ---------------------------------------------------------------------------
