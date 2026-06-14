"""
Oracle Guard — Strategy #1 (Staleness Checks) + #2 (CEX Deviation Alerts)
For Aave V3 Arbitrum liquidation bot with FlashExecutorV3.

Strategy #1: Prevents submitting liquidations when Chainlink oracles or the
Arbitrum sequencer are stale, avoiding guaranteed-revert transactions.

Strategy #2: Monitors CEX (Binance/Coinbase) prices via WebSocket, compares
against on-chain Chainlink prices, and fires alerts when CEX deviation
exceeds a threshold BELOW Chainlink's own trigger — giving pre-computation
time before the oracle update lands on Arbitrum.
"""

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from collections import defaultdict

logger = logging.getLogger(__name__)

# ─── Chainlink Feed Addresses (Arbitrum One) ─────────────────────────
# These are the official Chainlink Data Feed proxy addresses for Arbitrum.
# Source: https://docs.chain.link/data-feeds/price-feeds/addresses?network=arbitrum

CHAINLINK_FEEDS: Dict[str, str] = {
    # Asset symbol → AggregatorV3 proxy address
    "ETH":  "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",
    "BTC":  "0x6ce185860a4963106506C203335A2910413708e9",
    "USDC": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3",
    "USDT": "0x3f3f5dF88dC9F13EaC63DF89EC16ef6e7E25DdE7",
    "LINK": "0x86E53CF1B870786351Da77A57575e79CB55812CB",
    "ARB":  "0xb2A824043730FE05F3DA2efaFa1CBbe83fa548D6",
    "DAI":  "0xc5C8E77B397E531B8EC06BFb0048328B30E9eCfB",
    "WBTC": "0xd0C7101eACbB49F3deCcCc166d238410D6D46d57",
    "WETH": "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",  # Same as ETH
    "wstETH": "0xb523AE262D20A936BC152e6023996e46FDC2A95D",
}

# Heartbeat thresholds (seconds) — Chainlink's max time between updates
HEARTBEAT: Dict[str, int] = {
    "ETH": 3600, "BTC": 3600, "WETH": 3600, "WBTC": 3600,  # 1 hour
    "USDC": 86400, "USDT": 86400, "DAI": 86400,             # 24 hours
    "LINK": 3600, "ARB": 86400, "wstETH": 86400,            # Mixed
}
DEFAULT_HEARTBEAT = 3600  # Conservative default: 1 hour

# Sequencer Uptime Feed (Arbitrum-specific)
# This feed reports 0 when the sequencer is down, 1 when operational.
# After sequencer downtime resumes, Aave blocks liquidations for GRACE_PERIOD seconds.
SEQUENCER_UPTIME_FEED = "0xFdB631F5EE196F0ed6FAa767959853A9F217697D"
SEQUENCER_GRACE_PERIOD = 3600  # Aave V3 sequencer grace period (1 hour)

# ─── Strategy #2: CEX Deviation Configuration ────────────────────────
# Chainlink deviation thresholds (the % move that triggers an on-chain update)
CHAINLINK_DEVIATION = {
    "ETH": 0.005, "BTC": 0.005,   # 0.5%
    "USDC": 0.003, "USDT": 0.003,  # 0.3% (stablecoins)
    "LINK": 0.01, "ARB": 0.01,     # 1.0%
}

# Per-asset minimum deviation to fire an alert.
# Set above CEX_ALERT_RATIO * Chainlink threshold for the assets that matter.
# ARB excluded entirely — too noisy at $0.10/token.
CEX_ALERT_THRESHOLD = {
    "ETH": 0.01,   # 1.0% — only alert on meaningful ETH moves
    "BTC": 0.01,   # 1.0% — same for BTC
    "LINK": 0.015, # 1.5% — LINK doesn't trade on Aave Arbitrum, low priority
}

# CEX alert ratio (legacy — used as fallback if asset not in CEX_ALERT_THRESHOLD)
CEX_ALERT_RATIO = 0.6  # Alert at 60% of Chainlink deviation threshold

# Binance WebSocket streams for tracked assets
BINANCE_STREAMS = {
    "ETH":  "ethusdt@ticker",
    "BTC":  "btcusdt@ticker",
    "LINK": "linkusdt@ticker",
}

# Coinbase WebSocket product IDs
COINBASE_PRODUCTS = ["ETH-USD", "BTC-USD", "LINK-USD"]

# ─── Aave V3 Asset → Chainlink Feed Mapping ──────────────────────────
# Maps Aave reserve symbols to their Chainlink feed symbols
AAVE_TO_CHAINLINK = {
    "WETH": "ETH", "ETH": "ETH", "wstETH": "wstETH",
    "WBTC": "BTC", "BTC": "BTC",
    "USDC": "USDC", "USDC.e": "USDC",
    "USDT": "USDT",
    "DAI": "DAI",
    "LINK": "LINK",
    "ARB": "ARB",
}

# Multicall3 on Arbitrum One
MULTICALL3_ARB = "0xcA11bde05977b3631167028862bE2a173976CA11"

# ABI fragment for latestRoundData()
CHAINLINK_ABI_LATEST_ROUND = [
    {"inputs": [], "name": "latestRoundData",
     "outputs": [
         {"name": "roundId", "type": "uint80"},
         {"name": "answer", "type": "int256"},
         {"name": "startedAt", "type": "uint256"},
         {"name": "updatedAt", "type": "uint256"},
         {"name": "answeredInRound", "type": "uint80"},
     ], "stateMutability": "view", "type": "function"},
]

# ─── Data Structures ──────────────────────────────────────────────────

@dataclass
class OracleStatus:
    """Result of a staleness check for one feed."""
    symbol: str
    feed_address: str
    round_id: int = 0
    answer: int = 0
    updated_at: int = 0
    is_stale: bool = False
    staleness_seconds: float = 0.0
    heartbeat: int = DEFAULT_HEARTBEAT
    error: Optional[str] = None

@dataclass
class SequencerStatus:
    """Result of the Arbitrum sequencer uptime feed check."""
    is_up: bool = True
    answer: int = 1
    updated_at: int = 0
    grace_period_active: bool = False
    seconds_since_downtime: float = 0.0
    error: Optional[str] = None

@dataclass
class CexPricePoint:
    """A price snapshot from a centralized exchange."""
    exchange: str
    pair: str
    price: float
    timestamp: float
    bid: float = 0.0
    ask: float = 0.0

@dataclass
class DeviationAlert:
    """Alert fired when CEX- Chainlink deviation exceeds threshold."""
    symbol: str
    cex_price: float
    chainlink_price: float
    deviation_pct: float
    chainlink_threshold: float
    cex_threshold: float
    timestamp: float
    # Pre-computed: which borrowers use this asset as collateral
    affected_borrowers: List[str] = field(default_factory=list)

# ─── Strategy #1: Oracle Staleness Checks ─────────────────────────────

class OracleStalenessGuard:
    """
    Prevents liquidation submissions when Chainlink oracles or the
    Arbitrum sequencer are stale. Uses Multicall3 batching for efficiency.
    """

    def __init__(self, w3, rpc_url: str):
        self.w3 = w3
        self.rpc_url = rpc_url
        self._feed_contracts: Dict[str, object] = {}

        # Pre-compute calldata for each feed (cached)
        self._calldata_cache: Dict[str, str] = {}
        self._sequencer_calldata: Optional[str] = None

    def _get_feed_contract(self, feed_addr: str):
        """Get or create a Chainlink AggregatorV3 contract instance."""
        if feed_addr not in self._feed_contracts:
            self._feed_contracts[feed_addr] = self.w3.eth.contract(
                address=self.w3.to_checksum_address(feed_addr),
                abi=CHAINLINK_ABI_LATEST_ROUND,
            )
        return self._feed_contracts[feed_addr]

    def _build_latest_round_calldata(self, feed_addr: str) -> str:
        """Build eth_call calldata for latestRoundData()."""
        if feed_addr not in self._calldata_cache:
            contract = self._get_feed_contract(feed_addr)
            self._calldata_cache[feed_addr] = contract.encode_abi(
                abi_element_identifier="latestRoundData", args=[]
            )
        return self._calldata_cache[feed_addr]

    def _build_aggregate_calldata(
        self, calls: List[Tuple[str, str, bool]]
    ) -> str:
        """
        Build Multicall3 aggregate3 calldata.
        calls: list of (target, calldata, allowFailure) tuples.
        """
        from eth_abi import encode as abi_encode
        from eth_utils import keccak as _keccak

        agg_selector = _keccak(text="aggregate3((address,bool,bytes)[]")[:4]
        structs = []
        for target, cd, allow_failure in calls:
            structs.append((
                self.w3.to_checksum_address(target),
                allow_failure,
                bytes.fromhex(cd[2:]) if cd.startswith("0x") else bytes.fromhex(cd),
            ))

        encoded = abi_encode(["(address,bool,bytes)[]"], [structs])
        return "0x" + agg_selector.hex() + encoded.hex()

    async def _rpc_call(self, method: str, params: list) -> dict:
        """Minimal async RPC call — reuse executor's method if available."""
        import aiohttp
        async with aiohttp.ClientSession() as session:
            payload = {
                "jsonrpc": "2.0",
                "method": method,
                "params": params,
                "id": 1,
            }
            async with session.post(
                self.rpc_url, json=payload,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                return await resp.json()

    async def check_sequencer_uptime(self) -> SequencerStatus:
        """Check if Arbitrum sequencer is operational and outside grace period."""
        from eth_abi import decode as abi_decode

        try:
            # latestRoundData() on sequencer feed returns (0=down, 1=up) as answer
            calldata = self._build_latest_round_calldata(SEQUENCER_UPTIME_FEED)
            result = await self._rpc_call("eth_call", [
                {"to": SEQUENCER_UPTIME_FEED, "data": calldata}, "latest"
            ])

            raw = result.get("result", "0x")
            if len(raw) < 130:
                return SequencerStatus(error="Short response from sequencer feed")

            decoded = abi_decode(
                ["uint80", "int256", "uint256", "uint256", "uint80"],
                bytes.fromhex(raw[2:])
            )
            answer = decoded[1]
            updated_at = decoded[3]

            is_up = answer == 1
            now = int(time.time())
            seconds_since = now - updated_at if updated_at > 0 else SEQUENCER_GRACE_PERIOD + 1
            grace_period_active = (
                not is_up or
                (is_up and seconds_since < SEQUENCER_GRACE_PERIOD and updated_at > 0)
            )

            return SequencerStatus(
                is_up=is_up,
                answer=answer,
                updated_at=updated_at,
                grace_period_active=grace_period_active,
                seconds_since_downtime=seconds_since,
            )
        except Exception as e:
            return SequencerStatus(error=str(e), grace_period_active=True)

    async def check_oracle_staleness(
        self, asset_symbol: str
    ) -> OracleStatus:
        """Check if the Chainlink feed for an asset is stale."""
        from eth_abi import decode as abi_decode

        cl_symbol = AAVE_TO_CHAINLINK.get(asset_symbol, asset_symbol)
        feed_addr = CHAINLINK_FEEDS.get(cl_symbol)
        if not feed_addr:
            return OracleStatus(
                symbol=asset_symbol,
                feed_address="unknown",
                error=f"No Chainlink feed for {asset_symbol}",
            )

        heartbeat = HEARTBEAT.get(cl_symbol, DEFAULT_HEARTBEAT)

        try:
            calldata = self._build_latest_round_calldata(feed_addr)
            result = await self._rpc_call("eth_call", [
                {"to": feed_addr, "data": calldata}, "latest"
            ])

            raw = result.get("result", "0x")
            if len(raw) < 130:
                return OracleStatus(
                    symbol=asset_symbol, feed_address=feed_addr,
                    error="Short response from feed", is_stale=True,
                )

            decoded = abi_decode(
                ["uint80", "int256", "uint256", "uint256", "uint80"],
                bytes.fromhex(raw[2:])
            )
            round_id = decoded[0]
            answer = decoded[1]
            updated_at = decoded[3]

            now = int(time.time())
            staleness = now - updated_at if updated_at > 0 else heartbeat + 1
            is_stale = staleness > heartbeat

            return OracleStatus(
                symbol=asset_symbol,
                feed_address=feed_addr,
                round_id=round_id,
                answer=answer,
                updated_at=updated_at,
                is_stale=is_stale,
                staleness_seconds=staleness,
                heartbeat=heartbeat,
            )
        except Exception as e:
            return OracleStatus(
                symbol=asset_symbol, feed_address=feed_addr,
                error=str(e), is_stale=True,
            )

    async def check_all(
        self, collateral_symbol: str
    ) -> Tuple[bool, str, Optional[SequencerStatus], Optional[OracleStatus]]:
        """
        Full staleness check: sequencer + collateral oracle.
        Returns (is_safe, reason, sequencer_status, oracle_status).
        Only returns is_safe=True if both are healthy.
        """
        # Run both checks in parallel
        seq_status, oracle_status = await asyncio.gather(
            self.check_sequencer_uptime(),
            self.check_oracle_staleness(collateral_symbol),
        )

        reasons = []

        if seq_status.error:
            reasons.append(f"Sequencer check failed: {seq_status.error}")
        elif seq_status.grace_period_active:
            if not seq_status.is_up:
                reasons.append("Arbitrum sequencer is DOWN")
            else:
                reasons.append(
                    f"Sequencer grace period active "
                    f"({seq_status.seconds_since_downtime:.0f}s since uptime)"
                )

        if oracle_status.error:
            reasons.append(f"Oracle check failed: {oracle_status.error}")
        elif oracle_status.is_stale:
            reasons.append(
                f"Chainlink {oracle_status.symbol} STALE: "
                f"{oracle_status.staleness_seconds:.0f}s since update "
                f"(heartbeat: {oracle_status.heartbeat}s)"
            )

        if reasons:
            return False, "; ".join(reasons), seq_status, oracle_status
        return True, "OK", seq_status, oracle_status


# ─── Strategy #2: CEX Deviation Alerts ────────────────────────────────

class CexDeviationMonitor:
    """
    Monitors Binance + Coinbase WebSocket prices and compares against
    on-chain Chainlink prices. Fires alerts when CEX price deviates beyond
    a threshold below Chainlink's own update trigger.

    This provides pre-computation time before oracle updates land on-chain.
    """

    def __init__(self, rpc_url: str, alert_callback=None):
        self.rpc_url = rpc_url
        self.alert_callback = alert_callback  # async fn(DeviationAlert)

        # Latest prices from each source
        self.cex_prices: Dict[str, CexPricePoint] = {}
        self.chainlink_prices: Dict[str, float] = {}

        # Track when we last checked on-chain prices
        self._last_cl_refresh: Dict[str, float] = defaultdict(float)
        self._cl_refresh_interval = 30  # Refresh on-chain prices every 30s

        # Alert tracking — prevent spam on same direction
        self._last_alert: Dict[str, float] = {}
        self._alert_cooldown = 60  # Minimum seconds between alerts per asset

        # Active monitoring
        self._running = False
        self._tasks: List[asyncio.Task] = []

    # ─── Chainlink Price Fetching ─────────────────────────────────

    async def _rpc_call(self, method: str, params: list) -> dict:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            payload = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
            async with session.post(
                self.rpc_url, json=payload,
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                return await resp.json()

    async def _get_chainlink_price(self, symbol: str) -> Optional[float]:
        """Fetch latest Chainlink price for a symbol via RPC."""
        feed_addr = CHAINLINK_FEEDS.get(symbol)
        if not feed_addr:
            return None

        try:
            from eth_abi import decode as abi_decode
            from eth_utils import keccak as _keccak

            selector = _keccak(text="latestRoundData()")[:4].hex()
            calldata = "0x" + selector

            result = await self._rpc_call("eth_call", [
                {"to": feed_addr, "data": calldata}, "latest"
            ])
            raw = result.get("result", "0x")
            if len(raw) < 130:
                return None

            decoded = abi_decode(
                ["uint80", "int256", "uint256", "uint256", "uint80"],
                bytes.fromhex(raw[2:])
            )
            return decoded[1] / 1e8  # Chainlink uses 8 decimals
        except Exception as e:
            logger.warning("Chainlink price fetch failed for %s: %s", symbol, e)
            return None

    async def _refresh_chainlink_prices(self):
        """Periodically refresh on-chain Chainlink prices."""
        assets = ["ETH", "BTC", "LINK"]
        while self._running:
            for symbol in assets:
                price = await self._get_chainlink_price(symbol)
                if price:
                    self.chainlink_prices[symbol] = price
                    self._last_cl_refresh[symbol] = time.time()
            await asyncio.sleep(self._cl_refresh_interval)

    # ─── Binance WebSocket ────────────────────────────────────────

    async def _connect_binance(self):
        """Connect to Binance WebSocket for ticker streams."""
        import aiohttp

        streams = "/".join(BINANCE_STREAMS.values())
        url = f"wss://stream.binance.us:9443/stream?streams={streams}"

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url) as ws:
                        logger.info("Binance WebSocket connected")
                        async for msg in ws:
                            if not self._running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await self._handle_binance_ticker(json.loads(msg.data))
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                logger.warning("Binance WS disconnected: %s. Reconnecting...", e)
                await asyncio.sleep(5)

    async def _handle_binance_ticker(self, data: dict):
        """Process a Binance ticker update."""
        stream = data.get("stream", "")
        ticker = data.get("data", {})

        # Map stream name back to symbol
        for symbol, stream_name in BINANCE_STREAMS.items():
            if stream == stream_name:
                price = float(ticker.get("c", 0))  # Last price
                bid = float(ticker.get("b", 0))
                ask = float(ticker.get("a", 0))

                self.cex_prices[f"BINANCE:{symbol}"] = CexPricePoint(
                    exchange="binance",
                    pair=f"{symbol}-USDT",
                    price=price,
                    bid=bid,
                    ask=ask,
                    timestamp=time.time(),
                )
                await self._check_deviation(symbol)
                break

    # ─── Coinbase WebSocket ──────────────────────────────────────

    async def _connect_coinbase(self):
        """Connect to Coinbase WebSocket for ticker updates."""
        import aiohttp

        subscribe_msg = json.dumps({
            "type": "subscribe",
            "product_ids": COINBASE_PRODUCTS,
            "channels": ["ticker"],
        })

        while self._running:
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(
                        "wss://ws-feed.exchange.coinbase.com"
                    ) as ws:
                        await ws.send_str(subscribe_msg)
                        logger.info("Coinbase WebSocket connected")

                        async for msg in ws:
                            if not self._running:
                                break
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                if data.get("type") == "ticker":
                                    await self._handle_coinbase_ticker(data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                logger.warning("Coinbase WS disconnected: %s. Reconnecting...", e)
                await asyncio.sleep(5)

    async def _handle_coinbase_ticker(self, data: dict):
        """Process a Coinbase ticker update."""
        product_id = data.get("product_id", "")
        symbol = product_id.split("-")[0]  # "ETH-USD" → "ETH"

        if symbol in ["ETH", "BTC", "LINK"]:
            price = float(data.get("price", 0))
            bid = float(data.get("best_bid", 0))
            ask = float(data.get("best_ask", 0))

            self.cex_prices[f"COINBASE:{symbol}"] = CexPricePoint(
                exchange="coinbase",
                pair=product_id,
                price=price,
                bid=bid,
                ask=ask,
                timestamp=time.time(),
            )
            await self._check_deviation(symbol)

    # ─── Deviation Detection ─────────────────────────────────────

    async def _check_deviation(self, symbol: str):
        """
        Compare CEX prices against Chainlink and fire alert
        if deviation exceeds our threshold (below Chainlink's trigger).
        """
        cl_price = self.chainlink_prices.get(symbol)
        if not cl_price:
            return

        # Get the best CEX price across exchanges
        cex_prices = []
        for key, point in self.cex_prices.items():
            if f":{symbol}" in key or key.endswith(symbol):
                # Check freshness (< 5 seconds old)
                if time.time() - point.timestamp < 5:
                    cex_prices.append(point.price)

        if not cex_prices:
            return

        # Use median of CEX prices to filter outliers
        cex_prices.sort()
        median_price = cex_prices[len(cex_prices) // 2]

        # Calculate deviation
        if cl_price == 0:
            return
        deviation = abs(median_price - cl_price) / cl_price

        # Get thresholds — per-asset minimum alert threshold
        alert_threshold = CEX_ALERT_THRESHOLD.get(
            symbol,
            CHAINLINK_DEVIATION.get(symbol, 0.01) * CEX_ALERT_RATIO,
        )

        if deviation >= alert_threshold:
            now = time.time()
            # Cooldown check — don't spam same direction
            last = self._last_alert.get(symbol, 0)
            if now - last < self._alert_cooldown:
                return

            self._last_alert[symbol] = now

            alert = DeviationAlert(
                symbol=symbol,
                cex_price=median_price,
                chainlink_price=cl_price,
                deviation_pct=deviation,
                chainlink_threshold=CHAINLINK_DEVIATION.get(symbol, 0.01),
                cex_threshold=alert_threshold,
                timestamp=now,
            )

            logger.info(
                "CEX DEVIATION: %s %.4f vs CL %.4f (%.2f%%, threshold=%.2f%%)",
                symbol, median_price, cl_price,
                deviation * 100, alert_threshold * 100,
            )

            if self.alert_callback:
                await self.alert_callback(alert)

    # ─── Lifecycle ───────────────────────────────────────────────

    async def start(self):
        """Start all WebSocket connections and price refresh loop."""
        self._running = True
        self._tasks = [
            asyncio.create_task(self._refresh_chainlink_prices()),
            asyncio.create_task(self._connect_binance()),
            asyncio.create_task(self._connect_coinbase()),
        ]
        logger.info("CEX Deviation Monitor started (%d tasks)", len(self._tasks))

    async def stop(self):
        """Stop all background tasks."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        logger.info("CEX Deviation Monitor stopped")


# ─── Utility: Get all collateral assets from Aave V3 ──────────────────

async def fetch_aave_collateral_assets(w3, pool_address: str) -> List[str]:
    """
    Fetch all reserve symbols from Aave V3 Pool that are enabled as collateral.
    Returns list of asset symbols (e.g., ["WETH", "WBTC", "USDC"]).
    """
    from eth_abi import decode as abi_decode
    from eth_utils import keccak as _keccak

    aave_pool_abi = [
        {"inputs": [], "name": "getReservesList",
         "outputs": [{"name": "", "type": "address[]"}],
         "stateMutability": "view", "type": "function"},
    ]

    contract = w3.eth.contract(
        address=w3.to_checksum_address(pool_address),
        abi=aave_pool_abi,
    )
    reserves = contract.functions.getReservesList().call()

    # Map reserve addresses to symbols (common Arbitrum Aave V3 assets)
    KNOWN_SYMBOLS = {
        "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1": "WETH",
        "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f": "WBTC",
        "0xaf88d065e77c8cC2239327C5EDb3A432268e5831": "USDC",
        "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9": "USDT",
        "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1": "DAI",
        "0xf97f4df75117a78c1A5a0DBb814Af92458539FB4": "LINK",
        "0x912CE59144191C1204E64559FE8253a0e49E6548": "ARB",
        "0x5979D7b546E38E414F7E9822514be443A4802509": "wstETH",
    }

    symbols = []
    for reserve in reserves:
        sym = KNOWN_SYMBOLS.get(reserve.lower() if isinstance(reserve, str) else reserve)
        if sym:
            symbols.append(sym)

    return symbols
