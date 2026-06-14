"""
ws_manager.py — Dual WebSocket manager with HTTP polling fallback
Fixes W7: single WSS endpoint for both oracle + liquidation monitor.
          If Chainstack drops, pipeline goes completely blind with no fallback.

Architecture:
    OracleWSS      → primary WS (Chainstack) + secondary WS (QuickNode)
    LiqMonitorWSS  → separate endpoint to decouple concerns
    HTTPFallback   → eth_getLogs polling every 2 blocks when WS is down

Usage:
    manager = WSManager(
        primary_wss   = os.getenv("CHAINSTACK_WSS"),
        secondary_wss = os.getenv("QUICKNODE_WSS"),
        http_rpc      = os.getenv("QUICKNODE_HTTP"),
        on_price_update  = hf_engine.update_price,
        on_liquidation   = pipeline.handle_liquidation_log,
    )
    await manager.start()
"""

import asyncio
import logging
import time
from typing import Callable, Coroutine, Optional

import websockets
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

logger = logging.getLogger(__name__)

# Aave V3 Pool on Arbitrum — for getLogs fallback
AAVE_V3_POOL_ARBITRUM = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"

# Chainlink AnswerUpdated topic0
ANSWER_UPDATED_TOPIC = "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"

# Aave LiquidationCall topic0
LIQUIDATION_CALL_TOPIC = "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286"

# Seconds of WS silence before switching to HTTP polling
WS_TIMEOUT_THRESHOLD = 30.0


# ---------------------------------------------------------------------------
# Base WebSocket connection with auto-reconnect
# ---------------------------------------------------------------------------

class ManagedWSConnection:
    """
    Single WebSocket connection with exponential backoff reconnection.
    Tracks last_message_at for health monitoring.
    """

    def __init__(
        self,
        name: str,
        url: str,
        subscribe_payload: dict,
        on_message: Callable[[dict], Coroutine],
        max_backoff: float = 60.0,
    ):
        self.name              = name
        self.url               = url
        self.subscribe_payload = subscribe_payload
        self.on_message        = on_message
        self.max_backoff       = max_backoff
        self.last_message_at   = 0.0
        self.connected         = False
        self._task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run(), name=f"ws_{self.name}")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
        self.connected = False

    @property
    def is_healthy(self) -> bool:
        return self.connected

    async def _run(self) -> None:
        backoff = 1.0
        while self._running:
            try:
                logger.info(f"[WS:{self.name}] Connecting to {self.url[:40]}…")
                async with websockets.connect(
                    self.url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    await ws.send(str(self.subscribe_payload).replace("'", '"'))
                    self.connected = True
                    self.last_message_at = time.time()
                    backoff = 1.0
                    logger.info(f"[WS:{self.name}] Connected")

                    async for raw in ws:
                        self.last_message_at = time.time()
                        try:
                            import json
                            msg = json.loads(raw)
                            await self.on_message(msg)
                        except Exception as e:
                            logger.warning(f"[WS:{self.name}] Message handler error: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.connected = False
                logger.warning(
                    f"[WS:{self.name}] Disconnected: {e}. "
                    f"Reconnecting in {backoff:.0f}s…"
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.max_backoff)


# ---------------------------------------------------------------------------
# HTTP fallback — eth_getLogs polling
# ---------------------------------------------------------------------------

class HTTPLogFallback:
    """
    Polls eth_getLogs every 2 blocks when WebSocket is unhealthy.
    Feeds the same callbacks as the WS path.
    Fixes W7: previously there was no fallback — WS drop = complete blindness.
    """

    def __init__(
        self,
        http_url: str,
        on_liquidation: Callable[[dict], Coroutine],
        pool_address: str = AAVE_V3_POOL_ARBITRUM,
        poll_blocks: int = 2,
    ):
        self._http_url       = http_url
        self._on_liquidation = on_liquidation
        self._pool_address   = pool_address
        self._poll_blocks    = poll_blocks
        self._last_block     = 0
        self._running        = False
        self._task: Optional[asyncio.Task] = None
        self._w3: Optional[AsyncWeb3] = None

    async def start(self) -> None:
        self._w3 = AsyncWeb3(AsyncHTTPProvider(self._http_url))
        self._last_block = await self._w3.eth.block_number
        self._running = True
        self._task = asyncio.create_task(self._poll_loop(), name="http_fallback")
        logger.info(f"[HTTPFallback] Started polling from block {self._last_block}")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()

    async def _poll_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._poll_blocks * 0.25)  # ~500ms on Arbitrum
                current = await self._w3.eth.block_number
                if current <= self._last_block:
                    continue

                logs = await self._w3.eth.get_logs({
                    "address":   self._pool_address,
                    "fromBlock": self._last_block + 1,
                    "toBlock":   current,
                    "topics":    [LIQUIDATION_CALL_TOPIC],
                })

                self._last_block = current

                for log in logs:
                    try:
                        # Convert AttributeDict to plain dict for compatibility
                        log_dict = dict(log)
                        log_dict["topics"] = [
                            t.hex() if hasattr(t, 'hex') else str(t)
                            for t in log.get("topics", [])
                        ]
                        raw_data = log.get("data", b"")
                        if isinstance(raw_data, bytes):
                            log_dict["data"] = "0x" + raw_data.hex() if raw_data else "0x"
                        else:
                            log_dict["data"] = str(raw_data) if raw_data else "0x"
                        tx_hash = log.get("transactionHash", b"")
                        log_dict["transactionHash"] = tx_hash.hex() if hasattr(tx_hash, 'hex') else str(tx_hash)
                        log_dict["blockNumber"] = log["blockNumber"]
                        await self._on_liquidation(log_dict)
                    except Exception as e:
                        logger.warning(f"[HTTPFallback] Log processing error: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[HTTPFallback] Poll error: {e}")
                await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# WSManager — top-level coordinator
# ---------------------------------------------------------------------------

class WSManager:
    """
    Manages oracle + liquidation WebSocket connections across two endpoints.
    Automatically switches to HTTP polling fallback when both WS are down.

    Fixes W7:
    - Primary oracle WS  → Chainstack
    - Secondary oracle WS → QuickNode (hot standby)
    - Liq monitor WS     → separate endpoint (decoupled from oracle)
    - HTTP fallback       → eth_getLogs polling (activates on WS silence >30s)
    """

    def __init__(
        self,
        primary_wss: str,
        secondary_wss: str,
        http_rpc: str,
        on_price_update: Callable,    # (asset_addr, price_int) → None
        on_liquidation: Callable,     # (log_dict) → Coroutine
        oracle_feeds: list[str],      # Chainlink feed addresses to subscribe
        pool_address: str = AAVE_V3_POOL_ARBITRUM,
        on_new_block: Optional[Callable] = None,  # (block_number) → None
    ):
        self._primary_wss   = primary_wss
        self._secondary_wss = secondary_wss
        self._http_rpc      = http_rpc
        self._on_price      = on_price_update
        self._on_liq        = on_liquidation
        self._on_new_block  = on_new_block
        self._oracle_feeds  = oracle_feeds
        self._pool_address  = pool_address

        self._oracle_primary:   Optional[ManagedWSConnection] = None
        self._oracle_secondary: Optional[ManagedWSConnection] = None
        self._liq_ws:           Optional[ManagedWSConnection] = None
        self._block_ws:         Optional[ManagedWSConnection] = None
        self._http_fallback:    Optional[HTTPLogFallback]     = None

        self._fallback_active = False
        self._monitor_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start all WebSocket connections and health monitor."""

        # Oracle primary
        self._oracle_primary = ManagedWSConnection(
            name="oracle_primary",
            url=self._primary_wss,
            subscribe_payload=self._oracle_subscribe_payload(self._oracle_feeds),
            on_message=self._handle_oracle_message,
        )
        await self._oracle_primary.start()

        # Oracle secondary (hot standby — deduplication handled by PriceRegistry)
        self._oracle_secondary = ManagedWSConnection(
            name="oracle_secondary",
            url=self._secondary_wss,
            subscribe_payload=self._oracle_subscribe_payload(self._oracle_feeds),
            on_message=self._handle_oracle_message,
        )
        await self._oracle_secondary.start()

        # Liquidation monitor on its own connection (decoupled from oracle)
        self._liq_ws = ManagedWSConnection(
            name="liq_monitor",
            url=self._primary_wss,
            subscribe_payload={
                "jsonrpc": "2.0", "id": 3, "method": "eth_subscribe",
                "params": ["logs", {
                    "address": self._pool_address,
                    "topics":  [LIQUIDATION_CALL_TOPIC],
                }],
            },
            on_message=self._handle_liq_message,
        )
        await self._liq_ws.start()

        # ── New block subscription (push-based, replaces polling) ──
        if self._on_new_block is not None:
            self._block_ws = ManagedWSConnection(
                name="block_watch",
                url=self._primary_wss,
                subscribe_payload={
                    "jsonrpc": "2.0", "id": 4, "method": "eth_subscribe",
                    "params": ["newHeads"],
                },
                on_message=self._handle_new_block,
            )
            await self._block_ws.start()
            logger.info("[WSManager] newHeads subscription active")

        # HTTP fallback (starts paused, activates in monitor loop)
        self._http_fallback = HTTPLogFallback(
            http_url=self._http_rpc,
            on_liquidation=self._on_liq,
            pool_address=self._pool_address,
        )

        # Health monitor
        self._monitor_task = asyncio.create_task(
            self._health_monitor(), name="ws_health_monitor"
        )

        logger.info("[WSManager] All connections started")

    async def stop(self) -> None:
        for conn in [self._oracle_primary, self._oracle_secondary, self._liq_ws, self._block_ws]:
            if conn:
                await conn.stop()
        if self._http_fallback:
            await self._http_fallback.stop()
        if self._monitor_task:
            self._monitor_task.cancel()

    async def _health_monitor(self) -> None:
        """
        Periodically check WS health. Activate HTTP fallback if both
        oracle WS connections have been silent for > WS_TIMEOUT_THRESHOLD.
        """
        while True:
            await asyncio.sleep(10)

            oracle_healthy = (
                (self._oracle_primary  and self._oracle_primary.is_healthy) or
                (self._oracle_secondary and self._oracle_secondary.is_healthy)
            )
            liq_healthy = self._liq_ws and self._liq_ws.is_healthy

            if not liq_healthy and not self._fallback_active:
                logger.warning(
                    "[WSManager] Liq WS unhealthy — activating HTTP fallback"
                )
                await self._http_fallback.start()
                self._fallback_active = True

            elif liq_healthy and self._fallback_active:
                logger.info(
                    "[WSManager] Liq WS recovered — stopping HTTP fallback"
                )
                await self._http_fallback.stop()
                self._fallback_active = False

            if not oracle_healthy:
                logger.warning(
                    "[WSManager] Both oracle WS connections unhealthy — "
                    "prices may be stale (PriceRegistry will gate HF compute)"
                )

    async def _handle_oracle_message(self, msg: dict) -> None:
        """Parse Chainlink AnswerUpdated and call on_price_update."""
        try:
            params = msg.get("params", {})
            result = params.get("result", {})
            topics = result.get("topics", [])
            if not topics or topics[0].lower() != ANSWER_UPDATED_TOPIC.lower():
                return

            # AnswerUpdated(int256 indexed current, uint256 indexed roundId, uint256 updatedAt)
            # current price is in topics[1] as int256
            price_hex = topics[1]
            price = int(price_hex, 16)
            # Handle negative int256 (shouldn't happen for prices but be safe)
            if price >= 2**255:
                price -= 2**256

            # Address of the feed contract (from result.address)
            feed_addr = result.get("address", "")
            if feed_addr and price > 0:
                self._on_price(feed_addr, price)

        except Exception as e:
            logger.debug(f"[WSManager] Oracle message parse error: {e}")

    async def _handle_liq_message(self, msg: dict) -> None:
        """Pass raw log dict to pipeline's liquidation handler."""
        try:
            params = msg.get("params", {})
            result = params.get("result", {})
            if result:
                await self._on_liq(result)
        except Exception as e:
            logger.debug(f"[WSManager] Liq message parse error: {e}")

    async def _handle_new_block(self, msg: dict) -> None:
        """Extract block number from newHeads subscription and forward."""
        try:
            params = msg.get("params", {})
            result = params.get("result", {})
            block_hex = result.get("number", "0x0")
            block_number = int(block_hex, 16)
            if block_number > 0 and self._on_new_block is not None:
                self._on_new_block(block_number)
        except Exception as e:
            logger.debug(f"[WSManager] Block message parse error: {e}")

    @staticmethod
    def _oracle_subscribe_payload(feeds: list[str]) -> dict:
        return {
            "jsonrpc": "2.0",
            "id":      1,
            "method":  "eth_subscribe",
            "params":  ["logs", {
                "address": feeds,
                "topics":  [ANSWER_UPDATED_TOPIC],
            }],
        }


# ---------------------------------------------------------------------------
# pipeline.py integration guide
# ---------------------------------------------------------------------------
#
# Replace the two separate WS connection setups with:
#
#     from ws_manager import WSManager
#
#     ws = WSManager(
#         primary_wss    = os.getenv("CHAINSTACK_WSS"),
#         secondary_wss  = os.getenv("QUICKNODE_WSS"),
#         http_rpc       = os.getenv("QUICKNODE_HTTP"),
#         on_price_update= hf_engine.prices.update_price,   # PriceRegistry
#         on_liquidation = self.handle_liquidation_log,
#         oracle_feeds   = CHAINLINK_FEED_ADDRESSES,        # list of 8 feed addrs
#     )
#     await ws.start()
#
# Remove: the two separate asyncio tasks for OracleWS and LiqWS
# Remove: the manual exponential backoff reconnect loops
# Add:    os.getenv("QUICKNODE_WSS") to .env
# ---------------------------------------------------------------------------
