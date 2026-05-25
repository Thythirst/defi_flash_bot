"""
scanner/websocket_monitor.py — WebSocket block subscription for sub-second detection.

Replaces 15-second HTTP polling with eth_subscribe newHeads.
Latency improvement: ~15,000ms → ~150ms block detection.
"""

import asyncio
import json
import logging
import time
from typing import Callable, Optional, Dict, Any

import websockets

logger = logging.getLogger("websocket_monitor")


class WebSocketBlockMonitor:
    """
    Monitors new blocks via WebSocket subscription.

    Usage:
        monitor = WebSocketBlockMonitor("wss://arb-mainnet.g.alchemy.com/v2/...")
        monitor.on_block = my_callback
        await monitor.start()
    """

    def __init__(
        self,
        ws_url: str,
        reconnect_delay: float = 5.0,
        max_reconnect_delay: float = 60.0,
    ):
        self.ws_url = ws_url
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_delay = max_reconnect_delay
        self.on_block: Optional[Callable[[Dict[str, Any]], None]] = None
        self.on_connect: Optional[Callable[[], None]] = None
        self.on_disconnect: Optional[Callable[[], None]] = None

        self._ws = None
        self._running = False
        self._sub_id: Optional[str] = None
        self._last_block_time: Optional[float] = None
        self._blocks_seen = 0
        self._reconnect_attempts = 0

    @property
    def is_connected(self) -> bool:
        return self._ws is not None and self._ws.open

    @property
    def latency_ms(self) -> Optional[float]:
        """Estimated latency based on time since last block."""
        if self._last_block_time is None:
            return None
        return (time.time() - self._last_block_time) * 1000

    async def start(self) -> None:
        """Start the WebSocket monitor loop."""
        self._running = True
        delay = self.reconnect_delay

        while self._running:
            try:
                await self._connect_and_listen()
                # If we get here, connection closed cleanly
                delay = self.reconnect_delay
                self._reconnect_attempts = 0
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._reconnect_attempts += 1
                logger.error(
                    "WebSocket error (attempt %d): %s",
                    self._reconnect_attempts, e
                )
                if self.on_disconnect:
                    try:
                        self.on_disconnect()
                    except Exception:
                        pass

            if not self._running:
                break

            # Exponential backoff
            logger.info("Reconnecting in %.1fs...", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, self.max_reconnect_delay)

    async def stop(self) -> None:
        """Stop the monitor."""
        self._running = False
        if self._ws:
            await self._ws.close()

    async def _connect_and_listen(self) -> None:
        """Establish WebSocket connection and listen for blocks."""
        logger.info("Connecting to %s...", self.ws_url.split("/")[-1][:20])

        async with websockets.connect(
            self.ws_url,
            ping_interval=20,
            ping_timeout=10,
        ) as ws:
            self._ws = ws
            logger.info("WebSocket connected")

            # Subscribe to newHeads
            subscribe_msg = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": ["newHeads"],
            }
            await ws.send(json.dumps(subscribe_msg))

            # Wait for subscription confirmation
            response = await asyncio.wait_for(ws.recv(), timeout=10)
            resp_data = json.loads(response)
            self._sub_id = resp_data.get("result")
            logger.info("Subscribed to newHeads (sub_id=%s)", self._sub_id)

            if self.on_connect:
                try:
                    self.on_connect()
                except Exception:
                    pass

            # Listen loop
            async for message in ws:
                if not self._running:
                    break

                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    continue

                # Handle subscription notifications
                if data.get("method") == "eth_subscription":
                    params = data.get("params", {})
                    result = params.get("result", {})
                    self._last_block_time = time.time()
                    self._blocks_seen += 1

                    if self.on_block:
                        try:
                            self.on_block(result)
                        except Exception as e:
                            logger.error("Block handler error: %s", e)

    async def send_rpc(self, method: str, params: list) -> Any:
        """Send an RPC request over the WebSocket and await response."""
        if not self.is_connected:
            raise ConnectionError("WebSocket not connected")

        req_id = int(time.time() * 1000) % 1000000
        msg = {
            "jsonrpc": "2.0",
            "id": req_id,
            "method": method,
            "params": params,
        }
        await self._ws.send(json.dumps(msg))

        # Wait for matching response
        while True:
            response = await self._ws.recv()
            data = json.loads(response)
            if data.get("id") == req_id:
                if "error" in data:
                    raise RuntimeError(data["error"])
                return data.get("result")


class HybridMonitor:
    """
    Hybrid WebSocket + HTTP fallback monitor.

    Uses WebSocket for real-time block detection.
    Falls back to HTTP polling if WebSocket disconnects for >30s.
    """

    def __init__(
        self,
        ws_url: str,
        http_url: str,
        fallback_interval: float = 5.0,
    ):
        self.ws = WebSocketBlockMonitor(ws_url)
        self.http_url = http_url
        self.fallback_interval = fallback_interval
        self.on_block: Optional[Callable[[Dict[str, Any]], None]] = None

        self._last_block_number: Optional[int] = None
        self._fallback_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self) -> None:
        """Start hybrid monitoring."""
        self._running = True
        self.ws.on_block = self._on_block
        self.ws.on_disconnect = self._on_disconnect

        # Start fallback poller
        self._fallback_task = asyncio.create_task(self._fallback_loop())

        # Start WebSocket (blocks until stopped)
        await self.ws.start()

    async def stop(self) -> None:
        """Stop all monitoring."""
        self._running = False
        await self.ws.stop()
        if self._fallback_task:
            self._fallback_task.cancel()
            try:
                await self._fallback_task
            except asyncio.CancelledError:
                pass

    def _on_block(self, block: Dict[str, Any]) -> None:
        """Handle new block from WebSocket."""
        block_number = int(block.get("number", "0x0"), 16)
        self._last_block_number = block_number

        if self.on_block:
            self.on_block(block)

    def _on_disconnect(self) -> None:
        """Handle WebSocket disconnect."""
        logger.warning("WebSocket disconnected, fallback polling active")

    async def _fallback_loop(self) -> None:
        """HTTP fallback polling loop."""
        from web3 import Web3
        w3 = Web3(Web3.HTTPProvider(self.http_url))

        while self._running:
            try:
                # Only poll if WebSocket is down
                if not self.ws.is_connected or self.ws.latency_ms is None or self.ws.latency_ms > 30000:
                    block = w3.eth.get_block("latest")
                    block_number = block.number

                    if self._last_block_number is None or block_number > self._last_block_number:
                        self._last_block_number = block_number
                        if self.on_block:
                            self.on_block({
                                "number": hex(block_number),
                                "hash": block.hash.hex(),
                                "timestamp": hex(block.timestamp),
                            })
            except Exception as e:
                logger.debug("Fallback poll error: %s", e)

            await asyncio.sleep(self.fallback_interval)
