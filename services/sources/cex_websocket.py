"""
services/sources/cex_websocket.py — CEX price stream listener.

Connects to Binance.US and Coinbase WebSockets, writes live trade prices
to Redis ZSETs for the oracle aggregator. Runs as an asyncio background task.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Dict, Optional

import aiohttp
import redis.asyncio as redis

logger = logging.getLogger("oracle.cex")

# Tracked pairs: symbol -> (binance_stream, coinbase_product)
TRACKED_PAIRS = {
    "ETH":  ("ethusdt", "ETH-USD"),
    "BTC":  ("btcusdt", "BTC-USD"),
    "LINK": ("linkusdt", "LINK-USD"),
}

# CEX WebSocket URLs
BINANCE_WS = "wss://stream.binance.us:9443/ws"  # US-hosted servers
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"

# Redis ZSET key pattern
CEX_PRICE_KEY = "price:cex:{pair}"   # pair = ETH-USDT, BTC-USDT, etc.


class CexWebSocketListener:
    """Listens to Binance + Coinbase trade streams, writes to Redis ZSETs."""

    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self.redis = redis_client
        self._session: Optional[aiohttp.ClientSession] = None
        self._tasks: list = []
        self._prices: Dict[str, Dict[str, float]] = {}  # pair -> {binance: price, coinbase: price}

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    def _binance_streams(self) -> list:
        """Build Binance stream names: ethusdt@trade/btcusdt@trade/..."""
        return [f"{s}@trade" for s, _ in TRACKED_PAIRS.values()]

    async def _listen_binance(self):
        """Connect to Binance.US WebSocket and write prices to Redis."""
        streams = self._binance_streams()
        url = f"{BINANCE_WS}/{ '/'.join(streams) }"
        logger.info("Binance WS connecting: %s", url[:80])

        while True:
            try:
                session = await self._get_session()
                async with session.ws_connect(url) as ws:
                    logger.info("Binance WS connected")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if "data" in data and "s" in data["data"]:
                                ticker_data = data["data"]
                                symbol = ticker_data["s"]  # e.g. "ETHUSDT"
                                price = float(ticker_data["p"])  # last price
                                pair = symbol.replace("USDT", "-USDT")
                                self._prices.setdefault(pair, {})["binance"] = price

                                # Write to Redis ZSET with TTL trim
                                if self.redis:
                                    ts = int(time.time() * 1000)
                                    key = CEX_PRICE_KEY.format(pair=pair)
                                    await self.redis.zadd(key, {str(price): ts})
                                    # Keep last 1 hour
                                    cutoff = ts - 3_600_000
                                    await self.redis.zremrangebyscore(key, 0, cutoff)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Binance WS disconnected: %s. Reconnecting in 5s...", e)
                await asyncio.sleep(5)

    async def _listen_coinbase(self):
        """Connect to Coinbase WebSocket and write prices to Redis."""
        product_ids = [cp for _, cp in TRACKED_PAIRS.values()]
        subscribe_msg = json.dumps({
            "type": "subscribe",
            "product_ids": product_ids,
            "channels": ["ticker"],
        })

        while True:
            try:
                session = await self._get_session()
                async with session.ws_connect(COINBASE_WS) as ws:
                    await ws.send_str(subscribe_msg)
                    logger.info("Coinbase WS connected")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if data.get("type") == "ticker" and "price" in data:
                                pair = data["product_id"].replace("-USD", "-USDT")  # normalize
                                price = float(data["price"])
                                self._prices.setdefault(pair, {})["coinbase"] = price

                                if self.redis:
                                    ts = int(time.time() * 1000)
                                    key = CEX_PRICE_KEY.format(pair=pair)
                                    await self.redis.zadd(key, {str(price): ts})
                                    cutoff = ts - 3_600_000
                                    await self.redis.zremrangebyscore(key, 0, cutoff)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Coinbase WS disconnected: %s. Reconnecting in 5s...", e)
                await asyncio.sleep(5)

    async def start(self):
        """Start both WebSocket listeners as background tasks."""
        self._tasks = [
            asyncio.create_task(self._listen_binance()),
            asyncio.create_task(self._listen_coinbase()),
        ]
        logger.info("CEX listeners started (%d tasks)", len(self._tasks))

    async def stop(self):
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info("CEX listeners stopped")

    def get_latest_prices(self) -> Dict[str, Dict[str, float]]:
        """Return latest CEX prices snapshot."""
        return dict(self._prices)

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
