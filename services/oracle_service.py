"""
services/oracle_service.py — Multi-source price oracle service.

Fetches Aave V3 oracle prices, Chainlink feed prices, and CEX (Binance + Coinbase)
prices. Aggregates via median, writes to Redis, and emits deviation alerts.

Redis keys written:
  price:aave:{asset}        STRING   raw oracle price (8 decimals)
  price:chainlink:{sym}     HASH     latest round data
  price:cex:{pair}          ZSET     rolling trade prices
  price:aggregate:{sym}     STRING   median aggregate price
  price:meta:{sym}          HASH     source metadata + circuit breaker

Usage:
  python -m services.oracle_service

Environment:
  QUICKNODE_HTTP_URL   — RPC for Aave + Chainlink (paid, low-latency)
  REDIS_URL            — Redis connection (default: redis://localhost:6379)
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from pathlib import Path

# Project root
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import redis.asyncio as redis
from dotenv import load_dotenv
load_dotenv(dotenv_path=project_root / ".env")

from services.sources.aave_oracle import AaveOracleFetcher
from services.sources.chainlink_feeds import ChainlinkFetcher
from services.sources.cex_websocket import CexWebSocketListener
from services.aggregator import PriceAggregator

# ─── Logging ──────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | oracle | %(message)s",
)
logger = logging.getLogger("oracle")


# ─── Main Service ─────────────────────────────────────────────

class OracleService:
    """Multi-source price oracle — fetches, aggregates, writes Redis."""

    def __init__(
        self,
        rpc_url: str,
        redis_url: str = "redis://localhost:6379",
        interval: float = 5.0,
    ):
        self.rpc_url = rpc_url
        self.redis_url = redis_url
        self.interval = interval

        self.redis: redis.Redis = None
        self.aave_fetcher: AaveOracleFetcher = None
        self.chainlink_fetcher: ChainlinkFetcher = None
        self.cex_listener: CexWebSocketListener = None
        self.aggregator: PriceAggregator = None

    async def start(self):
        """Initialize connections and start the update loop."""
        # Redis
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        await self.redis.ping()
        logger.info("Redis connected: %s", self.redis_url)

        # Sources
        self.aave_fetcher = AaveOracleFetcher(rpc_url=self.rpc_url)
        self.chainlink_fetcher = ChainlinkFetcher(rpc_url=self.rpc_url)
        self.cex_listener = CexWebSocketListener(redis_client=self.redis)

        # Aggregator
        self.aggregator = PriceAggregator(
            redis_client=self.redis,
            aave_fetcher=self.aave_fetcher,
            chainlink_fetcher=self.chainlink_fetcher,
            cex_listener=self.cex_listener,
        )

        # Start CEX WebSocket listeners (background)
        await self.cex_listener.start()

        # Initial fetch
        logger.info("Oracle service starting (interval=%.0fs)", self.interval)
        await self.aggregator.fetch_and_aggregate()

        # Main loop
        while True:
            try:
                await asyncio.sleep(self.interval)
                t0 = time.monotonic()
                results = await self.aggregator.fetch_and_aggregate()
                elapsed = time.monotonic() - t0

                # Summary log
                count = len(results)
                samples = []
                for sym, agg in sorted(results.items()):
                    if agg.num_sources >= 2:
                        samples.append(f"{sym}=${agg.aggregate:.2f}({agg.num_sources}s)")
                if samples:
                    logger.info("Prices: %s (%.2fs)", " | ".join(samples), elapsed)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Update cycle failed: %s", e, exc_info=True)
                await asyncio.sleep(self.interval)

    async def stop(self):
        """Graceful shutdown."""
        if self.cex_listener:
            await self.cex_listener.stop()
        if self.aave_fetcher:
            await self.aave_fetcher.close()
        if self.chainlink_fetcher:
            await self.chainlink_fetcher.close()
        if self.redis:
            await self.redis.aclose()
        logger.info("Oracle service stopped")


# ─── CLI ──────────────────────────────────────────────────────

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Multi-source price oracle service")
    parser.add_argument("--rpc", default=None, help="RPC URL for Aave + Chainlink")
    parser.add_argument("--redis", default="redis://localhost:6379", help="Redis URL")
    parser.add_argument("--interval", type=float, default=5.0, help="Update interval in seconds")
    args = parser.parse_args()

    rpc = args.rpc or os.getenv("QUICKNODE_HTTP_URL") or os.getenv("CHAINSTACK_ARBITRUM_HTTP_URL") or os.getenv("ARBITRUM_HTTP_URL", "")
    if not rpc:
        logger.error("No RPC URL configured. Set QUICKNODE_HTTP_URL or --rpc")
        sys.exit(1)

    svc = OracleService(rpc_url=rpc, redis_url=args.redis, interval=args.interval)
    try:
        await svc.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await svc.stop()


if __name__ == "__main__":
    asyncio.run(main())
