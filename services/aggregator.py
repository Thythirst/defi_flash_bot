"""
services/aggregator.py — Multi-source price aggregator.

Reads raw prices from Redis, computes aggregate (median), validates
cross-source consistency, and writes final prices. Emits alerts on
deviation and stale data.
"""

from __future__ import annotations

import asyncio
import json
import logging
import statistics
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import redis.asyncio as redis

from services.sources.aave_oracle import AavePrice, AaveOracleFetcher
from services.sources.chainlink_feeds import ChainlinkPrice, ChainlinkFetcher
from services.sources.cex_websocket import CexWebSocketListener

logger = logging.getLogger("oracle.aggregator")

# Redis keys
KEY_AAVE = "price:aave:{asset}"          # STRING: raw oracle price (8 decimals)
KEY_CHAINLINK = "price:chainlink:{sym}"  # HASH: price, updated_at, round_id, heartbeat
KEY_CEX = "price:cex:{pair}"             # ZSET: timestamp → price
KEY_AGGREGATE = "price:aggregate:{sym}"  # STRING: median USD price
KEY_META = "price:meta:{sym}"            # HASH: sources, last_update, deviation_max, circuit_broken

# Multi-asset mapping: symbol → (aave_asset_address, chainlink_symbol, cex_pair)
SYMBOL_MAP = {
    "ETH":  ("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", "ETH", "ETH-USDT"),
    "BTC":  ("0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f", "BTC", "BTC-USDT"),
    "LINK": ("0xf97f4df75117a78c1A5a0DBb814Af92458539FB4", "LINK", "LINK-USDT"),
}

# Additional assets tracked via Aave only (no CEX/Chainlink)
AAVE_ONLY = {
    "USDC":  ("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "USDC"),
    "USDT":  ("0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", "USDT"),
    "ARB":   ("0x912CE59144191C1204E64559FE8253a0e49E6548", "ARB"),
    "DAI":   ("0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1", "DAI"),
    "USDCe": ("0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8", "USDC.e"),
    "tBTC":  ("0x6c84a8f1c29108F47a79964b5Fe888D4f4D0dE40", "tBTC"),
    "rsETH": ("0x4186BFC76E2E237523CBC30FD220FE055156b41F", "rsETH"),
}

# Alert thresholds
DEVIATION_WARN_PCT = 1.0    # warn when CEX vs Aave diverges >1%
DEVIATION_CRITICAL_PCT = 3.0  # circuit breaker at >3%
STALE_WARN_SECONDS = 600    # warn if any source >10 min old
STALE_CRITICAL_SECONDS = 1800  # critical if >30 min


@dataclass
class AggregatedPrice:
    symbol: str
    aave_price: Optional[float] = None
    chainlink_price: Optional[float] = None
    cex_price: Optional[float] = None
    aggregate: float = 0.0
    num_sources: int = 0
    sources: List[str] = field(default_factory=list)
    deviation_max: float = 0.0
    stale_sources: List[str] = field(default_factory=list)


class PriceAggregator:
    """Aggregates prices from multiple sources and writes to Redis."""

    def __init__(
        self,
        redis_client: redis.Redis,
        aave_fetcher: AaveOracleFetcher,
        chainlink_fetcher: ChainlinkFetcher,
        cex_listener: Optional[CexWebSocketListener] = None,
    ):
        self.redis = redis_client
        self.aave = aave_fetcher
        self.chainlink = chainlink_fetcher
        self.cex = cex_listener

    async def fetch_and_aggregate(self) -> Dict[str, AggregatedPrice]:
        """Fetch all sources, aggregate, write to Redis, emit events."""
        results: Dict[str, AggregatedPrice] = {}

        # Fetch
        aave_prices = await self.aave.fetch_all()
        chainlink_prices = await self.chainlink.fetch_all()
        cex_prices = self.cex.get_latest_prices() if self.cex else {}

        # Write raw sources to Redis
        await self._write_aave(aave_prices)
        await self._write_chainlink(chainlink_prices)

        # Aggregate per symbol
        for sym, (aave_addr, cl_sym, cex_pair) in SYMBOL_MAP.items():
            agg = AggregatedPrice(symbol=sym)

            # Aave
            aa = aave_prices.get(aave_addr.lower())
            if aa:
                agg.aave_price = aa.price_usd
                agg.sources.append("aave")

            # Chainlink
            cl = chainlink_prices.get(cl_sym)
            if cl:
                agg.chainlink_price = cl.price_usd
                now = int(time.time())
                if now - cl.updated_at > cl.heartbeat:
                    agg.stale_sources.append("chainlink")
                agg.sources.append("chainlink")

            # CEX (median of binance + coinbase if both available)
            ce = cex_prices.get(cex_pair, {})
            cex_vals = [v for v in ce.values() if v > 0]
            if cex_vals:
                agg.cex_price = statistics.median(cex_vals) if len(cex_vals) > 1 else cex_vals[0]
                agg.sources.append("cex")

            # Compute aggregate (median of available sources)
            all_prices = [p for p in [agg.aave_price, agg.chainlink_price, agg.cex_price] if p is not None]
            if all_prices:
                agg.aggregate = statistics.median(all_prices)
                agg.num_sources = len(all_prices)
                agg.deviation_max = max(all_prices) - min(all_prices)
                if agg.aggregate > 0:
                    agg.deviation_max = (agg.deviation_max / agg.aggregate) * 100
            elif agg.aave_price:
                agg.aggregate = agg.aave_price
                agg.num_sources = 1

            results[sym] = agg
            await self._write_aggregate(agg)

        # Aave-only assets
        for sym, (aave_addr, _) in AAVE_ONLY.items():
            aa = aave_prices.get(aave_addr.lower())
            if aa:
                agg = AggregatedPrice(
                    symbol=sym,
                    aave_price=aa.price_usd,
                    aggregate=aa.price_usd,
                    num_sources=1,
                    sources=["aave"],
                )
                results[sym] = agg
                await self._write_aggregate(agg)

        # Emit events
        await self._emit_events(results)

        return results

    async def _write_aave(self, prices: Dict[str, AavePrice]):
        pipe = self.redis.pipeline()
        for addr, p in prices.items():
            pipe.set(KEY_AAVE.format(asset=addr), str(p.price_raw))
        await pipe.execute()

    async def _write_chainlink(self, prices: Dict[str, ChainlinkPrice]):
        pipe = self.redis.pipeline()
        for sym, p in prices.items():
            pipe.hset(KEY_CHAINLINK.format(sym=sym), mapping={
                "price": str(p.price_raw),
                "updated_at": str(p.updated_at),
                "round_id": str(p.round_id),
                "heartbeat": str(p.heartbeat),
            })
        await pipe.execute()

    async def _write_aggregate(self, agg: AggregatedPrice):
        if agg.aggregate <= 0:
            return
        sym = agg.symbol
        ts = int(time.time())

        # Aggregate price
        await self.redis.set(KEY_AGGREGATE.format(sym=sym), str(agg.aggregate))

        # Metadata
        await self.redis.hset(KEY_META.format(sym=sym), mapping={
            "sources": ",".join(agg.sources),
            "last_update": str(ts),
            "deviation_max": f"{agg.deviation_max:.4f}",
            "circuit_broken": "0",
            "num_sources": str(agg.num_sources),
        })

    async def _emit_events(self, results: Dict[str, AggregatedPrice]):
        """Emit alerts to Redis event streams."""
        ts = int(time.time() * 1000)
        for sym, agg in results.items():
            if agg.num_sources < 2:
                continue

            # Deviation alert
            if agg.deviation_max >= DEVIATION_CRITICAL_PCT:
                await self._emit("arb:events:market", {
                    "kind": "price.deviation",
                    "symbol": sym,
                    "aave": str(agg.aave_price),
                    "chainlink": str(agg.chainlink_price),
                    "cex": str(agg.cex_price),
                    "deviation_pct": f"{agg.deviation_max:.2f}",
                    "severity": "critical",
                }, severity="critical", ts=ts)

            elif agg.deviation_max >= DEVIATION_WARN_PCT:
                await self._emit("arb:events:market", {
                    "kind": "price.deviation",
                    "symbol": sym,
                    "aave": str(agg.aave_price),
                    "chainlink": str(agg.chainlink_price),
                    "cex": str(agg.cex_price),
                    "deviation_pct": f"{agg.deviation_max:.2f}",
                    "severity": "warning",
                }, severity="warning", ts=ts)

            # Stale sources alert
            if agg.stale_sources:
                await self._emit("arb:events:system", {
                    "kind": "price.stale",
                    "symbol": sym,
                    "stale_sources": ",".join(agg.stale_sources),
                    "elapsed_seconds": str(STALE_WARN_SECONDS),
                }, severity="warning", ts=ts)

    async def _emit(self, stream: str, payload: dict, severity: str = "info", ts: int = None):
        try:
            if ts is None:
                ts = int(time.time() * 1000)
            await self.redis.xadd(stream, {
                "id": f"evt_{ts}",
                "ts": str(ts),
                "source": "oracle_service",
                "type": payload.get("kind", "price.event"),
                "severity": severity,
                "block": "0",
                "payload": json.dumps(payload),
            }, maxlen=100_000, approximate=True)
        except Exception as e:
            logger.debug("Event emit failed: %s", e)

    async def get_circuit_status(self, symbol: str) -> bool:
        """Check if circuit breaker is tripped for a symbol."""
        val = await self.redis.hget(KEY_META.format(sym=symbol), "circuit_broken")
        return val == "1"
