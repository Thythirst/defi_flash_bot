"""
Redis -> PostgreSQL State Sync (ETL).

Syncs Aave indexer state, Chainlink feed data, and CEX market prices
from Redis into PostgreSQL for the Chainlink Impact Simulator.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional

import asyncpg
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

SYMBOL_TO_ADDR: Dict[str, str] = {
    "ETH":   "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
    "USDC":  "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
    "USDT":  "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
    "ARB":   "0x912ce59144191c1204e64559fe8253a0e49e6548",
    "WBTC":  "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
    "DAI":   "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1",
    "LINK":  "0xf97f4df75117a78c1a5a0dbb814af92458539fb4",
    "USDCe": "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8",
}
ADDR_TO_SYMBOL = {v: k for k, v in SYMBOL_TO_ADDR.items()}

CHAINLINK_FEEDS = {
    "ETH":   ("0x639fe6ab55c921f74e7fac1ee960c0b6293ba612", 3600, 500_000_000),
    "WBTC":  ("0x6ce185860a4963101606cfeebb16fe9f3b078330", 3600, 500_000_000),
    "LINK":  ("0x86e53cf1b870786351da77a57575e79cb55812cb", 3600, 1_500_000_000),
    "ARB":   ("0xb2a82404358fe83f1e1d4051b22525ccd4feb847", 86400, 1_000_000_000),
    "USDC":  ("0x50834f3163758fcc1df9973b6e91f0f0f0434ad3", 86400, 500_000_000),
    "USDT":  ("0x3f3f5df88dc9f13eac63df89ec16ef6e7e25d236", 86400, 500_000_000),
    "DAI":   ("0xc5c8e77b397e531b8ec06bfb0048328b30e9ec0b", 3600, 500_000_000),
}


class StateSync:
    """Syncs Redis state into PostgreSQL using asyncpg for performance."""

    def __init__(self, redis_url: str = "redis://localhost:6379", pg_url: str = ""):
        self.redis_url = redis_url
        self.pg_url = pg_url
        self.redis: Optional[aioredis.Redis] = None
        self.pg: Optional[asyncpg.Pool] = None

    async def connect(self):
        self.redis = aioredis.from_url(self.redis_url, decode_responses=True)
        await self.redis.ping()
        self.pg = await asyncpg.create_pool(dsn=self.pg_url, min_size=1, max_size=5)
        async with self.pg.acquire() as conn:
            await conn.execute("SELECT 1")

    async def close(self):
        if self.redis:
            await self.redis.aclose()
        if self.pg:
            await self.pg.close()

    async def sync_all(self):
        """Run all sync steps in sequence."""
        t0 = time.monotonic()
        await self._sync_reserve_configs()
        await self._sync_borrowers()
        await self._sync_chainlink_feeds()
        await self._sync_market_prices()
        elapsed = (time.monotonic() - t0) * 1000
        logger.info("State sync complete in %.0fms", elapsed)

    # ── Reserve configs ─────────────────────────────────────────────────

    async def _sync_reserve_configs(self):
        keys = await self.redis.keys("aave:reserve:*")
        records = []
        for key in keys:
            addr = key.replace("aave:reserve:", "")
            if ":" in addr:
                continue
            data = await self.redis.hgetall(key)
            if not data or "symbol" not in data:
                continue
            price_raw = int(data.get("price", "0") or "0")
            price_usd = Decimal(price_raw) / Decimal("1e8") if price_raw else Decimal("0")
            records.append((
                addr,
                data.get("symbol", "???"),
                int(data.get("decimals", "18")),
                int(data.get("ltv", "0")),
                int(data.get("liquidation_threshold", "8500")),
                int(data.get("liquidation_bonus", "500")),
                int(data.get("reserve_factor", "1000")),
                data.get("is_active", "1") == "1",
                data.get("is_frozen", "0") == "1",
                price_raw,
                price_usd,
            ))

        if not records:
            return

        async with self.pg.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO reserve_configs (
                    reserve_addr, symbol, decimals, ltv_bps, liq_threshold_bps,
                    liq_bonus_bps, reserve_factor_bps, is_active, is_frozen,
                    aave_price_raw, price_usd, updated_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
                ON CONFLICT (reserve_addr) DO UPDATE SET
                    price_usd = EXCLUDED.price_usd,
                    aave_price_raw = EXCLUDED.aave_price_raw,
                    updated_at = NOW()
                """,
                records,
            )

    # ── Borrower positions ──────────────────────────────────────────────

    async def _sync_borrowers(self):
        user_keys = set()
        reserve_keys = await self.redis.keys("aave:reserve:*:users")
        for key in reserve_keys:
            users = await self.redis.smembers(key)
            user_keys.update(users)

        if not user_keys:
            return

        user_list = list(user_keys)
        records = []
        batch_size = 200

        for i in range(0, len(user_list), batch_size):
            batch = user_list[i:i + batch_size]
            pipe = self.redis.pipeline()

            for ua in batch:
                pipe.hget(f"aave:user:{ua}", "positions")
                pipe.hget(f"aave:user:{ua}", "health_factor")
            results = await pipe.execute()

            for j, ua in enumerate(batch):
                pos_raw = results[j * 2]
                hf_raw = results[j * 2 + 1]
                if not pos_raw:
                    continue
                try:
                    if isinstance(pos_raw, bytes):
                        pos_raw = pos_raw.decode()
                    positions = json.loads(pos_raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                if not positions:
                    continue

                hf = None
                if hf_raw and hf_raw != "inf":
                    try:
                        hf = Decimal(str(hf_raw))
                        # Cap at NUMERIC(12,6) max (999999.999999) to prevent overflow
                        if hf > Decimal("999999.999999"):
                            hf = None
                    except Exception:
                        pass

                for reserve_addr, pos in positions.items():
                    if not pos.get("debt", 0) and not pos.get("collateral", 0):
                        continue
                    symbol = ADDR_TO_SYMBOL.get(reserve_addr, reserve_addr[:10])
                    records.append((
                        ua, reserve_addr, symbol,
                        int(pos.get("collateral", 0)),
                        int(pos.get("debt", 0)),
                        Decimal("0"), Decimal("0"),  # USD values computed by simulator
                        pos.get("is_collateral", True),
                        pos.get("is_isolated", False),
                        pos.get("e_mode_category", 0),
                        hf,
                    ))

        if not records:
            return

        async with self.pg.acquire() as conn:
            async with conn.transaction():
                # Clear and repopulate for snapshot consistency
                await conn.execute("DELETE FROM borrow_positions")
                await conn.copy_records_to_table(
                    "borrow_positions",
                    columns=[
                        "user_addr", "reserve_addr", "symbol",
                        "collateral", "debt", "collateral_usd", "debt_usd",
                        "is_collateral", "is_isolated", "e_mode_category",
                        "health_factor",
                    ],
                    records=records,
                )

    # ── Chainlink feeds ─────────────────────────────────────────────────

    async def _sync_chainlink_feeds(self):
        now = int(time.time())
        records = []

        for symbol, (feed_addr, heartbeat, deviation) in CHAINLINK_FEEDS.items():
            data = await self.redis.hgetall(f"price:chainlink:{symbol}")
            if not data:
                # Try meta as fallback
                data = await self.redis.hgetall(f"price:meta:{symbol}") or {}

            price_raw = int(data.get("price", "0") or "0")
            price_usd = Decimal(price_raw) / Decimal("1e8") if price_raw else Decimal("0")
            updated_at = int(data.get("updated_at", "0") or "0")
            round_id = int(data.get("round_id", "0") or "0")
            age = now - updated_at if updated_at else 0
            heartbeat_pct = min(
                (Decimal(str(age)) / Decimal(str(heartbeat)) * 100).quantize(Decimal("0.01")),
                Decimal("9999.99")
            ) if heartbeat > 0 else Decimal("0")

            records.append((
                symbol, feed_addr, round_id, price_raw, price_usd,
                8, updated_at, heartbeat, deviation, age,
                Decimal(str(heartbeat_pct)),
            ))

        if not records:
            return

        async with self.pg.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO chainlink_feeds (
                    symbol, feed_addr, round_id, price_raw, price_usd,
                    decimals, updated_at_ts, heartbeat_sec, deviation_ppb,
                    age_seconds, heartbeat_pct, snapshot_ts
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
                ON CONFLICT (symbol) DO UPDATE SET
                    price_raw = EXCLUDED.price_raw,
                    price_usd = EXCLUDED.price_usd,
                    round_id = EXCLUDED.round_id,
                    updated_at_ts = EXCLUDED.updated_at_ts,
                    age_seconds = EXCLUDED.age_seconds,
                    heartbeat_pct = EXCLUDED.heartbeat_pct,
                    snapshot_ts = NOW()
                """,
                records,
            )

    # ── Market prices ───────────────────────────────────────────────────

    async def _sync_market_prices(self):
        records = []
        for symbol in ["ETH", "WBTC", "LINK", "ARB", "USDC", "USDT", "DAI"]:
            agg = await self.redis.get(f"price:aggregate:{symbol}")
            meta = await self.redis.hgetall(f"price:meta:{symbol}") or {}

            mid_price = Decimal(agg) if agg else Decimal("0")

            cl_price = Decimal("0")
            cl_data = await self.redis.hgetall(f"price:chainlink:{symbol}") or {}
            cl_raw = cl_data.get("price", "0")
            if cl_raw:
                cl_price = Decimal(str(cl_raw)) / Decimal("1e8")

            cl_dev = Decimal("0")
            if cl_price > 0 and mid_price > 0:
                cl_dev = (mid_price - cl_price) / cl_price * 100

            records.append((symbol, mid_price, Decimal("0"), mid_price, Decimal("0"), cl_dev))

        if not records:
            return

        async with self.pg.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO market_prices (
                    symbol, mid_price, binance_price, coinbase_price,
                    spread_pct, cl_deviation_pct, snapshot_ts
                ) VALUES ($1,$2,$3,$4,$5,$6,NOW())
                ON CONFLICT (symbol) DO UPDATE SET
                    mid_price = EXCLUDED.mid_price,
                    cl_deviation_pct = EXCLUDED.cl_deviation_pct,
                    snapshot_ts = NOW()
                """,
                records,
            )
