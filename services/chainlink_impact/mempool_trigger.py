"""
Mempool Oracle Trigger.

Listens for Chainlink oracle update transactions detected by mempool-intel
(via Redis pub/sub channel `arb:signals:oracle_update` or stream `arb:events:system`).
When an oracle update is detected, checks for pre-computed liquidation bundles
and submits the highest-priority one to the execution engine.

Flow:
  mempool-intel detects transmit() → pub/sub → this trigger 
  → lookup precomputed_bundles by feed symbol → push to engine:queue
  → execution-engine broadcasts tx in same block
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Optional

logger = logging.getLogger(__name__)

# Chainlink aggregator address → feed symbol mapping
AGGREGATOR_TO_SYMBOL: Dict[str, str] = {
    "0x639fe6ab55c921f74e7fac1ee960c0b6293ba612": "ETH",
    "0x6ce185860a4963106506c203335a2910413708e9": "WBTC",
    "0x86e53cf1b870786351da77a57575e79cb55812cb": "LINK",
    "0xb2a824043730fe05f3da2efafa1cbbe83fa548d6": "ARB",
    "0x50834f3163758fcc1df9973b6e91f0f0f0434ad3": "USDC",
    "0x3f3f5df88dc9f13eac63df89ec16ef6e7e25dde7": "USDT",
    "0xc5c8e77b397e531b8ec06bfb0048328b30e9ecfb": "DAI",
}


class MempoolOracleTrigger:
    """
    Watches mempool-intel output for Chainlink oracle updates
    and triggers pre-computed liquidation bundles.
    """

    def __init__(self, redis_client, pg_pool, engine_queue_key: str = "engine:queue"):
        self.redis = redis_client
        self.pg = pg_pool
        self.engine_queue_key = engine_queue_key
        self._running = False
        self._trigger_count = 0
        self._last_trigger: Dict[str, float] = {}  # feed → timestamp (debounce)
        self._debounce_sec = int(__import__("os").getenv("MEMPOOL_DEBOUNCE_SEC", "5"))

    async def start(self):
        """Start listening for oracle updates."""
        self._running = True
        logger.info("Mempool oracle trigger started (debounce=%ds)", self._debounce_sec)

        # Subscribe to pub/sub channel
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("arb:signals:oracle_update")

        try:
            async for message in pubsub.listen():
                if not self._running:
                    break
                if message["type"] != "message":
                    continue
                await self._handle_oracle_update(message["data"])
        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe("arb:signals:oracle_update")
            logger.info("Mempool oracle trigger stopped (%d triggers)", self._trigger_count)

    async def stop(self):
        self._running = False

    async def _handle_oracle_update(self, raw: str):
        """
        Process an oracle update event from mempool-intel.
        
        Expected format (JSON from mempool-intel):
        {
          "type": "oracle_update",
          "tx_hash": "0x...",
          "to_addr": "0x639fe6...",  // aggregator address
          "detail": "Chainlink ETH/USD aggregator called",
          "timestamp": 1234567890.123
        }
        """
        try:
            data = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            logger.debug("Unparseable oracle update: %.100s", str(raw))
            return

        tx_hash = data.get("tx_hash", "")
        to_addr = (data.get("to_addr") or data.get("to") or "").lower()

        # Map aggregator address to feed symbol
        symbol = AGGREGATOR_TO_SYMBOL.get(to_addr)
        if not symbol:
            # Try to infer from detail string
            detail = data.get("detail", "")
            for sym in AGGREGATOR_TO_SYMBOL.values():
                if sym in detail.upper():
                    symbol = sym
                    break
        if not symbol:
            logger.debug("Unknown oracle update from %s", to_addr)
            return

        # Debounce — prevent duplicate triggers within debounce window
        now = time.monotonic()
        last = self._last_trigger.get(symbol, 0)
        if now - last < self._debounce_sec:
            logger.debug("Debounced %s oracle trigger (%.1fs since last)", symbol, now - last)
            return
        self._last_trigger[symbol] = now

        logger.info("Oracle update detected: %s/%s tx=%s", symbol, to_addr[:10], tx_hash[:16])
        await self._trigger_liquidation(symbol, to_addr, tx_hash)

    async def _trigger_liquidation(self, symbol: str, feed_addr: str, tx_hash: str):
        """
        Find the highest-priority pre-computed liquidation bundle for this feed
        and submit it to the execution engine.
        """
        t0 = time.monotonic()

        # Query pre-computed bundles for this feed, highest priority first
        rows = await self.pg.fetch(
            """SELECT bundle_id, user_addr, debt_asset, coll_asset, contract_addr,
                      calldata, value_wei, expected_profit_usd, priority, bundle_type
               FROM precomputed_bundles
               WHERE trigger_feed = $1 AND is_consumed = false AND expires_at > NOW()
               ORDER BY priority ASC, expected_profit_usd DESC
               LIMIT 1""",
            symbol,
        )

        if not rows:
            # Log the missed opportunity
            await self._log_trigger(symbol, feed_addr, tx_hash, None, False, int((time.monotonic() - t0) * 1000))
            logger.info("No pre-computed bundle for %s — missed opportunity", symbol)
            return

        row = rows[0]
        bundle_id = row["bundle_id"]
        reaction_ms = int((time.monotonic() - t0) * 1000)

        # Push to execution engine queue
        request_id = str(uuid.uuid4())
        priority_score = float(row["expected_profit_usd"]) * 100.0  # profit-weighted priority

        # Build execution request for engine:queue
        request_data = json.dumps({
            "request_id": request_id,
            "exec_type": "liquidation",
            "contract_address": row["contract_addr"],
            "calldata": row["calldata"],
            "value_wei": str(row["value_wei"]),
            "expected_profit_usd": float(row["expected_profit_usd"]),
            "priority": priority_score,
            "metadata": {
                "bundle_id": str(bundle_id),
                "user": row["user_addr"],
                "debt_asset": row["debt_asset"],
                "coll_asset": row["coll_asset"],
                "trigger_feed": symbol,
                "trigger_tx": tx_hash,
                "bundle_type": row["bundle_type"],
                "reaction_ms": reaction_ms,
            },
        })

        await self.redis.zadd(self.engine_queue_key, {request_id: priority_score})
        await self.redis.hset(f"engine:pending:{request_id}", mapping={
            "type": "liquidation",
            "contract": row["contract_addr"],
            "calldata": row["calldata"][:200],
            "expected_profit_usd": str(row["expected_profit_usd"]),
            "priority": str(priority_score),
            "status": "queued",
            "created_at": str(time.time()),
            "metadata": json.dumps({"bundle_id": str(bundle_id), "trigger": symbol}),
        })

        # Mark bundle as consumed
        await self.pg.execute(
            "UPDATE precomputed_bundles SET is_consumed = true WHERE bundle_id = $1",
            bundle_id,
        )

        self._trigger_count += 1

        # Log trigger
        await self._log_trigger(symbol, feed_addr, tx_hash, bundle_id, True, reaction_ms, request_id)

        logger.info(
            "TRIGGERED %s liquidation: user=%s debt=%s coll=%s profit=$%.0f "
            "reaction=%dms req=%s",
            symbol, row["user_addr"][:10], row["debt_asset"],
            row["coll_asset"], float(row["expected_profit_usd"]),
            reaction_ms, request_id[:8],
        )

    async def _log_trigger(
        self, symbol: str, feed_addr: str, tx_hash: str,
        bundle_id: Optional[uuid.UUID], was_triggered: bool,
        reaction_ms: int, execution_id: Optional[str] = None,
    ):
        """Log trigger event to PG for audit trail."""
        try:
            async with self.pg.acquire() as conn:
                await conn.execute(
                    """INSERT INTO oracle_trigger_log
                       (symbol, feed_addr, tx_hash, signal_id, was_triggered,
                        execution_id, reaction_ms)
                       VALUES ($1,$2,$3,$4,$5,$6,$7)""",
                    symbol, feed_addr, tx_hash, bundle_id, was_triggered,
                    execution_id, reaction_ms,
                )
        except Exception:
            logger.debug("Failed to log trigger", exc_info=True)

    @property
    def stats(self) -> dict:
        return {
            "trigger_count": self._trigger_count,
            "last_triggers": self._last_trigger,
        }
