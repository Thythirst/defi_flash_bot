"""
Chainlink Impact Simulator — Production Service v2.

Adds: velocity tracking, mempool trigger, precomputed bundles, engine integration.
"""

from __future__ import annotations

import asyncio
import json as _json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import asyncpg
import redis.asyncio as aioredis

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from dotenv import load_dotenv
load_dotenv()

from services.chainlink_impact.sync import StateSync
from services.chainlink_impact.simulator import (
    BatchSimulator, ChainlinkImpactSimulator, validate_column_clamps,
)
from services.chainlink_impact.velocity import DeviationVelocity
from services.chainlink_impact.precompute import PrecomputeEngine
from services.chainlink_impact.mempool_trigger import MempoolOracleTrigger

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
logger = logging.getLogger("chainlink-impact")
vlogger = logging.getLogger("chainlink-impact.velocity")
mlogger = logging.getLogger("chainlink-impact.mempool")

DB_URL = os.getenv("MEV_DATABASE_URL")
if not DB_URL:
    logger.error("MEV_DATABASE_URL not set")
    sys.exit(1)
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
SYNC_INTERVAL = int(os.getenv("SYNC_INTERVAL", "30"))
BATCH_INTERVAL = int(os.getenv("BATCH_SCENARIO_INTERVAL", "300"))
MEMPOOL_ENABLED = os.getenv("MEMPOOL_TRIGGER_ENABLED", "1") == "1"
PRECOMPUTE_ENABLED = os.getenv("PRECOMPUTE_ENABLED", "1") == "1"


async def _run_deviation_sim(pg_pool, redis_client, velocity, precompute, min_profit):
    sim = ChainlinkImpactSimulator(pg_pool, redis_client)
    await sim.load_state()
    batch = BatchSimulator(sim)
    results = await batch.run_deviation_based(min_profit_usd=min_profit)
    for r in results:
        await sim.emit_signals(r, velocity_tracker=velocity, precompute=precompute)
    return results


async def _run_scenarios(pg_pool, redis_client, velocity, precompute, min_profit):
    sim = ChainlinkImpactSimulator(pg_pool, redis_client)
    await sim.load_state()
    results = []
    for name, shocks in ChainlinkImpactSimulator.SCENARIOS.items():
        # ── Velocity Gate ──────────────────────────────────────────
        # liquidation_cascade requires a minimum market velocity to be
        # credible. In flat markets (ETH dev < 0.1%), the ETH −12% shock
        # is pure fantasy and generates 95%+ false-positive PRE-LIQ events.
        # Gate: at least one primary feed must show velocity ≥ 0.001%/s.
        if name == "liquidation_cascade":
            vel_results = velocity.get_summary()
            primary_feeds = {"ETH", "WBTC"}
            max_abs_vel = 0.0
            for vr in vel_results:
                if vr.symbol in primary_feeds:
                    max_abs_vel = max(max_abs_vel, float(abs(vr.velocity_pps)))
            # Require the largest primary feed to show meaningful movement
            if max_abs_vel < 0.0005:  # 0.0005 %/s ≈ 1.8%/hr
                logger.info(
                    "🚫 VELOCITY GATE: suppressing liquidation_cascade — "
                    "max_abs_vel=%.6f%%/s (threshold=0.0005%%/s) | "
                    "market is flat, scenario is unreachable",
                    max_abs_vel,
                )
                continue
            logger.info(
                "✅ VELOCITY GATE: liquidation_cascade enabled — "
                "max_abs_vel=%.6f%%/s",
                max_abs_vel,
            )

        result = await sim.simulate(shocks, scenario_name=name, min_profit_usd=min_profit)
        await sim.store_results(result)
        await sim.emit_signals(result, velocity_tracker=velocity, precompute=precompute)
        results.append(result)
        logger.info("scenario=%s new_liq=%d profit=$%.0f", name, result.newly_liquidatable, float(result.estimated_profit))
    return results


async def _health_writer(metrics: dict):
    log_dir = Path(__file__).parent.parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    health_file = log_dir / "chainlink_sim_health.json"
    while True:
        health = {
            "service": "chainlink-impact-simulator",
            "status": "healthy",
            "uptime_seconds": int(time.time() - metrics["start_time"]),
            "last_sync": metrics.get("last_sync"),
            "last_deviation_run": metrics.get("last_deviation_run"),
            "last_scenario_run": metrics.get("last_scenario_run"),
            "total_simulations": metrics.get("total_simulations", 0),
            "total_signals": metrics.get("total_signals", 0),
            "mempool_triggers": metrics.get("mempool_triggers", 0),
            "bundles_active": metrics.get("bundles_active", 0),
            "velocity_feeds": metrics.get("velocity_feeds", 0),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        health_file.write_text(_json.dumps(health, indent=2))
        await asyncio.sleep(10)


async def main():
    logger.info("Chainlink Impact Simulator v2 starting...")
    start_time = time.time()
    metrics = dict(start_time=start_time, total_simulations=0, total_signals=0, mempool_triggers=0, bundles_active=0, velocity_feeds=0)
    min_profit = Decimal(os.getenv("MIN_PROFIT_USD", "25"))

    pg_pool = await asyncpg.create_pool(dsn=DB_URL, min_size=2, max_size=10, command_timeout=30)
    logger.info("PostgreSQL connected")

    # Validate application clamps against actual PG schema (refuse to start on mismatch)
    if not await validate_column_clamps(pg_pool):
        logger.critical("Column clamp validation failed — exiting")
        sys.exit(1)

    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    await redis_client.ping()
    logger.info("Redis connected")

    syncer = StateSync(redis_url=REDIS_URL, pg_url=DB_URL)
    velocity = DeviationVelocity(pg_pool)
    precompute = PrecomputeEngine(pg_pool, redis_client) if PRECOMPUTE_ENABLED else None

    # Start mempool trigger as background task
    mempool_task = None
    if MEMPOOL_ENABLED:
        trigger = MempoolOracleTrigger(redis_client, pg_pool)
        mempool_task = asyncio.create_task(trigger.start(), name="mempool-trigger")
        logger.info("Mempool oracle trigger enabled")
    else:
        trigger = None
        logger.info("Mempool trigger disabled")

    monitor_task = asyncio.create_task(_health_writer(metrics))
    last_batch_time = time.monotonic()

    try:
        while True:
            cycle_start = time.monotonic()

            # Step 1: Sync state
            logger.info("Syncing Redis -> PostgreSQL...")
            try:
                if syncer.redis is None:
                    await syncer.connect()
                await syncer.sync_all()
            except Exception:
                logger.exception("Sync failed, reconnecting...")
                try: await syncer.close()
                except Exception: pass
                await syncer.connect()
                await syncer.sync_all()
            metrics["last_sync"] = datetime.now(timezone.utc).isoformat()

            # Step 2: Update deviation velocity
            try:
                async with pg_pool.acquire() as conn:
                    rows = await conn.fetch(
                        "SELECT symbol, mid_price, cl_deviation_pct FROM market_prices"
                    )
                    cl_rows = await conn.fetch(
                        "SELECT symbol, price_usd FROM chainlink_feeds"
                    )
                cl_prices = {r["symbol"]: Decimal(str(r["price_usd"] or 0)) for r in cl_rows}
                for row in rows:
                    sym = row["symbol"]
                    cl_price = cl_prices.get(sym, Decimal("0"))
                    mkt_price = Decimal(str(row["mid_price"] or 0))
                    if cl_price > 0 and mkt_price > 0:
                        await velocity.record(sym, cl_price, mkt_price)
                vel_results = velocity.get_summary()
                metrics["velocity_feeds"] = len(vel_results)
                for vr in vel_results[:5]:
                    vlogger.info("%s dev=%.2f%% vel=%.4fpps dir=%s conf=%.2f eta=%s",
                                 vr.symbol, float(vr.deviation_pct), float(vr.velocity_pps),
                                 vr.direction, float(vr.confidence),
                                 f"{vr.estimated_departure_sec:.0f}s" if vr.estimated_departure_sec else "N/A")
            except Exception:
                vlogger.debug("Velocity update failed", exc_info=True)

            # Step 3: Deviation-based simulation
            logger.info("Running deviation-based simulations...")
            dev_results = await _run_deviation_sim(pg_pool, redis_client, velocity, precompute, min_profit)
            metrics["last_deviation_run"] = datetime.now(timezone.utc).isoformat()
            metrics["total_simulations"] += len(dev_results)

            # Step 4: Full scenarios (wall-clock)
            if time.monotonic() - last_batch_time >= BATCH_INTERVAL:
                last_batch_time = time.monotonic()
                logger.info("Running full scenario batch...")
                scenario_results = await _run_scenarios(pg_pool, redis_client, velocity, precompute, min_profit)
                metrics["last_scenario_run"] = datetime.now(timezone.utc).isoformat()
                metrics["total_simulations"] += len(scenario_results)

            # Step 5: Purge expired bundles
            if precompute:
                await precompute.purge_expired()
                try:
                    async with pg_pool.acquire() as conn:
                        count = await conn.fetchval(
                            "SELECT count(*) FROM precomputed_bundles WHERE is_consumed = false AND expires_at > NOW()"
                        )
                        metrics["bundles_active"] = count
                except Exception:
                    pass

            # Step 6: Mempool trigger stats
            if trigger:
                metrics["mempool_triggers"] = trigger._trigger_count

            elapsed = time.monotonic() - cycle_start
            sleep_time = max(0, SYNC_INTERVAL - elapsed)
            logger.info("Cycle: %.1fs (vel=%d feeds, bundles=%d, triggers=%d, sleep %.1fs)",
                         elapsed, metrics["velocity_feeds"], metrics["bundles_active"],
                         metrics["mempool_triggers"], sleep_time)
            await asyncio.sleep(sleep_time)

    except asyncio.CancelledError:
        logger.info("Shutting down...")
    except Exception:
        logger.exception("Fatal error in main loop")
        sys.exit(1)
    finally:
        if mempool_task:
            mempool_task.cancel()
        monitor_task.cancel()
        if trigger:
            await trigger.stop()
        await redis_client.aclose()
        await pg_pool.close()
        logger.info("Shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())
