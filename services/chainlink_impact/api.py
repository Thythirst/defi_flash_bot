"""
Chainlink Impact Simulator — REST API (FastAPI).

Exposes simulation results, liquidation signals, deviation velocity,
and system health metrics via HTTP endpoints.

Endpoints:
  GET  /health                    — system health
  GET  /metrics                   — Prometheus metrics
  GET  /api/v1/simulations        — recent simulation runs
  GET  /api/v1/simulations/{id}   — single run with results
  GET  /api/v1/signals            — active liquidation signals
  GET  /api/v1/signals/top        — top-N by profit
  GET  /api/v1/velocity           — current deviation velocity
  GET  /api/v1/bundles            — active precomputed bundles
  GET  /api/v1/triggers           — oracle trigger log
  POST /api/v1/simulate           — trigger on-demand simulation
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

import asyncpg
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import Counter, Gauge, Histogram, generate_latest, REGISTRY

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from dotenv import load_dotenv
load_dotenv()

from services.chainlink_impact.simulator import ChainlinkImpactSimulator
from services.chainlink_impact.sync import StateSync
from services.chainlink_impact.velocity import DeviationVelocity
from services.chainlink_impact.precompute import PrecomputeEngine

logger = logging.getLogger("chainlink-api")

# ── App setup ─────────────────────────────────────────────────────────────

app = FastAPI(
    title="Chainlink Impact Simulator API",
    version="2.0.0",
    description="Pre-computed liquidation opportunities from Chainlink oracle deviation analysis",
)

DB_URL = os.getenv("MEV_DATABASE_URL", "postgresql://mev@localhost:5432/mev_simulator")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# ── Prometheus metrics ────────────────────────────────────────────────────

METRIC_PREFIX = "chainlink_sim"

simulations_total = Counter(
    f"{METRIC_PREFIX}_simulations_total", "Total simulations run",
    ["run_type"]
)
signals_active = Gauge(
    f"{METRIC_PREFIX}_signals_active", "Active liquidation signals"
)
bundles_active = Gauge(
    f"{METRIC_PREFIX}_bundles_active", "Active precomputed bundles"
)
velocity_feeds = Gauge(
    f"{METRIC_PREFIX}_velocity_feeds", "Feeds with velocity data"
)
mempool_triggers_total = Counter(
    f"{METRIC_PREFIX}_mempool_triggers_total", "Mempool oracle triggers"
)
simulation_duration = Histogram(
    f"{METRIC_PREFIX}_simulation_duration_seconds", "Simulation duration",
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0]
)
api_requests = Counter(
    f"{METRIC_PREFIX}_api_requests_total", "API requests",
    ["endpoint", "method"]
)
db_health = Gauge(
    f"{METRIC_PREFIX}_db_healthy", "PostgreSQL connectivity"
)
redis_health = Gauge(
    f"{METRIC_PREFIX}_redis_healthy", "Redis connectivity"
)


# ── Connection pools (initialized on startup) ─────────────────────────────

pg_pool: Optional[asyncpg.Pool] = None
redis_client: Optional[aioredis.Redis] = None
velocity_tracker: Optional[DeviationVelocity] = None
precompute: Optional[PrecomputeEngine] = None
syncer: Optional[StateSync] = None


@app.on_event("startup")
async def startup():
    global pg_pool, redis_client, velocity_tracker, precompute, syncer

    pg_pool = await asyncpg.create_pool(dsn=DB_URL, min_size=2, max_size=10, command_timeout=10)
    logger.info("PostgreSQL pool created")

    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    await redis_client.ping()
    logger.info("Redis connected")

    velocity_tracker = DeviationVelocity(pg_pool)
    precompute = PrecomputeEngine(pg_pool, redis_client)
    syncer = StateSync(redis_url=REDIS_URL, pg_url=DB_URL)

    # Seed velocity data from existing snapshots
    try:
        async with pg_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT symbol, cl_price, market_price, deviation_pct, "
                "EXTRACT(EPOCH FROM snapshot_ts) as ts "
                "FROM deviation_snapshots ORDER BY snapshot_ts DESC LIMIT 100"
            )
        for row in reversed(rows):
            if row["cl_price"] and row["market_price"]:
                # Inject into velocity tracker's history
                point = type("Point", (), {
                    "symbol": row["symbol"],
                    "cl_price": Decimal(str(row["cl_price"])),
                    "market_price": Decimal(str(row["market_price"])),
                    "deviation_pct": Decimal(str(row["deviation_pct"])),
                    "timestamp": float(row["ts"]),
                })
                if row["symbol"] not in velocity_tracker._history:
                    velocity_tracker._history[row["symbol"]] = []
                velocity_tracker._history[row["symbol"]].append(point)
        logger.info("Velocity tracker seeded with %d snapshots", len(rows))
    except Exception:
        logger.warning("Failed to seed velocity tracker", exc_info=True)

    logger.info("API startup complete")


@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        await redis_client.aclose()
    if pg_pool:
        await pg_pool.close()
    logger.info("API shutdown complete")


# ── Middleware ─────────────────────────────────────────────────────────────

@app.middleware("http")
async def metrics_middleware(request, call_next):
    start = time.monotonic()
    response = await call_next(request)
    elapsed = time.monotonic() - start
    api_requests.labels(
        endpoint=request.url.path,
        method=request.method,
    ).inc()
    return response


# ── Health ─────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    """System health check."""
    db_ok = False
    redis_ok = False

    try:
        async with pg_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        db_ok = True
    except Exception:
        pass

    try:
        await redis_client.ping()
        redis_ok = True
    except Exception:
        pass

    db_health.set(1 if db_ok else 0)
    redis_health.set(1 if redis_ok else 0)

    # Get active counts
    active_bundles = 0
    active_signals = 0
    try:
        async with pg_pool.acquire() as conn:
            active_bundles = await conn.fetchval(
                "SELECT count(*) FROM precomputed_bundles WHERE is_consumed = false AND expires_at > NOW()"
            )
            active_signals = await conn.fetchval(
                "SELECT count(*) FROM liquidation_signals WHERE expires_at > NOW()"
            )
    except Exception:
        pass

    bundles_active.set(active_bundles)
    signals_active.set(active_signals)

    vel_results = velocity_tracker.get_summary() if velocity_tracker else []
    velocity_feeds.set(len(vel_results))

    return {
        "service": "chainlink-impact-simulator",
        "version": "2.0.0",
        "status": "healthy" if (db_ok and redis_ok) else "degraded",
        "database": "connected" if db_ok else "disconnected",
        "redis": "connected" if redis_ok else "disconnected",
        "active": {
            "bundles": active_bundles,
            "signals": active_signals,
            "velocity_feeds": len(vel_results),
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/metrics")
async def metrics():
    """Prometheus metrics endpoint."""
    return PlainTextResponse(generate_latest(REGISTRY), media_type="text/plain")


# ── Simulation Results ────────────────────────────────────────────────────

@app.get("/api/v1/simulations")
async def list_simulations(
    limit: int = Query(default=20, le=100),
    run_type: Optional[str] = None,
    status: str = "completed",
):
    """List recent simulation runs."""
    query = "SELECT run_id, run_type, scenario_name, feed_symbols, price_shocks, "
    query += "total_borrowers, newly_liquidatable, total_opportunities, "
    query += "estimated_profit, top_opportunity_usd, status, elapsed_ms, "
    query += "started_at, completed_at "
    query += "FROM simulation_runs WHERE status = $1 "
    params = [status]
    idx = 2
    if run_type:
        query += f"AND run_type = ${idx} "
        params.append(run_type)
        idx += 1
    query += "ORDER BY started_at DESC LIMIT $" + str(idx)
    params.append(limit)

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return {
        "count": len(rows),
        "simulations": [
            {
                "run_id": str(r["run_id"]),
                "run_type": r["run_type"],
                "scenario": r["scenario_name"],
                "feeds": r["feed_symbols"],
                "shocks": dict(r["price_shocks"]) if r["price_shocks"] else None,
                "total_borrowers": r["total_borrowers"],
                "newly_liquidatable": r["newly_liquidatable"],
                "opportunities": r["total_opportunities"],
                "estimated_profit": float(r["estimated_profit"]) if r["estimated_profit"] else 0,
                "top_opportunity": float(r["top_opportunity_usd"]) if r["top_opportunity_usd"] else 0,
                "status": r["status"],
                "elapsed_ms": r["elapsed_ms"],
                "started_at": r["started_at"].isoformat() if r["started_at"] else None,
                "completed_at": r["completed_at"].isoformat() if r["completed_at"] else None,
            }
            for r in rows
        ],
    }


@app.get("/api/v1/simulations/{run_id}")
async def get_simulation(run_id: str):
    """Get a single simulation run with its results."""
    async with pg_pool.acquire() as conn:
        run = await conn.fetchrow(
            "SELECT * FROM simulation_runs WHERE run_id = $1",
            uuid.UUID(run_id),
        )
        if not run:
            raise HTTPException(status_code=404, detail="Simulation not found")

        results = await conn.fetch(
            """SELECT user_addr, hf_before, hf_after, hf_delta_pct,
                      is_liquidatable, debt_asset, debt_asset_usd,
                      coll_asset, coll_asset_usd, gross_profit_usd,
                      net_profit_usd, profit_rank
               FROM simulation_results
               WHERE run_id = $1
               ORDER BY net_profit_usd DESC NULLS LAST
               LIMIT 100""",
            uuid.UUID(run_id),
        )

    return {
        "run": {
            "run_id": str(run["run_id"]),
            "run_type": run["run_type"],
            "scenario": run["scenario_name"],
            "feeds": run["feed_symbols"],
            "shocks": dict(run["price_shocks"]) if run["price_shocks"] else None,
            "total_borrowers": run["total_borrowers"],
            "newly_liquidatable": run["newly_liquidatable"],
            "estimated_profit": float(run["estimated_profit"]) if run["estimated_profit"] else 0,
            "top_opportunity": float(run["top_opportunity_usd"]) if run["top_opportunity_usd"] else 0,
            "elapsed_ms": run["elapsed_ms"],
            "status": run["status"],
            "started_at": run["started_at"].isoformat() if run["started_at"] else None,
        },
        "results": [
            {
                "user": r["user_addr"],
                "hf_before": float(r["hf_before"]) if r["hf_before"] else None,
                "hf_after": float(r["hf_after"]) if r["hf_after"] else None,
                "hf_delta_pct": float(r["hf_delta_pct"]) if r["hf_delta_pct"] else None,
                "is_liquidatable": r["is_liquidatable"],
                "debt_asset": r["debt_asset"],
                "debt_usd": float(r["debt_asset_usd"]) if r["debt_asset_usd"] else 0,
                "coll_asset": r["coll_asset"],
                "coll_usd": float(r["coll_asset_usd"]) if r["coll_asset_usd"] else 0,
                "gross_profit": float(r["gross_profit_usd"]) if r["gross_profit_usd"] else 0,
                "net_profit": float(r["net_profit_usd"]) if r["net_profit_usd"] else 0,
                "profit_rank": r["profit_rank"],
            }
            for r in results
        ],
    }


# ── Liquidation Signals ───────────────────────────────────────────────────

@app.get("/api/v1/signals")
async def list_signals(
    limit: int = Query(default=20, le=100),
    min_profit: float = Query(default=0, ge=0),
    trigger_feed: Optional[str] = None,
):
    """List active liquidation signals."""
    query = "SELECT * FROM liquidation_signals WHERE expires_at > NOW() "
    params = []
    if trigger_feed:
        query += "AND trigger_feed = $1 "
        params.append(trigger_feed)
    query += f"AND net_profit_usd >= ${len(params) + 1} "
    params.append(min_profit)
    query += "ORDER BY priority ASC, net_profit_usd DESC "
    query += f"LIMIT ${len(params) + 1}"
    params.append(limit)

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return {
        "count": len(rows),
        "signals": [
            {
                "signal_id": str(r["signal_id"]),
                "user": r["user_addr"],
                "trigger_feed": r["trigger_feed"],
                "trigger_shock_pct": float(r["trigger_shock_pct"]) if r["trigger_shock_pct"] else 0,
                "debt_asset": r["debt_asset"],
                "debt_usd": float(r["debt_usd"]) if r["debt_usd"] else 0,
                "coll_asset": r["coll_asset"],
                "coll_usd": float(r["coll_usd"]) if r["coll_usd"] else 0,
                "hf_before": float(r["hf_before"]) if r["hf_before"] else None,
                "hf_after": float(r["hf_after"]) if r["hf_after"] else None,
                "net_profit": float(r["net_profit_usd"]) if r["net_profit_usd"] else 0,
                "priority": r["priority"],
                "confidence": float(r["confidence"]) if r["confidence"] else 0,
                "published_to_redis": r["published_to_redis"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "expires_at": r["expires_at"].isoformat() if r["expires_at"] else None,
            }
            for r in rows
        ],
    }


@app.get("/api/v1/signals/top")
async def top_signals(
    n: int = Query(default=10, le=50),
    trigger_feed: Optional[str] = None,
):
    """Top-N liquidation signals by net profit."""
    query = "SELECT * FROM liquidation_signals WHERE expires_at > NOW() "
    params = []
    if trigger_feed:
        query += "AND trigger_feed = $1 "
        params.append(trigger_feed)
    query += f"ORDER BY net_profit_usd DESC NULLS LAST LIMIT ${len(params) + 1}"
    params.append(n)

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return {
        "count": len(rows),
        "top_signals": [
            {
                "rank": i + 1,
                "user": r["user_addr"],
                "trigger": r["trigger_feed"],
                "debt": r["debt_asset"],
                "coll": r["coll_asset"],
                "net_profit": float(r["net_profit_usd"]) if r["net_profit_usd"] else 0,
                "confidence": float(r["confidence"]) if r["confidence"] else 0,
            }
            for i, r in enumerate(rows)
        ],
    }


# ── Deviation Velocity ───────────────────────────────────────────────────

@app.get("/api/v1/velocity")
async def get_velocity():
    """Current deviation velocity for all tracked feeds."""
    results = velocity_tracker.get_summary() if velocity_tracker else []

    return {
        "feeds_tracked": len(results),
        "velocities": [
            {
                "symbol": r.symbol,
                "deviation_pct": float(r.deviation_pct),
                "velocity_pps": float(r.velocity_pps),
                "direction": r.direction,
                "confidence": float(r.confidence),
                "estimated_departure_sec": r.estimated_departure_sec,
            }
            for r in results
        ],
    }


# ── Precomputed Bundles ──────────────────────────────────────────────────

@app.get("/api/v1/bundles")
async def list_bundles(
    limit: int = Query(default=20, le=100),
    trigger_feed: Optional[str] = None,
):
    """List active precomputed liquidation bundles."""
    query = """SELECT bundle_id, user_addr, trigger_feed, bundle_type,
                      debt_asset, coll_asset, expected_profit_usd, priority,
                      created_at, expires_at
               FROM precomputed_bundles
               WHERE is_consumed = false AND expires_at > NOW() """
    params = []
    if trigger_feed:
        query += "AND trigger_feed = $1 "
        params.append(trigger_feed)
    query += f"ORDER BY priority ASC, expected_profit_usd DESC LIMIT ${len(params) + 1}"
    params.append(limit)

    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return {
        "count": len(rows),
        "bundles": [
            {
                "bundle_id": str(r["bundle_id"]),
                "user": r["user_addr"],
                "trigger_feed": r["trigger_feed"],
                "type": r["bundle_type"],
                "debt_asset": r["debt_asset"],
                "coll_asset": r["coll_asset"],
                "expected_profit": float(r["expected_profit_usd"]) if r["expected_profit_usd"] else 0,
                "priority": r["priority"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "expires_at": r["expires_at"].isoformat() if r["expires_at"] else None,
                "ttl_seconds": (
                    (r["expires_at"] - datetime.now(timezone.utc)).total_seconds()
                    if r["expires_at"] else 0
                ),
            }
            for r in rows
        ],
    }


# ── Oracle Trigger Log ────────────────────────────────────────────────────

@app.get("/api/v1/triggers")
async def list_triggers(limit: int = Query(default=50, le=200)):
    """Recent oracle trigger events."""
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT symbol, feed_addr, tx_hash, was_triggered,
                      reaction_ms, execution_id, snapshot_ts
               FROM oracle_trigger_log
               ORDER BY snapshot_ts DESC LIMIT $1""",
            limit,
        )

    return {
        "count": len(rows),
        "triggers": [
            {
                "symbol": r["symbol"],
                "feed": r["feed_addr"],
                "tx_hash": r["tx_hash"],
                "triggered": r["was_triggered"],
                "reaction_ms": r["reaction_ms"],
                "execution_id": r["execution_id"],
                "timestamp": r["snapshot_ts"].isoformat() if r["snapshot_ts"] else None,
            }
            for r in rows
        ],
    }


# ── On-Demand Simulation ──────────────────────────────────────────────────

@app.post("/api/v1/simulate")
async def trigger_simulation(
    shocks: dict = None,  # {"ETH": -5.0, "LINK": -10.0}
    scenario: Optional[str] = None,
    min_profit: float = 25.0,
):
    """Trigger an on-demand simulation with custom price shocks or named scenario."""
    if not shocks and not scenario:
        raise HTTPException(status_code=400, detail="Provide 'shocks' dict or 'scenario' name")

    if scenario:
        scenarios = ChainlinkImpactSimulator.SCENARIOS
        if scenario not in scenarios:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown scenario. Available: {list(scenarios.keys())}",
            )
        shocks = scenarios[scenario]
    else:
        # Validate shocks are floats
        shocks = {k: float(v) for k, v in shocks.items()}

    sim = ChainlinkImpactSimulator(pg_pool, redis_client)
    await sim.load_state()

    with simulation_duration.time():
        result = await sim.simulate(
            shocks, scenario_name=scenario,
            min_profit_usd=Decimal(str(min_profit)),
        )

    await sim.store_results(result)
    await sim.emit_signals(result, velocity_tracker=velocity_tracker, precompute=precompute)

    simulations_total.labels(run_type=result.run_type).inc()

    return {
        "run_id": str(result.run_id),
        "run_type": result.run_type,
        "scenario": result.scenario_name,
        "feeds": result.feed_symbols,
        "shocks": result.price_shocks,
        "total_borrowers": result.total_borrowers,
        "newly_liquidatable": result.newly_liquidatable,
        "total_opportunities": result.total_opportunities,
        "estimated_profit": float(result.estimated_profit),
        "top_opportunity": float(result.top_opportunity_usd),
        "elapsed_ms": int(result.elapsed_ms),
    }
