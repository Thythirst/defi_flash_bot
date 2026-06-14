"""
Chainlink Impact Simulator — Core Simulation Engine.

Simulates the effect of Chainlink oracle price updates on Aave V3
borrower health factors. Identifies positions that become liquidatable
and estimates liquidation profitability.

Key workflow:
  1. Load borrower state from PostgreSQL
  2. Detect deviations between Chainlink and market prices
  3. Simulate Chainlink price convergence to market
  4. Recalculate health factors for all affected borrowers
  5. Rank newly-liquidatable positions by estimated profit
  6. Store results in PostgreSQL, emit signals to Redis
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Dict, List, Optional, Tuple

import asyncpg
import redis.asyncio as aioredis
from .precompute import AAVE_V3_POOL, FLASH_EXECUTOR_V3

logger = logging.getLogger(__name__)

# ── Column-specific PG NUMERIC clamps (matched to actual schema) ──────────
# simulation_results column limits verified against information_schema.columns
_HF_MAX      = Decimal("999999.999999")           # NUMERIC(12,6) — hf_before, hf_after
_USD_MAX     = Decimal("9999999999999999.99999999")  # NUMERIC(24,8) — total_debt_usd, total_coll_usd, debt/coll_asset_usd, gross/net_profit_usd, gas_estimate_gwei
_RATIO_MAX   = Decimal("999.9999")                # NUMERIC(7,4)  — close_factor, liq_bonus_pct
_DELTA_MAX   = Decimal("999999.9999")             # NUMERIC(10,4) — hf_delta_pct

def _clamp_hf(val: Decimal) -> Decimal:
    """Clamp health factor to NUMERIC(12,6) — max ±999,999.999999."""
    if val is None:
        return Decimal("0")
    return max(Decimal("0"), min(_HF_MAX, val))

def _clamp_usd(val: Decimal) -> Decimal:
    """Clamp USD/gwei values to NUMERIC(24,8)."""
    if val is None:
        return Decimal("0")
    return max(-_USD_MAX, min(_USD_MAX, val))

def _clamp_ratio(val: Decimal) -> Decimal:
    """Clamp ratio/percentage to NUMERIC(7,4) — max ±999.9999."""
    if val is None:
        return Decimal("0")
    return max(Decimal("0"), min(_RATIO_MAX, val))

# Backward-compatible alias for callers that haven't been migrated
_clamp_numeric = _clamp_usd


# ── Data classes ──────────────────────────────────────────────────────────

@dataclass
class PositionSnapshot:
    """A borrower's position in a single reserve."""
    user_addr: str
    reserve_addr: str
    symbol: str
    collateral_native: Decimal
    debt_native: Decimal
    collateral_usd: Decimal
    debt_usd: Decimal
    is_collateral: bool
    e_mode: int

    # Reserve config
    ltv_bps: int
    liq_threshold_bps: int
    liq_bonus_bps: int
    decimals: int
    price_usd: Decimal


@dataclass
class BorrowerAggregate:
    """Aggregated view of a borrower across all reserves."""
    user_addr: str
    positions: List[PositionSnapshot] = field(default_factory=list)
    total_debt_usd: Decimal = Decimal("0")
    total_coll_usd: Decimal = Decimal("0")       # raw (unadjusted)
    total_coll_adj_usd: Decimal = Decimal("0")   # risk-adjusted (× liq_threshold)
    health_factor: Decimal = Decimal("Infinity")


@dataclass
class SimulatedUser:
    """Result of simulating a price shock on one borrower."""
    user_addr: str
    hf_before: Decimal
    hf_after: Decimal
    is_liquidatable: bool
    total_debt_usd: Decimal
    total_coll_usd: Decimal

    # Best liquidation target (if liquidatable)
    debt_asset: Optional[str] = None
    debt_asset_usd: Optional[Decimal] = None
    coll_asset: Optional[str] = None
    coll_asset_usd: Optional[Decimal] = None

    # Profit
    close_factor: Decimal = Decimal("0.5")
    liq_bonus_pct: Decimal = Decimal("0")
    gross_profit_usd: Decimal = Decimal("0")
    gas_estimate_gwei: Decimal = Decimal("500000")
    net_profit_usd: Decimal = Decimal("0")
    profit_rank: int = 0


@dataclass
class SimulationResult:
    """Complete result of a simulation run."""
    run_id: uuid.UUID
    run_type: str
    scenario_name: Optional[str]
    feed_symbols: List[str]
    price_shocks: Dict[str, float]
    total_borrowers: int
    newly_liquidatable: int
    total_opportunities: int
    estimated_profit: Decimal
    top_opportunity_usd: Decimal
    users: List[SimulatedUser] = field(default_factory=list)
    elapsed_ms: float = 0.0


# ── Simulator ─────────────────────────────────────────────────────────────

class ChainlinkImpactSimulator:
    """
    Core simulation engine.

    Loads borrower state from PostgreSQL, applies price shocks to
    Chainlink feeds, recalculates health factors, and identifies
    profitable liquidation opportunities.
    """

    # Known scenario presets
    SCENARIOS: Dict[str, Dict[str, float]] = {
        "crash": {
            "ETH": -8.0, "WBTC": -6.0, "ARB": -15.0, "LINK": -12.0,
        },
        "pump": {
            "ETH": +5.0, "WBTC": +4.0, "ARB": +8.0, "LINK": +6.0,
        },
        "alt_crash": {
            "ARB": -20.0, "LINK": -18.0,
        },
        "btc_dump": {
            "WBTC": -10.0,
        },
        "correlation": {
            "ETH": -5.0, "LINK": -10.0,
        },
        "liquidation_cascade": {
            "ETH": -12.0, "WBTC": -10.0, "ARB": -25.0, "LINK": -20.0,
        },
    }

    GAS_LIMIT_LIQUIDATION = 500_000
    GAS_PRICE_GWEI = Decimal("0.1")  # Arbitrum L2

    def __init__(self, pg_pool, redis_client=None):
        self.pg = pg_pool
        self.redis = redis_client

        # In-memory state loaded from PG
        self.reserves: Dict[str, dict] = {}         # reserve_addr → config
        self.borrowers: Dict[str, BorrowerAggregate] = {}
        self.cl_feeds: Dict[str, dict] = {}          # symbol → feed state
        self.market_prices: Dict[str, dict] = {}      # symbol → market price

    # ── State Loading ───────────────────────────────────────────────────

    async def load_state(self) -> int:
        """Load full state from PostgreSQL. Returns borrower count."""
        # Load into locals first — atomically assign on full success
        reserves, cl_feeds, market_prices = await asyncio.gather(
            self._fetch_reserves(),
            self._fetch_feeds(),
            self._fetch_market_prices(),
        )
        borrowers, count = await self._fetch_borrowers(reserves)

        self.reserves = reserves
        self.cl_feeds = cl_feeds
        self.market_prices = market_prices
        self.borrowers = borrowers
        return count

    async def _fetch_reserves(self) -> dict:
        rows = await self.pg.fetch(
            "SELECT reserve_addr, symbol, decimals, ltv_bps, liq_threshold_bps, "
            "liq_bonus_bps, price_usd FROM reserve_configs WHERE is_active = true"
        )
        reserves = {}
        for r in rows:
            reserves[r["reserve_addr"]] = {
                "symbol": r["symbol"],
                "decimals": r["decimals"],
                "ltv_bps": r["ltv_bps"],
                "liq_threshold_bps": r["liq_threshold_bps"],
                "liq_bonus_bps": r["liq_bonus_bps"],
                "price_usd": Decimal(str(r["price_usd"])) if r["price_usd"] else Decimal("0"),
            }
        return reserves

    async def _fetch_feeds(self) -> dict:
        rows = await self.pg.fetch(
            "SELECT symbol, price_usd, heartbeat_sec, age_seconds, deviation_ppb "
            "FROM chainlink_feeds"
        )
        cl_feeds = {}
        for r in rows:
            cl_feeds[r["symbol"]] = {
                "price_usd": Decimal(str(r["price_usd"])) if r["price_usd"] else Decimal("0"),
                "heartbeat_sec": r["heartbeat_sec"],
                "age_seconds": r["age_seconds"] or 0,
                "deviation_ppb": r["deviation_ppb"],
            }
        return cl_feeds

    async def _fetch_market_prices(self) -> dict:
        rows = await self.pg.fetch(
            "SELECT symbol, mid_price, cl_deviation_pct FROM market_prices"
        )
        market_prices = {}
        for r in rows:
            market_prices[r["symbol"]] = {
                "mid_price": Decimal(str(r["mid_price"])) if r["mid_price"] else Decimal("0"),
                "cl_deviation_pct": (
                    Decimal(str(r["cl_deviation_pct"])) if r["cl_deviation_pct"]
                    else Decimal("0")
                ),
            }
        return market_prices

    async def _fetch_borrowers(self, reserves: dict) -> tuple:
        rows = await self.pg.fetch(
            "SELECT user_addr, reserve_addr, symbol, collateral, debt, "
            "collateral_usd, debt_usd, is_collateral, e_mode_category, health_factor "
            "FROM borrow_positions ORDER BY user_addr"
        )
        borrowers = {}
        for r in rows:
            ua = r["user_addr"]
            if ua not in borrowers:
                borrowers[ua] = BorrowerAggregate(user_addr=ua)
                if r["health_factor"] is not None:
                    borrowers[ua].health_factor = Decimal(str(r["health_factor"]))

            reserve_cfg = reserves.get(r["reserve_addr"])
            if not reserve_cfg:
                continue

            pos = PositionSnapshot(
                user_addr=ua,
                reserve_addr=r["reserve_addr"],
                symbol=r["symbol"],
                collateral_native=Decimal(str(r["collateral"])),
                debt_native=Decimal(str(r["debt"])),
                collateral_usd=Decimal(str(r["collateral_usd"])),
                debt_usd=Decimal(str(r["debt_usd"])),
                is_collateral=r["is_collateral"],
                e_mode=r["e_mode_category"],
                ltv_bps=reserve_cfg["ltv_bps"],
                liq_threshold_bps=reserve_cfg["liq_threshold_bps"],
                liq_bonus_bps=reserve_cfg["liq_bonus_bps"],
                decimals=reserve_cfg["decimals"],
                price_usd=reserve_cfg["price_usd"],
            )
            borrowers[ua].positions.append(pos)

        # Compute aggregate values
        for ua, agg in borrowers.items():
            total_debt = Decimal("0")
            total_coll = Decimal("0")
            total_coll_adj = Decimal("0")
            for pos in agg.positions:
                total_debt += pos.debt_usd
                total_coll += pos.collateral_usd
                if pos.is_collateral and pos.liq_threshold_bps > 0:
                    total_coll_adj += pos.collateral_usd * Decimal(pos.liq_threshold_bps) / 10000
            agg.total_debt_usd = total_debt
            agg.total_coll_usd = total_coll
            agg.total_coll_adj_usd = total_coll_adj

        return borrowers, len(borrowers)

    # ── Simulation ──────────────────────────────────────────────────────

    async def simulate(
        self,
        price_shocks: Dict[str, float],
        scenario_name: Optional[str] = None,
        min_profit_usd: Decimal = Decimal("25"),
        run_id: Optional[uuid.UUID] = None,
    ) -> SimulationResult:
        """
        Apply price shocks to Chainlink feeds and recalculate.
        """
        t0 = time.monotonic()
        if run_id is None:
            run_id = uuid.uuid4()

        # 1. Apply shocks → compute new prices for affected reserves
        shocked_prices: Dict[str, Decimal] = {}
        for symbol, pct in price_shocks.items():
            cl_feed = self.cl_feeds.get(symbol, {})
            base_price = cl_feed.get("price_usd", Decimal("0"))
            if base_price > 0:
                shocked_prices[symbol] = base_price * (Decimal("1") + Decimal(str(pct)) / 100)

        # Map symbols to reserve addresses
        symbol_to_addr = {}
        for addr, cfg in self.reserves.items():
            symbol_to_addr[cfg["symbol"]] = addr

        # 2. Simulate each borrower
        sim_users: List[SimulatedUser] = []
        for ua, agg in self.borrowers.items():
            sim = self._simulate_borrower(agg, shocked_prices, symbol_to_addr, min_profit_usd)
            if sim is not None:
                sim_users.append(sim)

        # 3. Rank by net profit
        sim_users.sort(key=lambda s: float(s.net_profit_usd), reverse=True)
        for i, s in enumerate(sim_users):
            s.profit_rank = i + 1

        newly_liq = sum(1 for s in sim_users if s.is_liquidatable)
        total_profit = sum(s.net_profit_usd for s in sim_users if s.is_liquidatable)
        top_profit = sim_users[0].net_profit_usd if sim_users else Decimal("0")

        elapsed = (time.monotonic() - t0) * 1000

        return SimulationResult(
            run_id=run_id,
            run_type="scenario" if scenario_name else "manual",
            scenario_name=scenario_name,
            feed_symbols=list(price_shocks.keys()),
            price_shocks=price_shocks,
            total_borrowers=len(self.borrowers),
            newly_liquidatable=newly_liq,
            total_opportunities=len([s for s in sim_users if s.is_liquidatable]),
            estimated_profit=total_profit,
            top_opportunity_usd=top_profit,
            users=sim_users,
            elapsed_ms=elapsed,
        )

    def _simulate_borrower(
        self,
        agg: BorrowerAggregate,
        shocked_prices: Dict[str, Decimal],
        symbol_to_addr: Dict[str, str],
        min_profit_usd: Decimal,
    ) -> Optional[SimulatedUser]:
        """
        Apply price shocks to one borrower and recalculate health factor.
        """
        total_debt = Decimal("0")
        total_coll_adj = Decimal("0")
        position_details = []  # (is_coll, symbol, coll_usd_new, liq_thresh, liq_bonus, debt_usd, reserve_addr)

        for pos in agg.positions:
            new_price = shocked_prices.get(pos.symbol, pos.price_usd)
            debt_usd = pos.debt_native * new_price / (10 ** pos.decimals) if pos.debt_native > 0 else Decimal("0")
            coll_usd = pos.collateral_native * new_price / (10 ** pos.decimals) if pos.collateral_native > 0 else Decimal("0")

            total_debt += debt_usd
            if pos.is_collateral and pos.liq_threshold_bps > 0:
                adjusted = coll_usd * Decimal(pos.liq_threshold_bps) / 10000
                total_coll_adj += adjusted

            position_details.append({
                "is_coll": pos.is_collateral,
                "symbol": pos.symbol,
                "coll_usd": coll_usd,
                "liq_threshold_bps": pos.liq_threshold_bps,
                "liq_bonus_bps": pos.liq_bonus_bps,
                "debt_usd": debt_usd,
                "reserve_addr": pos.reserve_addr,
            })

        # Health factor
        if total_debt == 0:
            return None

        hf_before = agg.health_factor if agg.health_factor != Decimal("Infinity") else Decimal("999")
        if total_coll_adj == 0:
            hf_after = Decimal("0")
        else:
            hf_after = (total_coll_adj / total_debt).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)

        is_liquidatable = hf_after < Decimal("1.0")

        sim = SimulatedUser(
            user_addr=agg.user_addr,
            hf_before=hf_before,
            hf_after=hf_after,
            is_liquidatable=is_liquidatable,
            total_debt_usd=total_debt,
            total_coll_usd=agg.total_coll_usd,  # raw unadjusted
        )

        if not is_liquidatable:
            return sim

        # Find best liquidation pair (max bonus collateral vs highest debt)
        best = self._find_best_liquidation(position_details, total_debt, min_profit_usd)
        if best:
            sim.debt_asset = best["debt_asset"]
            sim.debt_asset_usd = best["debt_usd"]
            sim.coll_asset = best["coll_asset"]
            sim.coll_asset_usd = best["coll_seized_usd"]
            sim.liq_bonus_pct = Decimal(str(best["liq_bonus_pct"]))
            sim.gross_profit_usd = best["gross_profit"]
            sim.net_profit_usd = best["net_profit"]

        return sim

    def _find_best_liquidation(
        self,
        positions: List[dict],
        total_debt: Decimal,
        min_profit_usd: Decimal,
    ) -> Optional[dict]:
        """Find highest-profit (debt_asset, coll_asset) pair for liquidation."""
        best = None
        best_net = Decimal("-Infinity")
        close_factor = Decimal("0.5")

        collaterals = [p for p in positions if p["is_coll"] and p["coll_usd"] > 0]
        debts = [p for p in positions if p["debt_usd"] > 0]

        for debt_pos in debts:
            debt_to_cover = debt_pos["debt_usd"] * close_factor
            for coll_pos in collaterals:
                if coll_pos["reserve_addr"] == debt_pos.get("reserve_addr"):
                    continue  # skip same-asset pair

                bonus = Decimal(coll_pos["liq_bonus_bps"]) / 10000
                coll_seized = debt_to_cover * (Decimal("1") + bonus)
                gross = coll_seized - debt_to_cover
                gas_cost = self._gas_cost_usd()
                net = gross - gas_cost

                if net > best_net and net >= min_profit_usd:
                    best_net = net
                    # liq_bonus_pct stores the bonus PERCENTAGE (e.g. 5.0 = 5% bonus).
                    # Aave liquidationBonus = 10000 + bonus_bps, so bonus = bonus_bps / 10000.
                    # (bonus - 1) * 100 converts the multiplier (1.05) to percentage (5.0).
                    bonus_pct = max(Decimal("0"), min(Decimal("999.9999"),
                                      (bonus - Decimal("1")) * 100))
                    best = {
                        "debt_asset": debt_pos["symbol"],
                        "debt_usd": debt_to_cover,
                        "coll_asset": coll_pos["symbol"],
                        "coll_seized_usd": coll_seized,
                        "liq_bonus_pct": float(bonus_pct),
                        "gross_profit": gross,
                        "net_profit": net,
                    }

        return best

    def _gas_cost_usd(self) -> Decimal:
        """Estimate Arbitrum L2 gas cost in USD using market ETH price."""
        gas_eth = Decimal(str(self.GAS_LIMIT_LIQUIDATION)) * self.GAS_PRICE_GWEI / Decimal("1e9")
        # ETH price from reserves, fallback to market price, fallback to 3000
        eth_price = Decimal("0")
        for cfg in self.reserves.values():
            if cfg["symbol"] == "ETH" and cfg.get("price_usd", Decimal("0")) > 0:
                eth_price = cfg["price_usd"]
                break
        if eth_price <= 0:
            mkt = self.market_prices.get("ETH", {})
            eth_price = mkt.get("mid_price", Decimal("0"))
        if eth_price <= 0:
            eth_price = Decimal("3000")  # absolute fallback
        return (gas_eth * eth_price).quantize(Decimal("0.01"))

    # ── Persistence ─────────────────────────────────────────────────────

    async def store_results(self, result: SimulationResult):
        """Write simulation results to PostgreSQL."""
        async with self.pg.acquire() as conn:
            async with conn.transaction():
                # Insert run
                await conn.execute(
                    """
                    INSERT INTO simulation_runs (
                        run_id, run_type, scenario_name, feed_symbols, price_shocks,
                        min_profit_usd, total_borrowers, newly_liquidatable,
                        total_opportunities, estimated_profit, top_opportunity_usd,
                        status, elapsed_ms, completed_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'completed',$12,NOW())
                    """,
                    result.run_id,
                    result.run_type,
                    result.scenario_name,
                    result.feed_symbols,
                    json.dumps(result.price_shocks),
                    Decimal("25"),
                    result.total_borrowers,
                    result.newly_liquidatable,
                    result.total_opportunities,
                    result.estimated_profit,
                    result.top_opportunity_usd,
                    int(result.elapsed_ms),
                )

                # Insert results (bulk)
                if result.users:
                    await conn.copy_records_to_table(
                        "simulation_results",
                        columns=[
                            "run_id", "user_addr", "hf_before", "total_debt_usd",
                            "total_coll_usd", "hf_after", "hf_delta_pct", "is_liquidatable",
                            "debt_asset", "debt_asset_usd", "coll_asset", "coll_asset_usd",
                            "close_factor", "liq_bonus_pct", "gross_profit_usd",
                            "gas_estimate_gwei", "net_profit_usd", "profit_rank",
                        ],
                        records=[
                            (
                                result.run_id,
                                s.user_addr,
                                _clamp_hf(s.hf_before),
                                _clamp_numeric(s.total_debt_usd),
                                _clamp_numeric(s.total_coll_usd),
                                _clamp_hf(s.hf_after),
                                # Clamp hf_delta_pct to column capacity (NUMERIC(10,4) → max 999,999.9999)
                                max(
                                    Decimal("-" + str(_DELTA_MAX)),
                                    min(
                                        _DELTA_MAX,
                                        ((s.hf_after - s.hf_before) / s.hf_before * 100).quantize(Decimal("0.01"))
                                    )
                                )
                                if s.hf_before > 0 and s.hf_before != Decimal("999") else None,
                                s.is_liquidatable,
                                s.debt_asset,
                                _clamp_numeric(s.debt_asset_usd or Decimal("0")),
                                s.coll_asset,
                                _clamp_numeric(s.coll_asset_usd or Decimal("0")),
                                _clamp_ratio(s.close_factor),
                                # Clamp liq_bonus_pct to column capacity (NUMERIC(7,4) → max 999.9999)
                                max(Decimal("0"), min(_RATIO_MAX, s.liq_bonus_pct or Decimal("0"))),
                                _clamp_numeric(s.gross_profit_usd),
                                _clamp_numeric(s.gas_estimate_gwei),
                                _clamp_numeric(s.net_profit_usd),
                                s.profit_rank,
                            )
                            for s in result.users
                        ],
                    )

    async def emit_signals(self, result: SimulationResult, velocity_tracker=None, precompute=None):
        """Write liquidation signals to PG, build pre-computed bundles, push to engine queue."""
        signals = []
        for s in result.users:
            if not s.is_liquidatable or s.net_profit_usd <= 0:
                continue

            trigger_feed = result.feed_symbols[0] if result.feed_symbols else "unknown"
            shock_pct = result.price_shocks.get(trigger_feed, 0.0)

            # Use velocity-based confidence if available, else fallback
            if velocity_tracker:
                velocity = velocity_tracker._compute_velocity(trigger_feed) if trigger_feed in velocity_tracker._history else None
                confidence = velocity.confidence if velocity else self._compute_confidence_legacy(trigger_feed, shock_pct)
            else:
                confidence = self._compute_confidence_legacy(trigger_feed, shock_pct)

            signal = {
                "user_addr": s.user_addr,
                "run_id": result.run_id,
                "trigger_feed": trigger_feed,
                "trigger_shock_pct": Decimal(str(shock_pct)),
                "debt_asset": s.debt_asset or "unknown",
                "debt_usd": s.debt_asset_usd or Decimal("0"),
                "coll_asset": s.coll_asset or "unknown",
                "coll_usd": s.coll_asset_usd or Decimal("0"),
                "hf_before": s.hf_before,
                "hf_after": s.hf_after,
                "net_profit_usd": s.net_profit_usd,
                "priority": s.profit_rank,
                "confidence": confidence,
            }
            signals.append(signal)

        if not signals:
            return

        # Bulk insert to PG
        async with self.pg.acquire() as conn:
            await conn.copy_records_to_table(
                "liquidation_signals",
                columns=[
                    "user_addr", "run_id", "trigger_feed", "trigger_shock_pct",
                    "debt_asset", "debt_usd", "coll_asset", "coll_usd",
                    "hf_before", "hf_after", "net_profit_usd", "priority", "confidence",
                ],
                records=[
                    (
                        sig["user_addr"], sig["run_id"], sig["trigger_feed"],
                        sig["trigger_shock_pct"], sig["debt_asset"], sig["debt_usd"],
                        sig["coll_asset"], sig["coll_usd"], sig["hf_before"],
                        sig["hf_after"], sig["net_profit_usd"],
                        sig["priority"], sig["confidence"],
                    )
                    for sig in signals
                ],
            )

        # Publish top signals to Redis AND push to execution engine queue
        if self.redis:
            scenario = result.scenario_name or "unknown"
            for sig in signals[:10]:  # top 10
                signal_id = str(uuid.uuid4())
                payload = json.dumps({
                    "signal_id": signal_id,
                    "user": sig["user_addr"],
                    "debt_asset": sig["debt_asset"],
                    "coll_asset": sig["coll_asset"],
                    "net_profit_usd": float(sig["net_profit_usd"]),
                    "hf_before": float(sig["hf_before"]),
                    "hf_after": float(sig["hf_after"]),
                    "trigger": sig["trigger_feed"],
                    "scenario": scenario,
                    "priority": sig["priority"],
                    "confidence": float(sig["confidence"]),
                })
                await self.redis.publish("arb:signals:liquidation", payload)

                # Push to execution engine queue for immediate execution
                request_id = str(uuid.uuid4())
                priority_score = float(sig["net_profit_usd"]) * 100.0
                engine_request = json.dumps({
                    "request_id": request_id,
                    "exec_type": "liquidation",
                    "contract_address": FLASH_EXECUTOR_V3,
                    "calldata": "",  # filled by precompute engine
                    "value_wei": "0",
                    "expected_profit_usd": float(sig["net_profit_usd"]),
                    "priority": priority_score,
                    "metadata": {
                        "user": sig["user_addr"],
                        "debt_asset": sig["debt_asset"],
                        "coll_asset": sig["coll_asset"],
                        "trigger_feed": sig["trigger_feed"],
                        "signal_id": signal_id,
                        "confidence": float(sig["confidence"]),
                    },
                })
                await self.redis.zadd("engine:queue", {request_id: priority_score})
                await self.redis.hset(f"engine:pending:{request_id}", mapping={
                    "type": "liquidation",
                    "contract": FLASH_EXECUTOR_V3,
                    "calldata": "",  # REQUIRED by execution engine — filled by precompute
                    "expected_profit_usd": str(sig["net_profit_usd"]),
                    "priority": str(priority_score),
                    "status": "queued",
                    "created_at": str(time.time()),
                    "metadata": json.dumps({"user": sig["user_addr"], "trigger": sig["trigger_feed"]}),
                })

        # Build pre-computed bundles if precompute engine available
        if precompute:
            for sig in signals[:10]:
                if float(sig["confidence"]) < 0.3:
                    continue
                await precompute.build_bundle(
                    user_addr=sig["user_addr"],
                    debt_asset_addr="",  # resolved by precompute from reserves
                    debt_asset_symbol=sig["debt_asset"],
                    coll_asset_addr="",
                    coll_asset_symbol=sig["coll_asset"],
                    debt_to_cover_wei=0,  # computed by precompute
                    trigger_feed=sig["trigger_feed"],
                    expected_profit_usd=sig["net_profit_usd"],
                    priority=sig["priority"],
                    use_flash_loan=True,
                )

    def _compute_confidence_legacy(self, trigger_feed: str, shock_pct: float) -> Decimal:
        """
        Estimate how likely this signal triggers soon (0-1).
        Based on: how close the Chainlink feed is to a heartbeat update,
        and how large the deviation from market price is.
        """
        score = Decimal("0.5")  # baseline

        feed = self.cl_feeds.get(trigger_feed, {})
        if feed:
            heartbeat = feed.get("heartbeat_sec", 3600)
            age = feed.get("age_seconds", 0)
            if heartbeat > 0:
                pct_elapsed = min(Decimal(str(age)) / Decimal(str(heartbeat)), Decimal("1"))
                score += pct_elapsed * Decimal("0.25")  # up to 0.25 bonus

        # Closer to threshold = more likely
        abs_shock = abs(Decimal(str(shock_pct)))
        if abs_shock < 3:
            score += Decimal("0.15")
        elif abs_shock < 5:
            score += Decimal("0.10")

        market = self.market_prices.get(trigger_feed, {})
        dev = abs(market.get("cl_deviation_pct", Decimal("0")))
        if dev > Decimal("1"):
            score += Decimal("0.10")

        return min(score, Decimal("1.0")).quantize(Decimal("0.01"))


# ── Batch Runner ─────────────────────────────────────────────────────────

class BatchSimulator:
    """
    Runs multiple simulation scenarios in batch.
    Used for scheduled sweep of all Chainlink feed combinations.
    """

    def __init__(self, simulator: ChainlinkImpactSimulator):
        self.sim = simulator

    async def run_full_batch(
        self,
        min_profit_usd: Decimal = Decimal("25"),
    ) -> List[SimulationResult]:
        """
        Run all standard scenarios + individual feed shocks.
        """
        results = []

        # 1. Named scenarios
        for name, shocks in ChainlinkImpactSimulator.SCENARIOS.items():
            sim = ChainlinkImpactSimulator(self.sim.pg, self.sim.redis)
            await sim.load_state()
            result = await sim.simulate(shocks, scenario_name=name, min_profit_usd=min_profit_usd)
            await sim.store_results(result)
            await sim.emit_signals(result)
            results.append(result)
            logger.info(
                "scenario=%s liquidatable=%d profit=$%.0f",
                name, result.newly_liquidatable, float(result.estimated_profit),
            )

        return results

    async def run_deviation_based(self, min_profit_usd: Decimal = Decimal("25")):
        """
        For each Chainlink feed, simulate convergence to market price.
        Only run if deviation is meaningful (>0.5%).
        """
        results = []
        sim = ChainlinkImpactSimulator(self.sim.pg, self.sim.redis)
        await sim.load_state()

        for symbol, market in sim.market_prices.items():
            cl_feed = sim.cl_feeds.get(symbol, {})
            cl_price = cl_feed.get("price_usd", Decimal("0"))
            market_price = market.get("mid_price", Decimal("0"))

            if cl_price <= 0 or market_price <= 0:
                continue

            deviation_pct = float(
                (market_price - cl_price) / cl_price * 100
            )
            if abs(deviation_pct) < 0.5:
                continue  # skip negligible deviations

            # Re-load state fresh per shock
            sim2 = ChainlinkImpactSimulator(self.sim.pg, self.sim.redis)
            await sim2.load_state()

            shocks = {symbol: deviation_pct}
            result = await sim2.simulate(
                shocks,
                scenario_name=None,
                min_profit_usd=min_profit_usd,
            )
            await sim2.store_results(result)
            await sim2.emit_signals(result)
            results.append(result)
            logger.info(
                "deviation %s=%+.1f%% liquidatable=%d profit=$%.0f",
                symbol, deviation_pct, result.newly_liquidatable,
                float(result.estimated_profit),
            )

        return results


# ── Startup schema validation ──────────────────────────────────────────

_COLUMN_CLAMP_MAP = {
    ("hf_before", "hf_after"):      ("NUMERIC(12,6)", _clamp_hf,       _HF_MAX),
    ("total_debt_usd", "total_coll_usd", "debt_asset_usd", "coll_asset_usd",
     "gross_profit_usd", "net_profit_usd", "gas_estimate_gwei"):
                                      ("NUMERIC(24,8)", _clamp_usd,     _USD_MAX),
    ("close_factor", "liq_bonus_pct"): ("NUMERIC(7,4)",  _clamp_ratio,  _RATIO_MAX),
    ("hf_delta_pct",):                ("NUMERIC(10,4)",  None,          _DELTA_MAX),
}

async def validate_column_clamps(pg_pool) -> bool:
    """Compare application clamps against actual PG schema. CRITICAL on mismatch."""
    async with pg_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT column_name, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_name = 'simulation_results' AND data_type = 'numeric'
            ORDER BY ordinal_position
        """)
        schema = {}
        for r in rows:
            prec, scale = r["numeric_precision"], r["numeric_scale"]
            max_val = Decimal("9" * (prec - scale) + "." + "9" * scale) if prec and scale else None
            schema[r["column_name"]] = (prec, scale, max_val)

    all_ok = True
    for col_names, (expected_type, clamp_fn, clamp_max) in _COLUMN_CLAMP_MAP.items():
        for col in col_names:
            if col not in schema:
                logger.error("VALIDATE: column %s missing from simulation_results schema", col)
                all_ok = False
                continue
            prec, scale, pg_max = schema[col]
            actual_type = f"NUMERIC({prec},{scale})"
            if clamp_max is not None and pg_max is not None and clamp_max > pg_max:
                logger.critical(
                    "VALIDATE: %s clamp (%.0e) EXCEEDS column %s (%.0e) — WILL CAUSE OVERFLOW",
                    col, float(clamp_max), actual_type, float(pg_max),
                )
                all_ok = False
            else:
                logger.info(
                    "VALIDATE: %s %s clamp=%.0e pg_max=%.0e OK",
                    col, actual_type, float(clamp_max) if clamp_max else 0, float(pg_max) if pg_max else 0,
                )

    if not all_ok:
        logger.critical("VALIDATE: CLAMP MISMATCH DETECTED — refusing to start")
        return False
    logger.info("VALIDATE: all %d numeric columns within schema limits", len(schema))
    return True
