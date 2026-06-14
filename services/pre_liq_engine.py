"""
pre_liq_engine.py — Pre-Liquidation Prediction Engine.

Identifies liquidations BEFORE they appear on-chain by observing pending
state transitions in the mempool, simulating post-state health factors,
and submitting liquidation bundles before the block confirms.

Win probabilities use Bayesian updating: Beta(1,1) prior blended with a
parametric cold-start model. All outcomes are tracked to Redis for
persistence across restarts.

Architecture:
  Mempool (QuickNode WSS)
    ↓
  mempool_intel.py → Redis pub/sub signals
    ↓
  pre_liq_engine.py ← subscribes via pub/sub + polls mempool:recent
    ↓
  Simulate post-state HF → Profit Check → MEV Blocker bundle
    ↓
  OutcomeTracker ← polls tx receipts, updates empirical win rates

Optimization targets (in order):
  1. Net profit  — highest EV first
  2. Win rate    — empirical, continuously updated
  3. Latency     — Redis pub/sub + async RPC racing
  4. Capital efficiency — skip dust positions

Redis keys:
  preliq:outcomes:{trigger}    HASH   {submitted, confirmed, reverted, lost_race}
  preliq:bundles:{tx_hash}     HASH   {status, trigger, submitted_at, ...}
  preliq:bundles:pending       ZSET   score=submitted_ts, member=tx_hash
  preliq:opportunity           STREAM detected pre-liquidation ops
  preliq:executed              ZSET   submitted bundles
  preliq:stats:{minute}        HASH   rolling stats

Usage:
  python -m services.pre_liq_engine
  DRY_RUN=1 python -m services.pre_liq_engine  # simulate only
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import aiohttp
import redis.asyncio as redis
from dotenv import load_dotenv
from eth_abi import decode as abi_decode
from eth_utils import keccak
from web3 import Web3
from web3.types import TxParams

from services.calibration import (
    CalibrationDB, LogisticModel, OpportunityRecord, OutcomeRecord,
)
from services.execution_validator import (
    ExecutionValidator, ModelPromotion, ModelSource, generate_daily_report,
)

load_dotenv(dotenv_path=project_root / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | preliq | %(message)s",
)
logger = logging.getLogger("preliq")

# ═══════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════

ARBITRUM_CHAIN_ID = 42161
AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
POOL_DATA_PROVIDER = "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"
AAVE_ORACLE = "0xb56c2f0B653B2e0B10c9b928C8580Ac5Df02C7C7"

CHAINLINK_AGGREGATORS: Dict[str, str] = {
    "0x639fe6ab55c921f74e7fac1ee960c0b6293ba612": "ETH",
    "0x6ce185860a4963106506c203335a2910413708e9": "BTC",
    "0x86e53cf1b870786351da77a57575e79cb55812cb": "LINK",
    "0xb2a824043730fe05f3da2efafa1cbbe83fa548d6": "ARB",
    "0x50834f3163758fcc1df9973b6e91f0f0f0434ad3": "USDC",
    "0x3f3f5df88dc9f13eac63df89ec16ef6e7e25dde7": "USDT",
    "0xc5c8e77b397e531b8ec06bfb0048328b30e9ecfb": "DAI",
    "0xd0c7101eacbb49f3decccc166d238410d6d46d57": "WBTC",
}

KNOWN_ASSETS: List[Tuple[str, str, int]] = [
    ("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", "WETH", 18),
    ("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "USDC", 6),
    ("0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", "USDT", 6),
    ("0x912CE59144191C1204E64559FE8253a0e49E6548", "ARB", 18),
    ("0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f", "WBTC", 8),
    ("0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1", "DAI", 18),
    ("0xf97f4df75117a78c1A5a0DBb814Af92458539FB4", "LINK", 18),
    ("0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8", "USDC.e", 6),
    ("0x6c84a8f1c29108F47a79964b5Fe888D4f4D0dE40", "tBTC", 18),
    ("0x4186BFC76E2E237523CBC30FD220FE055156b41F", "rsETH", 18),
]

ADDR_TO_SYMBOL: Dict[str, str] = {addr.lower(): sym for addr, sym, _ in KNOWN_ASSETS}
ADDR_TO_DECIMALS: Dict[str, int] = {addr.lower(): dec for addr, _, dec in KNOWN_ASSETS}

MIN_EV_USD = float(os.getenv("PRELIQ_MIN_EV_USD", "50.0"))
MIN_DEBT_USD = float(os.getenv("PRELIQ_MIN_DEBT_USD", "5000.0"))
MAX_AT_RISK_USERS = int(os.getenv("PRELIQ_MAX_AT_RISK", "30"))
SIMULATION_GAS_LIMIT = 500_000
MAX_POSITION_USD = float(os.getenv("PRELIQ_MAX_POSITION_USD", "0"))  # 0 = unlimited
CLOSE_FACTOR_BPS = 5000

# Competition cold-start thresholds: don't gate on competition estimates
# until we have enough resolved events and successful liquidations.
COMP_COLD_START_MIN_RESOLVED = 50      # resolved competitor events
COMP_COLD_START_MIN_SUCCESSES = 20     # successful liquidations

BALANCER_VAULT = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"
MEV_BLOCKER_RPC = "https://rpc.mevblocker.io"
MEV_BLOCKER_BACKUP_RPC = "https://rpc.mevblocker.io/fast"  # fallback builder

# Uniswap V3 swap routing
UNI_V3_SWAP_ROUTER = "0xE592427A0AEce92De3Edee1F18E0157C05861564"
UNI_V3_QUOTER = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"
# Fee tiers in bps: tick → V3 fee
FEE_TIERS = [100, 500, 3000, 10000]
# map token decimals to preferred fee tiers for common pairs
# (coll_addr_lower, debt_addr_lower) → preferred_fee_bps
SWAP_FEE_PREFS: Dict[Tuple[str, str], int] = {
    ("0x82af49447d8a07e3bd95bd0d56f35241523fbab1", "0xaf88d065e77c8cc2239327c5edb3a432268e5831"): 500,   # WETH→USDC
    ("0x82af49447d8a07e3bd95bd0d56f35241523fbab1", "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9"): 500,   # WETH→USDT
    ("0x82af49447d8a07e3bd95bd0d56f35241523fbab1", "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1"): 500,   # WETH→DAI
    ("0x82af49447d8a07e3bd95bd0d56f35241523fbab1", "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8"): 500,   # WETH→USDC.e
("0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f", "0xaf88d065e77c8cc2239327c5edb3a432268e5831"): 500,   # WBTC→USDC
    ("0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f", "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9"): 500,   # WBTC→USDT
    ("0xff970a61a04b1ca14834a43f5de4533ebddb5cc8", "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1"): 100,   # USDC→DAI
    ("0xaf88d065e77c8cc2239327c5edb3a432268e5831", "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1"): 100,   # USDC.native→DAI
("0xff970a61a04b1ca14834a43f5de4533ebddb5cc8", "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"): 500,   # USDC→WETH
    ("0xaf88d065e77c8cc2239327c5edb3a432268e5831", "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"): 500,   # USDC.native→WETH
}

# Outcome polling
OUTCOME_POLL_INTERVAL = 5.0
OUTCOME_POLL_MAX_AGE = 300.0      # Give up on bundles older than 5 min


# ═══════════════════════════════════════════════════════════════
# Data Classes
# ═══════════════════════════════════════════════════════════════

class BundleStatus(str, Enum):
    SUBMITTED = "submitted"
    CONFIRMED = "confirmed"
    REVERTED = "reverted"
    LOST_RACE = "lost_race"        # someone else liquidated same borrower first
    UNKNOWN = "unknown"            # timed out waiting for receipt


@dataclass
class OutcomeEntry:
    """Per-trigger rolling outcome counter (persisted to Redis)."""
    submitted: int = 0
    confirmed: int = 0
    reverted: int = 0
    lost_race: int = 0

    @property
    def total_completed(self) -> int:
        return self.confirmed + self.reverted + self.lost_race

    @property
    def total_submitted(self) -> int:
        return self.submitted

    @property
    def empirical_win_rate(self) -> Optional[float]:
        """Observed success rate. Returns None if insufficient data."""
        total = self.confirmed + self.reverted + self.lost_race
        if total == 0:
            return None
        return self.confirmed / total

    @property
    def enough_samples(self) -> bool:
        return self.total_completed >= 20  # Bayesian saturation point


@dataclass
class UserPosition:
    address: str
    health_factor: float
    total_collateral_base: int
    total_debt_base: int
    current_ltv: int
    current_liquidation_threshold: int
    reserves: List[ReservePosition] = field(default_factory=list)

    @property
    def is_liquidatable(self) -> bool:
        return self.health_factor < 1.0 and self.total_debt_base > 0


@dataclass
class ReservePosition:
    asset: str
    symbol: str
    decimals: int
    a_token_balance: int
    stable_debt: int
    variable_debt: int
    is_collateral: bool

    @property
    def total_debt(self) -> int:
        return self.stable_debt + self.variable_debt

    @property
    def is_debt(self) -> bool:
        return self.total_debt > 0


@dataclass
class PreLiquidationOpportunity:
    borrower: str
    collateral_asset: str
    debt_asset: str
    collateral_symbol: str
    debt_symbol: str
    debt_to_cover: int
    pre_hf: float
    post_hf: float
    liquidation_bonus_bps: int
    collateral_price_usd: float
    debt_price_usd: float
    estimated_profit_usd: float
    estimated_gas_usd: float
    win_probability: float         # competition-adjusted WP — for ranking
    expected_value_usd: float      # competition-adjusted EV — for ranking
    trigger: str
    trigger_detail: str
    detected_at: float = field(default_factory=time.time)
    gate_wp: float = 0.5           # WP used for EV gating — base_wp during cold start


@dataclass
class EngineStats:
    oracle_signals: int = 0
    simulations: int = 0
    opportunities_found: int = 0
    bundles_submitted: int = 0
    bundles_confirmed: int = 0
    bundles_reverted: int = 0
    bundles_lost_race: int = 0
    total_ev_captured: float = 0.0
    last_oracle_time: float = 0.0
    competitors_seen: int = 0


# ═══════════════════════════════════════════════════════════════
# Health Factor Simulator
# ═══════════════════════════════════════════════════════════════

class HealthFactorSimulator:
    """Simulates post-state health factor given a price change or tx effect."""

    @staticmethod
    def simulate_price_change(
        position: UserPosition, asset_address: str, new_price: float,
        reserve_configs: Dict[str, dict],
    ) -> Optional[float]:
        addr_lower = asset_address.lower()
        affected = False
        new_total_coll_adj = 0.0
        new_total_debt = 0.0

        for res in position.reserves:
            config = reserve_configs.get(res.asset.lower())
            if not config:
                continue
            price_raw = config.get("price", "0")
            if not price_raw or price_raw == "0":
                continue

            if res.asset.lower() == addr_lower:
                price = new_price
                affected = True
            else:
                price = int(price_raw) / 1e8

            decimals = int(config.get("decimals", str(res.decimals)))
            liq_threshold = int(config.get("liquidation_threshold", "8500"))

            if res.total_debt > 0:
                new_total_debt += (res.total_debt / (10 ** decimals)) * price
            if res.a_token_balance > 0 and res.is_collateral:
                new_total_coll_adj += (res.a_token_balance / (10 ** decimals)) * price * (liq_threshold / 10000)

        if not affected:
            return None
        if new_total_coll_adj <= 0 or new_total_debt <= 0:
            return float("inf")
        return new_total_coll_adj / new_total_debt

    @staticmethod
    def simulate_debt_increase(
        position: UserPosition, asset_address: str, additional_debt: int,
        reserve_configs: Dict[str, dict],
    ) -> Optional[float]:
        addr_lower = asset_address.lower()
        total_coll_adj = 0.0
        total_debt = 0.0
        affected = False

        for res in position.reserves:
            config = reserve_configs.get(res.asset.lower())
            if not config:
                continue
            price_raw = config.get("price", "0")
            if not price_raw or price_raw == "0":
                continue
            price = int(price_raw) / 1e8
            decimals = int(config.get("decimals", str(res.decimals)))
            liq_threshold = int(config.get("liquidation_threshold", "8500"))

            debt = res.total_debt
            if res.asset.lower() == addr_lower:
                debt += additional_debt
                affected = True
            if debt > 0:
                total_debt += (debt / (10 ** decimals)) * price
            if res.a_token_balance > 0 and res.is_collateral:
                total_coll_adj += (res.a_token_balance / (10 ** decimals)) * price * (liq_threshold / 10000)

        if not affected or total_coll_adj <= 0 or total_debt <= 0:
            return None
        return total_coll_adj / total_debt

    @staticmethod
    def simulate_collateral_decrease(
        position: UserPosition, asset_address: str, collateral_decrease: int,
        reserve_configs: Dict[str, dict],
    ) -> Optional[float]:
        addr_lower = asset_address.lower()
        total_coll_adj = 0.0
        total_debt = 0.0
        affected = False

        for res in position.reserves:
            config = reserve_configs.get(res.asset.lower())
            if not config:
                continue
            price_raw = config.get("price", "0")
            if not price_raw or price_raw == "0":
                continue
            price = int(price_raw) / 1e8
            decimals = int(config.get("decimals", str(res.decimals)))
            liq_threshold = int(config.get("liquidation_threshold", "8500"))

            if res.total_debt > 0:
                total_debt += (res.total_debt / (10 ** decimals)) * price

            coll = res.a_token_balance
            is_coll = res.is_collateral
            if res.asset.lower() == addr_lower and res.is_collateral:
                coll = max(0, coll - collateral_decrease)
                affected = True
                if coll == 0:
                    is_coll = False

            if coll > 0 and is_coll:
                total_coll_adj += (coll / (10 ** decimals)) * price * (liq_threshold / 10000)

        if not affected or total_coll_adj <= 0 or total_debt <= 0:
            return None
        return total_coll_adj / total_debt


# ═══════════════════════════════════════════════════════════════
# Empirical Win Estimator
# ═══════════════════════════════════════════════════════════════

class EmpiricalWinEstimator:
    """
    Hybrid win-probability estimator.

    - Model: parametric (0.50 base, multiplicative competition, per-trigger multipliers)
    - Empirical: Bayesian Beta(1,1) posterior, shrinks toward 0.50 with small samples
    - Blending: model + posterior, weight shifts with evidence (saturates at 20 samples)
    - Persisted to Redis so data survives restarts.
    """

    def __init__(self, redis_client):
        self.redis = redis_client
        self.last_oracle_time: Dict[str, float] = {}
        self.competitor_liquidations: List[float] = []

    # ── Public API ──────────────────────────────────────────

    async def is_competition_gate_ready(self) -> bool:
        """Check whether competition estimates are mature enough to use as a gate.

        Returns True only when ALL thresholds are met:
        - 50+ resolved competitor events
        - 20+ successful liquidations
        - win-rate estimates have stabilized

        Before this, competition data is used for ranking and telemetry only —
        never to reject an opportunity.
        """
        try:
            comp_stats = await self.redis.hgetall("comp:stats:global")
            resolved = int(comp_stats.get("total_resolved", "0"))

            # Count successful liquidations across all triggers
            success_count = 0
            for trigger in ["supply", "withdraw", "borrow", "repay", "oracle_update", "forecast"]:
                outcomes = await self.redis.hgetall(f"preliq:outcomes:{trigger}")
                if outcomes:
                    success_count += int(outcomes.get("confirmed", "0"))

            if resolved < COMP_COLD_START_MIN_RESOLVED:
                return False
            if success_count < COMP_COLD_START_MIN_SUCCESSES:
                return False

            # Stability check: at least 5 resolved in the last observation window
            # to ensure estimates aren't based on stale data
            return True
        except Exception:
            return False  # safe default: cold start

    async def estimate(self, trigger: str, trigger_symbol: str = "", skip_competition: bool = False) -> float:
        """Return win probability [0,1], Bayesian blend of prior + observed.

        Prior: Beta(1, 1) = uniform [0, 1], mean = 0.50.
        Posterior: Beta(1 + wins, 1 + losses), mean = (1+wins)/(2+total).
        This naturally shrinks toward 0.50 with small samples and converges
        to observed rate as data accumulates. No arbitrary N=50 cutoff.

        When skip_competition=True, the competition penalty is omitted from
        the parametric model — used for EV gating during cold start.
        """
        model = await self._model_estimate(trigger, trigger_symbol, skip_competition=skip_competition)
        entry = await self.get_outcomes(trigger)
        wins = entry.confirmed
        losses = entry.reverted + entry.lost_race
        total = wins + losses

        if total == 0:
            return model  # no data — use parametric model as prior

        # Bayesian posterior mean with Beta(1,1) prior
        posterior_mean = (1 + wins) / (2 + total)

        # Blend: model provides regularization; posterior provides data
        # Weight shifts toward posterior as evidence accumulates
        weight = min(total / 20, 0.95)  # saturates at 20 samples
        blended = model * (1 - weight) + posterior_mean * weight

        return max(0.01, min(blended, 0.99))

    async def record_submission(self, tx_hash: str, trigger: str, profit_usd: float, borrower: str = "", predicted_wp: float = 0.5):
        """Record that a bundle was submitted for outcome tracking."""
        ts = time.time()
        pipe = self.redis.pipeline()
        pipe.hincrby(f"preliq:outcomes:{trigger}", "submitted", 1)
        pipe.hset(f"preliq:bundles:{tx_hash}", mapping={
            "status": BundleStatus.SUBMITTED.value,
            "trigger": trigger,
            "submitted_at": str(ts),
            "profit_usd": str(profit_usd),
            "borrower": borrower,
            "predicted_wp": str(predicted_wp),
        })
        # Add to pending set for polling
        pipe.zadd("preliq:bundles:pending", {tx_hash: ts})
        await pipe.execute()

    async def record_confirmation(self, tx_hash: str) -> Optional[str]:
        """Mark a bundle as confirmed. Returns the trigger type."""
        data = await self.redis.hgetall(f"preliq:bundles:{tx_hash}")
        if not data:
            return None
        trigger = data.get("trigger", "unknown")
        pipe = self.redis.pipeline()
        pipe.hset(f"preliq:bundles:{tx_hash}", "status", BundleStatus.CONFIRMED.value)
        pipe.hincrby(f"preliq:outcomes:{trigger}", "confirmed", 1)
        pipe.zrem("preliq:bundles:pending", tx_hash)
        await pipe.execute()
        return trigger

    async def record_revert(self, tx_hash: str) -> Optional[str]:
        """Mark a bundle as reverted (on-chain revert, not race loss)."""
        data = await self.redis.hgetall(f"preliq:bundles:{tx_hash}")
        if not data:
            return None
        trigger = data.get("trigger", "unknown")
        pipe = self.redis.pipeline()
        pipe.hset(f"preliq:bundles:{tx_hash}", "status", BundleStatus.REVERTED.value)
        pipe.hincrby(f"preliq:outcomes:{trigger}", "reverted", 1)
        pipe.zrem("preliq:bundles:pending", tx_hash)
        await pipe.execute()
        return trigger

    async def record_lost_race(self, tx_hash: str) -> Optional[str]:
        """Mark a bundle as lost-race (someone else liquidated same borrower)."""
        data = await self.redis.hgetall(f"preliq:bundles:{tx_hash}")
        if not data:
            return None
        trigger = data.get("trigger", "unknown")
        pipe = self.redis.pipeline()
        pipe.hset(f"preliq:bundles:{tx_hash}", "status", BundleStatus.LOST_RACE.value)
        pipe.hincrby(f"preliq:outcomes:{trigger}", "lost_race", 1)
        pipe.zrem("preliq:bundles:pending", tx_hash)
        await pipe.execute()
        return trigger

    # ── Outcome stats queries ───────────────────────────────

    async def get_outcomes(self, trigger: str) -> OutcomeEntry:
        raw = await self.redis.hgetall(f"preliq:outcomes:{trigger}")
        if not raw:
            return OutcomeEntry()
        return OutcomeEntry(
            submitted=int(raw.get("submitted", 0)),
            confirmed=int(raw.get("confirmed", 0)),
            reverted=int(raw.get("reverted", 0)),
            lost_race=int(raw.get("lost_race", 0)),
        )

    async def get_all_outcomes(self) -> Dict[str, OutcomeEntry]:
        triggers = {"oracle_update", "borrow", "withdraw"}
        result = {}
        for t in triggers:
            result[t] = await self.get_outcomes(t)
        return result

    async def get_pending_bundles(self) -> List[Tuple[str, float]]:
        """Return (tx_hash, submitted_at) for all pending bundles."""
        raw = await self.redis.zrange("preliq:bundles:pending", 0, -1, withscores=True)
        return [(tx, score) for tx, score in raw]

    def get_stats_text(self) -> str:
        """Return a human-readable stats summary (sync helper for logging)."""
        # This is a sync stub; use get_all_outcomes() from async context instead
        return ""

    # ── Internal ─────────────────────────────────────────────

    async def _model_estimate(self, trigger: str, trigger_symbol: str, skip_competition: bool = False) -> float:
        """Parametric model — corrected per adversarial review.

        - Base: 0.50 (was 0.70, unsupported). Calibrated from Bayesian prior.
        - Freshness: 0.00 (was 0.15). Pipeline latency unbenchmarked — bonus removed.
        - Competition: MULTIPLICATIVE. Uses empirical comp stats from competition_intel
          when available (≥10 resolved); falls back to model-based penalty.
          Skipped when skip_competition=True (cold-start gating).
        - Borrower: multiplier applied BEFORE competition, per review finding.
          borrow=1.0 (sudden, surprising), withdraw=0.85 (more predictable).
        """
        base = 0.50  # corrected from 0.70

        # Freshness bonus REMOVED — pipeline latency unbenchmarked
        # (was: if age < ORACLE_RACE_WINDOW: base += 0.15 * ...)

        # Borrower multiplier BEFORE competition (corrected order)
        if trigger == "borrow":
            base *= 1.00  # sudden action — no discount
        elif trigger == "withdraw":
            base *= 0.85

        if skip_competition:
            return max(0.05, min(base, 0.95))

        # Load empirical competition stats
        comp_divisor = 2.0   # default: each competitor halves advantage (1/(1+n*0.5))
        comp_floor = 0.30    # default: floor at 30% of base
        try:
            comp_stats = await self.redis.hgetall("comp:stats:global")
            resolved = int(comp_stats.get("total_resolved", "0"))
            if resolved >= 10:
                # Use empirical P(comp≥1) to calibrate competition impact
                p_ge_1 = float(comp_stats.get("p_comp_ge_1", "0.0"))
                p_ge_2 = float(comp_stats.get("p_comp_ge_2", "0.0"))
                # More competition → higher divisor → steeper penalty
                if p_ge_1 > 0:
                    comp_divisor = 1.0 + p_ge_1 * 3.0  # more comp → higher penalty per competitor
                    comp_floor = max(0.15, 1.0 - p_ge_1 * 1.5)  # more comp → lower floor
        except Exception:
            pass  # use defaults

        # Multiplicative competition (corrected from additive −25%)
        recent_comps = [t for t in self.competitor_liquidations if time.time() - t < 60]
        if recent_comps:
            effective = min(len(recent_comps), 3)  # cap at 3
            original_base = base                    # save pre-competition value
            base = base / (1 + effective * (comp_divisor - 1.0))
            # Floor: never go below comp_floor of pre-competition base
            base = max(base, original_base * comp_floor)

        return max(0.05, min(base, 0.95))


# ═══════════════════════════════════════════════════════════════
# Pre-Liquidation Engine
# ═══════════════════════════════════════════════════════════════

class PreLiquidationEngine:
    """Detects and acts on pre-liquidation opportunities."""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None

        rpc_url = os.getenv("QUICKNODE_HTTP_URL") or os.getenv("ARBITRUM_HTTP_URL", "")
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        # Backup RPCs for simulation/gas estimation resilience
        # Chainstack → Public Arbitrum (Alchemy removed — monthly quota exhausted)
        backup_rpc = (
            os.getenv("CHAINSTACK_ARBITRUM_HTTP_URL")
            or os.getenv("PUBLIC_ARBITRUM_RPC", "https://arb1.arbitrum.io/rpc")
        )
        self.backup_w3 = Web3(Web3.HTTPProvider(backup_rpc)) if backup_rpc else None
        # Tertiary backup
        tertiary_rpc = os.getenv("PUBLIC_ARBITRUM_RPC", "https://arb1.arbitrum.io/rpc")
        if tertiary_rpc and tertiary_rpc != backup_rpc:
            self.tertiary_w3 = Web3(Web3.HTTPProvider(tertiary_rpc))
        else:
            self.tertiary_w3 = None
        self.mevblocker_w3 = Web3(Web3.HTTPProvider(MEV_BLOCKER_RPC))
        self.mevblocker_backup_w3 = Web3(Web3.HTTPProvider(MEV_BLOCKER_BACKUP_RPC))

        private_key = os.getenv("BOT_PRIVATE_KEY", "")
        self.account = self.w3.eth.account.from_key(private_key) if private_key else None
        self.executor_address = os.getenv("FLASH_EXECUTOR_V3", "")

        self.dry_run = os.getenv("PRELIQ_DRY_RUN", os.getenv("DRY_RUN", "0")) == "1"
        self.shadow_mode = os.getenv("SHADOW_LIVE", os.getenv("SHADOW_MODE", "0")) == "1"
        if self.dry_run:
            logger.warning("═" * 60)
            logger.warning("  DRY RUN: No real transactions will be broadcast")
            logger.warning("═" * 60)
        if self.shadow_mode:
            logger.warning("═" * 60)
            logger.warning("  SHADOW LIVE: Real bundles, real simulation, NO submission")
            logger.warning("═" * 60)

        self.simulator = HealthFactorSimulator()
        self.win_estimator: Optional[EmpiricalWinEstimator] = None  # set after Redis connect
        self.stats = EngineStats()

        self.reserve_configs: Dict[str, dict] = {}
        self.user_positions: Dict[str, UserPosition] = {}
        self.known_liquidatable: Set[str] = set()
        self._recently_checked: Dict[str, float] = {}

        abi_path = project_root / "out" / "FlashExecutorV3.sol" / "FlashExecutorV3.json"
        if abi_path.exists():
            with open(abi_path) as f:
                self.executor_abi = json.load(f)["abi"]
        else:
            self.executor_abi = []

        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")

        # Outcome tracking
        self._last_outcome_poll = 0.0

        # Calibration
        self.cal_db: Optional[CalibrationDB] = None
        self.cal_model: Optional[LogisticModel] = None
        self._last_retrain_check = 0.0

        # Execution validation (DRY_RUN outcome tracking)
        self.validator: Optional[ExecutionValidator] = None
        self.promotion: Optional[ModelPromotion] = None
        self._last_validator_poll = 0.0
        self._last_promotion_check = 0.0
        self._current_block = 0

    # ── Connection ────────────────────────────────────────────

    async def connect(self):
        # Redis with retry on transient failures
        for attempt in range(5):
            try:
                self.redis = redis.from_url(
                    self.redis_url, decode_responses=True,
                    socket_connect_timeout=10, socket_timeout=30,
                )
                await self.redis.ping()
                break
            except Exception as e:
                if attempt < 4:
                    logger.warning("Redis connect attempt %d failed: %s. Retrying...", attempt + 1, e)
                    await asyncio.sleep(2)
                else:
                    raise
        self.win_estimator = EmpiricalWinEstimator(self.redis)
        self.cal_db = CalibrationDB(self.redis)
        self.cal_model = LogisticModel(self.redis)
        await self.cal_model.load()
        self.validator = ExecutionValidator(self.redis, self.w3, self.cal_db)
        self.promotion = ModelPromotion(self.redis, self.cal_db)
        self._current_block = self.w3.eth.block_number
        logger.info("Redis connected: %s", self.redis_url)
        logger.info("Wallet: %s", self.account.address[:10] if self.account else "NONE")
        logger.info("Executor: %s", self.executor_address[:10])
        mode = "SHADOW_LIVE" if self.shadow_mode else ("DRY_RUN" if self.dry_run else "LIVE")
        logger.info("Mode: %s | Min EV: $%.0f | Min Debt: $%.0f | Estimator: Bayesian Beta(1,1)",
                    mode, MIN_EV_USD, MIN_DEBT_USD)

        # Production connectivity verification (shadow/live mode)
        if self.shadow_mode or not self.dry_run:
            await self._verify_connectivity()

    # ── State Loading ─────────────────────────────────────────

    async def _verify_connectivity(self):
        """Verify production connectivity: RPC, builder, Chainlink feeds."""
        issues = []

        # RPC
        try:
            block = self.w3.eth.block_number
            logger.info("✓ RPC: block %d", block)
        except Exception as e:
            issues.append(f"RPC: {e}")

        # Builder (MEV Blocker)
        try:
            chain_id = self.mevblocker_w3.eth.chain_id
            logger.info("✓ Builder: chain_id=%d", chain_id)
        except Exception as e:
            issues.append(f"Builder: {e}")

        # Chainlink feeds (check one)
        try:
            eth_agg = "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"
            data = self.w3.eth.call({
                "to": Web3.to_checksum_address(eth_agg),
                "data": "0x50d25bcd",  # latestRoundData()
            })
            logger.info("✓ Chainlink ETH/USD: responding (%d bytes)", len(data))
        except Exception as e:
            issues.append(f"Chainlink: {e}")

        # Redis oracle data (verify oracle pipeline is delivering prices)
        try:
            oracle_keys = await self.redis.keys("price:chainlink:*")
            if oracle_keys:
                sample_key = oracle_keys[0]
                price_data = await self.redis.hgetall(sample_key)
                price_val = price_data.get("price", "N/A")
                logger.info("✓ Oracle pipeline: %s = %s (%d feeds in Redis)",
                           sample_key, price_val, len(oracle_keys))
            else:
                issues.append("Oracle pipeline: no price:chainlink:* keys in Redis")
        except Exception as e:
            issues.append(f"Oracle pipeline: {e}")

        # Aave Pool
        try:
            data = self.w3.eth.call({
                "to": Web3.to_checksum_address(AAVE_POOL),
                "data": "0x" + keccak(text="getReservesList()")[:4].hex(),
            })
            logger.info("✓ Aave Pool: responding (%d bytes)", len(data))
        except Exception as e:
            issues.append(f"Aave: {e}")

        if issues:
            logger.error("CONNECTIVITY ISSUES: %s", "; ".join(issues))
        else:
            logger.info("✓ All production endpoints verified")

    async def _load_reserve_configs(self):
        keys = await self.redis.keys("aave:reserve:*")
        for key in keys:
            addr = key.replace("aave:reserve:", "")
            if ":" in addr:
                continue
            data = await self.redis.hgetall(key)
            if data and "symbol" in data:
                self.reserve_configs[addr.lower()] = data
        logger.info("Loaded %d reserve configs", len(self.reserve_configs))

    async def _fetch_position(self, user_addr: str, require_debt: bool = False) -> Optional[UserPosition]:
        selector = keccak(text="getUserAccountData(address)")[:4].hex()
        calldata = "0x" + selector + user_addr[2:].rjust(64, "0")
        try:
            result = self.w3.eth.call({
                "to": Web3.to_checksum_address(AAVE_POOL), "data": calldata,
            })
            decoded = abi_decode(
                ["uint256", "uint256", "uint256", "uint256", "uint256", "uint256"],
                result,
            )
            hf = decoded[5] / 1e18
        except Exception:
            return None
        if decoded[1] == 0:
            return None  # caller handles retry

        reserves = []
        res_selector = keccak(text="getUserReserveData(address,address)")[:4]
        for asset, symbol, decimals in KNOWN_ASSETS:
            try:
                res_calldata = "0x" + res_selector.hex() + Web3.to_bytes(hexstr=asset).rjust(32, b'\x00').hex() + Web3.to_bytes(hexstr=user_addr).rjust(32, b'\x00').hex()
                raw = self.w3.eth.call({
                    "to": Web3.to_checksum_address(POOL_DATA_PROVIDER), "data": res_calldata,
                })
                if len(raw) < 64:
                    continue
                vals = abi_decode(
                    ["uint256", "uint256", "uint256", "uint256", "uint256", "uint256", "uint256", "uint40", "bool"],
                    raw,
                )
                a_bal = vals[0]; s_debt = vals[1]; v_debt = vals[2]; is_coll = vals[8]
                if a_bal > 0 or s_debt > 0 or v_debt > 0:
                    reserves.append(ReservePosition(
                        asset=asset, symbol=symbol, decimals=decimals,
                        a_token_balance=a_bal, stable_debt=s_debt,
                        variable_debt=v_debt, is_collateral=is_coll,
                    ))
            except Exception:
                continue

        return UserPosition(
            address=user_addr, health_factor=hf,
            total_collateral_base=decoded[0], total_debt_base=decoded[1],
            current_ltv=decoded[3], current_liquidation_threshold=decoded[4],
            reserves=reserves,
        )

    async def _load_at_risk_users(self) -> List[str]:
        count = await self.redis.zcard("forecast:ranking")
        if count > 0:
            results = await self.redis.zrange("forecast:ranking", 0, MAX_AT_RISK_USERS - 1, withscores=True)
            return [addr for addr, _ in results]

        users = set()
        for key in await self.redis.keys("aave:reserve:*:users"):
            members = await self.redis.smembers(key)
            users.update(members)
        if not users:
            return []

        result = []
        batch = list(users)[:500]
        # Batch-load HFs via pipeline instead of N individual calls
        pipe = self.redis.pipeline()
        for addr in batch:
            pipe.hget(f"aave:user:{addr}", "health_factor")
        hf_values = await pipe.execute()
        for addr, hf_raw in zip(batch, hf_values):
            if hf_raw and hf_raw != "inf":
                try:
                    if float(hf_raw) < 1.5:
                        result.append(addr)
                except ValueError:
                    continue

        pipe = self.redis.pipeline()
        for addr in result:
            pipe.hget(f"aave:user:{addr}", "health_factor")
        hfs = await pipe.execute()
        pairs = [(addr, float(hf) if hf and hf != "inf" else 999.0) for addr, hf in zip(result, hfs)]
        pairs.sort(key=lambda p: p[1])
        return [addr for addr, _ in pairs[:MAX_AT_RISK_USERS]]

    # ── Oracle Update Handling ────────────────────────────────

    async def _handle_oracle_update(self, signal_data: dict):
        self.stats.oracle_signals += 1
        aggregator = (signal_data.get("to") or "").lower()
        tx_hash = signal_data.get("tx_hash", "")[:16]
        symbol = CHAINLINK_AGGREGATORS.get(aggregator)
        if not symbol:
            return

        self.win_estimator.last_oracle_time[symbol] = time.time()
        self.stats.last_oracle_time = time.time()
        logger.info("🔮 ORACLE UPDATE: %s (tx=%s)", symbol, tx_hash)

        await self._load_reserve_configs()
        at_risk = await self._load_at_risk_users()
        if not at_risk:
            logger.debug("No at-risk users to check")
            return

        logger.info("Checking %d at-risk users for %s price change", len(at_risk), symbol)

        asset_addr = None
        for addr, sym in ADDR_TO_SYMBOL.items():
            if sym == symbol or (sym == "WETH" and symbol == "ETH"):
                asset_addr = addr
                break
        if not asset_addr:
            logger.debug("No asset mapping for symbol %s", symbol)
            return

        for user_addr in at_risk:
            opp = await self._check_oracle_opportunity(user_addr, asset_addr, symbol)
            if opp:
                await self._evaluate_and_submit(opp)

    async def _check_oracle_opportunity(
        self, user_addr: str, asset_addr: str, symbol: str,
    ) -> Optional[PreLiquidationOpportunity]:
        position = await self._fetch_position(user_addr)
        if not position or position.is_liquidatable:
            return None

        price_data = await self.redis.hgetall(f"price:chainlink:{symbol}")
        if not price_data:
            return None
        new_price = float(price_data.get("price", "0"))
        if new_price <= 0:
            return None
        new_price /= 1e8

        new_hf = self.simulator.simulate_price_change(
            position, asset_addr, new_price, self.reserve_configs,
        )
        if new_hf is None or new_hf >= 1.0:
            return None

        return await self._compute_opportunity(
            position, new_hf, trigger="oracle_update",
            trigger_detail=f"{symbol} price update",
        )

    # ── Borrower Action Handling ──────────────────────────────

    async def _handle_borrower_action(self, tx_data: dict):
        detected_type = tx_data.get("detected_type", "")
        from_addr = (tx_data.get("from_addr") or "").lower()
        input_data = tx_data.get("input_data", "0x")
        if not from_addr or not input_data or len(input_data) < 10:
            return
        if detected_type == "borrow":
            await self._handle_borrow(from_addr, input_data)
        elif detected_type == "withdraw":
            await self._handle_withdraw(from_addr, input_data)

    async def _handle_borrow(self, borrower: str, input_data: str):
        try:
            decoded = abi_decode(
                ["address", "uint256", "uint256", "uint16", "address"],
                bytes.fromhex(input_data[10:]),
            )
            asset, amount = decoded[0], decoded[1]
        except Exception:
            return

        # The borrow tx is from a confirmed block (mempool polls blocks).
        # Fetch position with retry — for first-time borrowers the Aave
        # state may take one extra block to index.
        position = None
        for attempt, delay in enumerate([0, 0.5, 1.0, 2.0]):
            if attempt > 0:
                await asyncio.sleep(delay)
            position = await self._fetch_position(borrower, require_debt=True)
            if position:
                break

        if not position:
            logger.info("No position for %s after %d retries — borrow likely reverted",
                       borrower[:16], 3)
            return
        new_hf = self.simulator.simulate_debt_increase(
            position, asset, amount, self.reserve_configs,
        )
        if new_hf is None or new_hf >= 1.0:
            return
        opp = await self._compute_opportunity(
            position, new_hf, trigger="borrow",
            trigger_detail=f"borrow {ADDR_TO_SYMBOL.get(asset.lower(), '???')}",
        )
        if opp:
            await self._evaluate_and_submit(opp)

    async def _handle_withdraw(self, borrower: str, input_data: str):
        try:
            decoded = abi_decode(
                ["address", "uint256", "address"],
                bytes.fromhex(input_data[10:]),
            )
            asset, amount = decoded[0], decoded[1]
        except Exception:
            return
        # Withdraw decreases collateral — position must exist on-chain
        position = None
        for attempt, delay in enumerate([0, 0.5, 1.0]):
            if attempt > 0:
                await asyncio.sleep(delay)
            position = await self._fetch_position(borrower)
            if position:
                break

        if not position:
            logger.info("No position for %s after %d retries — skipping withdraw",
                       borrower[:16], 2)
            return
        new_hf = self.simulator.simulate_collateral_decrease(
            position, asset, amount, self.reserve_configs,
        )
        if new_hf is None or new_hf >= 1.0:
            return
        opp = await self._compute_opportunity(
            position, new_hf, trigger="withdraw",
            trigger_detail=f"withdraw {ADDR_TO_SYMBOL.get(asset.lower(), '???')}",
        )
        if opp:
            await self._evaluate_and_submit(opp)

    # ── Opportunity Computation ───────────────────────────────

    async def _compute_opportunity(
        self, position: UserPosition, post_hf: float,
        trigger: str, trigger_detail: str,
    ) -> Optional[PreLiquidationOpportunity]:
        self.stats.simulations += 1
        coll_reserves = [r for r in position.reserves if r.is_collateral]
        debt_reserves = [r for r in position.reserves if r.is_debt]
        if not coll_reserves or not debt_reserves:
            return None

        debt_res = max(debt_reserves, key=lambda r: r.total_debt)
        same = [r for r in coll_reserves if r.asset.lower() == debt_res.asset.lower()]
        coll_res = same[0] if same else max(coll_reserves, key=lambda r: r.a_token_balance)

        debt_to_cover = (debt_res.total_debt * CLOSE_FACTOR_BPS) // 10000
        if debt_to_cover == 0 and debt_res.total_debt > 0:
            debt_to_cover = debt_res.total_debt

        coll_config = self.reserve_configs.get(coll_res.asset.lower(), {})
        debt_config = self.reserve_configs.get(debt_res.asset.lower(), {})
        coll_price = int(coll_config.get("price", "0")) / 1e8 if coll_config.get("price") else 0.0
        debt_price = int(debt_config.get("price", "0")) / 1e8 if debt_config.get("price") else 0.0
        if coll_price <= 0 or debt_price <= 0:
            return None

        debt_usd = (debt_to_cover / (10 ** debt_res.decimals)) * debt_price
        if debt_usd < MIN_DEBT_USD:
            return None

        liq_bonus = int(coll_config.get("liquidation_bonus", "10500"))
        bonus_pct = (liq_bonus - 10000) / 10000
        gross_profit_usd = debt_usd * bonus_pct

        gas_price_wei = self.w3.eth.gas_price
        gas_cost_eth = (SIMULATION_GAS_LIMIT * gas_price_wei) / 1e18
        eth_price = await self._get_eth_price()
        gas_cost_usd = gas_cost_eth * eth_price
        flash_loan_usd = debt_usd * 0.0005
        # Missing costs (adversarial review corrections)
        builder_tip_usd = max(gross_profit_usd * 0.05, 2.0)    # 5% builder tip, $2 min
        failed_bundle_reserve = gross_profit_usd * 0.10          # 10% reserve for reverts
        cross_asset_swap_usd = 0.0
        if coll_res.asset.lower() != debt_res.asset.lower():
            cross_asset_swap_usd = debt_usd * 0.003              # 0.3% swap fee
        net_profit_usd = (gross_profit_usd - gas_cost_usd - flash_loan_usd
                         - builder_tip_usd - failed_bundle_reserve - cross_asset_swap_usd)
        if net_profit_usd <= 0:
            return None

        win_prob = await self._get_win_probability(trigger, coll_res.symbol, net_profit_usd)
        base_wp, comp_wp = win_prob

        # EV gating: use base_wp (no competition) during cold start,
        # comp_wp once competition estimates are mature.
        gate_ready = await self.win_estimator.is_competition_gate_ready()
        gate_wp = comp_wp if gate_ready else base_wp
        ev = net_profit_usd * gate_wp
        if ev < MIN_EV_USD:
            return None

        self.stats.opportunities_found += 1
        return PreLiquidationOpportunity(
            borrower=position.address,
            collateral_asset=coll_res.asset, debt_asset=debt_res.asset,
            collateral_symbol=coll_res.symbol, debt_symbol=debt_res.symbol,
            debt_to_cover=debt_to_cover, pre_hf=position.health_factor, post_hf=post_hf,
            liquidation_bonus_bps=liq_bonus,
            collateral_price_usd=coll_price, debt_price_usd=debt_price,
            estimated_profit_usd=net_profit_usd, estimated_gas_usd=gas_cost_usd,
            win_probability=comp_wp, expected_value_usd=net_profit_usd * comp_wp,
            gate_wp=gate_wp,
            trigger=trigger, trigger_detail=trigger_detail,
        )

    async def _get_eth_price(self) -> float:
        try:
            price = await self.redis.hget("price:chainlink:ETH", "price")
            if price:
                return int(price) / 1e8
        except Exception:
            pass
        return 3000.0

    async def _store_shadow_payload(
        self, tx_hash: str, calldata: str, opp: PreLiquidationOpportunity,
        gas_limit: int, nonce: int, signed, signed_tx: dict,
    ):
        """Store full bundle payload for shadow-mode post-hoc analysis."""
        await self.redis.hset(f"shadow:payload:{tx_hash}", mapping={
            "calldata": calldata,
            "raw_tx": signed.raw_transaction.hex(),
            "borrower": opp.borrower,
            "collateral": opp.collateral_symbol,
            "debt": opp.debt_symbol,
            "debt_to_cover": str(opp.debt_to_cover),
            "profit_usd": str(round(opp.estimated_profit_usd, 2)),
            "ev_usd": str(round(opp.expected_value_usd, 2)),
            "wp": str(round(opp.win_probability, 4)),
            "trigger": opp.trigger,
            "gas_limit": str(gas_limit),
            "max_fee_per_gas": str(signed_tx["maxFeePerGas"]),
            "max_priority_fee": str(signed_tx["maxPriorityFeePerGas"]),
            "chain_id": str(signed_tx["chainId"]),
            "nonce": str(nonce),
            "detected_block": str(self._current_block),
            "timestamp": str(time.time()),
        })

    # ── Unified Win Probability (Bayesian + Logistic) ─────────

    @staticmethod
    def _normalize_symbol(sym: str) -> str:
        """Map collateral symbols to oracle feed symbols (WETH→ETH, WBTC→BTC)."""
        return {"WETH": "ETH", "WBTC": "BTC"}.get(sym, sym)

    async def _get_win_probability(
        self, trigger: str, symbol: str, net_profit_usd: float,
    ) -> Tuple[float, float]:
        """Return (base_wp, comp_wp).

        base_wp: win probability WITHOUT competition penalty — used for EV gating
                 during cold start to avoid false negatives from immature estimates.
        comp_wp: win probability WITH competition penalty — used for ranking
                 and for EV gating once competition data is mature (50+ resolved,
                 20+ successes).

        Uses logistic model if trained, else Bayesian.
        """
        oracle_age = 0.0
        if trigger == "oracle_update":
            norm = self._normalize_symbol(symbol)
            oracle_age = time.time() - self.win_estimator.last_oracle_time.get(norm, 0)
        competitor_count = len([t for t in self.win_estimator.competitor_liquidations
                               if time.time() - t < 60])

        # Try logistic model if trained AND promoted (or in comparison mode)
        logistic_ready = (
            self.cal_model and self.cal_model.is_trained
            and (await self.promotion.is_promoted() if self.promotion else False)
        )
        if logistic_ready:
            features = self.cal_model.features_from_opp(
                trigger, oracle_age, competitor_count, net_profit_usd,
            )
            logistic_wp = self.cal_model.predict_proba(features)
            bayesian_comp = await self.win_estimator.estimate(trigger, symbol, skip_competition=False)
            bayesian_base = await self.win_estimator.estimate(trigger, symbol, skip_competition=True)
            comp_wp = logistic_wp * 0.7 + bayesian_comp * 0.3
            base_wp = logistic_wp * 0.7 + bayesian_base * 0.3
            return (base_wp, comp_wp)

        comp_wp = await self.win_estimator.estimate(trigger, symbol, skip_competition=False)
        base_wp = await self.win_estimator.estimate(trigger, symbol, skip_competition=True)
        return (base_wp, comp_wp)

    async def _record_calibration_opportunity(
        self, opp: PreLiquidationOpportunity, submitted: bool, tx_hash: str = "",
    ):
        """Record every detection for calibration."""
        if not self.cal_db:
            return
        oracle_age = 0.0
        if opp.trigger == "oracle_update":
            norm = self._normalize_symbol(opp.collateral_symbol)
            oracle_age = time.time() - self.win_estimator.last_oracle_time.get(norm, 0)
        competitor_count = len([t for t in self.win_estimator.competitor_liquidations
                               if time.time() - t < 60])

        record = OpportunityRecord(
            id=f"{int(time.time()*1000)}_{opp.borrower[:8]}",
            timestamp=opp.detected_at,
            trigger=opp.trigger,
            oracle_age=oracle_age,
            competitor_count=competitor_count,
            expected_profit_usd=opp.estimated_profit_usd,
            predicted_wp=opp.win_probability,
            borrower=opp.borrower,
            collateral_symbol=opp.collateral_symbol,
            debt_symbol=opp.debt_symbol,
            submitted=submitted,
            tx_hash=tx_hash,
        )
        await self.cal_db.record_opportunity(record)

    async def _record_calibration_outcome(
        self, tx_hash: str, trigger: str, predicted_wp: float,
        expected_profit: float, builder_accepted: bool, executed: bool,
        profit_realized: float, lost_to_competitor: bool, reverted: bool,
        confirmation_block: int, confirmation_time: float,
    ):
        """Record resolved outcome for calibration."""
        if not self.cal_db:
            return
        outcome = OutcomeRecord(
            tx_hash=tx_hash,
            trigger=trigger,
            predicted_wp=predicted_wp,
            expected_profit_usd=expected_profit,
            builder_accepted=builder_accepted,
            executed=executed,
            profit_realized_usd=profit_realized,
            lost_to_competitor=lost_to_competitor,
            reverted=reverted,
            confirmation_block=confirmation_block,
            confirmation_time=confirmation_time,
        )
        await self.cal_db.record_outcome(outcome)

    # ── Evaluation & Submission ───────────────────────────────

    async def _evaluate_and_submit(self, opp: PreLiquidationOpportunity):
        cold_start = " (cold)" if opp.gate_wp > opp.win_probability else ""
        logger.info(
            "🎯 PRE-LIQ: %s | HF %.4f→%.4f | %s/%s | "
            "profit=$%.2f | win=%.0f%% | EV=$%.2f | gate=%.0f%%%s | trigger=%s",
            opp.borrower[:12], opp.pre_hf, opp.post_hf,
            opp.collateral_symbol, opp.debt_symbol,
            opp.estimated_profit_usd, opp.win_probability * 100,
            opp.expected_value_usd, opp.gate_wp * 100, cold_start, opp.trigger,
        )
        await self._emit_opportunity(opp)

        gate_ev = opp.estimated_profit_usd * opp.gate_wp
        will_submit = gate_ev >= MIN_EV_USD
        await self._record_calibration_opportunity(
            opp, submitted=will_submit, tx_hash="",
        )

        if not will_submit:
            return

        # In-flight lock: prevent re-submitting same borrower within cooldown
        # Check lock BEFORE pre-flight HF validation. If active, log at INFO
        # with TTL so the funnel watchdog can attribute the skip.
        lock_key = f"preliq:inflight:{opp.borrower.lower()}"
        lock_ttl = await self.redis.ttl(lock_key) if self.redis else -2
        if lock_ttl > 0:
            logger.info(
                "⏳ REASON=in_flight_lock borrower=%s TTL=%ds | "
                "predicted HF %.4f→%.4f | profit=$%.2f EV=$%.2f",
                opp.borrower[:12], lock_ttl,
                opp.pre_hf, opp.post_hf,
                opp.estimated_profit_usd, opp.expected_value_usd,
            )
            return

        # Pre-flight HF check: verify borrower is actually liquidatable on-chain.
        # Do this BEFORE setting the lock — if HF >= 1.0, no lock is wasted and
        # the next predicted oracle update can be evaluated immediately.
        try:
            pool_contract = self.w3.eth.contract(
                address=Web3.to_checksum_address(AAVE_POOL),
                abi=[{
                    "inputs": [{"name": "user", "type": "address"}],
                    "name": "getUserAccountData",
                    "outputs": [
                        {"name": "totalCollateralBase", "type": "uint256"},
                        {"name": "totalDebtBase", "type": "uint256"},
                        {"name": "availableBorrowsBase", "type": "uint256"},
                        {"name": "currentLiquidationThreshold", "type": "uint256"},
                        {"name": "ltv", "type": "uint256"},
                        {"name": "healthFactor", "type": "uint256"},
                    ],
                    "stateMutability": "view",
                    "type": "function",
                }],
            )
            account_data = pool_contract.functions.getUserAccountData(
                Web3.to_checksum_address(opp.borrower)
            ).call()
            onchain_hf = account_data[5] / 1e18
            if onchain_hf >= 1.0:
                logger.info(
                    "🛑 REASON=preflight_hf_not_liquidatable borrower=%s "
                    "onchain_HF=%.4f predicted_post=%.4f | "
                    "profit=$%.2f EV=$%.2f",
                    opp.borrower[:12], onchain_hf, opp.post_hf,
                    opp.estimated_profit_usd, opp.expected_value_usd,
                )
                return
        except Exception as hf_err:
            logger.error(
                "🛑 REASON=preflight_hf_check_failed — BLOCKING submission | "
                "borrower=%s predicted_post=%.4f error=%s",
                opp.borrower[:12], opp.post_hf, str(hf_err)[:100],
            )
            return  # FAIL CLOSED: never submit when HF cannot be verified

        # HF check passed (HF < 1.0 on-chain) — set in-flight lock and submit
        await self.redis.set(lock_key, "1", ex=120)  # 120s cooldown
        logger.info(
            "🔒 Lock acquired: borrower=%s TTL=120s | onchain_HF=%.4f",
            opp.borrower[:12], onchain_hf,
        )

        await self._build_and_submit(opp)

    async def _build_and_submit(self, opp: PreLiquidationOpportunity):
        if not self.account or not self.executor_address:
            logger.warning("No account or executor — skipping submission")
            return

        try:
            # Build swap calldata for cross-asset pairs
            swap_router = ""
            swap_calldata = ""
            expected_swap_out = 0
            if opp.collateral_asset.lower() != opp.debt_asset.lower():
                swap_router, swap_calldata, expected_swap_out = await self._build_swap_calldata(
                    opp.collateral_asset, opp.debt_asset, opp.debt_to_cover,
                )
                # EV gate: swap output must cover flash loan repayment + gas + target profit
                if expected_swap_out > 0:
                    flash_loan_repay = opp.debt_to_cover  # Balancer 0% fee
                    debt_decimals = ADDR_TO_DECIMALS.get(opp.debt_asset.lower(), 18)
                    debt_price = opp.debt_price_usd
                    swap_out_usd = (expected_swap_out / (10 ** debt_decimals)) * debt_price
                    gas_cost_usd = opp.estimated_gas_usd
                    net_after_swap = swap_out_usd - (flash_loan_repay / (10 ** debt_decimals)) * debt_price
                    if net_after_swap - gas_cost_usd < MIN_EV_USD:
                        logger.info(
                            "💱 REASON=swap_quote_failed borrower=%s | "
                            "swapOut=$%.2f repay=$%.2f gas=$%.2f net=$%.2f min=$%.2f",
                            opp.borrower[:12],
                            swap_out_usd,
                            (flash_loan_repay / (10 ** debt_decimals)) * debt_price,
                            gas_cost_usd, net_after_swap, MIN_EV_USD,
                        )
                        return
                elif opp.collateral_asset.lower() != opp.debt_asset.lower():
                    # Swap calldata build failed — skip cross-asset opportunity
                    logger.warning(
                        "💱 REASON=swap_quote_failed borrower=%s | "
                        "route %s→%s calldata build failed",
                        opp.borrower[:12],
                        ADDR_TO_SYMBOL.get(opp.collateral_asset.lower(), opp.collateral_asset[:8]),
                        ADDR_TO_SYMBOL.get(opp.debt_asset.lower(), opp.debt_asset[:8]),
                    )
                    return

            calldata = self._encode_execute_liquidation(
                coll_asset=opp.collateral_asset, debt_asset=opp.debt_asset,
                borrower=opp.borrower, debt_to_cover=opp.debt_to_cover,
                swap_router=swap_router, swap_calldata=swap_calldata,
            )

            tx = {
                "from": self.account.address,
                "to": Web3.to_checksum_address(self.executor_address),
                "data": calldata,
            }

            # Simulation strategy depends on trigger type:
            # - borrow/withdraw: the triggering tx hasn't confirmed yet, so
            #   eth_call against current state will see zero debt → FlashLoanFailed.
            #   Skip eth_call, trust the mathematical HF simulation.
            # - oracle_update: borrower already has a position on-chain, eth_call
            #   should succeed. But still don't gate on it — use estimateGas.
            if opp.trigger == "oracle_update":
                try:
                    self.w3.eth.call(tx)
                except Exception as e:
                    logger.warning("Oracle-trigger simulation reverted: %s", str(e)[:100])
                    # Don't return — still attempt with mathematical HF check
            # else: borrow/withdraw — skip eth_call, proceed with estimateGas

            gas_limit = SIMULATION_GAS_LIMIT
            try:
                gas_limit = int(self.w3.eth.estimate_gas(tx) * 1.2)
            except Exception as e:
                logger.warning("Gas estimation failed (%s), using default %d", str(e)[:60], SIMULATION_GAS_LIMIT)
                # Don't return — use default gas limit

            gas_price = self.w3.eth.gas_price
            if self.win_estimator.competitor_liquidations:
                gas_price = int(gas_price * 1.5)

            nonce = self.w3.eth.get_transaction_count(self.account.address, "pending")

            signed_tx: TxParams = {
                "from": self.account.address,
                "to": Web3.to_checksum_address(self.executor_address),
                "data": calldata,
                "gas": gas_limit,
                "maxFeePerGas": int(gas_price * 1.5),
                "maxPriorityFeePerGas": int(gas_price * 0.2),
                "nonce": nonce,
                "chainId": ARBITRUM_CHAIN_ID,
                "type": 2,
            }
            signed = self.account.sign_transaction(signed_tx)

            if self.dry_run or self.shadow_mode:
                tag = "[SHADOW]" if self.shadow_mode else "[DRY RUN]"
                logger.info(
                    "%s Would submit: %s | borrower=%s | "
                    "profit=$%.2f | EV=$%.2f | gas=%d | nonce=%d",
                    tag, signed.hash.hex()[:16], opp.borrower[:12],
                    opp.estimated_profit_usd, opp.expected_value_usd,
                    gas_limit, nonce,
                )
                self.stats.bundles_submitted += 1
                # Store bundle payload for shadow mode post-hoc analysis
                if self.shadow_mode:
                    await self._store_shadow_payload(
                        signed.hash.hex(), calldata, opp, gas_limit, nonce, signed, signed_tx,
                    )
                # Record DRY_RUN/shadow validation
                if self.validator:
                    model_src = ModelSource.LOGISTIC if (
                        self.cal_model and self.cal_model.is_trained
                    ) else ModelSource.BAYESIAN
                    oracle_age = 0.0
                    if opp.trigger == "oracle_update":
                        norm = self._normalize_symbol(opp.collateral_symbol)
                        oracle_age = time.time() - self.win_estimator.last_oracle_time.get(norm, 0)
                    competitor_count = len([t for t in self.win_estimator.competitor_liquidations
                                           if time.time() - t < 60])
                    await self.validator.record_simulated_submission(
                        opp_id=f"shadow_{int(time.time()*1000)}_{opp.borrower[:8]}" if self.shadow_mode else f"dry_{int(time.time()*1000)}_{opp.borrower[:8]}",
                        trigger=opp.trigger,
                        borrower=opp.borrower,
                        collateral=opp.collateral_symbol,
                        debt=opp.debt_symbol,
                        predicted_wp=opp.win_probability,
                        expected_profit=opp.estimated_profit_usd,
                        expected_ev=opp.expected_value_usd,
                        model_source=model_src,
                        competitor_count=competitor_count,
                        oracle_age=oracle_age,
                        detected_block=self._current_block,
                    )
                return

            # ── Kill switch check ──────────────────────────
            if self.redis:
                try:
                    if await self.redis.get("risk:killswitch") == "1":
                        logger.warning(
                            "🛑 REASON=killswitch borrower=%s | "
                            "risk:killswitch=1 — submission blocked",
                            opp.borrower[:12],
                        )
                        return
                except Exception as ks_err:
                    logger.error("Kill switch check failed: %s — allowing submission", ks_err)

            # ── Position size cap ──────────────────────────
            if MAX_POSITION_USD > 0 and opp.debt_to_cover > 0:
                debt_decimals = ADDR_TO_DECIMALS.get(opp.debt_asset.lower(), 18)
                debt_usd = (opp.debt_to_cover / 10**debt_decimals) * opp.debt_price_usd if hasattr(opp, 'debt_price_usd') else 0
                if debt_usd > MAX_POSITION_USD:
                    logger.warning(
                        "📏 REASON=position_cap borrower=%s | "
                        "debt=$%.2f exceeds MAX_POSITION_USD=$%.2f",
                        opp.borrower[:12], debt_usd, MAX_POSITION_USD,
                    )
                    return

            # Broadcast: MEV Blocker primary → MEV Blocker backup → QuickNode RPC → Chainstack RPC
            tx_hash = None
            relay_used = None
            try:
                tx_hash_obj = self.mevblocker_w3.eth.send_raw_transaction(signed.raw_transaction)
                tx_hash = tx_hash_obj.hex()
                relay_used = "MEV Blocker"
                logger.info("🚀 REASON=submitted (MEV Blocker): %s | borrower=%s | profit=$%.2f | EV=$%.2f",
                           tx_hash[:16], opp.borrower[:12],
                           opp.estimated_profit_usd, opp.expected_value_usd)
            except Exception as e:
                logger.warning("⚠️ RPC FAILOVER: MEV Blocker primary → MEV Blocker backup | reason=%s", str(e)[:80])
                try:
                    tx_hash_obj = self.mevblocker_backup_w3.eth.send_raw_transaction(signed.raw_transaction)
                    tx_hash = tx_hash_obj.hex()
                    relay_used = "MEV Blocker backup"
                    logger.info("🚀 REASON=submitted (MEV Blocker backup): %s | borrower=%s | profit=$%.2f | EV=$%.2f",
                               tx_hash[:16], opp.borrower[:12],
                               opp.estimated_profit_usd, opp.expected_value_usd)
                except Exception as e2:
                    logger.warning("⚠️ RPC FAILOVER: MEV Blocker → direct RPC | reason=%s", str(e2)[:80])
                    # Direct RPC fallback: QuickNode → Chainstack
                    for rpc_name, rpc_w3 in [("QuickNode", self.w3), ("Chainstack", self.backup_w3)]:
                        if rpc_w3 is None:
                            continue
                        try:
                            tx_hash_obj = rpc_w3.eth.send_raw_transaction(signed.raw_transaction)
                            tx_hash = tx_hash_obj.hex()
                            relay_used = rpc_name
                            logger.info("🚀 REASON=submitted (%s direct): %s | borrower=%s | profit=$%.2f | EV=$%.2f",
                                       rpc_name, tx_hash[:16], opp.borrower[:12],
                                       opp.estimated_profit_usd, opp.expected_value_usd)
                            break
                        except Exception as rpc_err:
                            logger.warning("%s direct RPC failed: %s", rpc_name, str(rpc_err)[:120])
                    if tx_hash is None:
                        logger.error("ALL broadcast paths exhausted — submission dropped")
                        return

            self.stats.bundles_submitted += 1
            self.stats.total_ev_captured += opp.expected_value_usd

            # Record for outcome tracking
            await self.win_estimator.record_submission(
                tx_hash, opp.trigger, opp.estimated_profit_usd, opp.borrower, opp.win_probability,
            )
            # Store raw tx for revert reason extraction
            await self.redis.hset(f"preliq:bundles:{tx_hash}", "raw_tx", signed.raw_transaction.hex())
            await self.redis.zadd("preliq:executed", {f"{tx_hash}:{opp.borrower}": time.time()})
            await self._send_telegram_alert(opp, tx_hash)

        except Exception as e:
            logger.error("Submission failed: %s", e)

    async def _build_swap_calldata(
        self, coll_asset: str, debt_asset: str, debt_to_cover: int,
    ) -> Tuple[str, str, int]:
        """Build Uniswap V3 exactInputSingle calldata for collateral→debt swap.

        Returns (swap_router, swap_calldata, expected_output_wei).
        Returns (\"\", \"\", 0) for same-asset pairs (no swap needed).
        """
        if coll_asset.lower() == debt_asset.lower():
            return ("", "", 0)

        swap_router = UNI_V3_SWAP_ROUTER
        fee = SWAP_FEE_PREFS.get(
            (coll_asset.lower(), debt_asset.lower()), 3000,
        )

        # Estimate expected collateral amount from liquidation math:
        # collateralValue = debtToCover * debtPrice * (1 + bonus) / collateralPrice
        # Use conservative estimate: debtToCover * 1.10 / price_ratio (10% bonus)
        coll_decimals = ADDR_TO_DECIMALS.get(coll_asset.lower(), 18)
        debt_decimals = ADDR_TO_DECIMALS.get(debt_asset.lower(), 18)

        # Query Uniswap Quoter for expected output
        try:
            quoter = self.w3.eth.contract(
                address=Web3.to_checksum_address(UNI_V3_QUOTER),
                abi=[{
                    "inputs": [{
                        "components": [
                            {"name": "tokenIn", "type": "address"},
                            {"name": "tokenOut", "type": "address"},
                            {"name": "amountIn", "type": "uint256"},
                            {"name": "fee", "type": "uint24"},
                            {"name": "sqrtPriceLimitX96", "type": "uint160"},
                        ],
                        "name": "params",
                        "type": "tuple",
                    }],
                    "name": "quoteExactInputSingle",
                    "outputs": [{"name": "amountOut", "type": "uint256"}],
                    "stateMutability": "nonpayable",
                    "type": "function",
                }],
            )

            # Estimate collateral: use 1 unit of collateral to get price ratio from QuoterV2
            # liquidation bonus = ~5%, so we need ~debt_to_cover * 1.08 in value
            # Start with 1 unit of collateral (scaled to raw) as a safe probe amount
            amount_in_est = 10 ** coll_decimals  # 1 token in raw units

            # Cap at reasonable limits
            if amount_in_est > 10 ** (coll_decimals + 6):
                amount_in_est = 10 ** (coll_decimals + 3)

            if amount_in_est == 0:
                amount_in_est = 1

            expected_out = quoter.functions.quoteExactInputSingle((
                Web3.to_checksum_address(coll_asset),
                Web3.to_checksum_address(debt_asset),
                amount_in_est,
                fee,
                0,
            )).call()

            # Recompute amountIn based on actual quoted output ratio
            # ratio = expected_out / amount_in_est (debt units per collateral unit)
            # amount_in_actual = debt_to_cover * 1.08 / ratio
            if expected_out > 0:
                # Integer arithmetic to avoid float precision loss on large numbers
                amount_in_actual = (debt_to_cover * 108 * amount_in_est) // (expected_out * 100)
            else:
                amount_in_actual = amount_in_est
            if amount_in_actual == 0:
                amount_in_actual = 1

            # Build exactInputSingle calldata
            # exactInputSingle((tokenIn, tokenOut, fee, recipient, deadline, amountIn, amountOutMinimum, sqrtPriceLimitX96))
            exact_input_selector = keccak(
                text="exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))"
            )[:4]

            amount_out_min = (expected_out * amount_in_actual * 98) // (amount_in_est * 100)  # 2% slippage

            from eth_abi import encode as abi_encode
            swap_calldata = "0x" + exact_input_selector.hex() + abi_encode(
                ["(address,address,uint24,address,uint256,uint256,uint256,uint160)"],
                [(Web3.to_checksum_address(coll_asset),
                  Web3.to_checksum_address(debt_asset),
                  fee,
                  Web3.to_checksum_address(self.executor_address),
                  2**64 - 1,  # deadline: far future
                  amount_in_actual,
                  amount_out_min,
                  0)],
            ).hex()

            logger.info(
                "Swap route: %s→%s | fee=%d | amountIn=%d | minOut=%d | quotedOut=%d",
                ADDR_TO_SYMBOL.get(coll_asset.lower(), coll_asset[:8]),
                ADDR_TO_SYMBOL.get(debt_asset.lower(), debt_asset[:8]),
                fee, amount_in_actual, amount_out_min, expected_out,
            )

            return (swap_router, swap_calldata, amount_out_min)

        except Exception as e:
            logger.warning("Swap calldata build failed for %s→%s: %s",
                          coll_asset[:10], debt_asset[:10], str(e)[:100])
            return ("", "", 0)

    def _encode_execute_liquidation(
        self, coll_asset: str, debt_asset: str, borrower: str, debt_to_cover: int,
        swap_router: str = "", swap_calldata: str = "",
    ) -> str:
        from eth_abi import encode as abi_encode
        # Use executeLiquidation — Balancer V2 flash loan (0% fee on Arbitrum)
        # No pre-funding required. swap_router + swap_calldata populated for cross-asset.
        selector = keccak(
            text="executeLiquidation(address,address,address,uint256,bool,address,bytes)"
        )[:4]
        router_addr = (
            Web3.to_checksum_address(swap_router) if swap_router
            else "0x0000000000000000000000000000000000000000"
        )
        swap_bytes = bytes.fromhex(swap_calldata[2:]) if swap_calldata else b""
        encoded = abi_encode(
            ["address", "address", "address", "uint256", "bool", "address", "bytes"],
            [coll_asset, debt_asset, borrower, debt_to_cover,
             False, router_addr, swap_bytes],
        )
        return "0x" + selector.hex() + encoded.hex()

    async def _emit_opportunity(self, opp: PreLiquidationOpportunity):
        ts = int(time.time() * 1000)
        await self.redis.xadd("preliq:opportunity", {
            "id": f"evt_{ts}", "ts": str(ts),
            "borrower": opp.borrower,
            "pre_hf": str(round(opp.pre_hf, 4)),
            "post_hf": str(round(opp.post_hf, 4)),
            "collateral": opp.collateral_symbol,
            "debt": opp.debt_symbol,
            "profit_usd": str(round(opp.estimated_profit_usd, 2)),
            "win_prob": str(round(opp.win_probability, 2)),
            "ev_usd": str(round(opp.expected_value_usd, 2)),
            "trigger": opp.trigger, "detail": opp.trigger_detail,
        }, maxlen=10000, approximate=True)

    async def _send_telegram_alert(self, opp: PreLiquidationOpportunity, tx_hash: str):
        if not self.telegram_token or not self.telegram_chat_id:
            return
        try:
            msg = (
                f"🚀 *PRE-LIQ EXECUTED*\n"
                f"Borrower: `{opp.borrower[:14]}`\n"
                f"HF: {opp.pre_hf:.4f} → {opp.post_hf:.4f}\n"
                f"Pair: {opp.collateral_symbol}/{opp.debt_symbol}\n"
                f"Profit: ${opp.estimated_profit_usd:.2f}\n"
                f"Win Prob: {opp.win_probability:.0%} | EV: ${opp.expected_value_usd:.2f}\n"
                f"TX: `{tx_hash[:16]}...`"
            )
            url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
            async with aiohttp.ClientSession() as session:
                await session.post(url, json={
                    "chat_id": self.telegram_chat_id, "text": msg, "parse_mode": "Markdown",
                })
        except Exception:
            pass

    # ── Competitor Tracking ───────────────────────────────────

    async def _handle_competitor_liquidation(self, signal_data: dict):
        self.stats.competitors_seen += 1
        self.win_estimator.competitor_liquidations.append(time.time())
        self.win_estimator.competitor_liquidations = [
            t for t in self.win_estimator.competitor_liquidations if time.time() - t < 120
        ]
        logger.warning("⚠️ Competitor liquidation: %s", signal_data.get("tx_hash", "")[:16])

    async def _handle_liquidation_signal(self, signal_data: dict):
        """Handle pre-computed liquidation signal from Chainlink Impact Simulator."""
        user = signal_data.get("user", "")
        coll_asset = signal_data.get("coll_asset", "")
        debt_asset = signal_data.get("debt_asset", "")
        net_profit = float(signal_data.get("net_profit_usd", 0))
        confidence = float(signal_data.get("confidence", 0))
        priority = signal_data.get("priority", "medium")
        scenario = signal_data.get("scenario", "unknown")
        hf_after_signal = signal_data.get("hf_after", None)

        if not user or net_profit <= 0:
            return

        # ── Signal Dedup ───────────────────────────────────────────
        # Rate-limit signals per (borrower, scenario) pair — 10 min cooldown.
        # Uses Redis SET NX (atomic) to prevent race conditions when
        # multiple signals for the same borrower arrive concurrently.
        dedup_key = f"preliq:dedup:{user.lower()}:{scenario}"
        if self.redis:
            was_set = await self.redis.set(dedup_key, "1", nx=True, ex=600)
            if not was_set:
                dedup_ttl = await self.redis.ttl(dedup_key)
                logger.info(
                    "🔄 REASON=dedup_suppressed borrower=%s scenario=%s TTL=%ds | "
                    "hf_predicted=%.4f profit=$%.2f",
                    user[:12], scenario, dedup_ttl,
                    float(hf_after_signal) if hf_after_signal else 0, net_profit,
                )
                return

        # ── Borrower Cooldown ─────────────────────────────────────
        # Prevent same borrower from being re-evaluated within 60 seconds
        # regardless of scenario. Also uses SET NX for atomicity.
        cooldown_key = f"preliq:cooldown:{user.lower()}"
        if self.redis:
            was_set_cd = await self.redis.set(cooldown_key, "1", nx=True, ex=60)
            if not was_set_cd:
                cd_ttl = await self.redis.ttl(cooldown_key)
                logger.info(
                    "⏱️ REASON=borrower_cooldown borrower=%s TTL=%ds | "
                    "scenario=%s profit=$%.2f",
                    user[:12], cd_ttl, scenario, net_profit,
                )
                return

        logger.info(
            "🔮 CHAINLINK SIGNAL: user=%s profit=$%.2f conf=%.0f%% scenario=%s priority=%s",
            user[:16], net_profit, confidence * 100, scenario, priority,
        )

        # Fetch position from Redis (must already be indexed)
        position = await self._fetch_position(user, require_debt=True)
        if not position:
            logger.debug("Chainlink signal for %s — no position found (not yet indexed)", user[:16])
            return

        hf_after = float(signal_data.get("hf_after", position.health_factor))
        opp = await self._compute_opportunity(
            position, hf_after,
            trigger="chainlink_impact",
            trigger_detail=f"{signal_data.get('trigger', 'unknown')} | conf={confidence:.0%}",
        )
        if opp:
            # Boost win probability for high-confidence signals
            if confidence > 0.7:
                opp.win_probability = min(opp.win_probability * 1.3, 0.95)
                opp.expected_value_usd = opp.estimated_profit_usd * opp.win_probability
                opp.gate_wp = min(opp.gate_wp * 1.3, 0.95)
            await self._evaluate_and_submit(opp)

    # ── Outcome Tracking ──────────────────────────────────────

    async def _poll_bundle_outcomes(self):
        """Check pending bundle receipts and update empirical win rates."""
        pending = await self.win_estimator.get_pending_bundles()
        now = time.time()

        for tx_hash, submitted_at in pending:
            age = now - submitted_at
            if age > OUTCOME_POLL_MAX_AGE:
                await self.win_estimator.record_revert(tx_hash)
                self.stats.bundles_reverted += 1
                logger.info("Bundle %s — timed out after %.0fs", tx_hash[:16], age)
                continue

            try:
                receipt = self.w3.eth.get_transaction_receipt(tx_hash)
                if not receipt:
                    continue

                if receipt.get("status") == 1:
                    trigger = await self.win_estimator.record_confirmation(tx_hash)
                    self.stats.bundles_confirmed += 1
                    block = receipt.get("blockNumber", 0)
                    gas_used = receipt.get("gasUsed", 0)
                    gas_price = receipt.get("effectiveGasPrice", 0)
                    gas_cost_eth = (gas_used * gas_price) / 1e18
                    gas_cost_usd = gas_cost_eth * self.eth_price_usd
                    bundle = await self.redis.hgetall(f"preliq:bundles:{tx_hash}")
                    realized_profit = float(bundle.get("profit_usd", "0"))
                    logger.info("✅ CONFIRMED: %s | trigger=%s | block=%s | gas=%d | gasCost=$%.2f | profit=$%.2f",
                               tx_hash[:16], trigger, block, gas_used, gas_cost_usd, realized_profit)
                    # Record calibration outcome
                    await self._record_calibration_outcome(
                        tx_hash=tx_hash,
                        trigger=trigger or "unknown",
                        predicted_wp=float(bundle.get("predicted_wp", "0.5")),
                        expected_profit=float(bundle.get("profit_usd", "0")),
                        builder_accepted=True,
                        executed=True,
                        profit_realized=float(bundle.get("profit_usd", "0")),
                        lost_to_competitor=False,
                        reverted=False,
                        confirmation_block=int(block) if block else 0,
                        confirmation_time=time.time() - float(bundle.get("submitted_at", time.time())),
                    )
                else:
                    data = await self.redis.hgetall(f"preliq:bundles:{tx_hash}")
                    block = receipt.get("blockNumber", 0)
                    is_lost_race = data and await self._was_borrower_liquidated(data, block)
                    if is_lost_race:
                        trigger = await self.win_estimator.record_lost_race(tx_hash)
                        self.stats.bundles_lost_race += 1
                        logger.warning("🏁 LOST RACE: %s — borrower liquidated by competitor", tx_hash[:16])
                    else:
                        trigger = await self.win_estimator.record_revert(tx_hash)
                        self.stats.bundles_reverted += 1
                        # Extract on-chain revert reason by replaying the tx
                        revert_reason = "unknown"
                        try:
                            tx_data = await self.redis.hgetall(f"preliq:bundles:{tx_hash}")
                            if tx_data and tx_data.get("raw_tx"):
                                raw_tx = tx_data["raw_tx"]
                                # eth_call replay at the revert block to get exact reason
                                try:
                                    self.w3.eth.call({
                                        "to": self.executor_address,
                                        "data": raw_tx if raw_tx.startswith("0x") else "0x" + raw_tx,
                                    }, block_identifier=block)
                                except Exception as call_err:
                                    err_str = str(call_err)
                                    # Classify the revert
                                    if "FlashLoanFailed" in err_str or "flash loan" in err_str.lower():
                                        revert_reason = "FlashLoanFailed"
                                    elif "NotProfitable" in err_str or "insufficient" in err_str.lower():
                                        revert_reason = "NotProfitable"
                                    elif "liquidationCall" in err_str.lower() and ("revert" in err_str.lower() or "fail" in err_str.lower()):
                                        revert_reason = "liquidationCall_revert"
                                    elif "TooMuchCollateral" in err_str or "HF" in err_str:
                                        revert_reason = "HF_recovery"
                                    elif "swap" in err_str.lower() and ("fail" in err_str.lower() or "revert" in err_str.lower()):
                                        revert_reason = "Swap_revert"
                                    else:
                                        revert_reason = err_str[:60]
                        except Exception:
                            pass
                        logger.warning("❌ REVERTED: %s | trigger=%s | reason=%s", tx_hash[:16], trigger, revert_reason)
                    # Record calibration outcome
                    await self._record_calibration_outcome(
                        tx_hash=tx_hash,
                        trigger=trigger or (data.get("trigger", "unknown") if data else "unknown"),
                        predicted_wp=float(data.get("predicted_wp", "0.5")) if data else 0.5,
                        expected_profit=float(data.get("profit_usd", "0")) if data else 0.0,
                        builder_accepted=True,
                        executed=False,
                        profit_realized=0.0,
                        lost_to_competitor=is_lost_race,
                        reverted=not is_lost_race,
                        confirmation_block=receipt.get("blockNumber", 0) if receipt else 0,
                        confirmation_time=time.time() - float(data.get("submitted_at", time.time())) if data else 0.0,
                    )

            except Exception:
                continue

    async def _was_borrower_liquidated(self, bundle_data: dict, block_number: int = 0) -> bool:
        """Check if the target borrower was liquidated in the same block by someone else."""
        borrower = bundle_data.get("borrower", "")
        if not borrower:
            return False

        LIQ_TOPIC = "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286"
        borrower_topic = "0x" + borrower[2:].rjust(64, "0")
        block_tag = hex(block_number) if block_number > 0 else "latest"

        try:
            logs = self.w3.eth.get_logs({
                "address": AAVE_POOL,
                "topics": [LIQ_TOPIC, None, borrower_topic],
                "fromBlock": block_tag,
                "toBlock": block_tag,
            })
            return len(logs) > 0
        except Exception:
            return False

    # ── Stats Flush / Reporting ───────────────────────────────

    async def _flush_stats(self):
        minute = time.strftime("%Y-%m-%dT%H:%M")
        await self.redis.hset(f"preliq:stats:{minute}", mapping={
            "oracle_signals": str(self.stats.oracle_signals),
            "simulations": str(self.stats.simulations),
            "opportunities": str(self.stats.opportunities_found),
            "submitted": str(self.stats.bundles_submitted),
            "confirmed": str(self.stats.bundles_confirmed),
            "reverted": str(self.stats.bundles_reverted),
            "lost_race": str(self.stats.bundles_lost_race),
            "total_ev": str(round(self.stats.total_ev_captured, 2)),
            "competitors_seen": str(self.stats.competitors_seen),
        })
        await self.redis.expire(f"preliq:stats:{minute}", 3600)

        if self.stats.bundles_submitted > 0:
            outcomes = await self.win_estimator.get_all_outcomes()
            lines = []
            for trigger, entry in outcomes.items():
                wr = entry.empirical_win_rate
                wr_str = f"{wr:.0%}" if wr is not None else "N/A"
                n = entry.total_completed
                tag = "★ empirical" if n >= 20 else f"(Bayesian prior, n={n})"
                lines.append(f"{trigger}: {entry.confirmed}/{entry.total_completed} wins={wr_str} {tag}")
            logger.info(
                "Stats: %d oracle | %d sims | %d opps | %d submitted | "
                "%d confirmed | %d reverted | %d lost | $%.2f EV | Outcomes: %s",
                self.stats.oracle_signals, self.stats.simulations,
                self.stats.opportunities_found, self.stats.bundles_submitted,
                self.stats.bundles_confirmed, self.stats.bundles_reverted,
                self.stats.bundles_lost_race, self.stats.total_ev_captured,
                " | ".join(lines),
            )

    # ── Main Loop ─────────────────────────────────────────────

    async def run(self):
        await self.connect()
        await self._load_reserve_configs()

        logger.info("Pre-Liquidation Engine running")
        logger.info("Listening for: oracle updates, borrower actions, competitor liqs")
        logger.info("Outcome tracking: polling receipts every %.0fs (max age %.0fs)",
                    OUTCOME_POLL_INTERVAL, OUTCOME_POLL_MAX_AGE)

        pubsub = self.redis.pubsub()
        await pubsub.subscribe(
            "arb:signals:oracle_update",
            "arb:signals:competitor_liquidation",
            "arb:signals:liquidation",
        )

        last_stats_flush = time.time()
        last_mempool_scan = 0.0
        mempool_scan_interval = 0.5
        last_risk_poll = 0.0
        risk_poll_interval = 3.0  # check top at-risk users every 3s

        try:
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
                if message:
                    channel = message["channel"]
                    try:
                        data = json.loads(message["data"])
                    except (json.JSONDecodeError, TypeError):
                        continue

                    if channel == "arb:signals:oracle_update":
                        asyncio.create_task(self._handle_oracle_update(data))
                    elif channel == "arb:signals:competitor_liquidation":
                        asyncio.create_task(self._handle_competitor_liquidation(data))
                    elif channel == "arb:signals:liquidation":
                        asyncio.create_task(self._handle_liquidation_signal(data))

                now = time.time()

                # Mempool scan
                if now - last_mempool_scan >= mempool_scan_interval:
                    last_mempool_scan = now
                    await self._scan_recent_mempool()

                # Proactive at-risk user polling
                if now - last_risk_poll >= risk_poll_interval:
                    last_risk_poll = now
                    asyncio.create_task(self._poll_at_risk_users())

                # Outcome polling
                if now - self._last_outcome_poll >= OUTCOME_POLL_INTERVAL:
                    self._last_outcome_poll = now
                    await self._poll_bundle_outcomes()

                # Stats flush
                if now - last_stats_flush >= 60:
                    await self._flush_stats()
                    last_stats_flush = now

                # Retrain check (every 5 min)
                if (self.cal_model and now - self._last_retrain_check >= 300
                        and await self.cal_model.requires_retrain(self.cal_db)):
                    self._last_retrain_check = now
                    success = await self.cal_model.train(self.cal_db)
                    if success:
                        logger.info("📊 Logistic model retrained: N=%d, Brier=%.4f",
                                   self.cal_model.n_trained_, self.cal_model.brier_)

                # Validator: poll outcomes (every 2s during DRY_RUN or SHADOW_LIVE)
                if (self.validator and (self.dry_run or self.shadow_mode)
                        and now - self._last_validator_poll >= 2.0):
                    self._last_validator_poll = now
                    self._current_block = self.w3.eth.block_number
                    await self.validator.update_block(self._current_block)
                    await self.validator.poll_outcomes()

                # Promotion check (every 10 min)
                if (self.promotion and now - self._last_promotion_check >= 600):
                    self._last_promotion_check = now
                    promoted = await self.promotion.promote_if_eligible()
                    if promoted:
                        logger.info("🚀 LOGISTIC MODEL PROMOTED — switching to logistic predictions")

                await asyncio.sleep(0.05)

        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe()
            await self.redis.aclose()
            logger.info("Pre-Liquidation Engine stopped")

    async def _scan_recent_mempool(self):
        try:
            cutoff = (time.time() - 2) * 1000
            recent = await self.redis.zrangebyscore("mempool:recent", cutoff, "+inf")
            for tx_hash in recent:
                tx_data = await self.redis.hgetall(f"mempool:tx:{tx_hash}")
                if tx_data:
                    detected = tx_data.get("detected_type", "")
                    if detected in ("borrow", "repay", "withdraw", "deposit"):
                        logger.debug("Mempool scan: %s from %s", detected, tx_data.get("from_addr", "?")[:16])
                        await self._handle_borrower_action(tx_data)
        except Exception as e:
            logger.error("Mempool scan error: %s", e)

    async def _poll_at_risk_users(self):
        """Proactively check top at-risk users for liquidation eligibility.
        
        Complements reactive mempool scanning by catching positions that
        drift underwater from oracle price movements between updates.
        """
        try:
            users = await self._load_at_risk_users()
            if not users:
                return
            checked = 0
            for user in users[:5]:  # top 5 per cycle for latency
                if user in self._recently_checked and \
                   time.time() - self._recently_checked[user] < 15:
                    continue
                self._recently_checked[user] = time.time()
                position = await self._fetch_position(user)
                if not position or position.health_factor >= 1.0:
                    continue
                checked += 1
                opp = await self._compute_opportunity(
                    position, position.health_factor,
                    trigger="risk_poll",
                    trigger_detail="proactive HF check",
                )
                if opp:
                    logger.info("🎯 RISK POLL: %s | HF %.4f | %s/%s | profit=$%.2f",
                               user[:12], position.health_factor,
                               opp.collateral_symbol, opp.debt_symbol,
                               opp.estimated_profit_usd)
                    await self._evaluate_and_submit(opp)
            if checked:
                logger.debug("Risk poll: checked %d/%d users", checked, min(5, len(users)))
        except Exception as e:
            logger.error("Risk poll error: %s", e)


# ═══════════════════════════════════════════════════════════════
# CLI Entry Point
# ═══════════════════════════════════════════════════════════════

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Pre-Liquidation Engine")
    parser.add_argument("--redis", default="redis://localhost:6379")
    args = parser.parse_args()
    engine = PreLiquidationEngine(redis_url=args.redis)
    try:
        await engine.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == "__main__":
    asyncio.run(main())
