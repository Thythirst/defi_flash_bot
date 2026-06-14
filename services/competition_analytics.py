"""
services/competition_analytics.py — Competition Analytics v1.

Reads competition data from Redis (populated by competition_intel.py)
and computes multi-dimensional analytics: asset-specific, time-of-day,
debt-bucket, builder, tip percentiles, and bid recommendations.

Generates daily reports: Competition Summary, Builder Summary,
Asset Summary, Time-of-Day Summary, Bid Recommendations.

Usage:
  python -m services.competition_analytics --report   # one-shot full report
  python -m services.competition_analytics --report daily  # daily summary
  python -m services.competition_analytics --export-csv /tmp/report.csv
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import redis.asyncio as redis
from dotenv import load_dotenv

load_dotenv(dotenv_path=project_root / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | analytics | %(message)s",
)
logger = logging.getLogger("analytics")

# ═══════════════════════════════════════════════════════════════
# Constants
# ═══════════════════════════════════════════════════════════════

# Aave V3 liquidation bonus (basis points) — varies by asset
# Default: 5% = 10500 bps. Some assets (stablecoins) may differ.
DEFAULT_LIQUIDATION_BONUS_BPS = 10500
LIQUIDATION_BONUS_PCT = (DEFAULT_LIQUIDATION_BONUS_BPS - 10000) / 10000  # 0.05

# Debt size buckets (USD)
DEBT_BUCKETS = [
    (0, 5_000, "< $5K"),
    (5_000, 25_000, "$5K–$25K"),
    (25_000, 100_000, "$25K–$100K"),
    (100_000, 1_000_000, "$100K–$1M"),
    (1_000_000, float("inf"), "> $1M"),
]

# Aave V3 pool address
AAVE_POOL = "0x794a61358d6845594f94dc1db02a252b5b4814ad"

# Flashbots/MEV Blocker builder addresses (for builder identification)
# On Arbitrum L2, the liquidator is the effective "builder" since the
# sequencer orders transactions. We track liquidator for builder analytics.
KNOWN_BUILDERS = {
    "0x1f9090aae28b8a3dceadf281b0f12828e676c326": "MEV Blocker",
    "0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5": "beaverbuild",
    "0x4838b106fce9647bdf1e7877bf73ce8b0bad5f97": "rsync-builder",
    "0xa1d76a7ca91f398c0a533b92c0b2f2f5549d22a9": "Titan",
    "0x690b9a9e9aa1c9db991c7721a73d351ec4badb91": "JetBuilder",
}

# ═══════════════════════════════════════════════════════════════
# Data Classes
# ═══════════════════════════════════════════════════════════════

@dataclass
class ResolvedOpportunity:
    """Fully resolved opportunity with all analytics fields."""
    opp_id: str
    tx_hash: str
    timestamp: float
    block_number: int
    borrower: str
    collateral_asset: str
    collateral_symbol: str
    debt_asset: str
    debt_symbol: str
    debt_size: int           # raw units
    debt_size_usd: float     # USD value
    trigger_type: str        # oracle, borrow, withdraw
    competitor_count: int
    winning_liquidator: str
    winning_builder: str     # liquidator on L2, or MEV Blocker if detected
    builder_tip_gwei: float
    gas_used: int
    effective_gas_price: int
    liquidation_bonus_usd: float
    estimated_profit_usd: float
    realized_profit_usd: float
    our_would_have_won: bool
    detection_hour: int      # UTC hour


@dataclass
class AssetAnalytics:
    """Per-asset competition analytics."""
    symbol: str
    opportunity_count: int = 0
    total_competitors: int = 0
    total_tip_gwei: float = 0.0
    tip_count: int = 0
    total_liquidation_size_usd: float = 0.0
    total_profit_usd: float = 0.0
    wins: int = 0             # number of times we would have won

    @property
    def avg_competitors(self) -> float:
        return self.total_competitors / max(self.opportunity_count, 1)

    @property
    def avg_tip_gwei(self) -> float:
        return self.total_tip_gwei / max(self.tip_count, 1)

    @property
    def avg_liquidation_size_usd(self) -> float:
        return self.total_liquidation_size_usd / max(self.opportunity_count, 1)

    @property
    def avg_profit_usd(self) -> float:
        return self.total_profit_usd / max(self.opportunity_count, 1)

    @property
    def win_probability(self) -> float:
        return self.wins / max(self.opportunity_count, 1)


@dataclass
class BuilderAnalytics:
    """Per-builder/liquidator analytics."""
    name: str
    address: str
    opportunities_won: int = 0
    total_tip_gwei: float = 0.0
    tip_count: int = 0
    total_gas_used: int = 0

    @property
    def avg_tip_gwei(self) -> float:
        return self.total_tip_gwei / max(self.tip_count, 1)

    @property
    def avg_gas_used(self) -> int:
        return self.total_gas_used // max(self.tip_count, 1)


@dataclass
class HourlyAnalytics:
    """Per-UTC-hour analytics."""
    hour: int
    opportunity_count: int = 0
    total_competitors: int = 0
    total_tip_gwei: float = 0.0
    tip_count: int = 0
    total_liquidation_size_usd: float = 0.0
    total_profit_usd: float = 0.0

    @property
    def avg_competitors(self) -> float:
        return self.total_competitors / max(self.opportunity_count, 1)

    @property
    def avg_tip_gwei(self) -> float:
        return self.total_tip_gwei / max(self.tip_count, 1)

    @property
    def avg_liquidation_size_usd(self) -> float:
        return self.total_liquidation_size_usd / max(self.opportunity_count, 1)

    @property
    def avg_profit_usd(self) -> float:
        return self.total_profit_usd / max(self.opportunity_count, 1)


# ═══════════════════════════════════════════════════════════════
# Competition Analytics Engine
# ═══════════════════════════════════════════════════════════════

class CompetitionAnalyticsEngine:
    """Reads competition data from Redis and computes analytics."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        rpc_url: Optional[str] = None,
    ):
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None
        self._price_cache: Dict[str, float] = {}
        self._symbol_cache: Dict[str, str] = {}
        self._bonus_cache: Dict[str, int] = {}  # asset → bonus bps

    async def connect(self):
        """Connect to Redis."""
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        await self.redis.ping()
        await self._load_caches()
        logger.info("Analytics connected to Redis")

    async def _load_caches(self):
        """Load price, symbol, and bonus caches from Redis."""
        # Price cache from chainlink
        price_keys = await self.redis.keys("price:chainlink:*")
        for key in price_keys:
            symbol = key.replace("price:chainlink:", "")
            try:
                data = await self.redis.hgetall(key)
                price = int(data.get("price", "0")) / 1e8
                if price > 0:
                    self._price_cache[symbol] = price
            except Exception:
                pass
        logger.info("Loaded %d price feeds", len(self._price_cache))

        # Symbol cache from Aave reserves
        reserve_keys = await self.redis.keys("aave:reserve:*")
        for key in reserve_keys:
            addr = key.replace("aave:reserve:", "").lower()
            if ":" in addr:
                continue
            try:
                data = await self.redis.hgetall(key)
                symbol = data.get("symbol", "")
                if symbol:
                    self._symbol_cache[addr] = symbol
                    bonus = int(data.get("liquidationBonus", "0"))
                    if bonus > 0:
                        self._bonus_cache[addr] = bonus
            except Exception:
                pass
        logger.info("Loaded %d reserve symbols, %d bonus configs",
                   len(self._symbol_cache), len(self._bonus_cache))

    # ── Price Helpers ────────────────────────────────────────────

    def _get_price_usd(self, asset_address: str) -> float:
        """Look up USD price for an asset address using symbol resolution."""
        addr_lower = asset_address.lower()
        symbol = self._symbol_cache.get(addr_lower, "")
        if not symbol:
            # Try WETH→ETH, WBTC→BTC normalization
            raw_symbol = self._symbol_cache.get(addr_lower, "")
            symbol = {"WETH": "ETH", "WBTC": "BTC"}.get(raw_symbol, raw_symbol)
        return self._price_cache.get(symbol, 0.0)

    def _get_symbol(self, asset_address: str) -> str:
        return self._symbol_cache.get(asset_address.lower(), asset_address[:10])

    def _get_liquidation_bonus_pct(self, asset_address: str) -> float:
        """Return liquidation bonus as decimal (e.g., 0.05 for 5%)."""
        bonus_bps = self._bonus_cache.get(asset_address.lower(), DEFAULT_LIQUIDATION_BONUS_BPS)
        return (bonus_bps - 10000) / 10000

    def _debt_bucket(self, debt_usd: float) -> str:
        for low, high, label in DEBT_BUCKETS:
            if low <= debt_usd < high:
                return label
        return "> $1M"

    # ── Data Collection ──────────────────────────────────────────

    async def collect_resolved_opportunities(
        self, days: int = 30,
    ) -> List[ResolvedOpportunity]:
        """Collect all resolved opportunity records from Redis."""
        opportunities = []

        # Collect from comp:opportunity:* keys
        opp_keys = await self.redis.keys("comp:opportunity:*")
        cutoff = time.time() - (days * 86400)

        for key in opp_keys:
            try:
                data = await self.redis.hgetall(key)
                last_detected = float(data.get("last_detected_at", "0"))
                if last_detected < cutoff:
                    continue

                # Get the winning competitor — iterate to find which one won
                comp_count = int(data.get("competitor_count", "0"))
                winner_tx = ""
                winner_liquidator = ""
                winner_tip = 0.0
                winner_gas = 0
                winner_won = False
                for i in range(1, comp_count + 1):
                    won = data.get(f"comp_{i}_won", "0") == "1"
                    if won:
                        winner_tx = data.get(f"comp_{i}_tx", "")
                        winner_liquidator = data.get(f"comp_{i}_liquidator", "")
                        winner_tip = float(data.get(f"comp_{i}_tip_gwei", "0"))
                        winner_gas = int(data.get(f"comp_{i}_gas", "0"))
                        winner_won = True
                        break
                # Fallback: if no explicit winner, use last competitor (all reverted)
                if not winner_tx and comp_count > 0:
                    winner_tx = data.get(f"comp_{comp_count}_tx", "")
                    winner_liquidator = data.get(f"comp_{comp_count}_liquidator", "")
                    winner_tip = float(data.get(f"comp_{comp_count}_tip_gwei", "0"))
                    winner_gas = int(data.get(f"comp_{comp_count}_gas", "0"))

                # Our opportunity data
                our_debt_raw = int(data.get("our_debt", "0"))
                our_profit = float(data.get("our_profit_usd", "0"))
                our_ev = float(data.get("our_ev_usd", "0"))
                our_wp = float(data.get("our_wp", "0"))

                # Fetch detection data for the winner
                detection = {}
                if winner_tx:
                    detection = await self.redis.hgetall(f"comp:detection:{winner_tx}")

                borrower = data.get("borrower", "")
                coll_addr = detection.get("collateral_asset", "")
                debt_addr = detection.get("debt_asset", "")

                # USD values
                coll_price = self._get_price_usd(coll_addr)
                debt_price = self._get_price_usd(debt_addr)

                # Determine debt decimals from symbol
                debt_sym = self._get_symbol(debt_addr)
                debt_sym_normalized = {"WETH": "ETH", "WBTC": "BTC"}.get(debt_sym, debt_sym)
                if debt_sym_normalized in ("ETH", "BTC"):
                    debt_decimals = 18
                elif debt_sym_normalized in ("USDC", "USDT", "USDCe", "DAI", "FRAX", "LUSD", "GHO"):
                    debt_decimals = 6
                else:
                    debt_decimals = 18  # default 18 for most ERC20s

                debt_usd = (our_debt_raw / (10 ** debt_decimals)) * debt_price if our_debt_raw > 0 else 0.0

                # Liquidation bonus
                bonus_pct = self._get_liquidation_bonus_pct(coll_addr)
                bonus_usd = debt_usd * bonus_pct

                # Tip from detection
                tip_gwei = float(detection.get("tip_gwei", "0") or "0")
                gas_used = int(detection.get("gas_used", "0") or "0")
                eff_gas = int(detection.get("effective_gas_price", "0") or "0")
                block = int(detection.get("confirmation_block", "0") or "0")

                # Builder: check if liquidator is a known builder
                builder_name = KNOWN_BUILDERS.get(
                    winner_liquidator.lower(),
                    winner_liquidator[:10] if winner_liquidator else "unknown",
                )

                opp = ResolvedOpportunity(
                    opp_id=key.split(":")[-1],
                    tx_hash=winner_tx,
                    timestamp=last_detected,
                    block_number=block,
                    borrower=borrower,
                    collateral_asset=coll_addr,
                    collateral_symbol=self._get_symbol(coll_addr),
                    debt_asset=debt_addr,
                    debt_symbol=self._get_symbol(debt_addr),
                    debt_size=our_debt_raw,
                    debt_size_usd=debt_usd,
                    trigger_type=data.get("trigger", "unknown"),
                    competitor_count=comp_count,
                    winning_liquidator=winner_liquidator,
                    winning_builder=builder_name,
                    builder_tip_gwei=tip_gwei,
                    gas_used=gas_used,
                    effective_gas_price=eff_gas,
                    liquidation_bonus_usd=bonus_usd,
                    estimated_profit_usd=our_profit,
                    realized_profit_usd=our_profit if winner_won else 0.0,
                    our_would_have_won=not winner_won,  # if competitor won, we lost
                    detection_hour=int(time.strftime("%H", time.gmtime(last_detected))),
                )
                opportunities.append(opp)
            except Exception as e:
                logger.debug("Error collecting opp %s: %s", key, e)

        logger.info("Collected %d resolved opportunities (past %d days)",
                   len(opportunities), days)
        return opportunities

    # ── Analytics Computations ───────────────────────────────────

    async def compute_competitor_analytics(
        self, opportunities: List[ResolvedOpportunity],
    ) -> dict:
        """Compute competitor count analytics with breakdowns."""
        if not opportunities:
            return {"avg_competitors": 0, "distribution": {}, "by_trigger": {}}

        comps = [o.competitor_count for o in opportunities]
        avg = sum(comps) / len(comps)

        # Distribution
        dist = {
            "0": sum(1 for c in comps if c == 0),
            "1": sum(1 for c in comps if c == 1),
            "2": sum(1 for c in comps if c == 2),
            "3": sum(1 for c in comps if c == 3),
            "4+": sum(1 for c in comps if c >= 4),
        }
        total = len(comps)
        p_ge_1 = 1 - (dist["0"] / total) if total > 0 else 0
        p_ge_2 = (dist["2"] + dist["3"] + dist["4+"]) / total if total > 0 else 0
        p_ge_3 = (dist["3"] + dist["4+"]) / total if total > 0 else 0

        # By trigger
        by_trigger = defaultdict(list)
        for o in opportunities:
            by_trigger[o.trigger_type].append(o.competitor_count)
        trigger_stats = {
            t: {"avg": sum(vals) / len(vals), "count": len(vals)}
            for t, vals in by_trigger.items()
        }

        # By debt bucket
        by_bucket = defaultdict(list)
        for o in opportunities:
            bucket = self._debt_bucket(o.debt_size_usd)
            by_bucket[bucket].append(o.competitor_count)
        bucket_stats = {
            b: {"avg": sum(vals) / len(vals), "count": len(vals)}
            for b, vals in by_bucket.items()
        }

        return {
            "avg_competitors": round(avg, 2),
            "distribution": dist,
            "p_ge_1": round(p_ge_1, 4),
            "p_ge_2": round(p_ge_2, 4),
            "p_ge_3": round(p_ge_3, 4),
            "by_trigger": dict(trigger_stats),
            "by_debt_bucket": dict(bucket_stats),
        }

    async def compute_tip_analytics(
        self, opportunities: List[ResolvedOpportunity],
    ) -> dict:
        """Compute tip analytics: avg, median, 95th percentile, breakdowns."""
        tips = sorted([o.builder_tip_gwei for o in opportunities if o.builder_tip_gwei > 0])
        if not tips:
            return {"avg": 0, "median": 0, "p95": 0, "count": 0}

        n = len(tips)
        avg = sum(tips) / n
        median = tips[n // 2] if n % 2 == 1 else (tips[n // 2 - 1] + tips[n // 2]) / 2
        p95_idx = int(n * 0.95)
        p95 = tips[min(p95_idx, n - 1)]

        # Tip as % of bonus
        tip_pcts = []
        for o in opportunities:
            if o.builder_tip_gwei > 0 and o.liquidation_bonus_usd > 0:
                # Convert tip gwei to USD: tip_gwei * 1e-9 * gas_used * eth_price
                # For simplicity, tip in gwei relative to bonus
                tip_pcts.append(o.builder_tip_gwei)

        # By competitor count
        by_comp = defaultdict(list)
        for o in opportunities:
            if o.builder_tip_gwei > 0:
                by_comp[o.competitor_count].append(o.builder_tip_gwei)
        by_comp_stats = {}
        for c, vals in sorted(by_comp.items()):
            svals = sorted(vals)
            m = svals[len(svals) // 2] if svals else 0
            by_comp_stats[str(c)] = {
                "avg": round(sum(vals) / len(vals), 2),
                "median": round(m, 2),
                "count": len(vals),
            }

        # By builder
        by_builder = defaultdict(list)
        for o in opportunities:
            if o.builder_tip_gwei > 0:
                by_builder[o.winning_builder].append(o.builder_tip_gwei)
        builder_tip_stats = {}
        for b, vals in sorted(by_builder.items(), key=lambda x: -len(x[1])):
            svals = sorted(vals)
            m = svals[len(svals) // 2] if svals else 0
            builder_tip_stats[b] = {
                "avg": round(sum(vals) / len(vals), 2),
                "median": round(m, 2),
                "count": len(vals),
            }

        return {
            "avg": round(avg, 2),
            "median": round(median, 2),
            "p95": round(p95, 2),
            "count": n,
            "by_competitor_count": by_comp_stats,
            "by_builder": builder_tip_stats,
        }

    async def compute_asset_analytics(
        self, opportunities: List[ResolvedOpportunity],
    ) -> Dict[str, AssetAnalytics]:
        """Compute per-asset competition analytics."""
        assets: Dict[str, AssetAnalytics] = {}
        for o in opportunities:
            sym = o.collateral_symbol or o.collateral_asset[:10]
            if sym not in assets:
                assets[sym] = AssetAnalytics(symbol=sym)
            a = assets[sym]
            a.opportunity_count += 1
            a.total_competitors += o.competitor_count
            if o.builder_tip_gwei > 0:
                a.total_tip_gwei += o.builder_tip_gwei
                a.tip_count += 1
            a.total_liquidation_size_usd += o.debt_size_usd
            a.total_profit_usd += o.estimated_profit_usd
            if o.our_would_have_won:
                a.wins += 1
        return assets

    async def compute_builder_analytics(
        self, opportunities: List[ResolvedOpportunity],
    ) -> Dict[str, BuilderAnalytics]:
        """Compute per-builder/liquidator analytics."""
        builders: Dict[str, BuilderAnalytics] = {}
        for o in opportunities:
            addr = o.winning_liquidator or "unknown"
            name = o.winning_builder
            key = f"{name}:{addr[:10]}"
            if key not in builders:
                builders[key] = BuilderAnalytics(name=name, address=addr)
            b = builders[key]
            b.opportunities_won += 1
            if o.builder_tip_gwei > 0:
                b.total_tip_gwei += o.builder_tip_gwei
                b.tip_count += 1
            b.total_gas_used += o.gas_used
        return builders

    async def compute_hourly_analytics(
        self, opportunities: List[ResolvedOpportunity],
    ) -> Dict[int, HourlyAnalytics]:
        """Compute per-UTC-hour analytics."""
        hours: Dict[int, HourlyAnalytics] = {}
        for o in opportunities:
            h = o.detection_hour
            if h not in hours:
                hours[h] = HourlyAnalytics(hour=h)
            ha = hours[h]
            ha.opportunity_count += 1
            ha.total_competitors += o.competitor_count
            if o.builder_tip_gwei > 0:
                ha.total_tip_gwei += o.builder_tip_gwei
                ha.tip_count += 1
            ha.total_liquidation_size_usd += o.debt_size_usd
            ha.total_profit_usd += o.estimated_profit_usd
        return hours

    async def compute_bid_recommendations(
        self,
        opportunities: List[ResolvedOpportunity],
        tip_analytics: dict,
    ) -> dict:
        """Generate bid multiplier recommendations based on competition."""
        comp_analytics = await self.compute_competitor_analytics(opportunities)
        avg_comp = comp_analytics["avg_competitors"]
        p_ge_1 = comp_analytics["p_ge_1"]

        median_tip = tip_analytics["median"]
        p95_tip = tip_analytics["p95"]

        # Recommendations
        recommendations = {
            "baseline_tip_gwei": round(median_tip, 2),
            "competitive_tip_gwei": round(p95_tip, 2),
            "multiplier_by_competition": {},
        }

        for c in range(0, 5):
            base = max(median_tip, 0.05)
            multiplier = 1.0 + (c * 0.5)  # 50% more per competitor
            recommendations["multiplier_by_competition"][str(c)] = {
                "suggested_tip_gwei": round(base * multiplier, 2),
                "multiplier": round(multiplier, 2),
            }

        # Overall recommendation
        if p_ge_1 > 0.5:
            strategy = "AGGRESSIVE"
            suggested_mult = 2.0
        elif p_ge_1 > 0.25:
            strategy = "COMPETITIVE"
            suggested_mult = 1.5
        else:
            strategy = "CONSERVATIVE"
            suggested_mult = 1.0

        recommendations["strategy"] = strategy
        recommendations["suggested_multiplier"] = suggested_mult
        recommendations["avg_competitors"] = avg_comp
        recommendations["p_competition_ge_1"] = round(p_ge_1, 4)

        return recommendations

    # ── Report Generation ────────────────────────────────────────

    async def generate_daily_report(
        self, opportunities: List[ResolvedOpportunity],
    ) -> str:
        """Generate the full daily competition analytics report."""
        if not opportunities:
            return "No resolved opportunities available for analysis."

        comp = await self.compute_competitor_analytics(opportunities)
        tip = await self.compute_tip_analytics(opportunities)
        assets = await self.compute_asset_analytics(opportunities)
        builders = await self.compute_builder_analytics(opportunities)
        hours = await self.compute_hourly_analytics(opportunities)
        bids = await self.compute_bid_recommendations(opportunities, tip)

        lines = []
        lines.append("═" * 60)
        lines.append("  COMPETITION ANALYTICS v1 — DAILY REPORT")
        lines.append("═" * 60)
        lines.append(f"  Generated: {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}")
        lines.append(f"  Sample size: {len(opportunities)} resolved opportunities")
        lines.append("")

        # ── Section 1: Competition Summary ──
        lines.append("── 1. COMPETITION SUMMARY ──")
        lines.append(f"  Avg competitors per opportunity: {comp['avg_competitors']}")
        lines.append(f"  P(≥1 competitor): {comp['p_ge_1']:.2%}")
        lines.append(f"  P(≥2 competitors): {comp['p_ge_2']:.2%}")
        lines.append(f"  P(≥3 competitors): {comp['p_ge_3']:.2%}")
        lines.append("")
        lines.append("  Competitor Distribution:")
        for k, v in comp["distribution"].items():
            pct = (v / max(len(opportunities), 1)) * 100
            bar = "█" * int(pct / 5)
            lines.append(f"    {k:>3} comp: {v:>4} ({pct:5.1f}%) {bar}")
        lines.append("")

        # By trigger
        if comp.get("by_trigger"):
            lines.append("  By Trigger Type:")
            for t, stats in sorted(comp["by_trigger"].items()):
                lines.append(f"    {t:>12}: avg={stats['avg']:.2f} comps (n={stats['count']})")
            lines.append("")

        # By debt bucket
        if comp.get("by_debt_bucket"):
            lines.append("  By Debt Size:")
            for b, stats in sorted(comp["by_debt_bucket"].items()):
                lines.append(f"    {b:>15}: avg={stats['avg']:.2f} comps (n={stats['count']})")
            lines.append("")

        # ── Section 2: Winning Tip Analytics ──
        lines.append("── 2. WINNING TIP ANALYTICS ──")
        lines.append(f"  Average tip:        {tip['avg']:.2f} gwei")
        lines.append(f"  Median tip:         {tip['median']:.2f} gwei")
        lines.append(f"  95th percentile:    {tip['p95']:.2f} gwei")
        lines.append(f"  Tip samples:        {tip['count']}")
        lines.append("")

        if tip.get("by_competitor_count"):
            lines.append("  Tip by Competitor Count:")
            for c, stats in sorted(tip["by_competitor_count"].items()):
                lines.append(f"    {c} comps: median={stats['median']:.2f} gwei (n={stats['count']})")
            lines.append("")

        # ── Section 3: Asset-Specific Analytics ──
        lines.append("── 3. ASSET-SPECIFIC COMPETITION ──")
        ranked = sorted(assets.values(), key=lambda a: -a.opportunity_count)

        # Most competitive
        by_comp = sorted(assets.values(), key=lambda a: -a.avg_competitors)
        lines.append("  Most Competitive Assets:")
        for a in by_comp[:5]:
            lines.append(f"    {a.symbol:>6}: {a.avg_competitors:.2f} avg comps | "
                        f"{a.opportunity_count} opps | ${a.avg_liquidation_size_usd:,.0f} avg size")
        lines.append("")

        # Most profitable
        by_profit = sorted(assets.values(), key=lambda a: -a.total_profit_usd)
        lines.append("  Most Profitable Assets:")
        for a in by_profit[:5]:
            lines.append(f"    {a.symbol:>6}: ${a.total_profit_usd:,.0f} total | "
                        f"${a.avg_profit_usd:,.0f}/opp | {a.win_probability:.1%} win rate")
        lines.append("")

        # Full table
        lines.append("  Complete Asset Table:")
        lines.append(f"    {'Asset':<8} {'Opps':>5} {'AvgComp':>8} {'AvgTip':>8} {'AvgSize':>10} {'Prof/Opp':>10} {'WinRate':>8}")
        for a in ranked[:15]:
            lines.append(f"    {a.symbol:<8} {a.opportunity_count:>5} "
                        f"{a.avg_competitors:>8.2f} {a.avg_tip_gwei:>8.2f} "
                        f"${a.avg_liquidation_size_usd:>9,.0f} ${a.avg_profit_usd:>9,.0f} "
                        f"{a.win_probability:>7.1%}")
        lines.append("")

        # ── Section 4: Time-of-Day Analytics ──
        lines.append("── 4. TIME-OF-DAY ANALYTICS ──")

        # Competition heatmap
        lines.append("  Competition Heatmap (UTC):")
        max_opps = max((h.opportunity_count for h in hours.values()), default=1)
        for h in range(24):
            ha = hours.get(h, HourlyAnalytics(hour=h))
            bar_len = int((ha.opportunity_count / max(max_opps, 1)) * 30)
            bar = "█" * bar_len
            lines.append(f"    {h:02d}:00 | {bar:<30} | "
                        f"{ha.opportunity_count:>3} opps | "
                        f"{ha.avg_competitors:.1f} comps | "
                        f"tip={ha.avg_tip_gwei:.2f}")
        lines.append("")

        # Most/least competitive hours (exclude hours with no data)
        active_hours = [h for h in hours.values() if h.opportunity_count > 0]
        sorted_hours = sorted(active_hours, key=lambda h: -h.avg_competitors)
        if sorted_hours:
            most = sorted_hours[0]
            least = sorted_hours[-1]
            lines.append(f"  Most competitive: {most.hour:02d}:00 UTC — {most.avg_competitors:.2f} avg comps")
            lines.append(f"  Least competitive: {least.hour:02d}:00 UTC — {least.avg_competitors:.2f} avg comps")

        sorted_profit = sorted(active_hours, key=lambda h: -h.avg_profit_usd)
        if sorted_profit:
            most_p = sorted_profit[0]
            least_p = sorted_profit[-1]
            lines.append(f"  Most profitable: {most_p.hour:02d}:00 UTC — ${most_p.avg_profit_usd:,.0f}/opp")
            lines.append(f"  Least profitable: {least_p.hour:02d}:00 UTC — ${least_p.avg_profit_usd:,.0f}/opp")
        lines.append("")

        # ── Section 5: Builder/Liquidator Analytics ──
        lines.append("── 5. BUILDER / LIQUIDATOR ANALYTICS ──")
        ranked_builders = sorted(
            builders.values(), key=lambda b: -b.opportunities_won
        )
        total_wins = sum(b.opportunities_won for b in builders.values())
        lines.append(f"    {'Builder':<25} {'Wins':>6} {'Share':>8} {'AvgTip':>8} {'AvgGas':>10}")
        for b in ranked_builders[:10]:
            share = (b.opportunities_won / max(total_wins, 1)) * 100
            lines.append(f"    {b.name:<25} {b.opportunities_won:>6} "
                        f"{share:>7.1f}% {b.avg_tip_gwei:>8.2f} "
                        f"{b.avg_gas_used:>10,}")
        lines.append("")

        # ── Section 6: Bid Recommendations ──
        lines.append("── 6. BID RECOMMENDATIONS ──")
        lines.append(f"  Strategy: {bids['strategy']}")
        lines.append(f"  Baseline tip: {bids['baseline_tip_gwei']:.2f} gwei")
        lines.append(f"  Competitive tip: {bids['competitive_tip_gwei']:.2f} gwei")
        lines.append(f"  Suggested multiplier: {bids['suggested_multiplier']}x")
        lines.append("")
        lines.append("  By Competition Level:")
        for c, rec in sorted(bids.get("multiplier_by_competition", {}).items()):
            lines.append(f"    {c} comps: {rec['suggested_tip_gwei']:.2f} gwei ({rec['multiplier']}x)")
        lines.append("")
        lines.append("═" * 60)

        return "\n".join(lines)

    async def export_csv(
        self, opportunities: List[ResolvedOpportunity], path: str,
    ):
        """Export all resolved opportunities to CSV."""
        fieldnames = [
            "opp_id", "tx_hash", "timestamp", "block_number", "borrower",
            "collateral_asset", "collateral_symbol", "debt_asset", "debt_symbol",
            "debt_size", "debt_size_usd", "trigger_type", "competitor_count",
            "winning_liquidator", "winning_builder", "builder_tip_gwei",
            "gas_used", "effective_gas_price", "liquidation_bonus_usd",
            "estimated_profit_usd", "realized_profit_usd", "our_would_have_won",
            "detection_hour",
        ]
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for o in opportunities:
                writer.writerow({
                    "opp_id": o.opp_id,
                    "tx_hash": o.tx_hash,
                    "timestamp": o.timestamp,
                    "block_number": o.block_number,
                    "borrower": o.borrower,
                    "collateral_asset": o.collateral_asset,
                    "collateral_symbol": o.collateral_symbol,
                    "debt_asset": o.debt_asset,
                    "debt_symbol": o.debt_symbol,
                    "debt_size": o.debt_size,
                    "debt_size_usd": o.debt_size_usd,
                    "trigger_type": o.trigger_type,
                    "competitor_count": o.competitor_count,
                    "winning_liquidator": o.winning_liquidator,
                    "winning_builder": o.winning_builder,
                    "builder_tip_gwei": o.builder_tip_gwei,
                    "gas_used": o.gas_used,
                    "effective_gas_price": o.effective_gas_price,
                    "liquidation_bonus_usd": o.liquidation_bonus_usd,
                    "estimated_profit_usd": o.estimated_profit_usd,
                    "realized_profit_usd": o.realized_profit_usd,
                    "our_would_have_won": o.our_would_have_won,
                    "detection_hour": o.detection_hour,
                })
        logger.info("Exported %d records to %s", len(opportunities), path)


# ═══════════════════════════════════════════════════════════════
# CLI Entry Point
# ═══════════════════════════════════════════════════════════════

async def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Competition Analytics v1 — analyze and report on liquidation competition"
    )
    parser.add_argument("--redis", default="redis://localhost:6379")
    parser.add_argument("--rpc", default="")
    parser.add_argument("--report", nargs="?", const="full", default=None,
                       choices=["full", "daily"],
                       help="Generate report (full or daily) and exit")
    parser.add_argument("--days", type=int, default=30,
                       help="Lookback window in days (default: 30)")
    parser.add_argument("--export-csv", type=str, default="",
                       help="Export resolved opportunities to CSV file")
    args = parser.parse_args()

    engine = CompetitionAnalyticsEngine(
        redis_url=args.redis,
        rpc_url=args.rpc or os.getenv("QUICKNODE_HTTP_URL", os.getenv("ARBITRUM_HTTP_URL", "")),
    )
    await engine.connect()

    opportunities = await engine.collect_resolved_opportunities(days=args.days)

    if args.export_csv:
        await engine.export_csv(opportunities, args.export_csv)

    if args.report:
        report = await engine.generate_daily_report(opportunities)
        print(report)

    await engine.redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
