"""
services/liq_forecast.py — Liquidation Forecast Engine.

Predicts which Aave borrowers are closest to liquidation by computing
"distance-to-liquidation" — the price drop percentage that would push
each borrower's health factor below 1.0.

Cross-references Chainlink feed heartbeats to flag borrowers whose
collateral assets may see a price update imminently, enabling
pre-computation of liquidation calldata.

Architecture:
  1. Load all users with debt from Redis (Aave indexer)
  2. For each: compute liquidation thresholds per collateral asset
  3. Rank by buffer % (smallest = closest to liquidation)
  4. Cross-reference Chainlink heartbeat data (from oracle service)
  5. Output ranked dashboard with urgency scores

Redis reads:
  aave:user:{addr}           — positions, health_factor
  aave:reserve:{addr}         — config, current price
  price:chainlink:{sym}       — heartbeat, updated_at
  price:cex:{pair}            — live CEX prices
  aave:liquidatable           — currently liquidatable

Redis writes:
  forecast:ranking            ZSET   score=buffer_pct, member=user
  forecast:users:{addr}       HASH   alert_price, buffer_pct, urgency
  forecast:alerts             STREAM recent high-urgency alerts

Usage:
  python -m services.liq_forecast                    # one-shot dashboard
  python -m services.liq_forecast --watch --interval 30  # continuous
  python -m services.liq_forecast --alert-threshold 5   # alert if buffer < 5%
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import redis.asyncio as redis
from dotenv import load_dotenv
load_dotenv(dotenv_path=project_root / ".env")


# ────────────────────────────────────────────────────────────────
# Symbol / address mapping
# ────────────────────────────────────────────────────────────────

ADDR_TO_SYMBOL: Dict[str, str] = {
    "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": "ETH",
    "0xaf88d065e77c8cc2239327c5edb3a432268e5831": "USDC",
    "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": "USDT",
    "0x912ce59144191c1204e64559fe8253a0e49e6548": "ARB",
    "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f": "WBTC",
    "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1": "DAI",
    "0xf97f4df75117a78c1a5a0dbb814af92458539fb4": "LINK",
    "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8": "USDC.e",
    "0x6c84a8f1c29108f47a79964b5fe888d4f4d0de40": "tBTC",
    "0x4186bfc76e2e237523cbc30fd220fe055156b41f": "rsETH",
}

# ────────────────────────────────────────────────────────────────
# Data classes
# ────────────────────────────────────────────────────────────────

@dataclass
class BorrowerRisk:
    """A borrower ranked by distance to liquidation."""
    user: str
    health_factor: float
    total_debt_usd: float
    total_coll_usd: float             # risk-adjusted (× liq_threshold)
    raw_coll_usd: float               # unadjusted
    buffer_pct: float                 # price drop needed to reach HF=1
    liquidation_prices: Dict[str, float]  # per-asset trigger prices
    debt_assets: List[str]            # what they owe
    coll_assets: List[str]            # what they posted
    chainlink_risk: bool              # True if any collateral feed near heartbeat
    chainlink_symbols: List[str]      # which feeds are close to update
    urgency: str                      # critical/warning/elevated/safe


@dataclass
class ForecastResult:
    """Complete forecast output."""
    total_users: int
    users_with_debt: int
    currently_liquidatable: int
    high_risk: int                    # buffer < 5%
    medium_risk: int                  # buffer 5-15%
    low_risk: int                     # buffer > 15%
    rankings: List[BorrowerRisk] = field(default_factory=list)
    chainlink_alerts: List[dict] = field(default_factory=list)
    elapsed_ms: float = 0.0


# ────────────────────────────────────────────────────────────────
# Forecast Engine
# ────────────────────────────────────────────────────────────────

class LiquidationForecast:
    """Predicts which borrowers are closest to liquidation."""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None

        # Cached state
        self.reserve_configs: Dict[str, dict] = {}
        self.chainlink_data: Dict[str, dict] = {}
        self.users: Dict[str, dict] = {}

    async def connect(self):
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        await self.redis.ping()

    # ── State loading ───────────────────────────────────────────

    async def _load_reserves(self):
        """Load reserve configs and Chainlink data."""
        keys = await self.redis.keys("aave:reserve:*")
        for key in keys:
            addr = key.replace("aave:reserve:", "")
            if ":" in addr:
                continue
            data = await self.redis.hgetall(key)
            if data and "symbol" in data:
                self.reserve_configs[addr] = data

        # Chainlink data
        for sym in ["ETH", "BTC", "WBTC", "LINK", "ARB", "USDC", "USDT", "DAI"]:
            data = await self.redis.hgetall(f"price:chainlink:{sym}")
            if data:
                self.chainlink_data[sym] = data
            # Also try price:meta for fallback
            meta = await self.redis.hgetall(f"price:meta:{sym}")
            if meta and sym not in self.chainlink_data:
                self.chainlink_data[sym] = meta

    async def _load_users(self) -> int:
        """Load all users with debt positions using pipeline batching."""
        user_keys_set = set()
        for rk in self.reserve_configs:
            users = await self.redis.smembers(f"aave:reserve:{rk}:users")
            user_keys_set.update(users)

        user_list = list(user_keys_set)
        count = 0
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
                # Validate health factor — HF <= 0 means stale/unreconciled
                if not hf_raw:
                    continue
                try:
                    hf = float(hf_raw)
                except (ValueError, TypeError):
                    continue
                if hf <= 0 or hf == float("inf"):
                    continue
                try:
                    if isinstance(pos_raw, bytes):
                        pos_raw = pos_raw.decode()
                    positions = json.loads(pos_raw)
                except (json.JSONDecodeError, TypeError):
                    continue
                if not positions:
                    continue
                # Only track users with debt AND enabled collateral
                has_debt = any(v.get("debt", 0) > 0 for v in positions.values())
                if not has_debt:
                    continue
                has_collateral_enabled = any(
                    v.get("collateral", 0) > 0 and v.get("is_collateral", False)
                    for v in positions.values()
                )
                if not has_collateral_enabled:
                    continue
                self.users[ua] = {
                    "positions": positions,
                    "health_factor": hf,
                }
                count += 1

        return count

    # ── Risk computation ────────────────────────────────────────

    def _compute_risk(self, user_addr: str, user_data: dict) -> Optional[BorrowerRisk]:
        """Compute distance-to-liquidation for a single borrower."""
        positions = user_data["positions"]
        hf = user_data["health_factor"]

        # Calculate current exposure
        total_debt_usd = 0.0
        total_coll_adj_usd = 0.0   # risk-adjusted (× liq_threshold)
        total_coll_raw_usd = 0.0
        debt_assets = []
        coll_assets = []
        coll_breakdown = []  # (symbol, coll_usd_raw, liq_threshold_bps, current_price)

        for reserve_addr, pos in positions.items():
            config = self.reserve_configs.get(reserve_addr)
            if not config or "decimals" not in config:
                continue

            decimals = int(config["decimals"])
            price_raw = config.get("price", "0")
            if not price_raw or price_raw == "0":
                continue
            price = int(price_raw) / 1e8
            symbol = config.get("symbol", ADDR_TO_SYMBOL.get(reserve_addr, "???"))

            debt = pos.get("debt", 0)
            coll = pos.get("collateral", 0)
            is_coll = pos.get("is_collateral", False)

            if debt > 0:
                debt_usd = (debt / (10 ** decimals)) * price
                total_debt_usd += debt_usd
                if symbol not in debt_assets:
                    debt_assets.append(symbol)

            if coll > 0 and is_coll:
                coll_usd = (coll / (10 ** decimals)) * price
                total_coll_raw_usd += coll_usd
                liq_threshold = int(config.get("liquidation_threshold", "8500"))
                total_coll_adj_usd += coll_usd * (liq_threshold / 10000)
                if symbol not in coll_assets:
                    coll_assets.append(symbol)
                coll_breakdown.append((symbol, coll_usd, liq_threshold, price))

        if total_debt_usd <= 0:
            return None

        # Buffer: derived from the *reconciled* chain health factor.
        # Aave's getUserAccountData() uses per-asset LT weighting that
        # our simple total_coll_adj/total_debt cannot replicate.
        # For a uniform collateral price drop X: HF' = HF × (1 - X)
        # Setting HF' = 1.0 gives: X = (HF - 1) / HF
        if hf > 0 and hf != float("inf"):
            buffer_pct = ((hf - 1.0) / hf) * 100
        elif hf == float("inf"):
            buffer_pct = 100.0  # infinite HF → effectively zero risk
        else:
            buffer_pct = -100.0  # HF ≤ 0 → already liquidatable

        # Per-asset liquidation prices: what price pushes HF → 1.0?
        #   HF > 1:  liq_price = current_price / hf   (price must drop to this)
        #   HF ≤ 1:  already underwater, show current price (liquidation is live)
        #   HF = inf: effectively no risk
        liq_prices: Dict[str, float] = {}
        for sym, coll_raw_usd, liq_thresh, current_price in coll_breakdown:
            if hf > 1.0 and hf != float("inf"):
                liq_prices[sym] = round(current_price / hf, 2)
            elif 0 < hf <= 1.0:
                liq_prices[sym] = round(current_price, 2)  # already liquidatable
            else:
                liq_prices[sym] = 0.0

        # Chainlink risk: are any collateral feeds close to heartbeat expiry?
        chainlink_risk = False
        chainlink_symbols = []
        now = int(time.time())
        for sym in coll_assets:
            cl = self.chainlink_data.get(sym, {})
            if cl:
                heartbeat = int(cl.get("heartbeat", "3600"))
                updated = int(cl.get("updated_at", "0"))
                if updated > 0 and (now - updated) > heartbeat * 0.8:
                    chainlink_risk = True
                    chainlink_symbols.append(sym)

        # Urgency
        if buffer_pct < 2:
            urgency = "critical"
        elif buffer_pct < 5:
            urgency = "warning"
        elif buffer_pct < 15:
            urgency = "elevated"
        else:
            urgency = "safe"

        return BorrowerRisk(
            user=user_addr,
            health_factor=hf,
            total_debt_usd=total_debt_usd,
            total_coll_usd=total_coll_adj_usd,
            raw_coll_usd=total_coll_raw_usd,
            buffer_pct=max(buffer_pct, -100.0),
            liquidation_prices=liq_prices,
            debt_assets=debt_assets,
            coll_assets=coll_assets,
            chainlink_risk=chainlink_risk,
            chainlink_symbols=chainlink_symbols,
            urgency=urgency,
        )

    # ── Main forecast ───────────────────────────────────────────

    async def forecast(self, alert_threshold: float = 5.0) -> ForecastResult:
        """Run full forecast."""
        t0 = time.monotonic()

        await self._load_reserves()
        user_count = await self._load_users()

        currently_liq = await self.redis.zcard("aave:liquidatable")

        # Compute risk for all users
        rankings = []
        high_risk = 0
        medium_risk = 0
        low_risk = 0

        for ua, data in self.users.items():
            risk = self._compute_risk(ua, data)
            if risk is None:
                continue
            rankings.append(risk)

            if risk.buffer_pct < 5:
                high_risk += 1
            elif risk.buffer_pct < 15:
                medium_risk += 1
            else:
                low_risk += 1

        # Sort: smallest buffer (most at risk) first
        rankings.sort(key=lambda r: r.buffer_pct)

        # Write to Redis for persistent access
        await self._write_rankings(rankings, alert_threshold)

        # Chainlink alerts
        chainlink_alerts = []
        for sym, data in self.chainlink_data.items():
            heartbeat = int(data.get("heartbeat", "3600"))
            updated = int(data.get("updated_at", "0"))
            now = int(time.time())
            if updated > 0:
                age = now - updated
                if age > heartbeat * 0.8:
                    chainlink_alerts.append({
                        "symbol": sym,
                        "age_seconds": age,
                        "heartbeat": heartbeat,
                        "pct_elapsed": round(age / heartbeat * 100, 1),
                    })

        elapsed = (time.monotonic() - t0) * 1000

        return ForecastResult(
            total_users=len(self.users),
            users_with_debt=user_count,
            currently_liquidatable=currently_liq,
            high_risk=high_risk,
            medium_risk=medium_risk,
            low_risk=low_risk,
            rankings=rankings,
            chainlink_alerts=chainlink_alerts,
            elapsed_ms=elapsed,
        )

    async def _write_rankings(self, rankings: List[BorrowerRisk], alert_threshold: float):
        """Write risk rankings to Redis for other services."""
        pipe = self.redis.pipeline()
        # Clear old rankings before rebuilding — avoids stale entries
        pipe.delete("forecast:ranking")
        # ZSET: score = buffer_pct (lower = more at risk)
        for r in rankings:
            pipe.zadd("forecast:ranking", {r.user: r.buffer_pct})
        # Expire after 10 minutes as safety net
        pipe.expire("forecast:ranking", 600)
        await pipe.execute()

        # Also emit high-risk alerts to event bus
        for r in rankings:
            if r.buffer_pct < alert_threshold:
                ts = int(time.time() * 1000)
                await self.redis.xadd("arb:events:system", {
                    "id": f"evt_{ts}",
                    "ts": str(ts),
                    "source": "liq_forecast",
                    "type": "risk.forecast",
                    "severity": r.urgency,
                    "block": "0",
                    "payload": json.dumps({
                        "user": r.user,
                        "buffer_pct": round(r.buffer_pct, 2),
                        "health_factor": round(r.health_factor, 4),
                        "debt_usd": round(r.total_debt_usd, 2),
                        "coll_usd": round(r.raw_coll_usd, 2),
                        "urgency": r.urgency,
                        "chainlink_risk": r.chainlink_risk,
                    }),
                }, maxlen=10000, approximate=True)

    # ── Output formatting ───────────────────────────────────────

    def print_dashboard(self, result: ForecastResult, top_n: int = 20):
        """Print a ranked dashboard."""
        print(f"\n{'='*80}")
        print(f"  LIQUIDATION FORECAST — {time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print(f"{'='*80}")

        # Summary
        print(f"\n  Users: {result.users_with_debt:,} with debt | "
              f"{result.currently_liquidatable:,} currently liquidatable")
        print(f"  Risk:  {result.high_risk} critical (<5%) | "
              f"{result.medium_risk} elevated (5-15%) | "
              f"{result.low_risk} safe (>15%)")

        # Chainlink alerts
        if result.chainlink_alerts:
            print(f"\n  ⚡ CHAINLINK FEEDS NEARING UPDATE:")
            for cl in result.chainlink_alerts:
                print(f"     {cl['symbol']}: {cl['age_seconds']}s since update "
                      f"({cl['pct_elapsed']:.0f}% of {cl['heartbeat']}s heartbeat)")

        # Rankings
        if not result.rankings:
            print("\n  No borrowers with debt found.")
            return

        print(f"\n  {'TOP'} {min(top_n, len(result.rankings))} BORROWERS BY RISK:")
        print(f"  {'Rank':<5} {'User':<18} {'HF':>8} {'Debt $':>10} {'Coll $':>10} "
              f"{'Buffer':>8} {'Urgency':<10} {'CL Risk':<8} {'Liq Prices'}")
        print(f"  {'-'*5} {'-'*18} {'-'*8} {'-'*10} {'-'*10} "
              f"{'-'*8} {'-'*10} {'-'*8} {'-'*20}")

        for i, r in enumerate(result.rankings[:top_n]):
            cl_risk = "⚡ YES" if r.chainlink_risk else "—"
            urgency_mark = {"critical": "🔴", "warning": "🟡", "elevated": "🟠", "safe": "🟢"}.get(r.urgency, "—")
            liq_prices_str = ", ".join(f"{s}=${p:,.0f}" for s, p in list(r.liquidation_prices.items())[:3])

            print(f"  {i+1:<5} {r.user[:16]:<18} {r.health_factor:>8.3f} "
                  f"${r.total_debt_usd:>9,.0f} ${r.raw_coll_usd:>9,.0f} "
                  f"{r.buffer_pct:>7.1f}% {urgency_mark} {r.urgency:<7} {cl_risk:<8} {liq_prices_str}")

        print(f"\n  Forecast completed in {result.elapsed_ms:.0f}ms")
        print(f"  Rankings written to Redis: ZSET forecast:ranking (TTL 10min)")

        # Highlight most at-risk asset
        if result.rankings:
            assets_at_risk: Dict[str, int] = {}
            for r in result.rankings[:50]:
                for sym in r.coll_assets:
                    assets_at_risk[sym] = assets_at_risk.get(sym, 0) + 1
            top_assets = sorted(assets_at_risk.items(), key=lambda x: -x[1])[:5]
            if top_assets:
                print(f"\n  Most exposed assets: {', '.join(f'{s}({c})' for s, c in top_assets)}")

    # ── Watch loop ──────────────────────────────────────────────

    async def watch(self, interval: float = 30.0, alert_threshold: float = 5.0):
        """Continuously update forecasts."""
        print(f"Watching — updating every {interval:.0f}s (Ctrl+C to stop)")
        while True:
            try:
                # Reload fresh data each cycle
                self.users.clear()
                self.reserve_configs.clear()
                self.chainlink_data.clear()

                result = await self.forecast(alert_threshold=alert_threshold)
                self.print_dashboard(result, top_n=15)
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Forecast error: {e}")
                await asyncio.sleep(interval)


# ─── CLI ──────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(description="Liquidation Forecast Engine")
    parser.add_argument("--redis", default="redis://localhost:6379")
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval", type=float, default=30.0)
    parser.add_argument("--alert-threshold", type=float, default=5.0,
                       help="Alert if buffer below this pct")
    parser.add_argument("--top", type=int, default=20)
    args = parser.parse_args()

    engine = LiquidationForecast(redis_url=args.redis)
    await engine.connect()

    if args.watch:
        await engine.watch(interval=args.interval, alert_threshold=args.alert_threshold)
    else:
        result = await engine.forecast(alert_threshold=args.alert_threshold)
        engine.print_dashboard(result, top_n=args.top)

    await engine.redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
