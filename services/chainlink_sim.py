"""
services/chainlink_sim.py — Chainlink price impact simulator.

Simulates the effect of Chainlink oracle price updates on Aave V3 borrower
health factors. Identifies which positions become liquidatable and estimates
profitability — enabling pre-computation of liquidation calldata before the
oracle update lands on-chain.

Architecture:
  1. Pull full state from Redis (Aave indexer + oracle service)
  2. Apply price shocks (percentage or absolute)
  3. Recalculate health factors for affected users
  4. Estimate profit for newly-liquidatable positions
  5. Rank by profitability, output ranked table

Modes:
  --shock ETH=-5%          Single asset shock
  --shock ETH=-5%,LINK=+10%  Multi-asset scenario
  --scenario crash          Named scenario (crash, pump, correlation)
  --watch                   Daemon mode — pre-compute on every Chainlink update

Redis reads:
  aave:user:{addr}         — positions, health_factor
  aave:reserve:{addr}      — config, current price
  aave:liquidatable        — current liquidatable set

Output:
  Ranked table of profitable liquidation opportunities with estimated
  profit, gas cost, and net return.

Usage:
  python -m services.chainlink_sim --shock ETH=-5%
  python -m services.chainlink_sim --scenario crash
  python -m services.chainlink_sim --shock ETH=-3%,BTC=-2% --min-profit 50
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
# Named scenarios
# ────────────────────────────────────────────────────────────────

SCENARIOS: Dict[str, Dict[str, float]] = {
    "crash": {      # Broad market crash — ETH drops 8%, alts drop 12-15%
        "ETH": -8.0, "WBTC": -6.0, "ARB": -15.0, "LINK": -12.0,
    },
    "pump": {       # ETH rally — everything up
        "ETH": +5.0, "WBTC": +4.0, "ARB": +8.0, "LINK": +6.0,
    },
    "alt_crash": {  # Altcoins crash, majors stable
        "ARB": -20.0, "LINK": -18.0,
    },
    "btc_dump": {   # BTC-specific crash
        "WBTC": -10.0,
    },
    "correlation": {  # ETH and LINK both drop (correlated liquidations)
        "ETH": -5.0, "LINK": -10.0,
    },
    "liquidation_cascade": {  # Worst case — everything down 15%+
        "ETH": -12.0, "WBTC": -10.0, "ARB": -25.0, "LINK": -20.0,
        "USDC": 0.0, "USDT": 0.0, "DAI": 0.0,  # stables don't move
    },
}

# Symbol → Aave asset address (lowercase)
SYMBOL_TO_ADDR: Dict[str, str] = {
    "ETH":   "0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
    "USDC":  "0xaf88d065e77c8cc2239327c5edb3a432268e5831",
    "USDT":  "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
    "ARB":   "0x912ce59144191c1204e64559fe8253a0e49e6548",
    "BTC":  "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",  # alias for WBTC
    "WBTC": "0x2f2a2543b76a4166549f7aab2e75bef0aefc5b0f",
    "DAI":   "0xda10009cbd5d07dd0cecc66161fc93d7c9000da1",
    "LINK":  "0xf97f4df75117a78c1a5a0dbb814af92458539fb4",
    "USDCe": "0xff970a61a04b1ca14834a43f5de4533ebddb5cc8",
    "tBTC":  "0x6c84a8f1c29108f47a79964b5fe888d4f4d0de40",
    "rsETH": "0x4186bfc76e2e237523cbc30fd220fe055156b41f",
}

ADDR_TO_SYMBOL = {v: k for k, v in SYMBOL_TO_ADDR.items()}


# ────────────────────────────────────────────────────────────────
# Data classes
# ────────────────────────────────────────────────────────────────

@dataclass
class SimResult:
    """Result of a simulated price shock."""
    scenario: str
    shocks: Dict[str, float]           # applied shocks
    total_users: int
    affected_users: int                 # users with positions in shocked assets
    newly_liquidatable: int             # became liquidatable after shock
    already_liquidatable: int           # were liquidatable before shock
    opportunities: List[LiqOpportunity] = field(default_factory=list)
    elapsed_ms: float = 0.0


@dataclass
class LiqOpportunity:
    """A profitable liquidation opportunity."""
    user: str
    debt_asset: str
    debt_symbol: str
    debt_usd: float
    coll_asset: str
    coll_symbol: str
    coll_usd: float
    health_factor_before: float
    health_factor_after: float
    gross_profit_usd: float             # bonus portion of collateral
    gas_cost_usd: float                 # estimated L2 gas cost
    net_profit_usd: float
    profit_rank: int                    # 1 = best


# ────────────────────────────────────────────────────────────────
# Core simulator
# ────────────────────────────────────────────────────────────────

class ChainlinkSimulator:
    """Simulates Chainlink price updates over Aave indexer state."""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None
        self.reserve_configs: Dict[str, dict] = {}
        self.users: Dict[str, dict] = {}  # addr → {positions, hf, emode}

        # Load indexer's HF calculator
        sys.path.insert(0, str(project_root))
        from indexers.aave_indexer import AaveIndexer
        self._indexer = AaveIndexer(rpc_url="http://noop", redis_url=redis_url)

    async def connect(self):
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        await self.redis.ping()

    async def load_state(self):
        """Load full state from Redis using pipeline batches (fast)."""
        t0 = time.monotonic()

        # Load reserve configs
        config_keys = await self.redis.keys("aave:reserve:*")
        for key in config_keys:
            addr = key.replace("aave:reserve:", "")
            if ":" in addr:  # skip reserve:*:users keys
                continue
            data = await self.redis.hgetall(key)
            if data and "symbol" in data:
                self.reserve_configs[addr] = data

        # Set in-memory configs on the indexer instance for HF calc
        self._indexer._reserve_configs = dict(self.reserve_configs)

        # Load users from reserve user sets (much faster than scanning 124K user:* keys)
        user_keys_set = set()
        for rk in self.reserve_configs:
            users = await self.redis.smembers(f"aave:reserve:{rk}:users")
            user_keys_set.update(users)

        # Batch-fetch positions + HF in pipeline groups of 200
        user_list = list(user_keys_set)
        user_count = 0
        position_count = 0
        batch_size = 200

        for i in range(0, len(user_list), batch_size):
            batch = user_list[i:i + batch_size]
            pipe = self.redis.pipeline()
            for ua in batch:
                pipe.hget(f"aave:user:{ua}", "positions")
                pipe.hget(f"aave:user:{ua}", "health_factor")
                pipe.hget(f"aave:user:{ua}", "eMode")
            results = await pipe.execute()

            for j, ua in enumerate(batch):
                pos_raw = results[j * 3]
                hf_raw = results[j * 3 + 1]
                emode_raw = results[j * 3 + 2]
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
                self.users[ua] = {
                    "positions": positions,
                    "health_factor": float(hf_raw) if hf_raw and hf_raw != "inf" else float("inf"),
                    "emode": emode_raw or "0",
                }
                user_count += 1
                for r, pos in positions.items():
                    if pos.get("debt", 0) > 0 or pos.get("collateral", 0) > 0:
                        position_count += 1

        elapsed = (time.monotonic() - t0) * 1000
        print(f"Loaded {len(self.reserve_configs)} reserves, {user_count} users, "
              f"{position_count} position entries in {elapsed:.0f}ms")
        return user_count

    def parse_shocks(self, shock_str: str) -> Dict[str, float]:
        """Parse 'ETH=-5%,LINK=+10%' into {symbol: pct_change}."""
        shocks = {}
        for part in shock_str.split(","):
            part = part.strip()
            if not part:
                continue
            if "=" not in part:
                print(f"Invalid shock format: {part} (expected SYM=PCT%)")
                continue
            sym, val = part.split("=", 1)
            sym = sym.strip().upper()
            val = val.strip().rstrip("%")
            try:
                shocks[sym] = float(val)
            except ValueError:
                print(f"Invalid percentage: {val}")
        return shocks

    async def simulate(self, shocks: Dict[str, float], min_profit_usd: float = 25.0) -> SimResult:
        """
        Apply price shocks and compute liquidation opportunities.

        Args:
            shocks: e.g. {"ETH": -5.0, "LINK": +10.0}
            min_profit_usd: minimum net profit to include

        Returns:
            SimResult with ranked opportunities
        """
        t0 = time.monotonic()

        # Map symbols to reserve addrs for quick lookup
        shocked_addrs = set()
        for sym in shocks:
            addr = SYMBOL_TO_ADDR.get(sym)
            if addr:
                shocked_addrs.add(addr)

        # Track state
        already_liquidatable = 0
        newly_liquidatable = 0
        affected_users = 0
        opportunities: List[LiqOpportunity] = []

        # Save original prices
        original_prices: Dict[str, str] = {}
        for addr in shocked_addrs:
            if addr in self.reserve_configs:
                original_prices[addr] = self.reserve_configs[addr].get("price", "0")

        # Apply shocks to reserve configs
        for sym, pct in shocks.items():
            addr = SYMBOL_TO_ADDR.get(sym)
            if not addr or addr not in self.reserve_configs:
                print(f"  ⚠ Unknown asset: {sym}")
                continue
            old_price = int(self.reserve_configs[addr].get("price", "0"))
            if old_price == 0:
                continue
            new_price = int(old_price * (1 + pct / 100.0))
            self.reserve_configs[addr]["price"] = str(new_price)
            # Update indexer's copy too
            if addr in self._indexer._reserve_configs:
                self._indexer._reserve_configs[addr]["price"] = str(new_price)

        # Recalculate HF for affected users
        for user_addr, user_data in self.users.items():
            positions = user_data["positions"]
            # Check if user has exposure to any shocked asset
            is_affected = any(
                reserve_addr in shocked_addrs
                for reserve_addr in positions
            )
            if not is_affected:
                continue

            affected_users += 1
            old_hf = user_data["health_factor"]

            # Compute new HF
            new_hf = self._indexer._compute_health_factor(positions)

            if old_hf < 1.0:
                already_liquidatable += 1

            if new_hf < 1.0 and old_hf >= 1.0:
                newly_liquidatable += 1

            # If liquidatable after shock, estimate profit
            if new_hf < 1.0:
                opp = self._estimate_profit(user_addr, positions, old_hf, new_hf, min_profit_usd)
                if opp:
                    opportunities.append(opp)

        # Restore original prices
        for addr, price in original_prices.items():
            self.reserve_configs[addr]["price"] = price
            if addr in self._indexer._reserve_configs:
                self._indexer._reserve_configs[addr]["price"] = price

        # Rank by profit
        opportunities.sort(key=lambda o: o.net_profit_usd, reverse=True)
        for i, opp in enumerate(opportunities):
            opp.profit_rank = i + 1

        elapsed = (time.monotonic() - t0) * 1000
        return SimResult(
            scenario="custom",
            shocks=shocks,
            total_users=len(self.users),
            affected_users=affected_users,
            newly_liquidatable=newly_liquidatable,
            already_liquidatable=already_liquidatable,
            opportunities=opportunities,
            elapsed_ms=elapsed,
        )

    def _estimate_profit(
        self, user_addr: str, positions: dict, old_hf: float, new_hf: float,
        min_profit_usd: float = 25.0,
    ) -> Optional[LiqOpportunity]:
        """Estimate liquidation profit for a borrower."""
        # Find best debt/collateral pair
        best_debt = None
        best_debt_reserve = None
        best_coll = None
        best_coll_reserve = None

        for reserve_addr, pos in positions.items():
            debt = pos.get("debt", 0)
            coll = pos.get("collateral", 0)
            is_coll = pos.get("is_collateral", False)

            if debt > 0 and (best_debt is None or debt > best_debt):
                best_debt = debt
                best_debt_reserve = reserve_addr
            if coll > 0 and is_coll and (best_coll is None or coll > best_coll):
                best_coll = coll
                best_coll_reserve = reserve_addr

        if not best_debt_reserve or not best_coll_reserve:
            return None

        debt_cfg = self.reserve_configs.get(best_debt_reserve, {})
        coll_cfg = self.reserve_configs.get(best_coll_reserve, {})

        if not debt_cfg or not coll_cfg:
            return None

        debt_decimals = int(debt_cfg.get("decimals", "18"))
        coll_decimals = int(coll_cfg.get("decimals", "18"))
        debt_price = float(debt_cfg.get("price", "0")) / 1e8
        coll_price = float(coll_cfg.get("price", "0")) / 1e8
        bonus_bps = int(coll_cfg.get("liquidation_bonus", "10500"))

        if debt_price <= 0 or coll_price <= 0:
            return None

        # 50% close factor
        debt_to_cover = best_debt // 2
        debt_to_cover_usd = (debt_to_cover / (10 ** debt_decimals)) * debt_price

        # Collateral seized = debt_covered * (1 + bonus/10000) / collateral_price
        bonus_mult = 1 + (bonus_bps / 10000)
        coll_seized = int((debt_to_cover_usd * bonus_mult) / coll_price * (10 ** coll_decimals))
        coll_seized_usd = (coll_seized / (10 ** coll_decimals)) * coll_price

        # Gas: 500K gas @ 0.1 gwei (Arbitrum L2)
        gas_cost_eth = 500_000 * 0.1e-9
        gas_cost_usd = gas_cost_eth * coll_price  # approximate

        gross_profit = coll_seized_usd - debt_to_cover_usd
        net_profit = gross_profit - gas_cost_usd

        if net_profit < min_profit_usd:
            return None

        return LiqOpportunity(
            user=user_addr,
            debt_asset=best_debt_reserve,
            debt_symbol=debt_cfg.get("symbol", "???"),
            debt_usd=debt_to_cover_usd,
            coll_asset=best_coll_reserve,
            coll_symbol=coll_cfg.get("symbol", "???"),
            coll_usd=coll_seized_usd,
            health_factor_before=old_hf,
            health_factor_after=new_hf,
            gross_profit_usd=gross_profit,
            gas_cost_usd=gas_cost_usd,
            net_profit_usd=net_profit,
            profit_rank=0,
        )

    # ── Output ──────────────────────────────────────────────────

    def print_result(self, result: SimResult):
        """Pretty-print simulation results."""
        print(f"\n{'='*70}")
        print(f"Chainlink Impact Simulation — {len(result.shocks)} asset(s)")
        print(f"{'='*70}")

        # Shock summary
        for sym, pct in sorted(result.shocks.items()):
            direction = "📉" if pct < 0 else "📈"
            print(f"  {direction} {sym}: {pct:+.1f}%")

        print(f"\n  Users: {result.total_users:,} total | "
              f"{result.affected_users:,} affected | "
              f"{result.elapsed_ms:.0f}ms elapsed")

        print(f"  {result.already_liquidatable:,} already liquidatable | "
              f"{result.newly_liquidatable:,} NEWLY liquidatable after shock")

        if not result.opportunities:
            print("\n  No profitable liquidation opportunities found.")
            return

        print(f"\n  Top {min(20, len(result.opportunities))} Profitable Opportunities:")
        print(f"  {'Rank':<5} {'User':<18} {'Debt':>10} {'Debt $':>10} {'Coll':>8} {'Coll $':>10} {'HF→':>10} {'Net $':>10}")
        print(f"  {'-'*5} {'-'*18} {'-'*10} {'-'*10} {'-'*8} {'-'*10} {'-'*10} {'-'*10}")

        for opp in result.opportunities[:20]:
            print(f"  {opp.profit_rank:<5} {opp.user[:16]:<18} "
                  f"{opp.debt_symbol:>10} {opp.debt_usd:>10,.0f} "
                  f"{opp.coll_symbol:>8} {opp.coll_usd:>10,.0f} "
                  f"{opp.health_factor_before:.3f}→{opp.health_factor_after:.3f} "
                  f"${opp.net_profit_usd:>9,.0f}")

        total_profit = sum(o.net_profit_usd for o in result.opportunities)
        print(f"\n  Total potential profit: ${total_profit:,.0f} "
              f"(across {len(result.opportunities)} opportunities)")


# ─── CLI ──────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser(
        description="Chainlink price impact simulator for Aave V3 liquidations"
    )
    parser.add_argument("--redis", default="redis://localhost:6379")
    parser.add_argument("--shock", default=None,
                       help="Price shocks, e.g. 'ETH=-5%,LINK=+10%'")
    parser.add_argument("--scenario", default=None,
                       choices=list(SCENARIOS.keys()),
                       help="Named scenario")
    parser.add_argument("--min-profit", type=float, default=25.0,
                       help="Minimum net profit in USD to report")
    parser.add_argument("--top", type=int, default=20,
                       help="Number of top opportunities to display")
    parser.add_argument("--watch", action="store_true",
                       help="Daemon mode — run on every price change")
    args = parser.parse_args()

    if not args.shock and not args.scenario and not args.watch:
        parser.print_help()
        print("\nExamples:")
        print("  python -m services.chainlink_sim --shock ETH=-5%")
        print("  python -m services.chainlink_sim --scenario crash")
        print("  python -m services.chainlink_sim --shock 'ETH=-3%,BTC=-2%' --min-profit 50")
        return

    sim = ChainlinkSimulator(redis_url=args.redis)
    await sim.connect()
    total = await sim.load_state()

    if total == 0:
        print("No user data in Redis — is the Aave indexer backfill complete?")
        return

    # Determine shocks
    if args.scenario:
        shocks = SCENARIOS[args.scenario]
        print(f"Scenario: {args.scenario}")
    elif args.shock:
        shocks = sim.parse_shocks(args.shock)
    else:
        shocks = {}

    if not shocks:
        print("No valid shocks specified.")
        return

    # Run simulation
    result = await sim.simulate(shocks, min_profit_usd=args.min_profit)
    sim.print_result(result)

    # Watch mode
    if args.watch:
        print("\nWatching for price changes... (Ctrl+C to stop)")
        # Monitor price:meta:{sym} for changes
        last_prices = {}
        for sym in shocks:
            data = await sim.redis.hgetall(f"price:meta:{sym}")
            last_prices[sym] = data.get("last_update", "0")

        while True:
            await asyncio.sleep(5)
            changed = False
            for sym in shocks:
                data = await sim.redis.hgetall(f"price:meta:{sym}")
                current = data.get("last_update", "0")
                if current != last_prices[sym]:
                    changed = True
                    last_prices[sym] = current
            if changed:
                print(f"\n[{time.strftime('%H:%M:%S')}] Price change detected, re-simulating...")
                result = await sim.simulate(shocks, min_profit_usd=args.min_profit)
                sim.print_result(result)

    await sim.redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
