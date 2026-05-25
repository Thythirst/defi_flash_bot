"""
scanner/backtest_engine.py — V3-only cross-pool arbitrage backtest.

Fetches Swap events from multiple pools, builds per-block snapshots,
and simulates sequential-leg flash-loan arbitrage using the exact
EVM integer math from scanner.v3_math.

Usage:
    python -m scanner.backtest_engine --strategy WETH_USDC_CROSS_FEE --blocks 10000
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from scanner.config_loader import load_config, Strategy, Pool
from scanner.multi_fetcher import MultiPoolFetcher, BlockSnapshot, PoolState
from scanner.v3_math import compute_v3_inline_swap, compute_max_single_tick_input, SwapResult
from scanner.v3_math_multitick import (
    compute_v3_multi_tick_swap,
    TickLiquidityMap,
    MultiTickSwapResult,
)
from scanner.gas import ArbitrumGasEstimator

logger = logging.getLogger("backtest_engine")

BPS_DENOM = 10000


@dataclass
class SimulationConfig:
    loan_size_wei: int
    min_profit_threshold_wei: int
    slippage_bps: int
    flash_premium_bps: int
    max_tick_cross: int
    enforce_single_tick: bool = True


@dataclass
class ArbitrageResult:
    block: int
    strategy: str
    pool_a: str
    pool_b: str
    leg0_out: int
    leg1_out: int
    gross_profit: int
    premium: int
    gas_cost: int
    net_profit: int
    price_impact_bps_a: int
    price_impact_bps_b: int
    crossed_tick_a: bool
    crossed_tick_b: bool


class ArbitrageBacktest:
    """
    Simulates two-leg sequential arbitrage across two V3/Algebra pools.

    Leg 0: borrow token0 → swap on pool A for token1
    Leg 1: swap token1 on pool B → token0, repay flash loan
    """

    def __init__(self, strategy: Strategy, sim: SimulationConfig):
        self.strategy = strategy
        self.sim = sim
        self.results: List[ArbitrageResult] = []
        self.stats = defaultdict(int)

        if len(strategy.pools) < 2:
            raise ValueError(f"Strategy {strategy.key} needs at least 2 pools")

        self.pool_a = strategy.pools[0]
        self.pool_b = strategy.pools[1]

        # Logging guard to avoid spamming logs about pool setup
        logger.info(
            "Backtest engine initialized: strategy=%s | pool_a=%s | pool_b=%s | loan=%s wei",
            strategy.key,
            self.pool_a.name,
            self.pool_b.name,
            sim.loan_size_wei,
        )

    def _fee_for_pool(self, pool: Pool) -> int:
        """
        Return effective fee tier.
        For Algebra (Camelot) dynamic-fee pools, we use the 0.05% (500)
        placeholder because the backtest only has snapshot data, not live
        fee queries.  A robust pipeline would fetch the fee from slot0().
        """
        if pool.pool_type == "concentrated_algebra" and pool.fee_tier is None:
            # Conservative mid-range assumption for Camelot dynamic fees
            return 500
        if pool.fee_tier is None:
            raise ValueError(f"Pool {pool.name} has no fee_tier and is not Algebra")
        return pool.fee_tier

    def _simulate_block(self, block: int, snap: BlockSnapshot) -> Optional[ArbitrageResult]:
        self.stats["blocks_scanned"] += 1

        # Require both pools to have state at this block
        if self.pool_a.name not in snap.pool_states:
            self.stats["missing_pool_a"] += 1
            return None
        if self.pool_b.name not in snap.pool_states:
            self.stats["missing_pool_b"] += 1
            return None

        state_a = snap.pool_states[self.pool_a.name]
        state_b = snap.pool_states[self.pool_b.name]

        # === Leg 0: token0 → token1 on pool A ===
        fee_a = self._fee_for_pool(self.pool_a)
        zero_for_one_a = self.pool_a.direction_for_swap0  # True if token0 → token1

        result_a = compute_v3_inline_swap(
            amount_in=self.sim.loan_size_wei,
            pool_liquidity=state_a.liquidity,
            current_tick=state_a.tick,
            fee_tier=fee_a,
            zero_for_one=zero_for_one_a,
            enforce_single_tick=self.sim.enforce_single_tick,
        )

        self.stats["leg0_attempts"] += 1
        if result_a.out_of_range:
            self.stats["leg0_failed"] += 1
            return None

        # ── Multi-tick fallback ──
        if result_a.crossed_tick:
            self.stats["leg0_crossed_single_tick"] += 1
            tick_map_a = TickLiquidityMap.from_constant_liquidity(
                state_a.tick, state_a.liquidity,
                tick_spacing=10, num_ticks_each_side=100
            )
            result_a = compute_v3_multi_tick_swap(
                amount_in=self.sim.loan_size_wei,
                current_tick=state_a.tick,
                current_liquidity=state_a.liquidity,
                fee_tier=fee_a,
                zero_for_one=zero_for_one_a,
                tick_map=tick_map_a,
            )
            if result_a.amount_out == 0:
                self.stats["leg0_failed_multi_tick"] += 1
                return None
            self.stats["leg0_multi_tick_success"] += 1
        elif result_a.amount_out == 0:
            self.stats["leg0_failed"] += 1
            return None

        # === Leg 1: token1 → token0 on pool B ===
        fee_b = self._fee_for_pool(self.pool_b)
        # Direction flips: if pool A was selling token0, pool B must sell token1
        zero_for_one_b = not self.pool_a.direction_for_swap0

        result_b = compute_v3_inline_swap(
            amount_in=result_a.amount_out,
            pool_liquidity=state_b.liquidity,
            current_tick=state_b.tick,
            fee_tier=fee_b,
            zero_for_one=zero_for_one_b,
            enforce_single_tick=self.sim.enforce_single_tick,
        )

        self.stats["leg1_attempts"] += 1
        if result_b.out_of_range:
            self.stats["leg1_failed"] += 1
            return None

        # ── Multi-tick fallback for leg 1 ──
        if result_b.crossed_tick:
            self.stats["leg1_crossed_single_tick"] += 1
            tick_map_b = TickLiquidityMap.from_constant_liquidity(
                state_b.tick, state_b.liquidity,
                tick_spacing=10, num_ticks_each_side=100
            )
            result_b = compute_v3_multi_tick_swap(
                amount_in=result_a.amount_out,
                current_tick=state_b.tick,
                current_liquidity=state_b.liquidity,
                fee_tier=fee_b,
                zero_for_one=zero_for_one_b,
                tick_map=tick_map_b,
            )
            if result_b.amount_out == 0:
                self.stats["leg1_failed_multi_tick"] += 1
                return None
            self.stats["leg1_multi_tick_success"] += 1
        elif result_b.amount_out == 0:
            self.stats["leg1_failed"] += 1
            return None

        gross_profit = result_b.amount_out - self.sim.loan_size_wei
        if gross_profit < self.sim.min_profit_threshold_wei:
            self.stats["below_min_profit"] += 1
            return None

        # Flash loan premium
        premium = (self.sim.loan_size_wei * self.sim.flash_premium_bps) // BPS_DENOM

        # Gas cost (placeholder — real estimation needs historical gas per block)
        # Arbitrum: ~$0.02–$0.05 per flash-loan tx at current L2 gas prices
        gas_cost = int(0.0002 * 10**18)  # ~0.0002 ETH placeholder

        net_profit = gross_profit - premium - gas_cost
        if net_profit < self.sim.min_profit_threshold_wei:
            self.stats["net_profit_rejected"] += 1
            return None

        self.stats["opportunities"] += 1
        self.stats["total_net_profit"] += net_profit
        self.stats["total_gross_profit"] += gross_profit

        return ArbitrageResult(
            block=block,
            strategy=self.strategy.key,
            pool_a=self.pool_a.name,
            pool_b=self.pool_b.name,
            leg0_out=getattr(result_a, 'amount_out', 0),
            leg1_out=getattr(result_b, 'amount_out', 0),
            gross_profit=gross_profit,
            premium=premium,
            gas_cost=gas_cost,
            net_profit=net_profit,
            price_impact_bps_a=getattr(result_a, 'price_shift_bps', 0),
            price_impact_bps_b=getattr(result_b, 'price_shift_bps', 0),
            crossed_tick_a=getattr(result_a, 'crossed_tick', False) or getattr(result_a, 'ticks_crossed', 0) > 0,
            crossed_tick_b=getattr(result_b, 'crossed_tick', False) or getattr(result_b, 'ticks_crossed', 0) > 0,
        )

    def run(self, snapshots: Dict[int, BlockSnapshot]) -> None:
        logger.info(
            "Running backtest '%s' over %d blocks (single-tick=%s, multi-tick fallback=%s)",
            self.strategy.key,
            len(snapshots),
            self.sim.enforce_single_tick,
            "True",
        )
        # Iterate in block order
        for block_num in sorted(snapshots):
            snap = snapshots[block_num]
            result = self._simulate_block(block_num, snap)
            if result:
                self.results.append(result)
                logger.info(
                    "Opp block=%d gross=%d net=%d bps_leg0=%d bps_leg1=%d",
                    block_num,
                    result.gross_profit,
                    result.net_profit,
                    result.price_impact_bps_a,
                    result.price_impact_bps_b,
                )

    def print_summary(self) -> None:
        logger.info("=" * 70)
        logger.info("BACKTEST SUMMARY: %s", self.strategy.key)
        logger.info("=" * 70)
        for key, val in sorted(self.stats.items()):
            logger.info("  %-30s %s", key + ":", val)

        if not self.results:
            logger.info("No profitable opportunities found.")
            return

        total_net = sum(r.net_profit for r in self.results)
        total_gross = sum(r.gross_profit for r in self.results)
        avg_net = total_net // len(self.results)
        avg_gross = total_gross // len(self.results)
        best = max(self.results, key=lambda x: x.net_profit)
        worst = min(self.results, key=lambda x: x.net_profit)

        logger.info("Blocks scanned:    %d", self.stats["blocks_scanned"])
        logger.info("Opportunities:     %d", len(self.results))
        logger.info("Total gross:       %d wei (%.6f ETH)", total_gross, total_gross / 1e18)
        logger.info("Total net:         %d wei (%.6f ETH)", total_net, total_net / 1e18)
        logger.info("Avg gross/opp:     %d wei", avg_gross)
        logger.info("Avg net/opp:       %d wei", avg_net)
        logger.info("Best block:        %d  net=%d wei", best.block, best.net_profit)
        logger.info("Worst block:       %d  net=%d wei", worst.block, worst.net_profit)
        logger.info("=" * 70)

    def save_csv(self, path: str) -> None:
        if not self.results:
            logger.info("No results to save.")
            return
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.results[0].__dataclass_fields__)
            writer.writeheader()
            for r in self.results:
                writer.writerow(r.__dict__)
        logger.info("CSV saved to %s", path)

    def save_json(self, path: str) -> None:
        if not self.results:
            logger.info("No results to save.")
            return
        Path(path).write_text(
            json.dumps([r.__dict__ for r in self.results], indent=2)
        )
        logger.info("JSON saved to %s", path)


async def main():
    parser = argparse.ArgumentParser(description="V3 Cross-Pool Arbitrage Backtest")
    parser.add_argument("--config", default="pairs.yaml", help="Path to pairs.yaml")
    parser.add_argument(
        "--strategy",
        default="WETH_USDC_CROSS_FEE",
        help="Strategy key in pairs.yaml (e.g. WETH_USDC_CROSS_FEE)",
    )
    parser.add_argument("--from-block", type=int, default=None)
    parser.add_argument("--to-block", type=int, default=None)
    parser.add_argument("--rpc-url", default=None, help="Override RPC URL")
    parser.add_argument("--out-csv", default=None)
    parser.add_argument("--out-json", default=None)
    parser.add_argument("--blocks", type=int, default=None, help="Shortcut: scan N blocks from pairs.yaml backtest.from_block")
    args = parser.parse_args()

    # ── Load config ──────────────────────────────────────────────────
    cfg = load_config(args.config)
    strategy = cfg.strategies.get(args.strategy)
    if not strategy:
        raise SystemExit(f"Strategy '{args.strategy}' not found in {args.config}")
    if not strategy.enabled:
        raise SystemExit(f"Strategy '{args.strategy}' is disabled in config.")

    # ── Resolve block range ──────────────────────────────────────────
    from_block = args.from_block or cfg.backtest.from_block
    to_block = args.to_block or cfg.backtest.to_block
    if args.blocks and from_block:
        to_block = from_block + args.blocks - 1
    if not from_block or not to_block:
        raise SystemExit("from_block and to_block required (CLI or YAML backtest section)")

    # ── Resolve RPC ──────────────────────────────────────────────────
    rpc_url = args.rpc_url or cfg.rpc.resolved_http_url()
    if "${" in rpc_url:
        raise SystemExit(
            f"RPC URL contains unresolved env var: {rpc_url}. "
            "Set ALCHEMY_HTTP_URL in your environment."
        )

    sim = SimulationConfig(
        loan_size_wei=strategy.loan_size_wei,
        min_profit_threshold_wei=strategy.min_profit_threshold_wei,
        slippage_bps=strategy.slippage_tolerance_bps,
        flash_premium_bps=strategy.flash_loan_premium_bps,
        max_tick_cross=strategy.max_tick_cross_allowed,
        enforce_single_tick=(strategy.max_tick_cross_allowed == 0),
    )

    logger.info(
        "Strategy: %s | Loan: %s wei | Min profit: %s wei | Block range: %d-%d",
        strategy.key,
        sim.loan_size_wei,
        sim.min_profit_threshold_wei,
        from_block,
        to_block,
    )

    # ── Fetch snapshots ──────────────────────────────────────────────
    pool_addrs = [p.address for p in strategy.pools]
    pool_names = [p.name for p in strategy.pools]

    async with MultiPoolFetcher(
        rpc_url=rpc_url,
        pool_addresses=pool_addrs,
        pool_names=pool_names,
        max_concurrent=cfg.rpc.max_concurrent_requests,
        chunk_size=cfg.rpc.chunk_size,
        delay_ms=cfg.rpc.request_delay_ms,
        cache_dir=cfg.rpc.cache_dir,
        max_retries=cfg.rpc.max_retries,
        backoff_base_ms=cfg.rpc.backoff_base_ms,
    ) as fetcher:
        snapshots = await fetcher.fetch_snapshots(from_block, to_block)

    # ── Run backtest ─────────────────────────────────────────────────
    engine = ArbitrageBacktest(strategy, sim)
    engine.run(snapshots)
    engine.print_summary()

    # ── Save results ─────────────────────────────────────────────────
    ts = f"{from_block}_{to_block}"
    out_csv = args.out_csv or f"backtest_{strategy.key}_{ts}.csv"
    out_json = args.out_json or f"backtest_{strategy.key}_{ts}.json"
    engine.save_csv(out_csv)
    engine.save_json(out_json)


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    asyncio.run(main())
