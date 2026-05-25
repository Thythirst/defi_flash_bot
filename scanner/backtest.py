"""
WETH/ARB spatial backtest engine: SushiSwap V2 vs Uniswap V3 on Arbitrum.

Usage:
    python -m scanner.backtest --pair-name WETH_ARB_SPATIAL_ARB

Flow:
1. Load config (pairs.yaml → specific pair section, e.g. WETH_ARB_SPATIAL_ARB).
2. Fetch historical pool states via async LogFetcher (cached).
3. For each block, simulate the two-leg flash loan:
       token0 → token1 on DEX A  (SushiSwap V2)
       token1 → token0 on DEX B  (Uniswap V3)
4. Subtract flash-loan premium + gas (Arbitrum Nitro model).
5. Report aggregated statistics and save detailed results.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from .fetcher import LogFetcher, BlockSnapshot
from .v3_math import compute_v3_inline_swap, SwapResult
from .gas import ArbitrumGasEstimator

logger = logging.getLogger("backtest")


def load_config(path: str, pair_name: str = "WETH_ARB_SPATIAL_ARB") -> dict:
    """
    Load pair-specific config from pairs.yaml.
    Supports both:
      1. Legacy top-level key (e.g. WETH_ARB_SPATIAL_ARB)
      2. New nested strategies section
    Falls back to legacy 'backtest:' section for block-range / RPC defaults.
    """
    raw = yaml.safe_load(Path(path).read_text())

    # Try legacy top-level key first
    pair_cfg = raw.get(pair_name, {})
    # If empty, try the new strategies section
    if not pair_cfg and "strategies" in raw:
        strategies = raw["strategies"]
        if pair_name in strategies:
            sraw = strategies[pair_name]
            # Flatten the nested pool list into legacy field names
            pools = sraw.get("pools", [])
            v3_pool = None
            v2_pool = None
            v3_fee = None
            for p in pools:
                if p.get("type") == "concentrated_v3" and v3_pool is None:
                    v3_pool = p["address"]
                    v3_fee = p.get("fee_tier", 500)
                if p.get("type") == "v2" and v2_pool is None:
                    v2_pool = p["address"]
            pair_cfg = {
                "token0": sraw.get("pair", ["", ""])[0] if sraw.get("pair") else "",
                "token1": sraw.get("pair", ["", ""])[1] if sraw.get("pair") else "",
                "v3_pool": v3_pool,
                "v3_fee_tier": v3_fee,
                "v2_pool": v2_pool,
                "loan_size_wei": sraw.get("loan_size_wei"),
                "min_profit_threshold_wei": sraw.get("min_profit_threshold_wei"),
                "slippage_tolerance_bps": sraw.get("slippage_tolerance_bps"),
            }

    legacy = raw.get("backtest", {})

    def _val(key: str):
        """Return first non-None from pair section, then legacy backtest, then None."""
        if key in pair_cfg and pair_cfg[key] is not None:
            return pair_cfg[key]
        if key in legacy and legacy[key] is not None:
            return legacy[key]
        return None

    # Normalise field names so the engine stays agnostic.
    token0 = pair_cfg.get("token0", legacy.get("asset"))
    token1 = pair_cfg.get("token1", legacy.get("intermediate"))

    # Backwards-compatibility: old config used 'asset'/'intermediate'
    if token0 is None:
        token0 = pair_cfg.get("asset")
    if token1 is None:
        token1 = pair_cfg.get("intermediate")

    v3_pool = pair_cfg.get("v3_pool", legacy.get("uniswap_v3_pool"))
    v3_fee = pair_cfg.get("v3_fee_tier", legacy.get("uniswap_v3_fee", 500))
    v2_pool = pair_cfg.get("v2_pool", legacy.get("sushiswap_v2_pool"))
    v2_fee = pair_cfg.get("v2_fee_bps", legacy.get("sushiswap_v2_fee", 30))

    # Loan / strategy parameters
    loan_size_wei = int(pair_cfg.get("loan_size_wei", legacy.get("amount_in", 0)))
    min_profit = int(pair_cfg.get("min_profit_threshold_wei", legacy.get("min_profit_threshold_wei", 10**15)))
    slippage_bps = int(pair_cfg.get("slippage_tolerance_bps", legacy.get("slippage_bps", 50)))

    # Block range defaults to CLI overrides, but we inherit legacy backtest window here
    from_block = legacy.get("from_block")
    to_block = legacy.get("to_block")

    # Fetcher tuning defaults
    chunk_size = legacy.get("blocks_per_batch", 2000)
    max_concurrent = legacy.get("max_concurrent_requests", 3)
    delay_ms = legacy.get("request_delay_ms", 250)
    cache_dir = legacy.get("cache_dir", "~/.defi_flash_bot/cache")
    rpc_url = legacy.get("rpc_url")

    return {
        "pair_name": pair_name,
        "asset": token0,
        "intermediate": token1,
        "uniswap_v3_pool": v3_pool,
        "uniswap_v3_fee": int(v3_fee) if v3_fee else 500,
        "sushiswap_v2_pool": v2_pool,
        "sushiswap_v2_fee": int(v2_fee) if v2_fee else 30,
        "amount_in": loan_size_wei,
        "min_profit_threshold_wei": min_profit,
        "slippage_bps": slippage_bps,
        "from_block": from_block,
        "to_block": to_block,
        "chunk_size": chunk_size,
        "max_concurrent_requests": max_concurrent,
        "request_delay_ms": delay_ms,
        "cache_dir": cache_dir,
        "rpc_url": rpc_url,
        "flash_loan_premium_bps": legacy.get("flash_loan_premium_bps", 5),
        "assume_private_mempool": legacy.get("assume_private_mempool", True),
    }


def v2_get_amount_out(amount_in: int, reserve_in: int, reserve_out: int, fee_bps: int = 30) -> int:
    """Constant-product AMM with fee (e.g. 30 bps for SushiSwap V2)."""
    amount_in_with_fee = amount_in * (10000 - fee_bps)
    numerator = amount_in_with_fee * reserve_out
    denominator = reserve_in * 10000 + amount_in_with_fee
    return numerator // denominator


class SpatialBacktest:
    """
    Generic V2-vs-V3 spatial arbitrage backtest.

    All pool / fee parameters are injected from the YAML config so the same
    engine can run WETH/USDC 0.05 %, WETH/ARB 0.05 %, or any other pair without
    code changes.
    """

    def __init__(self, config: dict):
        self.cfg = config
        self.pair_name = str(config["pair_name"])
        self.asset = config["asset"]
        self.intermediate = config["intermediate"]
        self.amount_in = int(config["amount_in"])
        self.v3_pool = config["uniswap_v3_pool"]
        self.v3_fee = int(config["uniswap_v3_fee"])
        self.v2_pool = config["sushiswap_v2_pool"]
        self.v2_fee = int(config["sushiswap_v2_fee"])
        self.from_block = int(config["from_block"])
        self.to_block = int(config["to_block"])
        self.flash_premium_bps = int(config.get("flash_loan_premium_bps", 5))
        self.min_profit_wei = int(config.get("min_profit_threshold_wei", 10**15))
        self.slippage_bps = int(config.get("slippage_bps", 50))
        self.assume_private = bool(config.get("assume_private_mempool", True))

        self.gas_estimator = ArbitrumGasEstimator(safety_multiplier=1.3)
        self.results: List[dict] = []
        self.opportunity_count = 0
        self.total_gross_profit = 0
        self.total_net_profit = 0

    def _simulate_block(self, snap: BlockSnapshot) -> Optional[dict]:
        """Simulate one block. Returns result dict if profitable, else None."""
        if snap.v2_reserve0 is None or snap.v2_reserve1 is None:
            return None
        if snap.v3_sqrt_price_x96 is None or snap.v3_liquidity is None:
            return None

        # Determine token ordering in V2 pool.
        # We assume token0=asset, token1=intermediate for the configured pair.
        # If your pool is reversed, invert reserve logic.
        v2_reserve_asset = snap.v2_reserve0
        v2_reserve_intermediate = snap.v2_reserve1

        # Leg 1: Swap asset → intermediate on SushiSwap V2
        intermediate_from_v2 = v2_get_amount_out(
            self.amount_in, v2_reserve_asset, v2_reserve_intermediate, self.v2_fee
        )

        # Leg 2: Swap intermediate → asset on Uniswap V3
        # In V3 pool: if asset is token0, then selling intermediate is zero_for_one=False
        zero_for_one = False  # selling intermediate (token1) for asset (token0)
        result_v3: SwapResult = compute_v3_inline_swap(
            amount_in=intermediate_from_v2,
            pool_liquidity=snap.v3_liquidity,
            current_tick=snap.v3_tick,
            fee_tier=self.v3_fee,
            zero_for_one=zero_for_one,
            enforce_single_tick=True,
        )

        # Single-tick HARD STOP: if the swap would cross a tick boundary, abort.
        if result_v3.amount_out == 0 or result_v3.crossed_tick or result_v3.out_of_range:
            return None

        asset_from_v3 = result_v3.amount_out

        # Gross profit in asset terms
        gross_profit = asset_from_v3 - self.amount_in
        if gross_profit < self.min_profit_wei:
            return None

        # Flash loan premium (Aave V3 typical on Arbitrum)
        premium = (self.amount_in * self.flash_premium_bps) // 10000

        # Gas cost (synthetic placeholder — real gas estimation requires tx structure)
        # For backtesting we use a conservative flat estimate since we don't have
        # the exact tx shape per block. In production, substitute with historical gas.
        gas_cost = int(0.0002 * 10**18)  # ~$0.40 at $2000 ETH placeholder

        net_profit = gross_profit - premium - gas_cost
        if net_profit < self.min_profit_wei:
            return None

        return {
            "block": snap.block_number,
            "v2_reserve_asset": v2_reserve_asset,
            "v2_reserve_intermediate": v2_reserve_intermediate,
            "v3_sqrt_price_x96": snap.v3_sqrt_price_x96,
            "v3_liquidity": snap.v3_liquidity,
            "intermediate_from_v2": intermediate_from_v2,
            "asset_from_v3": asset_from_v3,
            "gross_profit": gross_profit,
            "premium": premium,
            "gas_cost": gas_cost,
            "net_profit": net_profit,
        }

    # ------------------------------------------------------------------
    # Core loop — kept synchronous because snapshots are already in memory.
    # For 100 K blocks this is sub-second on modern hardware.
    # ------------------------------------------------------------------
    def run(self, snapshots: Dict[int, BlockSnapshot]) -> None:
        logger.info(
            "Running backtest '%s' over %d blocks (V3 fee=%s bps, V2 fee=%s bps)",
            self.pair_name,
            len(snapshots),
            self.v3_fee,
            self.v2_fee,
        )
        for block_num in range(self.from_block, self.to_block + 1):
            snap = snapshots.get(block_num)
            if not snap:
                continue
            result = self._simulate_block(snap)
            if result:
                self.results.append(result)
                self.opportunity_count += 1
                self.total_gross_profit += result["gross_profit"]
                self.total_net_profit += result["net_profit"]
                logger.info(
                    "Opp block=%d gross=%s net=%s",
                    block_num,
                    result["gross_profit"],
                    result["net_profit"],
                )

        self._print_summary()

    def _print_summary(self) -> None:
        if not self.results:
            logger.info("No profitable opportunities found in the backtest window.")
            return

        avg_gross = self.total_gross_profit // self.opportunity_count
        avg_net = self.total_net_profit // self.opportunity_count
        best = max(self.results, key=lambda x: x["net_profit"])
        worst = min(self.results, key=lambda x: x["net_profit"])

        logger.info("=" * 60)
        logger.info("BACKTEST SUMMARY: %s", self.pair_name)
        logger.info("=" * 60)
        logger.info("Blocks scanned:   %d", self.to_block - self.from_block + 1)
        logger.info("Opportunities:    %d", self.opportunity_count)
        logger.info("Total gross:      %s wei (%.6f asset)", self.total_gross_profit, self.total_gross_profit / 1e18)
        logger.info("Total net:        %s wei (%.6f asset)", self.total_net_profit, self.total_net_profit / 1e18)
        logger.info("Avg gross/opp:    %s wei", avg_gross)
        logger.info("Avg net/opp:      %s wei", avg_net)
        logger.info("Best block:       %d  net=%s wei", best["block"], best["net_profit"])
        logger.info("Worst block:      %d  net=%s wei", worst["block"], worst["net_profit"])
        logger.info("=" * 60)

    def save_csv(self, path: str) -> None:
        if not self.results:
            return
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.results[0].keys())
            writer.writeheader()
            writer.writerows(self.results)
        logger.info("Results saved to %s", path)

    def save_json(self, path: str) -> None:
        if not self.results:
            return
        Path(path).write_text(json.dumps(self.results, indent=2))
        logger.info("Results saved to %s", path)


async def main():
    parser = argparse.ArgumentParser(description="Spatial Arbitrage Backtest (V2 vs V3)")
    parser.add_argument("--config", default="pairs.yaml")
    parser.add_argument("--pair-name", default="WETH_ARB_SPATIAL_ARB", help="Top-level key in pairs.yaml")
    parser.add_argument("--from-block", type=int, default=None)
    parser.add_argument("--to-block", type=int, default=None)
    parser.add_argument("--rpc-url", default=None)
    parser.add_argument("--out-csv", default="backtest_results.csv")
    parser.add_argument("--out-json", default="backtest_results.json")
    args = parser.parse_args()

    config = load_config(args.config, pair_name=args.pair_name)

    if args.from_block is not None:
        config["from_block"] = args.from_block
    if args.to_block is not None:
        config["to_block"] = args.to_block

    # Die fast if block range is still missing
    if config["from_block"] is None or config["to_block"] is None:
        raise SystemExit(
            "Error: from_block and to_block are required. "
            "Add them to the YAML backtest section or pass via CLI."
        )

    rpc_url = args.rpc_url or config.get("rpc_url")
    if not rpc_url:
        rpc_url = input("Archive RPC URL: ")

    async with LogFetcher(
        rpc_url=rpc_url,
        max_concurrent=int(config.get("max_concurrent_requests", 3)),
        chunk_size=int(config.get("chunk_size", 50000)),
        delay_ms=float(config.get("request_delay_ms", 250)),
        cache_dir=config.get("cache_dir", "~/.defi_flash_bot/cache"),
    ) as fetcher:
        snapshots = await fetcher.fetch_snapshots(
            v3_pool=config["uniswap_v3_pool"],
            v2_pool=config["sushiswap_v2_pool"],
            from_block=int(config["from_block"]),
            to_block=int(config["to_block"]),
        )

    engine = SpatialBacktest(config)
    engine.run(snapshots)
    engine.save_csv(args.out_csv)
    engine.save_json(args.out_json)


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )
    asyncio.run(main())
