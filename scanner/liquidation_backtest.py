"""
scanner/liquidation_backtest.py — Historical Aave v3 liquidation fetcher + profit simulator.

Fetches LiquidationCall events from archive RPC, decodes on-chain data,
and simulates net profit using flash-loan cost model.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp
from eth_abi import decode
from eth_utils import keccak, to_hex

from scanner.aave_v3 import (
    POOL,
    POOL_DATA_PROVIDER,
    ORACLE,
    LIQUIDATION_CALL_TOPIC,
    LiquidationEvent,
    LiquidationProfitResult,
    calculate_liquidation_profit,
    FLASH_LOAN_PREMIUM_BPS,
    SWAP_FEE_BPS,
    LIQUIDATION_GAS_LIMIT,
)

logger = logging.getLogger("liquidation_backtest")


@dataclass
class BacktestConfig:
    from_block: int
    to_block: int
    chunk_size: int = 2000          # blocks per eth_getLogs request
    max_concurrent: int = 3         # parallel RPC requests
    delay_ms: int = 250
    pool_address: str = POOL


class LiquidationBacktest:
    """
    Fetches historical Aave v3 LiquidationCall events and simulates
    what profit a flash-loan liquidator would have earned.
    """

    def __init__(self, rpc_url: str, cfg: BacktestConfig):
        self.rpc_url = rpc_url
        self.cfg = cfg
        self.events: List[LiquidationEvent] = []
        self.profits: List[LiquidationProfitResult] = []
        self.stats = defaultdict(int)

    async def _rpc(self, method: str, params: list) -> dict:
        async with aiohttp.ClientSession() as session:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }
            async with session.post(
                self.rpc_url,
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as resp:
                return await resp.json()

    async def fetch_events(self) -> None:
        """Fetch all LiquidationCall events in block range via chunked parallel RPC."""
        cfg = self.cfg
        sem = asyncio.Semaphore(cfg.max_concurrent)

        async def _chunk(start: int, end: int) -> List[dict]:
            async with sem:
                result = await self._rpc(
                    "eth_getLogs",
                    [
                        {
                            "address": cfg.pool_address,
                            "topics": [LIQUIDATION_CALL_TOPIC],
                            "fromBlock": to_hex(start),
                            "toBlock": to_hex(end),
                        }
                    ],
                )
                logs = result.get("result", [])
                if cfg.delay_ms:
                    await asyncio.sleep(cfg.delay_ms / 1000)
                return logs

        total_blocks = cfg.to_block - cfg.from_block + 1
        chunks = [
            (i, min(i + cfg.chunk_size - 1, cfg.to_block))
            for i in range(cfg.from_block, cfg.to_block + 1, cfg.chunk_size)
        ]
        logger.info(
            "Fetching %d liquidation chunks (%.1fM blocks) ...",
            len(chunks),
            total_blocks / 1e6,
        )

        tasks = [_chunk(s, e) for s, e in chunks]
        chunk_results = await asyncio.gather(*tasks)

        for logs in chunk_results:
            for log in logs:
                data = bytes.fromhex(log["data"][2:])
                decoded = decode(
                    ["uint256", "uint256", "address", "bool"],
                    data,
                )
                self.events.append(
                    LiquidationEvent(
                        block=int(log["blockNumber"], 16),
                        tx_hash=log["transactionHash"],
                        collateral_asset="0x" + log["topics"][1][-40:],
                        debt_asset="0x" + log["topics"][2][-40:],
                        user="0x" + log["topics"][3][-40:],
                        debt_to_cover=decoded[0],
                        liquidated_collateral_amount=decoded[1],
                        liquidator=decoded[2],
                        receive_a_token=decoded[3],
                        log_index=int(log["logIndex"], 16),
                    )
                )

        logger.info("Fetched %d liquidation events", len(self.events))

    def simulate(self) -> None:
        """Run profit simulation on each fetched event."""
        if not self.events:
            logger.warning("No events to simulate.")
            return

        for ev in self.events:
            self.stats["total_events"] += 1

            # Skip aToken liquidations (harder to profit from without swap)
            if ev.receive_a_token:
                self.stats["atoken_liquidations_skipped"] += 1
                continue

            # Use approximate prices: for WETH/USDC/USDT/DAI we know 18/6/6/18
            # For simplicity, assume both assets are ~$1 until we have price oracle
            debt_decimals = 18 if "eth" in ev.debt_asset.lower() else 6
            collat_decimals = 18 if "eth" in ev.collateral_asset.lower() else 6

            profit = calculate_liquidation_profit(
                debt_to_cover=ev.debt_to_cover,
                liquidated_collateral_amount=ev.liquidated_collateral_amount,
                collateral_decimals=collat_decimals,
                debt_decimals=debt_decimals,
                collateral_price_usd=1.0,
                debt_price_usd=1.0,
                swap_fee_bps=0 if ev.collateral_asset == ev.debt_asset else SWAP_FEE_BPS,
            )
            profit.block = ev.block
            profit.user = ev.user
            profit.collateral_asset = ev.collateral_asset
            profit.debt_asset = ev.debt_asset

            self.profits.append(profit)
            if profit.net_profit > 0:
                self.stats["profitable_events"] += 1
                self.stats["total_net_profit"] += profit.net_profit
            else:
                self.stats["unprofitable_events"] += 1

        logger.info("Simulation complete.")

    def print_summary(self) -> None:
        print(f"\n{'='*70}")
        print(" AAVE v3 LIQUIDATION BACKTEST SUMMARY")
        print(f"{'='*70}")
        for k, v in sorted(self.stats.items()):
            print(f"  {k:<40s} {v}")

        if not self.profits:
            print("  No profit data.")
            return

        profitable = [p for p in self.profits if p.net_profit > 0]
        total_events = len(self.profits)
        profit_rate = len(profitable) / total_events * 100 if total_events else 0

        print(f"\n  Total events simulated:     {total_events}")
        print(f"  Profitable:               {len(profitable)} ({profit_rate:.1f}%)")
        if profitable:
            total_net = sum(p.net_profit for p in profitable)
            avg_net = total_net / len(profitable)
            best = max(profitable, key=lambda x: x.net_profit)
            print(f"  Total net profit (wei):   {total_net}")
            print(f"  Avg net profit (wei):     {avg_net:.0f}")
            print(f"  Best single liquidation:  block={best.block} net={best.net_profit} wei")
        print(f"{'='*70}\n")

    def save_csv(self, path: str) -> None:
        if not self.profits:
            logger.info("No profits to save.")
            return
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "block", "user", "collateral_asset", "debt_asset",
                    "debt_to_cover", "liquidated_amount", "gross_profit",
                    "flash_loan_fee", "gas_cost", "swap_slippage",
                    "net_profit", "net_profit_pct",
                ],
            )
            writer.writeheader()
            for p in self.profits:
                writer.writerow(p.__dict__)
        logger.info("CSV saved to %s", path)

    def save_json(self, path: str) -> None:
        if not self.profits:
            logger.info("No profits to save.")
            return
        Path(path).write_text(
            json.dumps([p.__dict__ for p in self.profits], indent=2)
        )
        logger.info("JSON saved to %s", path)


async def main():
    parser = argparse.ArgumentParser(description="Aave v3 Liquidation Backtest")
    parser.add_argument("--from-block", type=int, required=True)
    parser.add_argument("--to-block", type=int, required=True)
    parser.add_argument("--rpc-url", default=None)
    parser.add_argument("--chunk-size", type=int, default=2000)
    parser.add_argument("--out-csv", default="liquidation_backtest.csv")
    parser.add_argument("--out-json", default="liquidation_backtest.json")
    args = parser.parse_args()

    import os

    rpc_url = args.rpc_url or os.getenv("ALCHEMY_HTTP_URL", "")
    if not rpc_url:
        raise SystemExit("--rpc-url or ALCHEMY_HTTP_URL required")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    cfg = BacktestConfig(
        from_block=args.from_block,
        to_block=args.to_block,
        chunk_size=args.chunk_size,
    )

    backtest = LiquidationBacktest(rpc_url=rpc_url, cfg=cfg)
    await backtest.fetch_events()
    backtest.simulate()
    backtest.print_summary()
    backtest.save_csv(args.out_csv)
    backtest.save_json(args.out_json)


if __name__ == "__main__":
    asyncio.run(main())
