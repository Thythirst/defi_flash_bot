"""
Top-level runner for cross-pool V3 backtest.
Usage:
    cd /root/defi_flash_bot/prod
    python3 run_backtest.py --strategy WETH_USDC_CROSS_FEE --blocks 10000
"""

from __future__ import annotations

import asyncio
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)

from scanner.backtest_engine import main as backtest_main

if __name__ == "__main__":
    asyncio.run(backtest_main())
