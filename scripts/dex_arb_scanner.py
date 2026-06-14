"""
scripts/dex_arb_scanner.py — DEX-DEX arbitrage scanner runner.

Runs the dex_arb scanner module in its own asyncio loop, logging
opportunities to stdout in the same format as live_executor.

Usage:
    python -m scripts.dex_arb_scanner

Environment:
    ARBITRUM_HTTP_URL / ALCHEMY_HTTP_URL — Arbitrum RPC endpoint (required)
    MIN_PROFIT_USD                         — Override $5 default
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from web3 import Web3

# ─── Local imports ──────────────────────────────────────────
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scanner.dex_arb import (
    scan_arb_opportunities,
    format_opportunity,
    format_opportunities_table,
    ArbitrageOpportunity,
    MIN_PROFIT_USD,
    TOKENS,
    APPROX_PRICES_USD,
)
from scripts.dex_arb_executor import DexArbExecutor

# ─── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("dex_arb_scanner")


class DexArbScanner:
    """Thin wrapper that runs dex_arb.scan_arb_opportunities() on a loop."""

    def __init__(self, rpc_url: str, private_key: str = "",
                 executor_address: str = "",
                 min_profit_usd: float = MIN_PROFIT_USD):
        self.rpc_url = rpc_url
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        self.min_profit_usd = min_profit_usd
        self.dry_run = os.getenv("DRY_RUN", "1") == "1"

        if not self.w3.is_connected():
            raise ConnectionError(f"Cannot connect to RPC: {rpc_url}")

        chain_id = self.w3.eth.chain_id
        if chain_id != 42161:
            logger.warning(
                "Chain ID is %d, expected 42161 (Arbitrum). Proceeding anyway.",
                chain_id,
            )

        # Init executor (only if not dry run and we have a key)
        self.executor = None
        if not self.dry_run and private_key:
            self.executor = DexArbExecutor(
                rpc_url=rpc_url,
                private_key=private_key,
                contract_address=executor_address,
            )
            logger.info("DEX arb LIVE mode — will execute profitable opportunities")
        else:
            logger.info("DEX arb DRY RUN mode — scanning only, no execution")

        logger.info("DexArbScanner initialized: RPC=%s, min_profit=$%.2f",
                     rpc_url[:50], self.min_profit_usd)

    async def run_once(self) -> list:
        """Run a single scan cycle, return opportunities."""
        return await scan_arb_opportunities(
            w3=self.w3,
            rpc_url=self.rpc_url,
            min_profit_usd=self.min_profit_usd,
        )

    async def run_loop(self, interval: int = 60) -> None:
        """
        Run continuous scanning loop.

        Args:
            interval: Seconds between scans (default 60s).
        """
        logger.info("Starting DEX arb scan loop (interval=%ds)...", interval)

        while True:
            try:
                logger.debug("Scanning for DEX-DEX arb opportunities...")
                opportunities = await self.run_once()

                if opportunities:
                    logger.info(
                        "Found %d arbitrage opportunities",
                        len(opportunities),
                    )
                    for opp in opportunities:
                        logger.info(format_opportunity(opp))
                        # Execute if we have an executor and not in dry run
                        if self.executor:
                            self._execute_opportunity(opp)
                else:
                    logger.info("Scan complete: 0 opportunities")

            except asyncio.CancelledError:
                logger.info("Scan loop cancelled. Shutting down.")
                break
            except Exception as e:
                logger.error("Scan error: %s", e, exc_info=True)

            await asyncio.sleep(interval)

    def _execute_opportunity(self, opp: ArbitrageOpportunity) -> None:
        """Execute a single arbitrage opportunity via DexArbExecutor."""
        token_in_sym = _lookup_symbol_static(opp.token_in)
        token_out_sym = _lookup_symbol_static(opp.token_out)

        try:
            tx_hash = self.executor.execute(
                token_a=opp.token_in,
                token_b=opp.token_out,
                amount_a=opp.amount_in,
                sell_router_name=opp.sell_router,
                buy_router_name=opp.buy_router,
                fee_fwd=opp.fee_tier,
                dry_run=self.dry_run,
            )
            if tx_hash:
                logger.info(
                    "🚀 EXECUTED: %s→%s→%s | Sell: %s | Buy: %s | Net: $%.2f | TX: %s",
                    token_in_sym, token_out_sym, token_in_sym,
                    opp.sell_router, opp.buy_router,
                    opp.net_profit_usd, tx_hash,
                )
        except Exception as e:
            logger.error(
                "Execution failed for %s→%s: %s",
                token_in_sym, token_out_sym, e,
            )


def _lookup_symbol_static(address: str) -> str:
    """Look up token symbol by address (static, no import needed)."""
    addr_lower = address.lower()
    for token in TOKENS.values():
        if token.address.lower() == addr_lower:
            return token.symbol
    return address[:10] + "..."


# ─── Entry Point ────────────────────────────────────────────

def main():
    load_dotenv()

    rpc_url = (
        os.getenv("ARBITRUM_HTTP_URL")
        or os.getenv("ALCHEMY_HTTP_URL")
        or os.getenv("QUICKNODE_HTTP_URL")
    )
    if not rpc_url:
        print(
            "ERROR: No RPC URL set. Set ARBITRUM_HTTP_URL, ALCHEMY_HTTP_URL, "
            "or QUICKNODE_HTTP_URL.",
            file=sys.stderr,
        )
        sys.exit(1)

    min_profit = float(os.getenv("MIN_PROFIT_USD", str(MIN_PROFIT_USD)))
    interval = int(os.getenv("DEX_ARB_INTERVAL_SEC", "5"))
    private_key = os.getenv("BOT_PRIVATE_KEY", "")
    executor_addr = os.getenv("DEX_ARB_EXECUTOR", "")

    scanner = DexArbScanner(
        rpc_url=rpc_url,
        private_key=private_key,
        executor_address=executor_addr,
        min_profit_usd=min_profit,
    )

    try:
        asyncio.run(scanner.run_loop(interval=interval))
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Shutting down.")


if __name__ == "__main__":
    main()
