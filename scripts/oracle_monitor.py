"""
oracle_monitor.py — Chainlink Oracle Update Detector (Strategy: Same-Block Liquidation)

Monitors the Arbitrum sequencer feed for Chainlink oracle update transactions.
When an oracle update is detected, immediately checks borrowers affected by that
price feed and pre-computes liquidation calldata — enabling same-block execution
that beats all reactive bots.

Strategy:
  1. Build oracle→asset mapping from AaveOracle.getSourceOfAsset()
  2. Build asset→borrowers mapping from tracked positions
  3. Watch sequencer feed for tx to Chainlink oracle contracts
  4. On oracle update: immediate health check → pre-sign liquidation

Integration:
  from scripts.oracle_monitor import OracleUpdateWatcher
  watcher = OracleUpdateWatcher(w3, aave_oracle, known_assets)
  # In sequencer listener, pass tx messages to watcher.process_block(block_data)
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from web3 import Web3

logger = logging.getLogger("oracle_watcher")

# ─── Chainlink Oracle Feed Addresses (Arbitrum) ────────────────
# Fetched from AaveOracle.getSourceOfAsset() at startup.
# These are the most common ones; the watcher will discover the rest.

CHAINLINK_ORACLE_ABI = [
    {
        "inputs": [],
        "name": "latestRoundData",
        "outputs": [
            {"name": "roundId", "type": "uint80"},
            {"name": "answer", "type": "int256"},
            {"name": "startedAt", "type": "uint256"},
            {"name": "updatedAt", "type": "uint256"},
            {"name": "answeredInRound", "type": "uint80"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

AAVE_ORACLE_ABI = [
    {
        "inputs": [{"name": "asset", "type": "address"}],
        "name": "getSourceOfAsset",
        "outputs": [{"name": "source", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]


@dataclass
class OracleAlert:
    """An oracle update that may trigger liquidations."""
    oracle_address: str
    asset_symbol: str
    old_price: float
    new_price: float
    deviation_pct: float
    affected_borrowers: List[str] = field(default_factory=list)


class OracleUpdateWatcher:
    """
    Watches sequencer feed transactions for Chainlink oracle updates.
    When detected, triggers immediate health factor checks on affected borrowers.

    Integration pattern:
      - Pass tx hashes from sequencer feed to `check_tx(oracle_addresses_to_watch)`
      - When a match is found, call `trigger_oracle_alert(oracle_address, asset, borrowers)`
      - The caller (live_executor) handles the liquidation check + submission
    """

    def __init__(
        self,
        w3: Web3,
        aave_oracle_address: str = "0xa50ba011c48153de246e5192c8f9258a2ba79ca9",
    ):
        self.w3 = w3
        self.aave_oracle = w3.eth.contract(
            address=w3.to_checksum_address(aave_oracle_address),
            abi=AAVE_ORACLE_ABI,
        )

        # oracle_address → asset_symbol
        self.oracle_to_asset: Dict[str, str] = {}
        # asset_symbol → oracle_address
        self.asset_to_oracle: Dict[str, str] = {}
        # asset_symbol → list of borrower addresses
        self.asset_to_borrowers: Dict[str, List[str]] = {}
        # oracle_address → last known price
        self.last_prices: Dict[str, float] = {}
        # Set of oracle addresses for fast tx lookup
        self.watched_oracles: Set[str] = set()

        # Callback: async(asset_symbol, oracle_address, affected_borrowers)
        self._on_oracle_update = None

    def set_callback(self, callback):
        """Set async callback for oracle update events."""
        self._on_oracle_update = callback

    def register_asset(self, asset_address: str, symbol: str, oracle_address: str):
        """Register an asset and its Chainlink oracle."""
        chk_oracle = self.w3.to_checksum_address(oracle_address)
        self.oracle_to_asset[chk_oracle] = symbol
        self.asset_to_oracle[symbol] = chk_oracle
        self.watched_oracles.add(chk_oracle)
        logger.info("Oracle watcher: %s → %s (%s)", symbol, oracle_address[:10], chk_oracle[:10])

    def discover_oracles(self, assets: List[Tuple[str, str]]) -> Dict[str, str]:
        """
        Discover Chainlink oracle addresses from AaveOracle for given assets.
        Falls back to hardcoded known addresses if AaveOracle call fails.
        assets: list of (address, symbol) tuples.
        Returns: mapping of oracle_address → symbol.
        """
        # Known Chainlink feeds on Arbitrum (fallback if AaveOracle is unavailable)
        KNOWN_ORACLES = {
            "WETH": "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",
            "WBTC": "0x6ce185860a4963106506C203335A2910413708e9",
            "LINK": "0x86E53CF1B870786351Da77A57575e79CB55812CB",
            "ARB": "0xb2A824043730FE05F3DA2efaFa1CBbe83fa548D6",
            "USDC": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3",
            "USDC.e": "0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3",
            "USDT": "0x3f3f5dF88dC9F13eaCFBAE2E04f65C8C5De435E5",
            "DAI": "0xc5C8E77B397E531B8ec06bFB592832bFc8eF4321",
        }
        discovered = {}
        for asset_addr, symbol in assets:
            oracle_addr = None
            # Try AaveOracle first
            try:
                oracle_addr = self.aave_oracle.functions.getSourceOfAsset(
                    self.w3.to_checksum_address(asset_addr)
                ).call()
            except Exception:
                pass

            # Fall back to known addresses
            if (not oracle_addr or oracle_addr == "0x" + "0" * 40) and symbol in KNOWN_ORACLES:
                oracle_addr = KNOWN_ORACLES[symbol]
                logger.info("Oracle for %s: using known address %s", symbol, oracle_addr[:10])

            if oracle_addr and oracle_addr != "0x" + "0" * 40:
                chk = self.w3.to_checksum_address(oracle_addr)
                self.register_asset(asset_addr, symbol, chk)
                discovered[chk] = symbol
            else:
                logger.warning("No oracle found for %s", symbol)
        return discovered

    def map_borrowers(self, borrower_to_assets: Dict[str, Set[str]]):
        """
        Build asset→borrowers mapping from tracked positions.
        borrower_to_assets: {borrower_address: set of asset_symbols}
        """
        self.asset_to_borrowers = {}
        for borrower, assets in borrower_to_assets.items():
            for symbol in assets:
                self.asset_to_borrowers.setdefault(symbol, []).append(borrower)
        total = sum(len(b) for b in self.asset_to_borrowers.values())
        logger.info(
            "Oracle watcher: mapped %d borrowers across %d assets",
            len(borrower_to_assets), len(self.asset_to_borrowers),
        )

    async def check_oracle_tx(self, tx_to: str, tx_data: str) -> Optional[OracleAlert]:
        """
        Check if a transaction is a Chainlink oracle update we're watching.
        Call this for every tx in the sequencer feed.

        Returns OracleAlert if it's a relevant oracle update, None otherwise.
        """
        chk_to = self.w3.to_checksum_address(tx_to) if tx_to else None
        if not chk_to or chk_to not in self.watched_oracles:
            return None

        symbol = self.oracle_to_asset.get(chk_to, "UNKNOWN")
        affected = self.asset_to_borrowers.get(symbol, [])

        if not affected:
            return None

        # Read new price
        try:
            oracle = self.w3.eth.contract(address=chk_to, abi=CHAINLINK_ORACLE_ABI)
            round_data = oracle.functions.latestRoundData().call()
            new_price = round_data[1] / 1e8  # int256 answer with 8 decimals
            old_price = self.last_prices.get(chk_to, new_price)
            deviation = abs(new_price - old_price) / old_price * 100 if old_price > 0 else 0

            self.last_prices[chk_to] = new_price

            logger.info(
                "🔮 ORACLE UPDATE: %s $%.4f→$%.4f (%.3f%%) | %d borrowers affected",
                symbol, old_price, new_price, deviation, len(affected),
            )

            return OracleAlert(
                oracle_address=chk_to,
                asset_symbol=symbol,
                old_price=old_price,
                new_price=new_price,
                deviation_pct=deviation,
                affected_borrowers=affected,
            )
        except Exception as e:
            logger.error("Failed to read oracle price for %s: %s", symbol, e)
            return None

    # ─── Sequencer Feed Passthrough ───────────────────────────────

    def is_oracle_related(self, to_address: str) -> bool:
        """Quick check: is this tx to a watched oracle?"""
        try:
            return self.w3.to_checksum_address(to_address) in self.watched_oracles
        except Exception:
            return False

    def get_affected_borrowers(self, asset_symbol: str) -> List[str]:
        """Get borrowers affected by an asset's price change."""
        return self.asset_to_borrowers.get(asset_symbol, [])

    async def handle_oracle_update(self, oracle_addr: str):
        """
        Handle a detected oracle update: read price, find affected borrowers,
        trigger callback.
        """
        symbol = self.oracle_to_asset.get(oracle_addr)
        if not symbol:
            return

        affected = self.get_affected_borrowers(symbol)
        if not affected:
            return

        if self._on_oracle_update:
            await self._on_oracle_update(symbol, oracle_addr, affected)
