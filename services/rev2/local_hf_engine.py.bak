#!/usr/bin/env python3
"""
local_hf_engine.py — In-memory Health Factor computation.
Zero RPC calls in the hot path. Price updates trigger instant rescore.
"""
import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


@dataclass
class Position:
    address: str
    collateral: Dict[str, int] = field(default_factory=dict)   # asset_addr -> raw units
    debt: Dict[str, int] = field(default_factory=dict)          # asset_addr -> raw units
    liq_threshold: Dict[str, float] = field(default_factory=dict)
    liq_bonus: Dict[str, float] = field(default_factory=dict)

    @property
    def collateral_assets(self) -> Set[str]:
        return set(self.collateral.keys())

    @property
    def debt_assets(self) -> Set[str]:
        return set(self.debt.keys())


class LocalHFEngine:
    """Maintains full borrower state in memory. HF compute: <1ms."""

    def __init__(self, on_liquidatable: Callable, decimals: Dict[str, int]):
        self.positions: Dict[str, Position] = {}
        self.prices: Dict[str, int] = {}           # asset -> price in 8-decimal USD (Chainlink format)
        self.decimals: Dict[str, int] = decimals
        self.on_liquidatable = on_liquidatable
        self._asset_to_borrowers: Dict[str, Set[str]] = defaultdict(set)
        self._lock = asyncio.Lock()
        self._triggering: Set[str] = set()         # re-entrancy guard

    def upsert_position(self, address: str, collateral: Dict[str, int],
                        debt: Dict[str, int], liq_threshold: Dict[str, float],
                        liq_bonus: Dict[str, float]):
        pos = Position(address=address, collateral=collateral, debt=debt,
                       liq_threshold=liq_threshold, liq_bonus=liq_bonus)
        self.positions[address] = pos
        for asset in {*collateral.keys(), *debt.keys()}:
            self._asset_to_borrowers[asset].add(address)
        # Check immediately — catches positions already underwater at load time
        if (
            self.on_liquidatable is not None
            and address not in self._triggering
        ):
            total_debt = sum(debt.values())
            if total_debt > 0:
                hf = self.compute_hf(pos)
                if hf < 1.0:
                    self._triggering.add(address)
                    try:
                        logger.info(f"[HF] LIQUIDATABLE {address[:8]} HF={hf:.6f} (on load)")
                        self.on_liquidatable(address, hf, pos)
                    finally:
                        self._triggering.discard(address)

    def remove_position(self, address: str):
        pos = self.positions.pop(address, None)
        if pos:
            for asset in {*pos.collateral_assets, *pos.debt_assets}:
                self._asset_to_borrowers[asset].discard(address)

    def update_price(self, asset: str, new_price: int):
        """
        Update a single asset price and re-evaluate affected borrowers.
        Fixed: was self.prices[asset] = x which crashed silently on
        PriceRegistry. Now writes through the correct interface.
        """
        if hasattr(self.prices, "update_price"):
            self.prices.update_price(asset, new_price)      # PriceRegistry
        else:
            self.prices[asset] = new_price                  # plain dict

        affected = self._asset_to_borrowers.get(asset, set())
        for address in affected:
            pos = self.positions.get(address)
            if pos is None:
                continue
            hf = self.compute_hf(pos)
            if (
                hf < 1.0
                and address not in self._triggering
                and self.on_liquidatable is not None
            ):
                self._triggering.add(address)
                try:
                    self.on_liquidatable(address, hf, pos)
                finally:
                    self._triggering.discard(address)

    def compute_hf(self, pos: Position) -> float:
        weighted_collateral = 0.0
        for asset, raw_amount in pos.collateral.items():
            price = self.prices.get_price(asset)
            if price is None:
                continue
            dec = self.decimals.get(asset, 18)
            amount_usd = (raw_amount / 10**dec) * (price / 10**8)
            threshold = pos.liq_threshold.get(asset, 0.8)
            weighted_collateral += amount_usd * threshold
        total_debt = 0.0
        for asset, raw_amount in pos.debt.items():
            price = self.prices.get_price(asset)
            if price is None:
                continue
            dec = self.decimals.get(asset, 18)
            amount_usd = (raw_amount / 10**dec) * (price / 10**8)
            total_debt += amount_usd
        if total_debt == 0:
            return 999.0
        return weighted_collateral / total_debt

    def compute_hf_by_address(self, address: str) -> Optional[float]:
        pos = self.positions.get(address)
        if pos is None:
            return None
        return self.compute_hf(pos)

    def get_sorted_candidates(self, top_n: int = 50) -> List[tuple]:
        results = []
        for addr, pos in self.positions.items():
            hf = self.compute_hf(pos)
            results.append((addr, hf))
        results.sort(key=lambda x: x[1])
        return results[:top_n]

    @property
    def borrower_count(self) -> int:
        return len(self.positions)
