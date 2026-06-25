#!/usr/bin/env python3
"""
batch_liquidation_builder.py — #5 (reshaped around the #4 batch contract).

Replaces per-position presigning (which serialized on one nonce) with a per-block
batch: every block, take the watchlist positions that crossed HF<1.0, group them by
debt asset, and build ONE executeLiquidationBatch(debtAsset, items[]) tx per group.
One tx = one nonce = no self-collision; one flash loan covers the whole group.

Pure builder — no live wiring yet. Activates once FlashExecutorV3 with
executeLiquidationBatch is deployed and the executor ABI/address are updated.
"""
from __future__ import annotations
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List

from eth_abi import encode as abi_encode
from eth_utils import keccak

# executeLiquidationBatch(address debtAsset,
#   (address collateralAsset,address borrower,uint256 debtToCover,
#    bool receiveAToken,address swapRouter,bytes swapCalldata)[] items)
_ITEM_TUPLE = "(address,address,uint256,bool,address,bytes)"
_FN_SIG = f"executeLiquidationBatch(address,{_ITEM_TUPLE}[])"
SELECTOR = keccak(text=_FN_SIG)[:4]

# Max positions per batch — bounds gas (Balancer flash loan + N liquidations + N swaps).
# A 700k-gas single liq * ~N, kept well under Arbitrum block gas. Tune after fork-gas test.
MAX_ITEMS_PER_BATCH = 12


@dataclass
class LiqPosition:
    borrower: str
    collateral_asset: str
    debt_asset: str
    debt_to_cover: int          # raw units of debt_asset
    swap_router: str            # address(0) if same-asset (no swap)
    swap_calldata: bytes        # collateral->debt swap, b"" if none
    receive_atoken: bool = False
    est_profit_usd: float = 0.0


def group_by_debt_asset(positions: List[LiqPosition]) -> Dict[str, List[LiqPosition]]:
    """Group liquidatable positions by debt asset (one flash loan per group)."""
    groups: Dict[str, List[LiqPosition]] = defaultdict(list)
    for p in positions:
        groups[p.debt_asset.lower()].append(p)
    return groups


def _addr_bytes(addr: str) -> str:
    # eth_abi wants checksum-agnostic hex str; normalize to 0x-prefixed.
    a = addr.lower()
    return a if a.startswith("0x") else "0x" + a


def encode_batch_calldata(debt_asset: str, items: List[LiqPosition]) -> bytes:
    """ABI-encode an executeLiquidationBatch call for one debt-asset group."""
    if not items:
        raise ValueError("empty batch")
    if len(items) > MAX_ITEMS_PER_BATCH:
        raise ValueError(f"batch too large: {len(items)} > {MAX_ITEMS_PER_BATCH}")
    encoded_items = [
        (
            _addr_bytes(it.collateral_asset),
            _addr_bytes(it.borrower),
            int(it.debt_to_cover),
            bool(it.receive_atoken),
            _addr_bytes(it.swap_router),
            it.swap_calldata if isinstance(it.swap_calldata, (bytes, bytearray)) else bytes.fromhex(
                it.swap_calldata[2:] if str(it.swap_calldata).startswith("0x") else str(it.swap_calldata)
            ),
        )
        for it in items
    ]
    args = abi_encode(
        ["address", f"{_ITEM_TUPLE}[]"],
        [_addr_bytes(debt_asset), encoded_items],
    )
    return SELECTOR + args


def build_batches(positions: List[LiqPosition]) -> List[dict]:
    """Turn a block's liquidatable positions into batch call descriptors,
    chunking each debt-asset group to MAX_ITEMS_PER_BATCH and ordering items by
    estimated profit (highest first) so the most valuable land even if gas caps."""
    batches: List[dict] = []
    for debt_asset, group in group_by_debt_asset(positions).items():
        group = sorted(group, key=lambda p: p.est_profit_usd, reverse=True)
        for i in range(0, len(group), MAX_ITEMS_PER_BATCH):
            chunk = group[i:i + MAX_ITEMS_PER_BATCH]
            batches.append({
                "debt_asset": debt_asset,
                "items": chunk,
                "total_debt": sum(p.debt_to_cover for p in chunk),
                "est_profit_usd": sum(p.est_profit_usd for p in chunk),
                "calldata": encode_batch_calldata(debt_asset, chunk),
            })
    # Most profitable batch first.
    batches.sort(key=lambda b: b["est_profit_usd"], reverse=True)
    return batches
