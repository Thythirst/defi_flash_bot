#!/usr/bin/env python3
"""
Historical data fetcher for Arbitrum DEX backtesting.

Fetches Sync events from archive RPC or TheGraph subgraphs.
Caches results locally to avoid repeated API calls.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import requests
from web3 import Web3

logger = logging.getLogger("historical")

THEGRAPH_SUBGRAPH_URL = "https://api.thegraph.com/subgraphs/name/sushiswap/arbitrum-exchange"


@dataclass
class SyncSnapshot:
    block_number: int
    reserve0: int
    reserve1: int
    pair_address: str
    token0: str
    token1: str


class ArchiveRPCFetcher:
    """Fetches raw Sync event logs from an Arbitrum archive RPC."""

    def __init__(self, rpc_url: str):
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        self._cache_dir = Path.home() / ".defi_flash_bot" / "cache"
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def _cache_path(self, pair: str, from_block: int, to_block: int) -> Path:
        return self._cache_dir / f"{pair}_{from_block}_{to_block}.json"

    def fetch_sync_logs(
        self,
        pair_address: str,
        from_block: int,
        to_block: int,
        token0: str,
        token1: str,
    ) -> List[SyncSnapshot]:
        cache = self._cache_path(pair_address, from_block, to_block)
        if cache.exists():
            logger.info("Using cached Sync logs for %s", pair_address)
            raw = json.loads(cache.read_text())
            return [SyncSnapshot(**r) for r in raw]

        logger.info(
            "Fetching Sync logs for %s from block %d to %d",
            pair_address,
            from_block,
            to_block,
        )
        topic = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
        chunk_size = 2000
        all_logs = []

        for start in range(from_block, to_block + 1, chunk_size):
            end = min(start + chunk_size - 1, to_block)
            logs = self.w3.eth.get_logs({
                "address": self.w3.to_checksum_address(pair_address),
                "topics": [topic],
                "fromBlock": start,
                "toBlock": end,
            })
            for log in logs:
                data = log["data"]
                reserve0 = int(data[2:66], 16)
                reserve1 = int(data[66:130], 16)
                all_logs.append(
                    SyncSnapshot(
                        block_number=log["blockNumber"],
                        reserve0=reserve0,
                        reserve1=reserve1,
                        pair_address=pair_address,
                        token0=token0,
                        token1=token1,
                    )
                )
            time.sleep(0.2)  # Rate-limit protection

        # Save cache
        cache.write_text(
            json.dumps(
                [
                    {
                        "block_number": s.block_number,
                        "reserve0": s.reserve0,
                        "reserve1": s.reserve1,
                        "pair_address": s.pair_address,
                        "token0": s.token0,
                        "token1": s.token1,
                    }
                    for s in all_logs
                ],
                indent=2,
            )
        )
        return all_logs


class TheGraphFetcher:
    """Fetches historical pair data from TheGraph SushiSwap subgraph."""

    def __init__(self, url: str = THEGRAPH_SUBGRAPH_URL):
        self.url = url

    def fetch_pair_reserves(
        self,
        pair_address: str,
        from_block: int,
        to_block: int,
    ) -> List[SyncSnapshot]:
        query = """
        query($pair: String!, $fromBlock: Int!, $toBlock: Int!) {
            syncEvents(
                where: {
                    pair: $pair,
                    block_gte: $fromBlock,
                    block_lte: $toBlock
                },
                orderBy: block,
                orderDirection: asc,
                first: 1000
            ) {
                block
                reserve0
                reserve1
            }
        }
        """
        variables = {
            "pair": pair_address.lower(),
            "fromBlock": from_block,
            "toBlock": to_block,
        }
        resp = requests.post(
            self.url,
            json={"query": query, "variables": variables},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        events = data.get("data", {}).get("syncEvents", [])
        results = []
        for ev in events:
            results.append(
                SyncSnapshot(
                    block_number=int(ev["block"]),
                    reserve0=int(float(ev["reserve0"]) * 10**18),  # approximate; subgraph uses floats
                    reserve1=int(float(ev["reserve1"]) * 10**18),
                    pair_address=pair_address,
                    token0="",
                    token1="",
                )
            )
        return results


def build_reserve_map(
    snapshots: List[SyncSnapshot],
) -> Dict[str, Dict[int, SyncSnapshot]]:
    """Build a lookup map keyed by block number."""
    mapping: Dict[str, Dict[int, SyncSnapshot]] = {}
    for s in snapshots:
        key = f"{s.token0.lower()}_{s.token1.lower()}"
        if key not in mapping:
            mapping[key] = {}
        mapping[key][s.block_number] = s
    return mapping
