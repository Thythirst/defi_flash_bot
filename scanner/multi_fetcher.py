"""
scanner/multi_fetcher.py — Async multi-pool Swap-event fetcher for V3 / Algebra.

Fetches eth_getLogs concurrently across N pools, caches per pool, and builds
per-block snapshots tracking sqrtPrice + liquidity + tick for each pool.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import aiohttp
from eth_abi import decode
from eth_utils import to_checksum_address

logger = logging.getLogger("multi_fetcher")

# Keccak256("Swap(address,address,int256,int256,uint160,uint128,int24)")
V3_SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"


@dataclass
class PoolState:
    pool_name: str
    block_number: int
    sqrt_price_x96: int
    liquidity: int
    tick: int


@dataclass
class BlockSnapshot:
    block_number: int
    pool_states: Dict[str, PoolState] = field(default_factory=dict)


class MultiPoolFetcher:
    """Fetch Swap events from multiple pools and build unified block snapshots."""

    def __init__(
        self,
        rpc_url: str,
        pool_addresses: List[str],
        pool_names: Optional[List[str]] = None,
        max_concurrent: int = 8,
        chunk_size: int = 20000,
        delay_ms: float = 120.0,
        cache_dir: str = "~/.defi_flash_bot/cache",
        max_retries: int = 3,
        backoff_base_ms: int = 250,
    ):
        self.rpc_url = rpc_url
        self.pool_addresses = [a.lower() for a in pool_addresses]
        self.pool_names = pool_names or [f"pool_{i}" for i in range(len(pool_addresses))]
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.chunk_size = chunk_size
        self.delay_ms = delay_ms
        self.max_retries = max_retries
        self.backoff_base_ms = backoff_base_ms
        self.session: Optional[aiohttp.ClientSession] = None
        self._request_id = 0

        self.cache_dir = Path(cache_dir).expanduser()
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=120),
        )
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
            self.session = None

    def _cache_path(self, pool: str, start: int, end: int) -> Path:
        return self.cache_dir / f"{pool.lower()}_{start}_{end}.json"

    async def _post(self, payload: dict) -> dict:
        self._request_id += 1
        payload["jsonrpc"] = "2.0"
        payload["id"] = self._request_id

        async with self.semaphore:
            await asyncio.sleep(self.delay_ms / 1000.0)
            attempt = 0
            while attempt < self.max_retries:
                try:
                    async with self.session.post(self.rpc_url, json=payload) as resp:
                        if resp.status == 429:
                            delay = (2 ** attempt) * (self.backoff_base_ms / 1000.0)
                            logger.warning("Rate limited (429). Backing off %.2fs...", delay)
                            await asyncio.sleep(delay)
                            attempt += 1
                            continue
                        if resp.status >= 400:
                            text = await resp.text()
                            logger.warning("HTTP %d: %s", resp.status, text[:200])
                            if "block range" in text.lower():
                                raise RuntimeError(f"RPC block-range limit: {text[:300]}")
                            delay = (2 ** attempt) * (self.backoff_base_ms / 1000.0)
                            await asyncio.sleep(delay)
                            attempt += 1
                            continue
                        data = await resp.json()
                        if "error" in data:
                            raise RuntimeError(f"RPC error: {data['error']}")
                        return data
                except aiohttp.ClientError as exc:
                    delay = (2 ** attempt) * (self.backoff_base_ms / 1000.0)
                    logger.warning("Request error (%s). Retry in %.2fs...", exc, delay)
                    await asyncio.sleep(delay)
                    attempt += 1
            raise RuntimeError("Max RPC retries exceeded")

    async def _fetch_logs_chunk(self, pool_address: str, from_block: int, to_block: int) -> List[dict]:
        cache = self._cache_path(pool_address, from_block, to_block)
        if cache.exists():
            return json.loads(cache.read_text())

        payload = {
            "method": "eth_getLogs",
            "params": [{
                "address": to_checksum_address(pool_address),
                "topics": [V3_SWAP_TOPIC],
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
            }],
        }
        data = await self._post(payload)
        logs = data.get("result", [])
        cache.write_text(json.dumps(logs))
        logger.info("Fetched %d logs for %s blocks %d-%d", len(logs), pool_address, from_block, to_block)
        return logs

    async def fetch_all_logs(
        self,
        pool_address: str,
        from_block: int,
        to_block: int,
    ) -> List[dict]:
        tasks = []
        for start in range(from_block, to_block + 1, self.chunk_size):
            end = min(start + self.chunk_size - 1, to_block)
            tasks.append(self._fetch_logs_chunk(pool_address, start, end))
        results = await asyncio.gather(*tasks)
        all_logs = []
        for logs in results:
            all_logs.extend(logs)
        all_logs.sort(key=lambda x: (int(x["blockNumber"], 16), int(x.get("logIndex", "0x0"), 16)))
        return all_logs

    def _parse_v3_swaps(self, logs: List[dict], pool_name: str) -> List[PoolState]:
        """Parse Swap event logs into PoolState events.

        The V3 Swap event ABI layout (each param padded to 32 bytes):
          amount0     int256  → data[2:66]
          amount1     int256  → data[66:130]
          sqrtPriceX96 uint160→ data[130:194]
          liquidity   uint128 → data[194:258]
          tick        int24   → data[258:322]
        """
        events = []
        for log in logs:
            data = log["data"]
            # int256 fields (full 32-byte word)
            amount0 = int(data[2:66], 16)
            if amount0 >= 2 ** 255:
                amount0 -= 2 ** 256
            amount1 = int(data[66:130], 16)
            if amount1 >= 2 ** 255:
                amount1 -= 2 ** 256
            # uint160 — unsigned, full 32-byte word
            sqrt_price_x96 = int(data[130:194], 16)
            # uint128 — unsigned, lower 16 bytes of the 32-byte word
            liquidity = int(data[194:258], 16)
            # int24 — signed, lower 3 bytes of the 32-byte word.
            # We MUST use eth_abi.decode because Python int() treats the full
            # 32-byte word as unsigned, and a negative tick sign-extended to
            # 32 bytes becomes ~2^256 instead of a small negative int24.
            tick = decode(["int24"], bytes.fromhex(data[258:322]))[0]
            events.append(PoolState(
                pool_name=pool_name,
                block_number=int(log["blockNumber"], 16),
                sqrt_price_x96=sqrt_price_x96,
                liquidity=liquidity,
                tick=tick,
            ))
        return events

    async def fetch_snapshots(
        self,
        from_block: int,
        to_block: int,
    ) -> Dict[int, BlockSnapshot]:
        """Fetch logs for all pools, parse, and build unified block snapshots."""
        logger.info(
            "Fetching Swap logs for %d pools over blocks %d-%d",
            len(self.pool_addresses),
            from_block,
            to_block,
        )

        # Fetch all pools concurrently
        pool_logs_tasks = [
            self.fetch_all_logs(addr, from_block, to_block)
            for addr in self.pool_addresses
        ]
        all_logs_per_pool = await asyncio.gather(*pool_logs_tasks)

        # Parse each pool's logs
        pool_events: Dict[str, List[PoolState]] = {}
        for idx, logs in enumerate(all_logs_per_pool):
            name = self.pool_names[idx]
            events = self._parse_v3_swaps(logs, name)
            pool_events[name] = events
            logger.info("Parsed %d Swap events for %s", len(events), name)

        # Build per-block snapshots by forward-filling each pool's latest state
        snapshots: Dict[int, BlockSnapshot] = {}
        pool_indices: Dict[str, int] = {name: 0 for name in self.pool_names}
        pool_current: Dict[str, Optional[PoolState]] = {name: None for name in self.pool_names}

        for block in range(from_block, to_block + 1):
            snap = BlockSnapshot(block_number=block)
            for name in self.pool_names:
                events = pool_events[name]
                idx = pool_indices[name]
                while idx < len(events) and events[idx].block_number <= block:
                    pool_current[name] = events[idx]
                    idx += 1
                pool_indices[name] = idx
                if pool_current[name]:
                    snap.pool_states[name] = PoolState(
                        pool_name=name,
                        block_number=block,
                        sqrt_price_x96=pool_current[name].sqrt_price_x96,
                        liquidity=pool_current[name].liquidity,
                        tick=pool_current[name].tick,
                    )
            snapshots[block] = snap

        logger.info("Built %d block snapshots", len(snapshots))
        return snapshots
