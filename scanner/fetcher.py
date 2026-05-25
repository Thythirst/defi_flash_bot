"""
Async free-tier RPC log fetcher for Arbitrum archive nodes.

Design goals:
• Stay within free-tier rate limits (Alchemy: ~330 CU/s ≈ 3-5 req/s).
• Chunk large block ranges into digestible eth_getLogs calls.
• Parse Uniswap V3 Swap events and SushiSwap V2 Sync events.
• Cache everything to disk to avoid re-fetching.
• Reconstruct per-block pool state snapshots for backtesting.

Usage:
    fetcher = LogFetcher(rpc_url="https://arb-mainnet.g.alchemy.com/v2/...")
    snapshots = await fetcher.build_snapshots(
        v3_pool="0xC31E...",
        v2_pool="0x8e5E...",
        from_block=577_000_000,
        to_block=587_368_000,
    )
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Any

import aiohttp
from eth_utils import to_checksum_address

logger = logging.getLogger("fetcher")

# Event signatures
V3_SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
V2_SYNC_TOPIC = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"


@dataclass
class V3SwapEvent:
    block_number: int
    log_index: int
    tx_index: int
    amount0: int
    amount1: int
    sqrt_price_x96: int
    liquidity: int
    tick: int


@dataclass
class V2SyncEvent:
    block_number: int
    log_index: int
    reserve0: int
    reserve1: int


@dataclass
class BlockSnapshot:
    block_number: int
    # V3 state (from most recent Swap event at or before this block)
    v3_sqrt_price_x96: Optional[int] = None
    v3_liquidity: Optional[int] = None
    v3_tick: Optional[int] = None
    # V2 state (from most recent Sync event at or before this block)
    v2_reserve0: Optional[int] = None
    v2_reserve1: Optional[int] = None


class LogFetcher:
    """Async log fetcher with free-tier rate limiting and disk caching."""

    def __init__(
        self,
        rpc_url: str,
        max_concurrent: int = 3,
        chunk_size: int = 50000,
        delay_ms: float = 250.0,
        cache_dir: str = "~/.defi_flash_bot/cache",
    ):
        self.rpc_url = rpc_url
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.chunk_size = chunk_size
        self.delay_ms = delay_ms
        self.session: Optional[aiohttp.ClientSession] = None
        self._request_id = 0

        self.cache_dir = Path(cache_dir).expanduser()
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers={"Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=60),
        )
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
            self.session = None

    def _cache_path(self, pool: str, topic: str, start: int, end: int) -> Path:
        return self.cache_dir / f"{pool.lower()}_{topic[-8:]}_{start}_{end}.json"

    async def _post(self, payload: dict) -> dict:
        self._request_id += 1
        payload["jsonrpc"] = "2.0"
        payload["id"] = self._request_id

        async with self.semaphore:
            await asyncio.sleep(self.delay_ms / 1000.0)
            attempt = 0
            max_attempts = 5
            while attempt < max_attempts:
                try:
                    async with self.session.post(self.rpc_url, json=payload) as resp:
                        if resp.status == 429:
                            delay = 2 ** attempt
                            logger.warning("Rate limited (429). Backing off %ds...", delay)
                            await asyncio.sleep(delay)
                            attempt += 1
                            continue
                        # Alchemy returns 400 with JSON body for block-range violations
                        if resp.status >= 400:
                            text = await resp.text()
                            logger.warning("HTTP %d: %s", resp.status, text[:200])
                            # If it's a block-range limit, fail fast so caller can shrink chunk_size
                            if "block range" in text.lower() or "up to a" in text.lower():
                                raise RuntimeError(f"RPC block-range limit: {text[:300]}")
                            delay = 2 ** attempt
                            await asyncio.sleep(delay)
                            attempt += 1
                            continue
                        data = await resp.json()
                        if "error" in data:
                            raise RuntimeError(f"RPC error: {data['error']}")
                        return data
                except aiohttp.ClientError as exc:
                    delay = 2 ** attempt
                    logger.warning("Request error (%s). Retry in %ds...", exc, delay)
                    await asyncio.sleep(delay)
                    attempt += 1
            raise RuntimeError("Max RPC retries exceeded")

    async def _fetch_logs_chunk(
        self,
        pool_address: str,
        topic: str,
        from_block: int,
        to_block: int,
    ) -> List[dict]:
        cache = self._cache_path(pool_address, topic, from_block, to_block)
        if cache.exists():
            return json.loads(cache.read_text())

        payload = {
            "method": "eth_getLogs",
            "params": [{
                "address": to_checksum_address(pool_address),
                "topics": [topic],
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
        topic: str,
        from_block: int,
        to_block: int,
    ) -> List[dict]:
        """Fetch logs across the full range in chunks, concurrently within semaphore limit."""
        tasks = []
        for start in range(from_block, to_block + 1, self.chunk_size):
            end = min(start + self.chunk_size - 1, to_block)
            tasks.append(self._fetch_logs_chunk(pool_address, topic, start, end))
        results = await asyncio.gather(*tasks)
        all_logs = []
        for logs in results:
            all_logs.extend(logs)
        # Sort by block then log index
        all_logs.sort(key=lambda x: (int(x["blockNumber"], 16), int(x.get("logIndex", "0x0"), 16)))
        return all_logs

    def _parse_v3_swaps(self, logs: List[dict]) -> List[V3SwapEvent]:
        events = []
        for log in logs:
            data = log["data"]
            # Swap event data: int128 amount0, int128 amount1, uint160 sqrtPriceX96,
            #                   uint128 liquidity, int24 tick
            # Each field is 32 bytes except int128 which is also padded to 32 bytes.
            amount0 = int(data[2:66], 16)
            if amount0 >= 2 ** 127:
                amount0 -= 2 ** 256
            amount1 = int(data[66:130], 16)
            if amount1 >= 2 ** 127:
                amount1 -= 2 ** 256
            sqrt_price_x96 = int(data[130:194], 16)
            liquidity = int(data[194:258], 16)
            tick = int(data[258:322], 16)
            if tick >= 2 ** 23:
                tick -= 2 ** 24
            events.append(V3SwapEvent(
                block_number=int(log["blockNumber"], 16),
                log_index=int(log.get("logIndex", "0x0"), 16),
                tx_index=int(log.get("transactionIndex", "0x0"), 16),
                amount0=amount0,
                amount1=amount1,
                sqrt_price_x96=sqrt_price_x96,
                liquidity=liquidity,
                tick=tick,
            ))
        return events

    def _parse_v2_syncs(self, logs: List[dict]) -> List[V2SyncEvent]:
        events = []
        for log in logs:
            data = log["data"]
            reserve0 = int(data[2:66], 16)
            reserve1 = int(data[66:130], 16)
            events.append(V2SyncEvent(
                block_number=int(log["blockNumber"], 16),
                log_index=int(log.get("logIndex", "0x0"), 16),
                reserve0=reserve0,
                reserve1=reserve1,
            ))
        return events

    def _build_snapshots(
        self,
        v3_events: List[V3SwapEvent],
        v2_events: List[V2SyncEvent],
        from_block: int,
        to_block: int,
    ) -> Dict[int, BlockSnapshot]:
        """
        Build a map of block_number -> BlockSnapshot by forward-filling
        the most recent known pool state.
        """
        snapshots: Dict[int, BlockSnapshot] = {}

        # State carriers
        v3_idx = 0
        v2_idx = 0
        cur_v3: Optional[V3SwapEvent] = None
        cur_v2: Optional[V2SyncEvent] = None

        for block in range(from_block, to_block + 1):
            # Advance V3 pointer to latest event at or before this block
            while v3_idx < len(v3_events) and v3_events[v3_idx].block_number <= block:
                cur_v3 = v3_events[v3_idx]
                v3_idx += 1

            # Advance V2 pointer
            while v2_idx < len(v2_events) and v2_events[v2_idx].block_number <= block:
                cur_v2 = v2_events[v2_idx]
                v2_idx += 1

            snap = BlockSnapshot(block_number=block)
            if cur_v3:
                snap.v3_sqrt_price_x96 = cur_v3.sqrt_price_x96
                snap.v3_liquidity = cur_v3.liquidity
                snap.v3_tick = cur_v3.tick
            if cur_v2:
                snap.v2_reserve0 = cur_v2.reserve0
                snap.v2_reserve1 = cur_v2.reserve1
            snapshots[block] = snap

        return snapshots

    async def fetch_snapshots(
        self,
        v3_pool: str,
        v2_pool: str,
        from_block: int,
        to_block: int,
    ) -> Dict[int, BlockSnapshot]:
        logger.info("Starting log fetch: V3=%s V2=%s range=%d-%d", v3_pool, v2_pool, from_block, to_block)
        v3_logs, v2_logs = await asyncio.gather(
            self.fetch_all_logs(v3_pool, V3_SWAP_TOPIC, from_block, to_block),
            self.fetch_all_logs(v2_pool, V2_SYNC_TOPIC, from_block, to_block),
        )
        v3_events = self._parse_v3_swaps(v3_logs)
        v2_events = self._parse_v2_syncs(v2_logs)
        logger.info(
            "Parsed events: V3 swaps=%d, V2 syncs=%d",
            len(v3_events),
            len(v2_events),
        )
        return self._build_snapshots(v3_events, v2_events, from_block, to_block)
