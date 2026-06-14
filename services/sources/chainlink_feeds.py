"""
services/sources/chainlink_feeds.py — Chainlink price feed fetcher.

Fetches latest prices from Chainlink Data Feeds on Arbitrum.
Uses the aggregator contract's latestRoundData() for each tracked symbol.

Tracking: ETH/USD, BTC/USD, LINK/USD, ARB/USD.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Dict, Optional

import aiohttp
from eth_utils import keccak

logger = logging.getLogger("oracle.chainlink")

# Chainlink price feed addresses on Arbitrum Mainnet
# Source: https://docs.chain.link/data-feeds/price-feeds/addresses?network=arbitrum
CHAINLINK_FEEDS: Dict[str, tuple] = {
    # symbol: (aggregator_address, decimals, heartbeat_seconds)
    "ETH":  ("0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612", 8, 3600),
    "BTC":  ("0x6ce185860a4963106506C203335A2910413708e9", 8, 3600),
    "LINK": ("0x86E53CF1B870786351Da77A57575e79CB55812CB", 8, 3600),
    "ARB":  ("0xB2A824043730FE05F3DA2efafa1CBbe83fa548D6", 8, 3600),
    "USDC": ("0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3", 8, 86400),
    "USDT": ("0x3f3f5dF88dC9F13eac63DF89EC16ef6e7E25DdE7", 8, 86400),
    "DAI":  ("0xc5C8E77B397E531B8EC06BFb0048328B30E9eCfB", 8, 86400),
}

SELECTOR = "0x" + keccak(text="latestRoundData()").hex()[:8]


@dataclass
class ChainlinkPrice:
    symbol: str
    price_raw: int          # scaled by feed decimals (typically 8)
    price_usd: float
    round_id: int
    updated_at: int         # unix timestamp
    heartbeat: int          # max seconds between updates
    timestamp: float        # local fetch time


class ChainlinkFetcher:
    """Fetches latest round data from Chainlink price feeds."""

    def __init__(self, rpc_url: Optional[str] = None):
        self.rpc_url = rpc_url or os.getenv(
            "QUICKNODE_HTTP_URL",
            os.getenv("CHAINSTACK_ARBITRUM_HTTP_URL",
                os.getenv("ARBITRUM_HTTP_URL", ""))
        )
        self._session: Optional[aiohttp.ClientSession] = None
        self._rpc_id = 10000  # offset to avoid collision with aave fetcher

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def fetch_single(self, symbol: str, feed_addr: str, decimals: int, heartbeat: int) -> Optional[ChainlinkPrice]:
        """Fetch latest round data for one feed."""
        session = await self._get_session()
        self._rpc_id += 1
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": feed_addr, "data": SELECTOR}, "latest"],
            "id": self._rpc_id,
        }
        try:
            async with session.post(self.rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    logger.warning("Chainlink HTTP %d for %s", resp.status, symbol)
                    return None
                result = await resp.json()
                raw = result.get("result", "0x")
                if raw in ("0x", "0x0", "0x" + "0" * 64):
                    return None

                # Decode: 5 × 32-byte slots (uint80, int256, uint256, uint256, uint80)
                data = bytes.fromhex(raw[2:])
                if len(data) < 160:
                    logger.warning("Chainlink %s: short response (%d bytes)", symbol, len(data))
                    return None
                # roundId: uint80, right-aligned in 32 bytes → bytes[0:32]
                round_id = int.from_bytes(data[16:32], 'big')  # skip 16 leading zero bytes
                # answer: int256 → bytes[32:64]
                answer = int.from_bytes(data[32:64], 'big', signed=True)
                # updatedAt: uint256 → bytes[96:128]
                updated_at = int.from_bytes(data[96:128], 'big')

                if answer <= 0:
                    return None

                return ChainlinkPrice(
                    symbol=symbol,
                    price_raw=answer,
                    price_usd=answer / (10 ** decimals),
                    round_id=round_id,
                    updated_at=updated_at,
                    heartbeat=heartbeat,
                    timestamp=asyncio.get_event_loop().time(),
                )
        except Exception as e:
            logger.debug("Chainlink fetch failed for %s: %s", symbol, e)
            return None

    async def fetch_all(self) -> Dict[str, ChainlinkPrice]:
        """Fetch all configured feeds."""
        prices: Dict[str, ChainlinkPrice] = {}
        for symbol, (feed_addr, decimals, heartbeat) in CHAINLINK_FEEDS.items():
            result = await self.fetch_single(symbol, feed_addr, decimals, heartbeat)
            if result:
                prices[symbol] = result
            await asyncio.sleep(0.05)  # 50ms between calls

        if prices:
            logger.debug("Chainlink: %d/%d prices fetched", len(prices), len(CHAINLINK_FEEDS))
        return prices

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
