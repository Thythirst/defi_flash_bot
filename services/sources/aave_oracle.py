"""
services/sources/aave_oracle.py — Aave V3 oracle price fetcher.

Batch-fetches prices for all known assets from the Aave oracle contract.
Uses a dedicated RPC endpoint to avoid congestion with other services.
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import aiohttp
from eth_utils import keccak

logger = logging.getLogger("oracle.aave")

# Aave V3 Arbitrum oracle (from AddressesProvider.getPriceOracle())
AAVE_ORACLE = "0xb56c2f0b653b2e0b10c9b928c8580ac5df02c7c7"

# All known Aave V3 assets on Arbitrum: (address, symbol, decimals)
KNOWN_ASSETS: List[Tuple[str, str, int]] = [
    ("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", "ETH", 18),
    ("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "USDC", 6),
    ("0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", "USDT", 6),
    ("0x912CE59144191C1204E64559FE8253a0e49E6548", "ARB", 18),
    ("0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f", "WBTC", 8),
    ("0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1", "DAI", 18),
    ("0xf97f4df75117a78c1A5a0DBb814Af92458539FB4", "LINK", 18),
    ("0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8", "USDC.e", 6),
    ("0x6c84a8f1c29108F47a79964b5Fe888D4f4D0dE40", "tBTC", 18),
    ("0x4186BFC76E2E237523CBC30FD220FE055156b41F", "rsETH", 18),
]

SELECTOR = "0x" + keccak(text="getAssetPrice(address)").hex()[:8]


@dataclass
class AavePrice:
    asset: str
    symbol: str
    decimals: int
    price_raw: int          # oracle price (8 decimals)
    price_usd: float
    timestamp: float


class AaveOracleFetcher:
    """Batch-fetches prices from Aave V3 oracle."""

    def __init__(self, rpc_url: Optional[str] = None):
        # QuickNode (paid) as primary, Chainstack → PublicArb as fallback
        self.rpc_url = rpc_url or os.getenv(
            "QUICKNODE_HTTP_URL",
            os.getenv("CHAINSTACK_ARBITRUM_HTTP_URL",
                os.getenv("PUBLIC_ARBITRUM_RPC",
                    os.getenv("ARBITRUM_HTTP_URL", ""))),
        )
        # Backup RPC for when primary rate-limits
        self.backup_rpc_url = os.getenv("CHAINSTACK_ARBITRUM_HTTP_URL") or os.getenv("PUBLIC_ARBITRUM_RPC", "") if "quiknode" in (self.rpc_url or "").lower() else ""
        self._session: Optional[aiohttp.ClientSession] = None
        self._rpc_id = 0

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def fetch_single(self, asset: str) -> int:
        """Fetch a single asset price. Returns 0 on failure. Falls back to backup RPC on 429."""
        calldata = SELECTOR + asset[2:].lower().zfill(64)
        session = await self._get_session()

        async def _try_rpc(url: str) -> int:
            self._rpc_id += 1
            payload = {
                "jsonrpc": "2.0",
                "method": "eth_call",
                "params": [{"to": AAVE_ORACLE, "data": calldata}, "latest"],
                "id": self._rpc_id,
            }
            try:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 429 and url != self.backup_rpc_url and self.backup_rpc_url:
                        return -1  # signal to try backup
                    if resp.status != 200:
                        logger.warning("Aave oracle HTTP %d for %s", resp.status, asset[:10])
                        return 0
                    result = await resp.json()
                    raw = result.get("result", "0x")
                    if raw in ("0x", "0x0", "0x" + "0" * 64):
                        return 0
                    return int(raw, 16)
            except Exception as e:
                logger.debug("Aave oracle failed for %s: %s", asset[:10], e)
                return 0

        result = await _try_rpc(self.rpc_url)
        if result == -1 and self.backup_rpc_url:
            logger.info("Aave oracle 429 on primary, falling back to Alchemy for %s", asset[:10])
            result = await _try_rpc(self.backup_rpc_url)
        return max(result, 0)

    async def fetch_all(self) -> Dict[str, AavePrice]:
        """Fetch all known asset prices with rate-limiting delay."""
        prices: Dict[str, AavePrice] = {}
        ts = asyncio.get_event_loop().time()

        for addr, symbol, decimals in KNOWN_ASSETS:
            raw = await self.fetch_single(addr)
            if raw > 0:
                prices[addr.lower()] = AavePrice(
                    asset=addr,
                    symbol=symbol,
                    decimals=decimals,
                    price_raw=raw,
                    price_usd=raw / 1e8,
                    timestamp=ts,
                )
            await asyncio.sleep(0.1)  # 100ms between calls to avoid 429

        if prices:
            logger.debug("Aave: %d/%d prices fetched", len(prices), len(KNOWN_ASSETS))
        return prices

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
