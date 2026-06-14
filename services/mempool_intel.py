"""
services/mempool_intel.py — Arbitrum Mempool Intelligence Service.

Watches the Arbitrum mempool (via Chainstack WSS) for:
  1. Chainlink oracle updates — enables same-block liquidation
  2. Competitor liquidation calls — alerts when someone else liquidates
  3. Large DEX swaps — may create cross-pool arb opportunities
  4. Flash loan usage patterns — signals active MEV competition

Arbitrum L2 note:
  There is no traditional public mempool. The sequencer orders transactions.
  However, Chainstack's WSS endpoint provides eth_subscribe("newPendingTransactions")
  which streams transactions as they arrive at the sequencer — giving a ~250ms
  preview window before block inclusion. This is enough to:
    - Detect oracle updates and submit your liquidation in the same block
    - See competitor liquidations before they confirm
    - Track MEV activity patterns

Redis writes:
  mempool:recent              ZSET    score=timestamp, member=tx_hash
  mempool:tx:{hash}           HASH    decoded transaction data (TTL 5min)
  mempool:stats:{minute}      HASH    rolling minute-level stats

Event bus:
  arb:events:system           type=mempool.oracle_update
  arb:events:system           type=mempool.competitor_liquidation
  arb:events:market           type=mempool.large_swap

Usage:
  python -m services.mempool_intel
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import aiohttp
import redis.asyncio as redis
from dotenv import load_dotenv
from eth_utils import keccak

load_dotenv(dotenv_path=project_root / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | mempool | %(message)s",
)
logger = logging.getLogger("mempool")

# ────────────────────────────────────────────────────────────────
# Known contract addresses to watch
# ────────────────────────────────────────────────────────────────

# Chainlink oracle aggregators (Arbitrum)
CHAINLINK_AGGREGATORS: Dict[str, str] = {
    "0x639fe6ab55c921f74e7fac1ee960c0b6293ba612": "ETH/USD",
    "0x6ce185860a4963106506c203335a2910413708e9": "BTC/USD",
    "0x86e53cf1b870786351da77a57575e79cb55812cb": "LINK/USD",
    "0xb2a824043730fe05f3da2efafa1cbbe83fa548d6": "ARB/USD",
    "0x50834f3163758fcc1df9973b6e91f0f0f0434ad3": "USDC/USD",
    "0x3f3f5df88dc9f13eac63df89ec16ef6e7e25dde7": "USDT/USD",
    "0xc5c8e77b397e531b8ec06bfb0048328b30e9ecfb": "DAI/USD",
}

# Aave V3 Pool (liquidation calls)
AAVE_POOL = "0x794a61358d6845594f94dc1db02a252b5b4814ad"

# Major DEX routers (large swaps)
DEX_ROUTERS = {
    "0xe592427a0aece92de3edee1f18e0157c05861564": "UniswapV3",
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45": "UniswapUniversal",
    "0x8a21f6768c1f8075791d08546dadf6daa0be16ec": "SushiSwapV3",
    "0x1b81d678ffb9c0263b24a97847620c99d213eb14": "PancakeSwapV3",
}

# Balancer Vault (flash loans)
BALANCER_VAULT = "0xba12222222228d8ba445958a75a0704d566bf2c8"

# Own contracts (our activity — track for confirmation)
OWN_CONTRACTS = {
    "0x4cdaded000000000000000000000000000000000": "FlashExecutorV3",
    "0xdc8b7b7d33356a4dd72c44c2d8ff992ec086fbdc": "DexArbExecutor",
}

# LiquidationCall topic
LIQUIDATION_TOPIC = "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286"

# Minimal tx data decode: function selector (first 4 bytes of input)
# Chainlink: transmit(bytes) = 0x... (varies by oracle implementation)
# Chainlink: updateAnswer(int256) = 0x...
# Aave liquidation: liquidationCall(...) = 0xab9c4b5d (the selector)

# Common selectors
SELECTOR_LIQUIDATION_CALL = "0xab9c4b5d"
SELECTOR_FLASH_LOAN = "0xab9c4b5d"  # same interface prefix
SELECTOR_SWAP_EXACT_INPUT = "0xc04b8d59"  # exactInput on UniV3

# Aave Pool function selectors (for pre-liquidation tracking)
SELECTOR_BORROW = "0xa415bcad"
SELECTOR_REPAY = "0x573ade81"
SELECTOR_WITHDRAW = "0x69328dec"
SELECTOR_SUPPLY = "0x617ba037"


# ────────────────────────────────────────────────────────────────
# Data classes
# ────────────────────────────────────────────────────────────────

@dataclass
class MempoolTransaction:
    tx_hash: str
    from_addr: str
    to_addr: str
    value_eth: float
    input_data: str          # full input data
    selector: str            # first 4 bytes
    detected_type: str       # "liquidation", "oracle_update", "large_swap", "flash_loan", "unknown"
    detected_detail: str     # human-readable detail
    timestamp: float


# ────────────────────────────────────────────────────────────────
# Pattern Detectors
# ────────────────────────────────────────────────────────────────

class PatternDetector:
    """Detects known transaction patterns from raw tx data."""

    @staticmethod
    def detect(tx: dict) -> Optional[MempoolTransaction]:
        to_addr = (tx.get("to") or "").lower()
        from_addr = (tx.get("from") or "").lower()
        tx_hash = tx.get("hash", "")
        value = int(tx.get("value", "0x0"), 16) / 1e18
        input_data = tx.get("input", "0x")
        selector = input_data[:10].lower() if len(input_data) >= 10 else "0x"

        detected_type = "unknown"
        detected_detail = ""

        # 1. Chainlink oracle update
        if to_addr in CHAINLINK_AGGREGATORS:
            pair = CHAINLINK_AGGREGATORS[to_addr]
            detected_type = "oracle_update"
            detected_detail = f"Chainlink {pair} aggregator called"

        # 2. Aave liquidation call
        elif to_addr == AAVE_POOL and selector.startswith("0xab9c4b5d"):
            detected_type = "liquidation"
            detected_detail = "Aave V3 liquidationCall"

        # 3. Large DEX swap (>$100K inferred from value or common patterns)
        elif to_addr in DEX_ROUTERS:
            dex_name = DEX_ROUTERS[to_addr]
            detected_type = "dex_swap"
            detected_detail = f"{dex_name} swap"
            if value > 10:  # >10 ETH
                detected_type = "large_swap"
                detected_detail += f" ({value:.1f} ETH)"

        # 4. Flash loan
        elif to_addr == BALANCER_VAULT:
            detected_type = "flash_loan"
            detected_detail = "Balancer flash loan"

        # 5. Own contract activity
        elif to_addr in OWN_CONTRACTS:
            detected_type = "own_tx"
            detected_detail = OWN_CONTRACTS[to_addr]

        # 6. Aave borrower actions (borrow/repay/withdraw/supply)
        elif to_addr == AAVE_POOL and selector.startswith(SELECTOR_BORROW):
            detected_type = "borrow"
            detected_detail = f"Aave borrow from {from_addr[:12]}"
        elif to_addr == AAVE_POOL and selector.startswith(SELECTOR_REPAY):
            detected_type = "repay"
            detected_detail = f"Aave repay from {from_addr[:12]}"
        elif to_addr == AAVE_POOL and selector.startswith(SELECTOR_WITHDRAW):
            detected_type = "withdraw"
            detected_detail = f"Aave withdraw from {from_addr[:12]}"
        elif to_addr == AAVE_POOL and selector.startswith(SELECTOR_SUPPLY):
            detected_type = "deposit"
            detected_detail = f"Aave supply from {from_addr[:12]}"

        return MempoolTransaction(
            tx_hash=tx_hash,
            from_addr=from_addr,
            to_addr=to_addr,
            value_eth=value,
            input_data=input_data,
            selector=selector,
            detected_type=detected_type,
            detected_detail=detected_detail,
            timestamp=time.time(),
        )


# ────────────────────────────────────────────────────────────────
# Mempool Service
# ────────────────────────────────────────────────────────────────

class MempoolIntel:
    """Watches Arbitrum mempool via Chainstack WSS."""

    def __init__(self, ws_url: Optional[str] = None, redis_url: str = "redis://localhost:6379"):
        self.ws_url = ws_url or os.getenv(
            "QUICKNODE_WS_URL",
            os.getenv("CHAINSTACK_QUICKNODE_WS_URL", ""),
        )
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None
        self._session: Optional[aiohttp.ClientSession] = None

        # Track recent oracle updates for dedup
        self._recent_oracle_updates: Dict[str, float] = {}
        self._recent_liquidations: Dict[str, float] = {}

        # Stats
        self._stats = {
            "total_tx": 0,
            "oracle_updates": 0,
            "liquidations": 0,
            "large_swaps": 0,
            "dex_swaps": 0,
            "flash_loans": 0,
            "borrows": 0,
            "withdraws": 0,
            "deposits": 0,
            "repays": 0,
            "own_tx": 0,
        }

    async def connect(self):
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        await self.redis.ping()
        logger.info("Redis connected: %s", self.redis_url)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    # ── Event bus ───────────────────────────────────────────────

    async def _emit_alert(self, mtx: MempoolTransaction):
        """Emit detected mempool event to Redis streams AND pub/sub."""
        try:
            ts = int(time.time() * 1000)
            stream = "arb:events:system"

            if mtx.detected_type == "oracle_update":
                stream = "arb:events:market"
            elif mtx.detected_type in ("liquidation", "large_swap"):
                stream = "arb:events:market"

            payload_data = {
                "tx_hash": mtx.tx_hash,
                "from": mtx.from_addr,
                "to": mtx.to_addr,
                "value_eth": mtx.value_eth,
                "detail": mtx.detected_detail,
                "selector": mtx.selector,
            }

            await self.redis.xadd(stream, {
                "id": f"evt_{ts}",
                "ts": str(ts),
                "source": "mempool_intel",
                "type": f"mempool.{mtx.detected_type}",
                "severity": "info",
                "block": "0",
                "payload": json.dumps(payload_data),
            }, maxlen=100_000, approximate=True)

            # Also publish to pub/sub for real-time consumers (chainlink impact trigger)
            if mtx.detected_type == "oracle_update":
                await self.redis.publish(
                    "arb:signals:oracle_update",
                    json.dumps(payload_data),
                )
            elif mtx.detected_type == "liquidation":
                await self.redis.publish(
                    "arb:signals:competitor_liquidation",
                    json.dumps({
                        **payload_data,
                        "input_data": mtx.input_data,
                    }),
                )
        except Exception as e:
            logger.debug("Event emit failed: %s", e)

    async def _store_mempool_tx(self, mtx: MempoolTransaction):
        """Store transaction in Redis for historical lookup."""
        try:
            ts = int(mtx.timestamp * 1000)
            pipe = self.redis.pipeline()
            # ZSET: timestamp-ordered
            pipe.zadd("mempool:recent", {mtx.tx_hash: ts})
            # HASH: full tx data
            pipe.hset(f"mempool:tx:{mtx.tx_hash}", mapping={
                "from_addr": mtx.from_addr,
                "to_addr": mtx.to_addr,
                "value_eth": str(mtx.value_eth),
                "detected_type": mtx.detected_type,
                "detected_detail": mtx.detected_detail,
                "selector": mtx.selector,
                "input_data": mtx.input_data,
                "timestamp": str(mtx.timestamp),
            })
            pipe.expire(f"mempool:tx:{mtx.tx_hash}", 300)
            await pipe.execute()
        except Exception as e:
            logger.debug("Store mempool tx failed: %s", e)

    # ── Stats ───────────────────────────────────────────────────

    async def _flush_stats(self):
        """Write rolling stats to Redis every 60 seconds."""
        minute = time.strftime("%Y-%m-%dT%H:%M")
        await self.redis.hset(f"mempool:stats:{minute}", mapping={
            "total_tx": str(self._stats["total_tx"]),
            "oracle_updates": str(self._stats["oracle_updates"]),
            "liquidations": str(self._stats["liquidations"]),
            "large_swaps": str(self._stats["large_swaps"]),
            "dex_swaps": str(self._stats["dex_swaps"]),
            "flash_loans": str(self._stats["flash_loans"]),
            "borrows": str(self._stats["borrows"]),
            "withdraws": str(self._stats["withdraws"]),
            "deposits": str(self._stats["deposits"]),
            "repays": str(self._stats["repays"]),
            "own_tx": str(self._stats["own_tx"]),
        })
        await self.redis.expire(f"mempool:stats:{minute}", 3600)

        # Print summary
        if self._stats["total_tx"] > 0:
            logger.info(
                "Mempool stats: %d tx | %d oracle | %d liq | %d large_swap | %d dex_swap | %d flash | %d own | %d borrow | %d repay | %d withdraw | %d deposit",
                self._stats["total_tx"], self._stats["oracle_updates"],
                self._stats["liquidations"], self._stats["large_swaps"],
                self._stats["dex_swaps"], self._stats["flash_loans"],
                self._stats["own_tx"], self._stats["borrows"],
                self._stats["repays"], self._stats["withdraws"],
                self._stats["deposits"],
            )
        self._stats = {k: 0 for k in self._stats}

    # ── WebSocket listener ──────────────────────────────────────

    async def listen(self):
        """Connect to WSS and poll blocks for mempool transactions.

        On Arbitrum L2, the sequencer produces blocks every ~250ms with
        no traditional pending mempool. QuickNode's WSS eth_subscribe may
        confirm but never emit events on Arbitrum. Block polling via HTTP
        is the reliable fallback.

        Dual strategy: WSS for best-effort preview + block polling as
        guaranteed source. Both feed through the same detect→store→emit
        pipeline.
        """
        if not self.ws_url:
            logger.warning("No WSS URL — using block polling only")

        logger.info("Starting mempool listener (WSS + block polling)")
        session = await self._get_session()
        last_stats_flush = time.time()
        last_block_poll = 0.0
        last_block_seen = 0
        consecutive_empty_polls = 0
        wss_active = False

        async def process_transaction(tx: dict):
            """Process a single transaction through detect→store→emit."""
            if not tx or not tx.get("hash"):
                return
            mtx = PatternDetector.detect(tx)
            self._stats["total_tx"] += 1

            if mtx.detected_type != "unknown":
                if mtx.detected_type == "oracle_update":
                    self._stats["oracle_updates"] += 1
                    pair = mtx.detected_detail
                    last_time = self._recent_oracle_updates.get(pair, 0)
                    if time.time() - last_time > 30:
                        logger.info("🔮 ORACLE: %s — %s", pair, tx.get("hash", "")[:16])
                        self._recent_oracle_updates[pair] = time.time()
                elif mtx.detected_type == "liquidation":
                    self._stats["liquidations"] += 1
                    last_time = self._recent_liquidations.get(mtx.from_addr, 0)
                    if time.time() - last_time > 10:
                        logger.warning("⚠️ COMPETITOR LIQUIDATION: %s from %s",
                                      tx.get("hash", "")[:16], mtx.from_addr[:16])
                        self._recent_liquidations[mtx.from_addr] = time.time()
                elif mtx.detected_type == "large_swap":
                    self._stats["large_swaps"] += 1
                elif mtx.detected_type == "dex_swap":
                    self._stats["dex_swaps"] += 1
                elif mtx.detected_type == "flash_loan":
                    self._stats["flash_loans"] += 1
                elif mtx.detected_type == "borrow":
                    self._stats["borrows"] += 1
                elif mtx.detected_type == "repay":
                    self._stats["repays"] += 1
                elif mtx.detected_type == "withdraw":
                    self._stats["withdraws"] += 1
                elif mtx.detected_type == "deposit":
                    self._stats["deposits"] += 1
                elif mtx.detected_type == "own_tx":
                    self._stats["own_tx"] += 1
                await self._store_mempool_tx(mtx)
                await self._emit_alert(mtx)

        async def block_poll_loop():
            """Poll latest block for transactions every 0.25s."""
            nonlocal last_block_seen, consecutive_empty_polls
            rpc_url = os.getenv("QUICKNODE_HTTP_URL") or os.getenv("ARBITRUM_HTTP_URL", "")
            if not rpc_url:
                logger.error("No RPC URL for block polling")
                return

            while True:
                try:
                    payload = {
                        "jsonrpc": "2.0",
                        "method": "eth_getBlockByNumber",
                        "params": ["latest", True],
                        "id": 998,
                    }
                    async with session.post(
                        rpc_url, json=payload,
                        timeout=aiohttp.ClientTimeout(total=3),
                    ) as resp:
                        if resp.status == 200:
                            result = await resp.json()
                            block = result.get("result")
                            if block:
                                block_num = int(block.get("number", "0x0"), 16)
                                if block_num > last_block_seen:
                                    last_block_seen = block_num
                                    txs = block.get("transactions", [])
                                    for tx in txs:
                                        await process_transaction(tx)
                                    consecutive_empty_polls = 0
                                else:
                                    consecutive_empty_polls += 1
                except Exception:
                    consecutive_empty_polls += 1

                await asyncio.sleep(0.25)

        try:
            # Start block polling as primary source (works on all L2s)
            poll_task = asyncio.create_task(block_poll_loop())

            # WSS as best-effort early preview
            if self.ws_url:
                logger.info("Connecting to WSS: %s", self.ws_url[:60])
                try:
                    async with session.ws_connect(self.ws_url) as ws:
                        subscribe_msg = json.dumps({
                            "jsonrpc": "2.0",
                            "method": "eth_subscribe",
                            "params": ["newPendingTransactions"],
                            "id": 1,
                        })
                        await ws.send_str(subscribe_msg)
                        logger.info("WSS subscribed to newPendingTransactions")
                        wss_active = True

                        while True:
                            try:
                                msg = await asyncio.wait_for(
                                    ws.receive(), timeout=1.0,
                                )
                                if msg.type == aiohttp.WSMsgType.TEXT:
                                    data = json.loads(msg.data)
                                    if "id" in data and "result" in data:
                                        logger.info("WSS subscription confirmed: %s", data["result"])
                                        continue
                                    if "params" in data and "result" in data["params"]:
                                        tx_hash = data["params"]["result"]
                                        tx = await self._fetch_transaction(tx_hash)
                                        if tx:
                                            await process_transaction(tx)
                            except asyncio.TimeoutError:
                                pass  # no WSS message, block polling handles it
                            except Exception:
                                pass

                            now = time.time()
                            if now - last_stats_flush >= 60:
                                await self._flush_stats()
                                last_stats_flush = now

                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    logger.warning("WSS disconnected: %s. Block polling continues.", e)

            # If WSS disconnected or never started, run stats flush loop
            while True:
                await asyncio.sleep(1)
                now = time.time()
                if now - last_stats_flush >= 60:
                    await self._flush_stats()
                    last_stats_flush = now

        except asyncio.CancelledError:
            poll_task.cancel()
            try:
                await poll_task
            except asyncio.CancelledError:
                pass
        finally:
            if self._session and not self._session.closed:
                await self._session.close()
            if self.redis:
                await self.redis.aclose()
            logger.info("Mempool intel stopped")

    async def _fetch_transaction(self, tx_hash: str) -> Optional[dict]:
        """Fetch full transaction data via RPC."""
        session = await self._get_session()
        # Use QuickNode (paid) as primary, Alchemy as fallback
        rpc_url = os.getenv("QUICKNODE_HTTP_URL") or os.getenv("CHAINSTACK_ARBITRUM_HTTP_URL") or os.getenv("ARBITRUM_HTTP_URL", "")
        if not rpc_url:
            return None

        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getTransactionByHash",
            "params": [tx_hash],
            "id": 999,
        }
        try:
            async with session.post(rpc_url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    return None
                result = await resp.json()
                return result.get("result")
        except Exception:
            return None

    async def stop(self):
        if self._session and not self._session.closed:
            await self._session.close()
        if self.redis:
            await self.redis.aclose()
        logger.info("Mempool intel stopped")


# ─── CLI ──────────────────────────────────────────────────────

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Arbitrum Mempool Intelligence")
    parser.add_argument("--ws", default=None, help="WebSocket URL")
    parser.add_argument("--redis", default="redis://localhost:6379")
    args = parser.parse_args()

    service = MempoolIntel(ws_url=args.ws, redis_url=args.redis)
    await service.connect()

    try:
        await service.listen()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
