"""
watchlist_builder.py — Aave V3 Arbitrum watchlist builder
Replaces stale Redis bootstrap with a live, verified borrower list.

Three components:
    1. HistoricalBackfill  — scrapes all Borrow events from genesis to now
    2. RealtimeWatcher     — subscribes to new Borrow/Repay/Liquidation events
    3. DebtVerifier        — confirms totalDebtBase > 0 before adding to Redis

Usage:
    # Diagnose current watchlist staleness first:
    python watchlist_builder.py --mode ghost-check

    # Full pipeline — backfill then realtime forever:
    python watchlist_builder.py --mode run

    # Historical only:
    python watchlist_builder.py --mode backfill

    # Real-time only (after backfill done):
    python watchlist_builder.py --mode realtime

Aave V3 Arbitrum:
    Pool:         0x794a61358D6845594F94dc1DB02A252b5b4814aD
    Deploy block: 7742429 (Sep 2022)
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional

import redis.asyncio as aioredis
import websockets
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

AAVE_V3_POOL      = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
AAVE_DEPLOY_BLOCK = 7_742_429
MULTICALL3_ADDR   = "0xcA11bde05977b3631167028862bE2a173976CA11"

TOPIC_BORROW      = "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0"
TOPIC_REPAY       = "0xa534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051"
TOPIC_LIQUIDATION = "0xe413a321e8681d831f4dbccbeca18f026983974cfbbe2f13b9ac78f0ef6a008"

BLOCKS_PER_CHUNK  = 2_000
VERIFY_BATCH_SIZE = 200
WAD               = 10 ** 18

# ---------------------------------------------------------------------------
# ABIs
# ---------------------------------------------------------------------------

POOL_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "user", "type": "address"}],
        "name": "getUserAccountData",
        "outputs": [
            {"internalType": "uint256", "name": "totalCollateralBase", "type": "uint256"},
            {"internalType": "uint256", "name": "totalDebtBase",       "type": "uint256"},
            {"internalType": "uint256", "name": "availableBorrowsBase","type": "uint256"},
            {"internalType": "uint256", "name": "currentLiquidationThreshold","type": "uint256"},
            {"internalType": "uint256", "name": "ltv",                 "type": "uint256"},
            {"internalType": "uint256", "name": "healthFactor",        "type": "uint256"},
        ],
        "stateMutability": "view",
        "type": "function",
    }
]

MULTICALL3_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"internalType": "address", "name": "target",       "type": "address"},
                    {"internalType": "bool",    "name": "allowFailure", "type": "bool"},
                    {"internalType": "bytes",   "name": "callData",     "type": "bytes"},
                ],
                "internalType": "struct Multicall3.Call3[]",
                "name": "calls",
                "type": "tuple[]",
            }
        ],
        "name": "aggregate3",
        "outputs": [
            {
                "components": [
                    {"internalType": "bool",  "name": "success",    "type": "bool"},
                    {"internalType": "bytes", "name": "returnData", "type": "bytes"},
                ],
                "internalType": "struct Multicall3.Result[]",
                "name": "returnData",
                "type": "tuple[]",
            }
        ],
        "stateMutability": "payable",
        "type": "function",
    }
]

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class BackfillStats:
    start_block: int
    end_block: int
    blocks_scanned: int = 0
    raw_addresses_found: int = 0
    active_after_verify: int = 0
    ghosts_filtered: int = 0
    already_in_redis: int = 0
    newly_added: int = 0
    duration_seconds: float = 0.0
    errors: int = 0

    @property
    def ghost_ratio(self) -> float:
        if self.raw_addresses_found == 0:
            return 0.0
        return self.ghosts_filtered / self.raw_addresses_found

    def log_summary(self) -> None:
        logger.info(
            f"[Backfill] Complete — "
            f"blocks={self.blocks_scanned:,} "
            f"found={self.raw_addresses_found:,} "
            f"active={self.active_after_verify:,} "
            f"ghosts={self.ghosts_filtered:,} ({self.ghost_ratio:.1%}) "
            f"new_to_redis={self.newly_added:,} "
            f"duration={self.duration_seconds:.0f}s"
        )


@dataclass
class RealtimeStats:
    started_at: float = field(default_factory=time.time)
    borrow_events: int = 0
    repay_events: int = 0
    liquidation_events: int = 0
    addresses_added: int = 0
    addresses_removed: int = 0
    verify_failures: int = 0

    @property
    def uptime_hours(self) -> float:
        return (time.time() - self.started_at) / 3600


# ---------------------------------------------------------------------------
# DebtVerifier
# ---------------------------------------------------------------------------

class DebtVerifier:
    """
    Verifies active debt for a batch of addresses via Multicall3.
    Returns {address: hf_float} for addresses with totalDebtBase > 0.
    """

    def __init__(self, w3: AsyncWeb3):
        self._pool = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(AAVE_V3_POOL),
            abi=POOL_ABI,
        )
        self._mc = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(MULTICALL3_ADDR),
            abi=MULTICALL3_ABI,
        )

    async def verify_batch(self, addresses: list[str]) -> dict[str, float]:
        """
        Returns {address: health_factor} for addresses with active debt.
        Ghost addresses (zero debt) are excluded from the result.
        """
        active: dict[str, float] = {}
        chunks = [
            addresses[i:i + VERIFY_BATCH_SIZE]
            for i in range(0, len(addresses), VERIFY_BATCH_SIZE)
        ]

        for chunk in chunks:
            calls = []
            valid_addrs = []
            for addr in chunk:
                try:
                    ca = AsyncWeb3.to_checksum_address(addr)
                    # web3 v7: _encode_transaction_data() is sync, build_transaction() is async
                    call_data = self._pool.functions.getUserAccountData(ca)._encode_transaction_data()
                    calls.append({
                        "target":       AsyncWeb3.to_checksum_address(AAVE_V3_POOL),
                        "allowFailure": True,
                        "callData":     call_data,
                    })
                    valid_addrs.append(addr.lower())
                except Exception:
                    continue

            if not calls:
                continue

            try:
                results = await asyncio.wait_for(self._mc.functions.aggregate3(calls).call(), timeout=30.0)
            except asyncio.TimeoutError:
                logger.warning(f"[DebtVerifier] Multicall timed out")
                await asyncio.sleep(2)
                continue
            except Exception as e:
                logger.warning(f"[DebtVerifier] Multicall failed: {e}")
                await asyncio.sleep(2)
                continue

            for addr, (success, raw) in zip(valid_addrs, results):
                if not success or len(raw) < 192:
                    continue
                try:
                    h          = raw.hex()
                    total_debt = int(h[64:128],  16)   # slot 1
                    hf_raw     = int(h[320:384], 16)   # slot 5
                    if total_debt == 0:
                        continue
                    hf = min(hf_raw / WAD, 10.0)
                    active[addr] = hf
                except Exception:
                    continue

            await asyncio.sleep(0)

        return active


# ---------------------------------------------------------------------------
# HistoricalBackfill
# ---------------------------------------------------------------------------

class HistoricalBackfill:
    """
    Scrapes all Borrow events from Aave V3 Arbitrum genesis to current block.
    Verifies each address has active debt. Populates Redis ZSET.
    Checkpoints progress — safe to interrupt and resume.
    """

    def __init__(
        self,
        w3: AsyncWeb3,
        redis: aioredis.Redis,
        redis_key: str,
        verifier: DebtVerifier,
        start_block: int = AAVE_DEPLOY_BLOCK,
        blocks_per_chunk: int = BLOCKS_PER_CHUNK,
    ):
        self._w3         = w3
        self._redis      = redis
        self._key        = redis_key
        self._ckpt_key   = f"{redis_key}:backfill_checkpoint"
        self._verifier   = verifier
        self._start      = start_block
        self._chunk_size = blocks_per_chunk

    async def run(self) -> BackfillStats:
        t0      = time.time()
        current = await self._w3.eth.block_number
        start   = await self._get_checkpoint()

        stats = BackfillStats(start_block=start, end_block=current)
        raw_addresses: set[str] = set()
        block = start

        logger.info(
            f"[Backfill] Starting — "
            f"blocks {start:,} → {current:,} "
            f"({current - start:,} blocks, "
            f"~{(current - start) // self._chunk_size:,} chunks)"
        )

        while block < current:
            to_block = min(block + self._chunk_size - 1, current)

            try:
                logs = await self._w3.eth.get_logs({
                    "address":   AAVE_V3_POOL,
                    "fromBlock": block,
                    "toBlock":   to_block,
                    "topics":    [TOPIC_BORROW],
                })

                for log in logs:
                    topics = log.get("topics", [])
                    if len(topics) >= 4:
                        t3   = topics[3] if isinstance(topics[3], str) else topics[3].hex()
                        addr = "0x" + t3[-40:]
                        raw_addresses.add(addr.lower())

                stats.blocks_scanned += to_block - block + 1

                chunks_done = stats.blocks_scanned // self._chunk_size
                if chunks_done % 50 == 0 and chunks_done > 0 and stats.blocks_scanned > 0:
                    total_range = stats.end_block - stats.start_block
                    pct = (stats.blocks_scanned / total_range * 100) if total_range > 0 else 0
                    logger.info(
                        f"[Backfill] {pct:.1f}% — "
                        f"block {to_block:,}/{current:,} — "
                        f"{len(raw_addresses):,} addresses so far"
                    )

                if chunks_done % 100 == 0 and chunks_done > 0:
                    await self._save_checkpoint(to_block)

                block = to_block + 1
                await asyncio.sleep(0.05)

            except Exception as e:
                stats.errors += 1
                logger.warning(f"[Backfill] getLogs failed {block}→{to_block}: {e}")
                await asyncio.sleep(5)
                if any(k in str(e).lower() for k in ("limit", "too many", "rate")):
                    self._chunk_size = max(500, self._chunk_size // 2)
                    logger.info(f"[Backfill] Reduced chunk size to {self._chunk_size}")
                continue

        stats.raw_addresses_found = len(raw_addresses)
        logger.info(
            f"[Backfill] Scan done — {len(raw_addresses):,} unique addresses. "
            f"Verifying active debt..."
        )

        active = await self._verifier.verify_batch(list(raw_addresses))
        stats.active_after_verify = len(active)
        stats.ghosts_filtered     = len(raw_addresses) - len(active)

        existing     = await self._redis.zrange(self._key, 0, -1)
        existing_set = {
            m.decode().lower() if isinstance(m, bytes) else m.lower()
            for m in existing
        }

        pipe = self._redis.pipeline()
        for addr, hf in active.items():
            if addr not in existing_set:
                pipe.zadd(self._key, {addr: hf})
                stats.newly_added += 1
            else:
                pipe.zadd(self._key, {addr: hf}, xx=True)
                stats.already_in_redis += 1
        await pipe.execute()

        await self._redis.delete(self._ckpt_key)

        stats.duration_seconds = time.time() - t0
        stats.log_summary()
        return stats

    async def _get_checkpoint(self) -> int:
        ckpt = await self._redis.get(self._ckpt_key)
        if ckpt:
            block = int(ckpt)
            logger.info(f"[Backfill] Resuming from checkpoint block {block:,}")
            return block
        return self._start

    async def _save_checkpoint(self, block: int) -> None:
        await self._redis.set(self._ckpt_key, str(block), ex=86400 * 7)


# ---------------------------------------------------------------------------
# RealtimeWatcher
# ---------------------------------------------------------------------------

class RealtimeWatcher:
    """
    Subscribes to Aave V3 Pool Borrow/Repay/Liquidation events via WebSocket.
    Buffers new addresses and verifies debt every 30s before writing to Redis.
    Removes addresses when debt reaches zero.
    """

    def __init__(
        self,
        wss_url: str,
        redis: aioredis.Redis,
        redis_key: str,
        verifier: DebtVerifier,
    ):
        self._wss      = wss_url
        self._redis    = redis
        self._key      = redis_key
        self._verifier = verifier
        self._stats    = RealtimeStats()
        self._pending: set[str] = set()
        self._running  = False
        self._ws_task: Optional[asyncio.Task] = None
        self._vfy_task: Optional[asyncio.Task] = None
        self._hb_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._running  = True
        self._ws_task  = asyncio.create_task(self._ws_loop(),     name="rw_ws")
        self._vfy_task = asyncio.create_task(self._verify_loop(), name="rw_verify")
        self._hb_task  = asyncio.create_task(self._heartbeat_loop(), name="rw_hb")
        logger.info("[RealtimeWatcher] Started")

    async def stop(self) -> None:
        self._running = False
        for t in [self._ws_task, self._vfy_task, self._hb_task]:
            if t:
                t.cancel()

    @property
    def stats(self) -> RealtimeStats:
        return self._stats

    async def _ws_loop(self) -> None:
        backoff = 1.0
        while self._running:
            try:
                async with websockets.connect(
                    self._wss,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                ) as ws:
                    await ws.send(json.dumps({
                        "jsonrpc": "2.0", "id": 1,
                        "method":  "eth_subscribe",
                        "params":  ["logs", {
                            "address": AAVE_V3_POOL,
                            "topics":  [[TOPIC_BORROW, TOPIC_REPAY, TOPIC_LIQUIDATION]],
                        }],
                    }))
                    backoff = 1.0
                    logger.info("[RealtimeWatcher] WebSocket connected")

                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg    = json.loads(raw)
                            result = msg.get("params", {}).get("result", {})
                            if result:
                                await self._handle_event(result)
                        except Exception as e:
                            logger.debug(f"[RealtimeWatcher] Event parse error: {e}")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"[RealtimeWatcher] WS dropped: {e}. Retry in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _handle_event(self, log: dict) -> None:
        topics = log.get("topics", [])
        if not topics:
            return

        t0 = topics[0]
        t0 = t0 if isinstance(t0, str) else t0.hex()
        if not t0.startswith("0x"):
            t0 = "0x" + t0

        if len(topics) < 4:
            return

        t3   = topics[3] if isinstance(topics[3], str) else topics[3].hex()
        addr = ("0x" + t3[-40:]).lower()

        if t0.lower() == TOPIC_BORROW.lower():
            self._pending.add(addr)
            self._stats.borrow_events += 1
        elif t0.lower() == TOPIC_REPAY.lower():
            self._pending.add(addr)
            self._stats.repay_events += 1
        elif t0.lower() == TOPIC_LIQUIDATION.lower():
            self._pending.add(addr)
            self._stats.liquidation_events += 1

    async def _verify_loop(self) -> None:
        while self._running:
            await asyncio.sleep(30)
            if not self._pending:
                continue

            batch = list(self._pending)
            self._pending.clear()

            try:
                active = await self._verifier.verify_batch(batch)
                pipe   = self._redis.pipeline()

                for addr in batch:
                    if addr in active:
                        pipe.zadd(self._key, {addr: active[addr]})
                        self._stats.addresses_added += 1
                    else:
                        pipe.zrem(self._key, addr)
                        self._stats.addresses_removed += 1

                await pipe.execute()
                logger.info(
                    f"[RealtimeWatcher] Verified {len(batch)} — "
                    f"+{sum(1 for a in batch if a in active)} "
                    f"-{sum(1 for a in batch if a not in active)}"
                )

            except Exception as e:
                self._stats.verify_failures += 1
                logger.error(f"[RealtimeWatcher] Verify error: {e}")

    async def _heartbeat_loop(self) -> None:
        """Periodic heartbeat so watchdog doesn't kill for log silence."""
        while self._running:
            await asyncio.sleep(300)  # every 5 minutes
            count = await self._redis.zcard(self._key)
            logger.info(
                f"[RealtimeWatcher] heartbeat — "
                f"watchlist={count:,} "
                f"borrows={self._stats.borrow_events} "
                f"repays={self._stats.repay_events} "
                f"liqs={self._stats.liquidation_events} "
                f"added={self._stats.addresses_added} "
                f"removed={self._stats.addresses_removed} "
                f"uptime={self._stats.uptime_hours:.1f}h"
            )


# ---------------------------------------------------------------------------
# WatchlistBuilder
# ---------------------------------------------------------------------------

class WatchlistBuilder:
    """
    Top-level coordinator: backfill + realtime growth + debt verification.
    Run as a separate long-lived process alongside pipeline_v3.py.
    """

    def __init__(
        self,
        rpc_http: str,
        rpc_wss: str,
        redis: aioredis.Redis,
        redis_key: str = "watchlist",
        start_block: int = AAVE_DEPLOY_BLOCK,
        blocks_per_chunk: int = BLOCKS_PER_CHUNK,
    ):
        self._rpc_http    = rpc_http
        self._rpc_wss     = rpc_wss
        self._redis       = redis
        self._key         = redis_key
        self._start_block = start_block
        self._chunk_size  = blocks_per_chunk
        self._w3: Optional[AsyncWeb3]          = None
        self._verifier: Optional[DebtVerifier] = None
        self._backfill: Optional[HistoricalBackfill] = None
        self._realtime: Optional[RealtimeWatcher]    = None

    async def _setup(self) -> None:
        self._w3 = AsyncWeb3(AsyncHTTPProvider(
            self._rpc_http,
            request_kwargs={"timeout": __import__("aiohttp").ClientTimeout(total=30)},
        ))
        self._w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        self._verifier = DebtVerifier(self._w3)
        self._backfill = HistoricalBackfill(
            w3=self._w3, redis=self._redis,
            redis_key=self._key, verifier=self._verifier,
            start_block=self._start_block,
            blocks_per_chunk=self._chunk_size,
        )
        self._realtime = RealtimeWatcher(
            wss_url=self._rpc_wss, redis=self._redis,
            redis_key=self._key, verifier=self._verifier,
        )

    async def backfill(self) -> BackfillStats:
        await self._setup()
        return await self._backfill.run()

    async def start_realtime(self) -> None:
        if not self._realtime:
            await self._setup()
        await self._realtime.start()

    async def run(self) -> None:
        """Backfill then realtime. Blocks until cancelled."""
        await self._setup()

        count = await self._redis.zcard(self._key)
        ckpt  = await self._redis.get(f"{self._key}:backfill_checkpoint")

        if count > 0 and ckpt is None:
            logger.info(
                f"[WatchlistBuilder] {count:,} addresses in Redis, no checkpoint — "
                f"skipping backfill, starting realtime"
            )
        else:
            logger.info("[WatchlistBuilder] Running historical backfill...")
            stats = await self._backfill.run()
            logger.info(
                f"[WatchlistBuilder] Backfill done — "
                f"{stats.newly_added:,} new, ghost ratio {stats.ghost_ratio:.1%}"
            )

        logger.info("[WatchlistBuilder] Starting realtime watcher...")
        await self._realtime.start()

        try:
            while True:
                await asyncio.sleep(3600)
                count = await self._redis.zcard(self._key)
                rs    = self._realtime.stats
                logger.info(
                    f"[WatchlistBuilder] hourly — watchlist={count:,} "
                    f"borrows={rs.borrow_events} added={rs.addresses_added} "
                    f"removed={rs.addresses_removed} uptime={rs.uptime_hours:.1f}h"
                )
        except asyncio.CancelledError:
            await self._realtime.stop()
            logger.info("[WatchlistBuilder] Stopped cleanly")

    async def ghost_ratio_check(self) -> dict:
        """
        Diagnose current Redis watchlist staleness.
        Run this before deciding whether to rebuild.
        """
        await self._setup()
        members = await self._redis.zrange(self._key, 0, -1)
        addrs   = [
            m.decode().lower() if isinstance(m, bytes) else m.lower()
            for m in members
        ]

        if not addrs:
            return {"error": "Redis watchlist is empty"}

        logger.info(f"[GhostCheck] Verifying {len(addrs):,} addresses...")
        active = await self._verifier.verify_batch(addrs)

        ghosts = len(addrs) - len(active)
        result = {
            "total":       len(addrs),
            "active_debt": len(active),
            "ghosts":      ghosts,
            "ghost_ratio": ghosts / len(addrs),
            "verdict":     "STALE — rebuild recommended" if ghosts / len(addrs) > 0.3 else "OK",
        }

        logger.info(
            f"[GhostCheck] {result['active_debt']:,}/{result['total']:,} active — "
            f"ghost ratio {result['ghost_ratio']:.1%} — {result['verdict']}"
        )
        return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

async def _main():
    import argparse
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    parser = argparse.ArgumentParser(description="Aave V3 Arbitrum watchlist builder")
    parser.add_argument("--rpc-http",    default=os.getenv("QUICKNODE_HTTP"))
    parser.add_argument("--rpc-wss",     default=os.getenv("CHAINSTACK_WSS"))
    parser.add_argument("--redis",       default=os.getenv("REDIS_URL", "redis://localhost:6379"))
    parser.add_argument("--key",         default="watchlist")
    parser.add_argument("--mode",
        choices=["run", "backfill", "realtime", "ghost-check"],
        default="run",
    )
    parser.add_argument("--start-block", type=int, default=AAVE_DEPLOY_BLOCK)
    parser.add_argument("--chunk-size",  type=int, default=BLOCKS_PER_CHUNK)
    args = parser.parse_args()

    if not args.rpc_http:
        print("Error: --rpc-http or $QUICKNODE_HTTP required")
        return
    if not args.rpc_wss and args.mode in ("run", "realtime"):
        print("Error: --rpc-wss or $CHAINSTACK_WSS required")
        return

    redis = aioredis.from_url(args.redis, decode_responses=False)

    builder = WatchlistBuilder(
        rpc_http        = args.rpc_http,
        rpc_wss         = args.rpc_wss or "",
        redis           = redis,
        redis_key       = args.key,
        start_block     = args.start_block,
        blocks_per_chunk= args.chunk_size,
    )

    if args.mode == "ghost-check":
        result = await builder.ghost_ratio_check()
        print(f"\nResult: {result}\n")
    elif args.mode == "backfill":
        await builder.backfill()
    elif args.mode == "realtime":
        await builder.start_realtime()
        await asyncio.Event().wait()
    else:
        await builder.run()

    await redis.aclose()


if __name__ == "__main__":
    asyncio.run(_main())


# ---------------------------------------------------------------------------
# Deployment notes
# ---------------------------------------------------------------------------
#
# Run ghost-check first to see where you stand:
#   python watchlist_builder.py --mode ghost-check
#
# Expected output:
#   [GhostCheck] 1,200/2,877 active — ghost ratio 58.3% — STALE — rebuild recommended
#
# Then run the full backfill (10-30 min on QuickNode):
#   python watchlist_builder.py --mode run
#
# Expected backfill output:
#   [Backfill] Complete — blocks=270,000 found=18,000 active=9,000 ghosts=9,000 (50%)
#
# After backfill, the process stays alive in realtime mode watching for new borrowers.
# Run as a systemd service alongside pipeline_v3.py.
#
# The pipeline reads from the same Redis key — no changes needed to pipeline_v3.py.
# WatchlistManager.bootstrap() will pick up the fresh data on its next cycle.
# ---------------------------------------------------------------------------
