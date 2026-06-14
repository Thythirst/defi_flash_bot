"""
services/reconciler.py — On-Chain State Reconciliation Service.

Periodically samples aave:user:* from Redis, verifies against live
Aave V3 getUserAccountData() via Multicall3. Prunes exited users,
updates health factors, publishes stale-rate metrics.

Layered on top of the event indexer — does not modify event handlers.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

import aiohttp
import redis.asyncio as redis
from dotenv import load_dotenv
from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
load_dotenv(project_root / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | reconciler | %(message)s",
)
logger = logging.getLogger("reconciler")

# ── Constants ─────────────────────────────────────────────────

AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"

BATCH_SIZE = int(os.getenv("RECONCILER_BATCH_SIZE", "50"))
CYCLE_INTERVAL = int(os.getenv("RECONCILER_INTERVAL", "30"))
MAX_USERS_PER_CYCLE = int(os.getenv("RECONCILER_MAX_USERS", "500"))
RPC_URL = os.getenv("QUICKNODE_HTTP_URL") or os.getenv("ARBITRUM_HTTP_URL", "")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Redis metric keys
M_VERIFIED  = "reconciliation:verified_total"
M_PRUNED    = "reconciliation:pruned_total"
M_UPDATED   = "reconciliation:updated_total"
M_STALE_CNT = "reconciliation:stale_count"
M_STALE_RATE = "reconciliation:stale_rate"
M_LAST_TS   = "reconciliation:last_cycle_ts"
M_LAST_MS   = "reconciliation:last_cycle_ms"
M_SAMPLED   = "reconciliation:users_sampled"
M_ERRORS    = "reconciliation:errors_total"


class CycleResult:
    sampled: int = 0
    pruned: int = 0
    updated: int = 0
    stale: int = 0
    errors: int = 0
    elapsed_ms: float = 0.0


class AaveReconciler:
    """Periodic on-chain reconciliation for Aave V3 indexer state."""

    def __init__(self, rpc_url: str = "", redis_url: str = ""):
        self.rpc_url = rpc_url or RPC_URL
        self.redis_url = redis_url or REDIS_URL
        self.redis: Optional[redis.Redis] = None
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        self._session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
        )
        logger.info(
            "started rpc=%s batch=%d interval=%ds max=%d",
            self.rpc_url[:50], BATCH_SIZE, CYCLE_INTERVAL, MAX_USERS_PER_CYCLE,
        )

    async def stop(self):
        if self._session:
            await self._session.close()
        if self.redis:
            await self.redis.close()

    async def run_forever(self):
        await self.start()
        try:
            while True:
                t0 = time.monotonic()
                r = await self._cycle()
                r.elapsed_ms = (time.monotonic() - t0) * 1000
                await self._metrics(r)
                logger.info(
                    "sampled=%d pruned=%d updated=%d stale=%d errs=%d %.0fms",
                    r.sampled, r.pruned, r.updated, r.stale, r.errors, r.elapsed_ms,
                )
                await asyncio.sleep(CYCLE_INTERVAL)
        finally:
            await self.stop()

    # ── Cycle ──────────────────────────────────────────────

    async def _cycle(self) -> CycleResult:
        r = CycleResult()
        try:
            keys = await self.redis.keys("aave:user:*")
        except Exception as e:
            logger.error("KEYS: %s", e)
            r.errors += 1
            return r
        if not keys:
            return r

        addrs = [k.replace("aave:user:", "") for k in keys]
        random.shuffle(addrs)
        addrs = addrs[:MAX_USERS_PER_CYCLE]

        for i in range(0, len(addrs), BATCH_SIZE):
            batch = addrs[i : i + BATCH_SIZE]
            try:
                onchain = await self._multicall(batch)
            except Exception as e:
                logger.warning("multicall[%d]: %s", i // BATCH_SIZE, e)
                r.errors += 1
                continue

            r.sampled += len(batch)
            for addr, (coll, debt, _, lt, _, hf_raw) in zip(batch, onchain):
                hf = hf_raw / 1e18 if hf_raw < 2**255 else float("inf")
                if coll == 0 and debt == 0:
                    r.stale += 1
                    try:
                        await self._prune(addr)
                        r.pruned += 1
                    except Exception as e:
                        logger.debug("prune %s: %s", addr[:10], e)
                        r.errors += 1
                else:
                    try:
                        await self._update(addr, hf, lt)
                        r.updated += 1
                    except Exception as e:
                        logger.debug("update %s: %s", addr[:10], e)
                        r.errors += 1
        return r

    # ── Multicall3 ─────────────────────────────────────────

    async def _multicall(
        self, addrs: List[str],
    ) -> List[Tuple[int, int, int, int, int, int]]:
        """getUserAccountData batch via Multicall3.aggregate3()."""
        SEL = bytes.fromhex("bf92857c")
        calls = [
            (AAVE_POOL, True, SEL + abi_encode(["address"], [a]))
            for a in addrs
        ]
        mc_data = (
            bytes.fromhex("82ad56cb")
            + abi_encode(["(address,bool,bytes)[]"], [calls])
        )

        payload = {
            "jsonrpc": "2.0", "id": 1, "method": "eth_call",
            "params": [{"to": MULTICALL3, "data": "0x" + mc_data.hex()}, "latest"],
        }

        for attempt in range(4):
            async with self._session.post(self.rpc_url, json=payload) as resp:
                # Catch 429 at HTTP level OR text/plain rate-limit response
                if resp.status == 429:
                    wait = 2 ** attempt
                    logger.debug("rate-limited (HTTP 429), retry in %ds", wait)
                    await asyncio.sleep(wait)
                    continue

                # Read body as text first — handle text/plain rate-limit responses
                # that return HTTP 200 with a plain-text error body
                try:
                    raw_text = await resp.text()
                except Exception as exc:
                    if attempt < 3:
                        await asyncio.sleep(1)
                        continue
                    raise RuntimeError(f"RPC read failed: {exc}") from exc

                if resp.status != 200 or "429" in raw_text or "rate limit" in raw_text.lower():
                    wait = 2 ** attempt
                    logger.debug("rate-limited (body), retry in %ds", wait)
                    await asyncio.sleep(wait)
                    continue

                import json as _json
                try:
                    j = _json.loads(raw_text)
                except Exception:
                    if attempt < 3:
                        await asyncio.sleep(1)
                        continue
                    raise RuntimeError(
                        f"RPC returned non-JSON (status={resp.status}): "
                        f"{raw_text[:120]}"
                    )

                if "error" in j:
                    raise RuntimeError(str(j["error"]))
                raw = j["result"]
                if isinstance(raw, str) and raw.startswith("0x"):
                    raw = raw[2:]
                break
        else:
            raise RuntimeError("RPC: exhausted retries")

        decoded = abi_decode(["(bool,bytes)[]"], bytes.fromhex(raw))[0]
        out = []
        for ok, data in decoded:
            if not ok:
                out.append((0, 0, 0, 0, 0, 2**256 - 1))
                continue
            try:
                v = abi_decode(
                    ["uint256"] * 6,
                    data,
                )
                out.append(tuple(v))
            except Exception:
                out.append((0, 0, 0, 0, 0, 2**256 - 1))
        return out

    # ── Redis ops ──────────────────────────────────────────

    async def _prune(self, addr: str):
        ua = addr.lower()
        p = self.redis.pipeline()
        p.delete(f"aave:user:{ua}")
        p.zrem("aave:liquidatable", ua)
        p.incr(M_PRUNED)
        await p.execute()

    async def _update(self, addr: str, hf: float, lt: int):
        ua = addr.lower()
        p = self.redis.pipeline()
        p.hset(f"aave:user:{ua}", mapping={
            "health_factor": str(hf),
            "liq_threshold": str(lt),
            "reconciled_at": str(time.time()),
        })
        if 0 < hf < 1.0:
            p.zadd("aave:liquidatable", {ua: hf})
        else:
            p.zrem("aave:liquidatable", ua)
        p.incr(M_UPDATED)
        await p.execute()

    # ── Metrics ────────────────────────────────────────────

    async def _metrics(self, r: CycleResult):
        rate = r.stale / max(r.sampled, 1)
        try:
            p = self.redis.pipeline()
            p.incrby(M_VERIFIED, r.sampled)
            p.set(M_STALE_CNT, str(r.stale))
            p.set(M_STALE_RATE, str(round(rate, 4)))
            p.set(M_LAST_TS, str(time.time()))
            p.set(M_LAST_MS, str(round(r.elapsed_ms, 1)))
            p.set(M_SAMPLED, str(r.sampled))
            p.incrby(M_ERRORS, r.errors)
            await p.execute()
        except Exception as e:
            logger.warning("metrics: %s", e)


# ── Entry ─────────────────────────────────────────────────

async def main():
    r = AaveReconciler()
    await r.run_forever()

if __name__ == "__main__":
    asyncio.run(main())
