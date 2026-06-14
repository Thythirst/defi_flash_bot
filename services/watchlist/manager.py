#!/usr/bin/env python3
"""
manager.py — Per-block watchlist refresh service.

Every block:
  1. Fetches top 5 users by lowest health factor from Redis ZSET
  2. Calls getUserAccountData via fastest available RPC
  3. Updates Redis ZSET score + per-user hash
  4. Prunes users with HF == MAX_UINT256 (fully exited)
  5. Emits metrics to Redis

Usage:
  python -m services.watchlist.manager
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import redis
from dotenv import load_dotenv
load_dotenv(dotenv_path=project_root / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | watchlist | %(message)s",
)
logger = logging.getLogger("watchlist.manager")


# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
SELECTOR = "0xbf92857c"
WATCHLIST_KEY = "arb:watchlist:active"
META_KEY = "arb:watchlist:meta"
METRICS_KEY = "arb:watchlist:metrics"
USER_PREFIX = "arb:watchlist:user:"
MAX_UINT256 = 2**256 - 1

# RPC endpoints in priority order for refresh
RPC_URLS = [
    os.getenv("VALIDATOR_RPC_URL", "https://arb1.arbitrum.io/rpc"),
    os.getenv("DRPC_RPC_URL", ""),
    os.getenv("ANKR_RPC_URL", ""),
    os.getenv("QUICKNODE_HTTP_URL", ""),
]
RPC_URLS = [u for u in RPC_URLS if u]

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Refresh settings
REFRESH_COUNT = 5        # Users to refresh per block
BLOCK_POLL_INTERVAL = 0.25  # seconds between block checks
MAX_RETRIES = 2


# ═══════════════════════════════════════════════════════════════
# RPC
# ═══════════════════════════════════════════════════════════════

def call_rpc(url: str, method: str, params: list, timeout: float = 5.0) -> Optional[str]:
    """Single RPC call. Returns result or None."""
    body = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    data = json.dumps(body).encode()
    headers = {"Content-Type": "application/json"}
    if "arbitrum.io" in url:
        headers["User-Agent"] = "hermes-watchlist/1.0"
    if "drpc.live" in url and method == "eth_call" and isinstance(params[0], dict):
        params[0]["gas"] = "0xfffff"
    try:
        req = urllib.request.Request(url, data=data, headers=headers)
        resp = urllib.request.urlopen(req, timeout=timeout)
        result = json.loads(resp.read())
        return result.get("result")
    except Exception:
        return None


def get_block_number(rpc_url: str) -> Optional[int]:
    """Get current block number."""
    result = call_rpc(rpc_url, "eth_blockNumber", [])
    if result:
        return int(result, 16)
    return None


def get_user_account_data(address: str, rpc_url: str) -> Optional[dict]:
    """Call getUserAccountData. Returns parsed dict or None."""
    padded = address[2:].lower().rjust(64, "0")
    call_data = SELECTOR + padded
    result = call_rpc(rpc_url, "eth_call", [{"to": AAVE_POOL, "data": call_data}, "latest"])
    if result and len(result) >= 386:
        try:
            return {
                "health_factor": int(result[322:386], 16) / 1e18,
                "debt_usd": int(result[66:130], 16) / 1e8,
                "collateral_usd": int(result[2:66], 16) / 1e8,
            }
        except (ValueError, IndexError):
            pass
    return None


# ═══════════════════════════════════════════════════════════════
# WATCHLIST MANAGER
# ═══════════════════════════════════════════════════════════════

class WatchlistManager:
    """Per-block watchlist maintenance."""

    def __init__(self):
        self.r = redis.from_url(REDIS_URL, decode_responses=True)
        self.last_block = 0
        self.stats = {
            "total_refreshed": 0,
            "total_pruned": 0,
            "rpc_failures": 0,
            "peak_watchlist_size": 0,
        }

    def refresh_user(self, address: str) -> Optional[dict]:
        """Refresh single user. Try RPCs in order. Returns parsed data or None."""
        for rpc_url in RPC_URLS:
            data = get_user_account_data(address, rpc_url)
            if data:
                return data
            self.stats["rpc_failures"] += 1
        return None

    def update_redis(self, address: str, data: dict):
        """Update Redis with refreshed data."""
        hf = data["health_factor"]

        if hf >= MAX_UINT256 / 1e18:
            # Fully exited — prune
            self.r.zrem(WATCHLIST_KEY, address)
            self.r.delete(f"{USER_PREFIX}{address}")
            self.stats["total_pruned"] += 1
            logger.debug("PRUNE %s (HF=MAX, exited)", address[:14])
        else:
            # Update ZSET score + user hash
            self.r.zadd(WATCHLIST_KEY, {address: hf})
            self.r.hset(f"{USER_PREFIX}{address}", mapping={
                "health_factor": str(hf),
                "debt_usd": str(data["debt_usd"]),
                "collateral_usd": str(data["collateral_usd"]),
                "last_refresh_ts": str(time.time()),
                "last_refresh_block": str(self.last_block),
            })
            self.stats["total_refreshed"] += 1

    def refresh_top_n(self) -> dict:
        """Refresh top N users by lowest HF. Returns operation stats."""
        t0 = time.time()

        # Get top N by lowest score (HF)
        top_users = self.r.zrange(WATCHLIST_KEY, 0, REFRESH_COUNT - 1)
        watchlist_size = self.r.zcard(WATCHLIST_KEY)

        refreshed = 0
        pruned = 0
        candidates = []  # Users with HF < 1.0 for liquidation pipeline

        for address in top_users:
            data = self.refresh_user(address)
            if data:
                self.update_redis(address, data)
                refreshed += 1
                if data["health_factor"] < 1.0:
                    candidates.append({"address": address, **data})

        # Update peak
        if watchlist_size > self.stats["peak_watchlist_size"]:
            self.stats["peak_watchlist_size"] = watchlist_size

        # Update meta
        self.r.hset(META_KEY, mapping={
            "last_refresh_block": str(self.last_block),
            "last_refresh_ts": datetime.now(timezone.utc).isoformat(),
            "total_refreshed": str(self.stats["total_refreshed"]),
            "total_pruned": str(self.stats["total_pruned"]),
            "watchlist_size": str(watchlist_size),
        })

        latency_ms = (time.time() - t0) * 1000

        # Emit metrics
        metrics = {
            "refresh_latency_ms": str(round(latency_ms, 1)),
            "watchlist_size": str(watchlist_size),
            "refreshed_this_block": str(refreshed),
            "pruned_this_block": str(pruned),
            "rpc_failures_total": str(self.stats["rpc_failures"]),
            "candidates": json.dumps(candidates) if candidates else "",
        }
        self.r.hset(METRICS_KEY, mapping=metrics)

        return {
            "latency_ms": round(latency_ms, 1),
            "watchlist_size": watchlist_size,
            "refreshed": refreshed,
            "pruned": pruned,
            "candidates": len(candidates),
            "block": self.last_block,
        }

    def run_forever(self):
        """Main loop — poll blocks, refresh watchlist."""
        logger.info("WatchlistManager starting | RPCs=%s | refresh_count=%s",
                    len(RPC_URLS), REFRESH_COUNT)

        while True:
            try:
                # Get current block
                for rpc_url in RPC_URLS:
                    block = get_block_number(rpc_url)
                    if block:
                        break
                else:
                    logger.warning("All RPCs failed for block number")
                    time.sleep(1)
                    continue

                if block == self.last_block:
                    time.sleep(BLOCK_POLL_INTERVAL)
                    continue

                self.last_block = block

                # Refresh top N
                result = self.refresh_top_n()

                # Log every 100 blocks
                if block % 100 == 0:
                    logger.info("blk=%s | size=%s | refresh=%sms | pruned=%s | candidates=%s",
                               block, result["watchlist_size"], result["latency_ms"],
                               self.stats["total_pruned"], result["candidates"])

                # Push candidates to liquidation pipeline
                if result["candidates"] > 0:
                    self._push_candidates()

                time.sleep(BLOCK_POLL_INTERVAL)

            except KeyboardInterrupt:
                logger.info("Shutting down. Final stats: %s", self.stats)
                break
            except Exception as e:
                logger.error("Main loop error: %s", e)
                time.sleep(1)

    def _push_candidates(self):
        """Push HF < 1.0 candidates to the liquidation pipeline."""
        candidates_str = self.r.hget(METRICS_KEY, "candidates")
        if not candidates_str:
            return
        try:
            candidates = json.loads(candidates_str)
            for c in candidates:
                # Publish to Redis pub/sub for pre_liq_engine
                self.r.publish("arb:signals:liquidation", json.dumps({
                    "source": "watchlist",
                    "borrower": c["address"],
                    "health_factor": c["health_factor"],
                    "debt_usd": c["debt_usd"],
                }))
                # Also add to stream for durability
                self.r.xadd("arb:watchlist:candidates", {
                    "borrower": c["address"],
                    "health_factor": str(c["health_factor"]),
                    "debt_usd": str(c["debt_usd"]),
                    "ts": datetime.now(timezone.utc).isoformat(),
                }, maxlen=1000)
                logger.warning("🚨 CANDIDATE: %s HF=%.4f debt=$%.2f",
                             c["address"][:14], c["health_factor"], c["debt_usd"])
        except json.JSONDecodeError:
            pass


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    mgr = WatchlistManager()
    mgr.run_forever()
