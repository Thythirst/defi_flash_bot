#!/usr/bin/env python3
"""
oracle_monitor.py — Chainlink price deviation → watchlist refresh trigger.

Monitors AnswerUpdated events on 8 AggregatorProxy contracts for Aave reserve
assets. When price deviation exceeds threshold (default 2%), triggers:
  1. Force-refresh watchlist users holding affected collateral
  2. Optionally scan full bootstrap universe for affected collateral exposure
  3. Push HF < 1.0 candidates to liquidation pipeline

Usage:
  python -m services.watchlist.oracle_monitor
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
    format="%(asctime)s | %(levelname)-8s | oracle | %(message)s",
)
logger = logging.getLogger("watchlist.oracle")


# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
SELECTOR = "0xbf92857c"

# Chainlink AggregatorProxy addresses for Aave reserve assets (Arbitrum)
# Format: symbol -> (aggregator_address, decimals, collateral_flag)
FEEDS: dict[str, tuple[str, int, bool]] = {
    "WETH":   ("0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612", 8, True),
    "WBTC":   ("0x6ce185860a4963106506C203335A2910413708e9", 8, True),
    "USDC":   ("0x50834F3163758fcC1Df9973b6e91f0F0F0434aD3", 8, False),
    "USDT":   ("0x3f3f5dF88dC9F13eaCF39f76967e5ae6a44E2713", 8, False),
    "DAI":    ("0xc5C8E77B397E531B8EC06BFb0048326F1d3aC21c", 8, False),
    "ARB":    ("0xb2A82404358D0F8eE4f33A9c4aE3CFa01dD42857", 8, True),
    "LINK":   ("0x86E53CF1B870786351Da77A57575e79CB55812CB", 8, True),
    "wstETH": ("0xb523AE262D20A936BC152e60239920D1e3a3c3Ca", 8, True),
}

# Event signature
ANSWER_UPDATED_TOPIC = "0x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f"

RPC_URL = os.getenv("VALIDATOR_RPC_URL", "https://arb1.arbitrum.io/rpc")
RPC_URLS = [
    RPC_URL,
    os.getenv("DRPC_RPC_URL", ""),
    os.getenv("ANKR_RPC_URL", ""),
]
RPC_URLS = [u for u in RPC_URLS if u]
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# Thresholds
DEVIATION_THRESHOLD = 0.02     # 2% price deviation triggers refresh
FULL_SCAN_THRESHOLD = 0.05     # 5% triggers full bootstrap scan
POLL_INTERVAL = 2.0            # seconds between getLogs polls
CHUNK_BLOCKS = 50              # blocks per getLogs query


# ═══════════════════════════════════════════════════════════════
# RPC
# ═══════════════════════════════════════════════════════════════

def rpc_call(method: str, params: list, timeout: float = 10.0) -> Optional[dict]:
    """RPC call with automatic failover. Tries RPC_URLS in order. Returns full JSON response or None."""
    body = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    data = json.dumps(body).encode()
    headers = {"Content-Type": "application/json", "User-Agent": "hermes-oracle/1.0"}
    for url in RPC_URLS:
        try:
            # dRPC needs explicit gas on eth_call
            if "drpc.live" in url and method == "eth_call" and isinstance(params[0], dict):
                params[0]["gas"] = "0xfffff"
            req = urllib.request.Request(url, data=data, headers=headers)
            resp = urllib.request.urlopen(req, timeout=timeout)
            result = json.loads(resp.read())
            if "result" in result:
                return result
        except Exception as e:
            logger.debug("RPC error (%s): %s", url[:30], e)
    return None


# ═══════════════════════════════════════════════════════════════
# ORACLE MONITOR
# ═══════════════════════════════════════════════════════════════

class OracleMonitor:
    """Monitors Chainlink feeds and triggers watchlist refreshes."""

    def __init__(self):
        self.r = redis.from_url(REDIS_URL, decode_responses=True)
        self.last_block = 0
        self.prices: dict[str, tuple[float, int]] = {}  # symbol -> (price, block)
        self.deviations_triggered = 0
        self.scans_triggered = 0

    def get_block(self) -> Optional[int]:
        """Get current block number."""
        resp = rpc_call("eth_blockNumber", [])
        if resp and "result" in resp:
            return int(resp["result"], 16)
        return None

    def scan_answers(self, from_block: int, to_block: int) -> list[dict]:
        """Scan AnswerUpdated events across all feeds. Returns list of events."""
        events = []
        for sym, (agg_addr, _, _) in FEEDS.items():
            lp = {"address": agg_addr, "fromBlock": hex(from_block),
                  "toBlock": hex(to_block), "topics": [ANSWER_UPDATED_TOPIC]}
            resp = rpc_call("eth_getLogs", [lp], timeout=15)
            if resp and "result" in resp:
                for log in resp["result"]:
                    # Data: current (uint256), roundId (uint256), updatedAt (uint256)
                    data = log.get("data", "")
                    if len(data) >= 130:
                        answer = int(data[2:66], 16)
                        events.append({
                            "symbol": sym,
                            "aggregator": agg_addr,
                            "answer": answer,
                            "block": int(log["blockNumber"], 16),
                        })
        return events

    def check_deviation(self, symbol: str, new_price: float) -> Optional[float]:
        """Check if price deviated from last known. Returns deviation fraction or None."""
        if symbol in self.prices:
            old_price, _ = self.prices[symbol]
            if old_price > 0:
                deviation = abs(new_price - old_price) / old_price
                return deviation
        return None

    def trigger_watchlist_refresh(self, symbol: str, deviation: float, new_price: float):
        """Refresh watchlist users holding the affected collateral asset."""
        self.deviations_triggered += 1

        # Get users with this collateral from Redis set
        coll_key = f"arb:watchlist:collateral:{symbol}"
        users = self.r.smembers(coll_key)

        if not users:
            logger.info("DEV %.1f%% %s | No users with this collateral in watchlist",
                       deviation * 100, symbol)
            return

        # Force refresh these users
        logger.warning("DEV %.1f%% %s | Refreshing %s affected watchlist users",
                      deviation * 100, symbol, len(users))

        refreshed = 0
        for user in users:
            padded = user[2:].lower().rjust(64, "0")
            call_data = SELECTOR + padded
            resp = rpc_call("eth_call", [{"to": AAVE_POOL, "data": call_data}, "latest"])
            if resp and "result" in resp:
                result = resp["result"]
                if len(result) >= 386:
                    hf = int(result[322:386], 16) / 1e18
                    debt = int(result[66:130], 16) / 1e8
                    coll = int(result[2:66], 16) / 1e8

                    if hf >= (2**256 - 1) / 1e18:
                        # Prune
                        self.r.zrem("arb:watchlist:active", user)
                        self.r.srem(coll_key, user)
                    else:
                        self.r.zadd("arb:watchlist:active", {user: hf})
                        self.r.hset(f"arb:watchlist:user:{user}", mapping={
                            "health_factor": str(hf),
                            "debt_usd": str(debt),
                            "collateral_usd": str(coll),
                            "last_refresh_ts": str(time.time()),
                            "last_trigger": f"oracle:{symbol}",
                        })
                        refreshed += 1

                        if hf < 1.0:
                            logger.warning("🚨 ORACLE CANDIDATE: %s HF=%.4f debt=$%.2f",
                                         user[:14], hf, debt)
                            self.r.publish("arb:signals:liquidation", json.dumps({
                                "source": "oracle_monitor",
                                "trigger": symbol,
                                "borrower": user,
                                "health_factor": hf,
                                "debt_usd": debt,
                            }))

        # Update price
        self.prices[symbol] = (new_price, self.last_block)

        # Log
        self.r.xadd("arb:watchlist:oracle_events", {
            "symbol": symbol,
            "deviation_pct": str(round(deviation * 100, 2)),
            "users_affected": str(len(users)),
            "users_refreshed": str(refreshed),
            "ts": datetime.now(timezone.utc).isoformat(),
        }, maxlen=1000)

    def run_forever(self):
        """Main loop."""
        logger.info("OracleMonitor starting | feeds=%s | threshold=%.1f%%",
                    len(FEEDS), DEVIATION_THRESHOLD * 100)

        # Initialize prices
        block = self.get_block()
        if block:
            self.last_block = block - CHUNK_BLOCKS

        while True:
            try:
                block = self.get_block()
                if not block:
                    time.sleep(1)
                    continue

                if block <= self.last_block:
                    time.sleep(POLL_INTERVAL)
                    continue

                # Scan new blocks
                events = self.scan_answers(self.last_block + 1, block)
                self.last_block = block

                for evt in events:
                    sym = evt["symbol"]
                    decimals = FEEDS[sym][1]
                    price = evt["answer"] / (10 ** decimals)

                    deviation = self.check_deviation(sym, price)
                    if deviation is not None and deviation >= DEVIATION_THRESHOLD:
                        self.trigger_watchlist_refresh(sym, deviation, price)
                    else:
                        self.prices[sym] = (price, block)

                if events:
                    logger.debug("blk=%s | %s AnswerUpdated events", block, len(events))

                # Status log every 5 minutes
                if block % 1200 == 0:
                    logger.info("blk=%s | feeds=%s | triggers=%s",
                               block, len(self.prices), self.deviations_triggered)

                time.sleep(POLL_INTERVAL)

            except KeyboardInterrupt:
                logger.info("Shutting down. Triggers: %s", self.deviations_triggered)
                break
            except Exception as e:
                logger.error("Main loop error: %s", e)
                time.sleep(1)


if __name__ == "__main__":
    monitor = OracleMonitor()
    monitor.run_forever()
