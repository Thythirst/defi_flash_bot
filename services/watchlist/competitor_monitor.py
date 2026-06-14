#!/usr/bin/env python3
"""
competitor_monitor.py — LiquidationCall event tracking + competitor intelligence.

Monitors LiquidationCall events on Aave V3 Pool.
For each liquidation:
  - Identifies the liquidator
  - Tracks borrower, assets, size
  - Maintains competitor leaderboard in Redis
  - Emits metrics for dashboard

Usage:
  python -m services.watchlist.competitor_monitor
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import urllib.request
from collections import Counter
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
    format="%(asctime)s | %(levelname)-8s | competitor | %(message)s",
)
logger = logging.getLogger("watchlist.competitor")


# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
LIQ_TOPIC = "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286"

RPC_URL = os.getenv("VALIDATOR_RPC_URL", "https://arb1.arbitrum.io/rpc")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

POLL_INTERVAL = 1.0       # seconds between polls
CHUNK_BLOCKS = 200        # blocks per getLogs query
STREAM_MAXLEN = 5000      # max entries in liquidation stream

# Known competitors (will be augmented by monitoring)
KNOWN_COMPETITORS = {
    "0x919bb308e15d": "Competitor-A",   # Minor new entrant, 8 liqs total, rank #372
    "0xc70d0c7db577": "Competitor-B",
    "0xba76a7e22b0a": "Competitor-C",
}


# ═══════════════════════════════════════════════════════════════
# RPC
# ═══════════════════════════════════════════════════════════════

def rpc_call(method: str, params: list, timeout: float = 10.0) -> Optional[dict]:
    """RPC call."""
    body = {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}
    data = json.dumps(body).encode()
    headers = {"Content-Type": "application/json", "User-Agent": "hermes-competitor/1.0"}
    try:
        req = urllib.request.Request(RPC_URL, data=data, headers=headers)
        resp = urllib.request.urlopen(req, timeout=timeout)
        return json.loads(resp.read())
    except Exception:
        return None


def get_block() -> Optional[int]:
    resp = rpc_call("eth_blockNumber", [])
    if resp and "result" in resp:
        return int(resp["result"], 16)
    return None


# ═══════════════════════════════════════════════════════════════
# COMPETITOR MONITOR
# ═══════════════════════════════════════════════════════════════

class CompetitorMonitor:
    """Tracks liquidation events and maintains competitor database."""

    def __init__(self):
        self.r = redis.from_url(REDIS_URL, decode_responses=True)
        self.last_block = 0
        self.total_liquidations = 0
        self.competitor_counts: Counter = Counter()

    def parse_liquidation(self, log: dict) -> Optional[dict]:
        """Parse a LiquidationCall log. Returns structured dict or None."""
        topics = log.get("topics", [])
        data = log.get("data", "")

        if len(topics) < 4 or len(data) < 194:
            return None

        return {
            "collateral_asset": "0x" + topics[1][26:],
            "debt_asset": "0x" + topics[2][26:],
            "borrower": "0x" + topics[3][26:],
            "debt_to_cover": str(int(data[2:66], 16)),
            "liquidated_collateral": str(int(data[66:130], 16)),
            "liquidator": "0x" + data[130:194][24:],
            "receive_a_token": str(bool(int(data[194:258], 16))),
            "tx_hash": log.get("transactionHash", ""),
            "block": int(log["blockNumber"], 16),
        }

    def process_liquidations(self, from_block: int, to_block: int) -> list[dict]:
        """Scan and process liquidation events in block range. Returns parsed events."""
        lp = {"address": AAVE_POOL, "fromBlock": hex(from_block),
              "toBlock": hex(to_block), "topics": [LIQ_TOPIC]}
        resp = rpc_call("eth_getLogs", [lp], timeout=20)
        if not resp or "result" not in resp:
            return []

        events = []
        for log in resp["result"]:
            parsed = self.parse_liquidation(log)
            if parsed:
                events.append(parsed)

        return events

    def store_liquidation(self, event: dict):
        """Store a single liquidation in Redis."""
        self.total_liquidations += 1
        liquidator = event["liquidator"]
        self.competitor_counts[liquidator] += 1

        # Update competitor leaderboard ZSET
        self.r.zincrby("arb:watchlist:competitors", 1, liquidator)

        # Store competitor info
        label = KNOWN_COMPETITORS.get(liquidator, "")
        self.r.hset(f"arb:watchlist:competitor:{liquidator}", mapping={
            "label": label,
            "total_liquidations": str(self.competitor_counts[liquidator]),
            "last_seen_ts": datetime.now(timezone.utc).isoformat(),
            "last_seen_block": str(event["block"]),
            "last_borrower": event["borrower"],
            "last_collateral": event["collateral_asset"],
            "last_debt": event["debt_asset"],
        })

        # Add to liquidation stream
        self.r.xadd("arb:watchlist:liquidations", {
            "liquidator": liquidator,
            "borrower": event["borrower"],
            "collateral_asset": event["collateral_asset"],
            "debt_asset": event["debt_asset"],
            "debt_to_cover": event["debt_to_cover"],
            "liquidated_collateral": event["liquidated_collateral"],
            "tx_hash": event["tx_hash"],
            "block": str(event["block"]),
            "ts": datetime.now(timezone.utc).isoformat(),
        }, maxlen=STREAM_MAXLEN)

        # Add borrower to watchlist (they might have remaining positions)
        self.r.zadd("arb:watchlist:active", {event["borrower"]: 0.0})
        self.r.hset(f"arb:watchlist:user:{event['borrower']}", mapping={
            "health_factor": "0.0",
            "debt_usd": "0",
            "collateral_usd": "0",
            "last_refresh_ts": str(time.time()),
            "source": f"competitor:{liquidator[:12]}",
        })

    def get_top_competitors(self, n: int = 20) -> list[tuple[str, int]]:
        """Get top N competitors by liquidation count."""
        results = self.r.zrevrange("arb:watchlist:competitors", 0, n - 1, withscores=True)
        return [(addr, int(score)) for addr, score in results]

    def get_recent_liquidations(self, n: int = 20) -> list[dict]:
        """Get last N liquidation events."""
        entries = self.r.xrevrange("arb:watchlist:liquidations", count=n)
        return [{k: v for k, v in entry[1].items()} for entry in entries]

    def run_forever(self):
        """Main loop."""
        logger.info("CompetitorMonitor starting | known_competitors=%s",
                    len(KNOWN_COMPETITORS))

        # Initialize from last block
        block = get_block()
        if block:
            self.last_block = block - CHUNK_BLOCKS

        while True:
            try:
                block = get_block()
                if not block:
                    time.sleep(1)
                    continue

                if block <= self.last_block:
                    time.sleep(POLL_INTERVAL)
                    continue

                # Scan new blocks
                events = self.process_liquidations(self.last_block + 1, block)
                self.last_block = block

                for event in events:
                    self.store_liquidation(event)
                    label = KNOWN_COMPETITORS.get(event["liquidator"],
                                                  event["liquidator"][:14])
                    logger.info("LIQ blk=%s | %s → liquidated %s | debt=%s",
                               event["block"], label, event["borrower"][:14],
                               int(event["debt_to_cover"]))

                # Status log
                if events:
                    top = self.get_top_competitors(3)
                    top_str = ", ".join(f"{addr[:10]}...({cnt})" for addr, cnt in top)
                    logger.info("blk=%s | liquidations=%s | top: %s",
                               block, len(events), top_str)

                # Periodic summary every 600 blocks
                if block % 600 == 0:
                    top10 = self.get_top_competitors(10)
                    logger.info("=== COMPETITOR LEADERBOARD (top 10) ===")
                    for addr, count in top10:
                        label = KNOWN_COMPETITORS.get(addr, "unknown")
                        logger.info("  %s %s... (%s liquidations)", label, addr[:12], count)

                time.sleep(POLL_INTERVAL)

            except KeyboardInterrupt:
                logger.info("Shutting down. Total liquidations: %s", self.total_liquidations)
                break
            except Exception as e:
                logger.error("Main loop error: %s", e)
                time.sleep(1)


if __name__ == "__main__":
    monitor = CompetitorMonitor()
    monitor.run_forever()
