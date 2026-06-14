#!/usr/bin/env python3
"""
bootstrap.py — Progressive Watchlist bootstrap.

Loads all known Aave-interacting addresses from:
  - Supply-event datasets (/tmp/fresh_borrowers.json)
  - Borrow-event datasets (/tmp/borrowers_30d_raw.json)
  - Historical liquidation borrowers (on-chain, last 7 days)
  - Existing Redis borrower sets (arb:borrowers:*)

Calls getUserAccountData in parallel across 4 RPCs.
Filters: debt > $1,000 AND healthFactor < 2.0.
Stores in Redis ZSET arb:watchlist:active (scored by HF ascending).

Usage:
  python -m services.watchlist.bootstrap          # Full bootstrap
  python -m services.watchlist.bootstrap --daily   # Daily re-bootstrap (last 24h events)
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    format="%(asctime)s | %(levelname)-8s | bootstrap | %(message)s",
)
logger = logging.getLogger("bootstrap")


# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"

RPC_ENDPOINTS = {
    "public_arb": os.getenv("VALIDATOR_RPC_URL", "https://arb1.arbitrum.io/rpc"),
    "drpc": os.getenv("DRPC_RPC_URL", ""),
    "ankr": os.getenv("ANKR_RPC_URL", ""),
    "quicknode": os.getenv("QUICKNODE_HTTP_URL", ""),
}

# Remove empty endpoints
RPC_ENDPOINTS = {k: v for k, v in RPC_ENDPOINTS.items() if v}

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

# getUserAccountData selector
SELECTOR = "0xbf92857c"

# Filters
MIN_DEBT_USD = 1000.0
MAX_HF = 2.0

# Concurrency
MAX_WORKERS = 16
BATCH_SIZE = 500


# ═══════════════════════════════════════════════════════════════
# RPC CLIENT
# ═══════════════════════════════════════════════════════════════

class MultiRPCClient:
    """Parallel RPC client with automatic failover."""

    def __init__(self, endpoints: dict[str, str]):
        self.endpoints = endpoints
        self.stats: dict[str, int] = {name: 0 for name in endpoints}

    def _call_single(self, endpoint_name: str, url: str, params: list) -> Optional[str]:
        """Single RPC call with 5s timeout."""
        import urllib.request
        body = {"jsonrpc": "2.0", "method": "eth_call", "params": params, "id": 1}
        data = json.dumps(body).encode()
        headers = {"Content-Type": "application/json"}
        if "arbitrum.io" in url:
            headers["User-Agent"] = "hermes-watchlist/1.0"
        try:
            req = urllib.request.Request(url, data=data, headers=headers)
            resp = urllib.request.urlopen(req, timeout=5)
            result = json.loads(resp.read())
            if "result" in result:
                self.stats[endpoint_name] += 1
                return result["result"]
        except Exception:
            pass
        return None

    def get_user_account_data(self, address: str) -> Optional[dict]:
        """Call getUserAccountData on any available RPC. Returns parsed dict or None."""
        padded = address[2:].lower().rjust(64, "0")
        call_data = SELECTOR + padded
        params = [{"to": AAVE_POOL, "data": call_data}, "latest"]

        for name, url in self.endpoints.items():
            # dRPC needs explicit gas
            if "drpc" in name:
                params[0]["gas"] = "0xfffff"
            else:
                params[0].pop("gas", None)

            result = self._call_single(name, url, params)
            if result and len(result) >= 386:
                try:
                    return {
                        "address": address,
                        "collateral_usd": int(result[2:66], 16) / 1e8,
                        "debt_usd": int(result[66:130], 16) / 1e8,
                        "available_borrow_usd": int(result[130:194], 16) / 1e8,
                        "liq_threshold_bps": int(result[194:258], 16),
                        "ltv_bps": int(result[258:322], 16),
                        "health_factor": int(result[322:386], 16) / 1e18,
                    }
                except (ValueError, IndexError):
                    pass
        return None

    def batch_call(self, addresses: list[str]) -> list[dict]:
        """Parallel batch call across all addresses. Returns list of parsed results."""
        results = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(self.get_user_account_data, addr): addr for addr in addresses}
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception:
                    pass
        return results


# ═══════════════════════════════════════════════════════════════
# ADDRESS LOADING
# ═══════════════════════════════════════════════════════════════

def load_supply_events(path: str = "/tmp/fresh_borrowers.json") -> set[str]:
    """Load Supply-event addresses."""
    try:
        with open(path) as f:
            data = json.load(f)
        users = set(data.get("users", []))
        logger.info("Supply events: %s addresses from %s", f"{len(users):,}", path)
        return users
    except Exception as e:
        logger.warning("Supply events load failed: %s", e)
        return set()


def load_borrow_events(path: str = "/tmp/borrowers_30d_raw.json") -> set[str]:
    """Load Borrow-event addresses."""
    try:
        with open(path) as f:
            data = json.load(f)
        users = set(data.get("users", []))
        logger.info("Borrow events: %s addresses from %s", f"{len(users):,}", path)
        return users
    except Exception as e:
        logger.warning("Borrow events load failed: %s", e)
        return set()


def load_liquidation_borrowers() -> set[str]:
    """Load borrowers liquidated in the last 7 days from on-chain."""
    import urllib.request
    LIQ_TOPIC = "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286"

    # Get current block
    rpc = RPC_ENDPOINTS.get("public_arb", list(RPC_ENDPOINTS.values())[0])
    body = {"jsonrpc": "2.0", "method": "eth_blockNumber", "params": [], "id": 1}
    try:
        req = urllib.request.Request(rpc, data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
        current = int(json.loads(urllib.request.urlopen(req, timeout=10).read())["result"], 16)
    except Exception:
        logger.warning("Cannot get current block for liquidation scan")
        return set()

    start = current - 2_419_200  # ~7 days
    lp = {"address": AAVE_POOL, "fromBlock": hex(start), "toBlock": hex(current),
          "topics": [LIQ_TOPIC]}
    body = {"jsonrpc": "2.0", "method": "eth_getLogs", "params": [lp], "id": 1}
    try:
        req = urllib.request.Request(rpc, data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json",
                                              "User-Agent": "hermes-watchlist/1.0"})
        logs = json.loads(urllib.request.urlopen(req, timeout=30).read()).get("result", [])
        users = set()
        for log in logs:
            if len(log.get("topics", [])) >= 4:
                users.add("0x" + log["topics"][3][26:])
        logger.info("Liquidation borrowers (7d): %s addresses", f"{len(users):,}")
        return users
    except Exception as e:
        logger.warning("Liquidation scan failed: %s", e)
        return set()


def load_redis_borrowers() -> set[str]:
    """Load borrowers from existing Redis sets."""
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        keys = r.keys("arb:borrowers:*")
        users = set()
        for key in keys:
            members = r.smembers(key)
            users.update(members)
        logger.info("Redis borrowers: %s addresses from %s keys", f"{len(users):,}", len(keys))
        r.close()
        return users
    except Exception as e:
        logger.warning("Redis load failed: %s", e)
        return set()


# ═══════════════════════════════════════════════════════════════
# MAIN BOOTSTRAP
# ═══════════════════════════════════════════════════════════════

def bootstrap(daily: bool = False) -> dict:
    """
    Run full bootstrap. Returns stats dict.

    If daily=True, also scan last 24h Pool events for new addresses.
    """
    t0 = time.time()

    # 1. Load all address sources
    logger.info("=== PHASE 1: Address Loading ===")
    all_addrs = set()
    all_addrs |= load_supply_events()
    all_addrs |= load_borrow_events()
    all_addrs |= load_liquidation_borrowers()
    all_addrs |= load_redis_borrowers()

    # Remove invalid addresses (must be 0x + 40 hex chars)
    all_addrs = {a for a in all_addrs if a.startswith("0x") and len(a) == 42}

    n_loaded = len(all_addrs)
    logger.info("Total unique addresses loaded: %s", f"{n_loaded:,}")

    # 2. Parallel getUserAccountData
    logger.info("=== PHASE 2: On-Chain Verification ===")
    client = MultiRPCClient(RPC_ENDPOINTS)

    active = []
    addresses = list(all_addrs)
    batches = [addresses[i:i + BATCH_SIZE] for i in range(0, len(addresses), BATCH_SIZE)]

    for i, batch in enumerate(batches):
        batch_results = client.batch_call(batch)
        for r in batch_results:
            if r["debt_usd"] >= MIN_DEBT_USD and r["health_factor"] < MAX_HF:
                active.append(r)
        if (i + 1) % 5 == 0:
            elapsed = time.time() - t0
            pct = (i + 1) / len(batches) * 100
            logger.info("  Batch %s/%s (%.0f%%) | %ss elapsed | %s active | RPC: %s",
                        i + 1, len(batches), pct, f"{elapsed:.0f}",
                        len(active), {k: v for k, v in client.stats.items()})

    # 3. Store in Redis
    logger.info("=== PHASE 3: Redis Storage ===")
    r = redis.from_url(REDIS_URL, decode_responses=True)

    watchlist_key = "arb:watchlist:active"
    # Clear old watchlist
    r.delete(watchlist_key)

    if active:
        pipe = r.pipeline()
        for entry in active:
            pipe.zadd(watchlist_key, {entry["address"]: entry["health_factor"]})
        pipe.execute()

        # Store full data in hash per user
        for entry in active:
            r.hset(f"arb:watchlist:user:{entry['address']}", mapping={
                "health_factor": str(entry["health_factor"]),
                "debt_usd": str(entry["debt_usd"]),
                "collateral_usd": str(entry["collateral_usd"]),
                "liq_threshold_bps": str(entry["liq_threshold_bps"]),
                "last_refresh_ts": str(time.time()),
            })

    # Meta
    r.hset("arb:watchlist:meta", mapping={
        "bootstrap_ts": datetime.now(timezone.utc).isoformat(),
        "total_loaded": str(n_loaded),
        "total_active": str(len(active)),
        "bootstrap_duration_s": str(round(time.time() - t0, 1)),
        "rpc_stats": json.dumps(client.stats),
        "mode": "daily" if daily else "full",
    })

    r.close()

    elapsed = time.time() - t0
    logger.info("=== BOOTSTRAP COMPLETE: %.0fs ===", elapsed)
    logger.info("  Loaded: %s addresses", f"{n_loaded:,}")
    logger.info("  Active (debt > $%s, HF < %s): %s", f"{MIN_DEBT_USD:,.0f}", MAX_HF, len(active))
    logger.info("  RPC utilization: %s", client.stats)
    logger.info("  Watchlist key: %s", watchlist_key)

    # 4. Export CSV for audit
    if active:
        csv_path = "/tmp/watchlist_bootstrap.csv"
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["address", "health_factor", "debt_usd",
                                              "collateral_usd", "liq_threshold_bps",
                                              "available_borrow_usd", "ltv_bps"])
            w.writeheader()
            for entry in sorted(active, key=lambda x: x["health_factor"]):
                w.writerow(entry)
        logger.info("  CSV export: %s", csv_path)

    return {
        "elapsed_s": round(elapsed, 1),
        "loaded": n_loaded,
        "active": len(active),
        "rpc_stats": client.stats,
        "watchlist_key": watchlist_key,
    }


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Progressive Watchlist Bootstrap")
    parser.add_argument("--daily", action="store_true", help="Daily re-bootstrap mode")
    args = parser.parse_args()
    result = bootstrap(daily=args.daily)
    print(json.dumps(result, indent=2))
