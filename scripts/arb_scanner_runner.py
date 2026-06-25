#!/usr/bin/env python3
"""
arb_scanner_runner.py — Standalone DEX arbitrage scanner, logging to SQLite.

Runs ArbitrageScanner from services/rev2/dex_arbitrage.py in its own loop.
Collects: opportunities found, gas prices, competitor arb transactions.

Usage:
    venv/bin/python3 scripts/arb_scanner_runner.py

Env:
    RPC_URL — Arbitrum HTTP RPC (defaults to rpc_provider rotation)
    DRY_RUN=1 — scan only, no execution (default)
    SCAN_INTERVAL — seconds between scans (default 5)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Project setup — must be before local imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "services" / "rev2"))

from dotenv import load_dotenv
from async_web3 import AsyncRPCClient

load_dotenv(project_root / ".env")

from multi_dex_router import MultiDexRouter
from dex_arbitrage import (
    ArbitrageScanner, ArbOpportunity, MIN_PROFIT_USD,
    MONITORED_PAIRS, WETH, ARB_GAS_UNITS,
)
from execution_guards import PriceRegistry
from hot_path_fix import SharedState

# Chainlink price feed addresses for the scanner's monitored assets (Arbitrum)
# latestAnswer() returns 8-decimal price (e.g. 164000000000 = $1640.00)
CHAINLINK_FEEDS = {
    "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # ETH/USD → WETH
    "0x6ce185860a4963106506C203335A2910413708e9": "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f",  # BTC/USD → WBTC
    "0xb2A824043730FE05F3DA2efaFa1CBbe83fa548D6": "0x912CE59144191C1204E64559FE8253a0e49E6548",  # ARB/USD → ARB
}
# Aave V3 oracle — used for USDC native price (Chainlink lacks direct feed)
AAVE_ORACLE = "0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7"
AAVE_GET_PRICE_SEL = bytes.fromhex("b3596f07")  # getAssetPrice(address)
USDC_NATIVE = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"

async def _feed_prices(w3, price_reg: PriceRegistry) -> None:
    """Poll Chainlink + Aave Oracle every 60s to feed the PriceRegistry."""
    first = True
    failures = 0
    while True:
        try:
            updated = 0
            # Chainlink latestAnswer() for WETH, WBTC, ARB
            for feed_addr, asset_addr in CHAINLINK_FEEDS.items():
                try:
                    result = await w3.eth.call({
                        "to": feed_addr,
                        "data": "0x50d25bcd",  # latestAnswer()
                    })
                    price = int(result.hex(), 16) if result != b"" else 0
                    if price > 0:
                        price_reg.update_price(asset_addr, price)
                        updated += 1
                except Exception:
                    pass
            # Aave Oracle for USDC native
            try:
                calldata = AAVE_GET_PRICE_SEL + w3.codec.encode(["address"], [USDC_NATIVE])
                result = await w3.eth.call({"to": AAVE_ORACLE, "data": calldata})
                price = int.from_bytes(result, "big") if result != b"" else 0
                if price > 0:
                    price_reg.update_price(USDC_NATIVE, price)
                    updated += 1
            except Exception:
                pass
            if updated and first:
                logger.info(f"[PriceFeed] Initialized — {updated} price feeds live")
                first = False
            failures = 0
        except asyncio.CancelledError:
            return
        except Exception as e:
            failures += 1
            if failures == 1:
                logger.warning(f"[PriceFeed] fetch failed: {e} — will retry")
        await asyncio.sleep(60)


async def _feed_gas(w3, shared_state: SharedState) -> None:
    """Poll fee_history every block (~250ms on Arbitrum) for SharedState."""
    last_block = 0
    failures = 0
    while True:
        try:
            block_num = await w3.eth.block_number
            if block_num > last_block:
                fee_history = await w3.eth.fee_history(1, "latest", [50])
                base_fee = fee_history.get("baseFeePerGas", [100_000_000])[0]
                shared_state.on_new_block(block_num, base_fee)
                last_block = block_num
            failures = 0
        except asyncio.CancelledError:
            return
        except Exception as e:
            failures += 1
            if failures == 1:
                logger.warning(f"[GasFeed] fetch failed: {e} — will retry")
        await asyncio.sleep(1)  # poll every second, skip duplicate blocks

# ── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("arb_scanner")

# ── Config ────────────────────────────────────────────────────
DB_PATH = project_root / "data" / "arb_scanner.db"
SCAN_INTERVAL = float(os.getenv("SCAN_INTERVAL", "5"))
DRY_RUN = os.getenv("DRY_RUN", "1") == "1"
RPC_URL = os.getenv("RPC_URL", "https://arb1.arbitrum.io/rpc")

# Fallback RPC rotation for the scanner — tried in order on connection failure
SCANNER_RPC_URLS = [
    os.getenv("RPC_URL", "https://arb1.arbitrum.io/rpc"),
    "https://arbitrum-one.public.blastapi.io",
    os.getenv("DRPC_RPC_URL", ""),
    os.getenv("ALCHEMY_HTTP_URL", ""),
]
SCANNER_RPC_URLS = [u for u in SCANNER_RPC_URLS if u]  # filter empties

# Tenderly / Arbiscan tx URL for competitor analysis
ARBISCAN_TX = "https://arbiscan.io/tx"

# ── Database ──────────────────────────────────────────────────

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS opportunities (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT NOT NULL,
            token_in    TEXT NOT NULL,
            token_out   TEXT NOT NULL,
            buy_dex     TEXT NOT NULL,
            sell_dex    TEXT NOT NULL,
            buy_fee     INTEGER,
            sell_fee    INTEGER,
            amount_in   REAL NOT NULL,
            gross_profit_usd REAL,
            gas_cost_usd REAL,
            net_profit_usd   REAL,
            spread_pct  REAL,
            submitted   INTEGER DEFAULT 0,
            tx_hash     TEXT
        );

        CREATE TABLE IF NOT EXISTS gas_samples (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT NOT NULL,
            base_fee_gwei REAL,
            priority_fee_gwei REAL,
            eth_price_usd REAL
        );

        CREATE TABLE IF NOT EXISTS competitor_txns (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT NOT NULL,
            tx_hash     TEXT NOT NULL UNIQUE,
            from_addr   TEXT,
            to_addr     TEXT,
            gas_used    INTEGER,
            gas_price_gwei REAL,
            method      TEXT,
            value_eth   REAL
        );

        CREATE TABLE IF NOT EXISTS submissions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp   TEXT NOT NULL,
            opp_id      INTEGER,
            tx_hash     TEXT,
            status      TEXT NOT NULL,
            gas_used    INTEGER,
            actual_profit_usd REAL,
            revert_reason TEXT
        );

        CREATE TABLE IF NOT EXISTS daily_reports (
            date        TEXT PRIMARY KEY,
            report_json TEXT NOT NULL,
            delivered   INTEGER DEFAULT 0
        );
    """)
    conn.commit()
    return conn


def log_opportunity(conn: sqlite3.Connection, opp: ArbOpportunity):
    token_a_dec = 18  # default
    amount_in_human = opp.amount_in / (10 ** token_a_dec)
    conn.execute(
        """INSERT INTO opportunities
           (timestamp, token_in, token_out, buy_dex, sell_dex, buy_fee, sell_fee,
            amount_in, gross_profit_usd, gas_cost_usd, net_profit_usd, spread_pct)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            datetime.now(timezone.utc).isoformat(),
            opp.token_in, opp.token_out,
            opp.buy_dex, opp.sell_dex,
            opp.buy_fee, opp.sell_fee_tier,
            amount_in_human,
            round(opp.gross_profit_usd, 4),
            round(opp.gas_cost_usd, 4),
            round(opp.net_profit_usd, 4),
            round(opp.spread_pct, 4),
        ),
    )
    conn.commit()


def log_gas(conn: sqlite3.Connection, base_fee_wei: int, eth_price: Optional[float]):
    base_gwei = base_fee_wei / 1e9
    conn.execute(
        """INSERT INTO gas_samples (timestamp, base_fee_gwei, priority_fee_gwei, eth_price_usd)
           VALUES (?, ?, ?, ?)""",
        (
            datetime.now(timezone.utc).isoformat(),
            round(base_gwei, 4),
            round(base_gwei * 0.05, 4),  # priority ~5% of base on Arbitrum
            round(eth_price, 2) if eth_price else None,
        ),
    )
    conn.commit()


# ── Competitor Detection ──────────────────────────────────────

# Known MEV bot addresses on Arbitrum
KNOWN_MEV_BOTS = {
    "0x3b82d2f502a3D16eA6D0baB3469f673F57620108".lower(): "jaredfromsubway",
    "0xAE75E438DA2eE0e91e0DD7C77920aaAA421C0760".lower(): "beaverbuild",
    "0x00000000C2CFb9c2c4297D9eAf2DcF587c3f33f9".lower(): "MEV Blocker",
    "0x1f2F10D1C40777AE1Da742455c65828Ff36Df387".lower(): "rsync-builder",
}

# Common arb router addresses to watch
ARB_ROUTERS = {
    "0xE592427A0AEce92De3Edee1F18E0157C05861564".lower(): "UniV3",
    "0x1F721E2E82F6676FCE4eA07A5958cF098D339e18".lower(): "Camelot",
    "0x8A21F6768c1F8075791D08546dADF6daA0Be16eC".lower(): "SushiV3",
    "0x1b81D678ffb9C0263b24A97847620C99d213eB14".lower(): "PancakeSwapV3",
}

async def scan_competitor_block(w3: AsyncWeb3, block_number: int, conn: sqlite3.Connection):
    """Scan a single block for competitor arb transactions."""
    try:
        block = await w3.eth.get_block(block_number, full_transactions=True)
    except Exception:
        return

    for tx in block.transactions:
        to_addr = (tx.get("to") or "").lower()
        from_addr = (tx.get("from") or "").lower()

        # Check if from a known MEV bot
        bot_name = KNOWN_MEV_BOTS.get(from_addr)
        if not bot_name:
            # Check if calling a known arb router
            router_name = ARB_ROUTERS.get(to_addr)
            if not router_name:
                continue
            bot_name = f"unknown→{router_name}"

        try:
            conn.execute(
                """INSERT OR IGNORE INTO competitor_txns
                   (timestamp, tx_hash, from_addr, to_addr, gas_used, gas_price_gwei, method, value_eth)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    datetime.now(timezone.utc).isoformat(),
                    tx.hash.hex(),
                    from_addr,
                    to_addr,
                    tx.get("gas", 0),
                    (tx.get("gasPrice", 0) or 0) / 1e9,
                    bot_name,
                    (tx.get("value", 0) or 0) / 1e18,
                ),
            )
        except sqlite3.IntegrityError:
            pass  # duplicate tx_hash
    conn.commit()


# ── Scanner Loop ──────────────────────────────────────────────

async def run_scanner():
    conn = init_db()
    logger.info(f"DB initialized: {DB_PATH}")

    # RPC setup — try rotation on connect, fall back to next URL on failure
    rpc_client = None
    for url in SCANNER_RPC_URLS:
        try:
            rpc_client = AsyncRPCClient(http_url=url, request_timeout=10.0)
            await rpc_client.connect()
            block = await rpc_client.get_block_number()
            logger.info(f"RPC connected: {url[:50]}... block={block}")
            break
        except Exception as e:
            logger.warning(f"RPC {url[:40]}... failed: {e}")
            rpc_client = None
    if rpc_client is None:
        logger.error("All RPC URLs failed — exiting")
        return
    w3 = rpc_client.w3

    # MultiDexRouter (for quotes) — pass underlying w3
    multi_dex = MultiDexRouter(w3, os.getenv("BOT_ADDRESS", "0x1269800101780229B50919e1e27be62DC6279e9B"))

    # SharedState (gas oracle) — fed by _feed_gas background task
    shared_state = SharedState()

    # PriceRegistry (USD prices) — fed by _feed_prices background task
    price_reg = PriceRegistry(max_age_seconds=120.0)

    # Scanner — wired with live price + gas state
    scanner = ArbitrageScanner(multi_dex=multi_dex, shared_state=shared_state, price_reg=price_reg)

    # Stats
    scans = 0
    opps_found = 0
    raw_spreads_seen = 0
    raw_spreads_rejected = 0
    last_competitor_block = await w3.eth.block_number - 50  # start 50 blocks back
    last_gas_sample = 0

    logger.info(f"Starting scan loop — interval={SCAN_INTERVAL}s, pairs={len(MONITORED_PAIRS)}, dry_run={DRY_RUN}")
    logger.info("Collecting: opportunities, gas, competitor txns")

    # Launch background feeders for price + gas state
    # Use a separate RPC from the scanner's quote RPC (avoids rate limiting)
    feeder_url = os.getenv("FEEDER_RPC_URL", "https://arbitrum-one.public.blastapi.io")
    feeder_rpc = AsyncRPCClient(http_url=feeder_url, request_timeout=10.0)
    await feeder_rpc.connect()
    logger.info(f"Feeder RPC: {feeder_url[:40]}... block={await feeder_rpc.get_block_number()}")
    asyncio.create_task(_feed_prices(feeder_rpc.w3, price_reg))
    asyncio.create_task(_feed_gas(feeder_rpc.w3, shared_state))

    while True:
        try:
            t_start = time.monotonic()

            # ── Scan for arb opportunities ──────────────────
            try:
                opp = await asyncio.wait_for(scanner.scan_once(), timeout=min(SCAN_INTERVAL, 8.0))
            except asyncio.TimeoutError:
                logger.debug("Scan timed out — quotes too slow, skipping cycle")
                opp = None
            scans += 1

            if opp and opp.is_profitable():
                opps_found += 1
                log_opportunity(conn, opp)
                logger.info(
                    f"[Arb] #{opps_found} {opp.token_in[:8]}→{opp.token_out[:8]} "
                    f"buy={opp.buy_dex} sell={opp.sell_dex} "
                    f"net=${opp.net_profit_usd:.2f} spread={opp.spread_pct:.3f}%"
                )

            # ── Gas sample (every 30s) ──────────────────────
            now = time.monotonic()
            if now - last_gas_sample > 30:
                try:
                    fee_history = await w3.eth.fee_history(1, "latest", [50])
                    base_fee = fee_history.get("baseFeePerGas", [100_000_000])[0]
                    log_gas(conn, base_fee, None)
                    last_gas_sample = now
                except Exception:
                    pass

            # ── Competitor scan (every block) ───────────────
            current_block = await w3.eth.block_number
            if current_block > last_competitor_block:
                for blk in range(last_competitor_block + 1, min(current_block + 1, last_competitor_block + 4)):
                    await scan_competitor_block(w3, blk, conn)
                last_competitor_block = current_block

            # ── Stats every 60 scans ────────────────────────
            if scans % 60 == 0:
                s = scanner.stats
                logger.info(
                    f"[Stats] scans={scans} opps={opps_found} "
                    f"raw_spreads={s['raw_spreads_seen']} "
                    f"rejected={s['raw_spreads_rejected']} "
                    f"scan_time={((time.monotonic()-t_start)*1000):.0f}ms"
                )

            # ── Sleep ───────────────────────────────────────
            elapsed = time.monotonic() - t_start
            sleep_time = max(0.1, SCAN_INTERVAL - elapsed)
            await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            logger.info("Scanner cancelled — shutting down")
            break
        except Exception as e:
            logger.error(f"Scan error: {e}", exc_info=True)
            await asyncio.sleep(5)

    conn.close()


if __name__ == "__main__":
    asyncio.run(run_scanner())
