"""
test_arb_executor.py — Standalone arb scan + execute test.

Runs 10 scan rounds (1s apart), executes any profitable opportunity live.
Set ARB_DRY_RUN=1 in .env to build-but-not-send for sanity checks.

Usage:
    cd ~/defi_flash_bot && venv/bin/python scripts/test_arb_executor.py
"""

import asyncio
import logging
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "rev2"))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

ARB_EXECUTOR_ADDR = os.getenv("ARB_EXECUTOR_ADDR", "")
WALLET_ADDR       = os.getenv("BOT_ADDRESS", "")
PRIVATE_KEY       = os.getenv("BOT_PRIVATE_KEY", "")
HTTP_URL          = os.getenv("ARBITRUM_HTTP_URL") or os.getenv("ALCHEMY_HTTP_URL", "")
DRY_RUN           = os.getenv("ARB_DRY_RUN", "0") == "1"

ROUNDS = 10


async def main():
    if not ARB_EXECUTOR_ADDR:
        logger.error("ARB_EXECUTOR_ADDR not set")
        sys.exit(1)
    if not WALLET_ADDR or not PRIVATE_KEY:
        logger.error("BOT_ADDRESS / BOT_PRIVATE_KEY not set")
        sys.exit(1)
    if not HTTP_URL:
        logger.error("ARBITRUM_HTTP_URL / ALCHEMY_HTTP_URL not set")
        sys.exit(1)

    logger.info(f"ARB_EXECUTOR_ADDR : {ARB_EXECUTOR_ADDR}")
    logger.info(f"Wallet            : {WALLET_ADDR}")
    logger.info(f"RPC               : {HTTP_URL[:60]}")
    logger.info(f"DRY_RUN           : {DRY_RUN}")

    # ── Web3 ────────────────────────────────────────────────────────────────
    import aiohttp
    connector = aiohttp.TCPConnector(limit=10, keepalive_timeout=30, enable_cleanup_closed=True)
    session   = aiohttp.ClientSession(connector=connector)
    provider  = AsyncHTTPProvider(HTTP_URL)
    await provider.cache_async_session(session)
    w3 = AsyncWeb3(provider)

    block = await w3.eth.block_number
    logger.info(f"Connected — block {block}")

    # ── Sanity: verify contract owner ───────────────────────────────────────
    from dex_arbitrage import _load_arb_abi
    from web3 import Web3
    sync_w3 = Web3()
    contract = sync_w3.eth.contract(
        address=AsyncWeb3.to_checksum_address(ARB_EXECUTOR_ADDR),
        abi=_load_arb_abi(),
    )
    # On-chain read via async w3
    owner_call = contract.encode_abi("owner", args=[])
    result = await w3.eth.call({"to": ARB_EXECUTOR_ADDR, "data": owner_call})
    owner_addr = AsyncWeb3.to_checksum_address("0x" + result.hex()[-40:])
    wallet_cs  = AsyncWeb3.to_checksum_address(WALLET_ADDR)
    if owner_addr.lower() != wallet_cs.lower():
        logger.error(f"Contract owner {owner_addr} != wallet {wallet_cs} — aborting")
        sys.exit(1)
    logger.info(f"Contract owner check OK ({owner_addr})")

    # ── Wallet ETH balance ───────────────────────────────────────────────────
    bal_wei = await w3.eth.get_balance(wallet_cs)
    logger.info(f"Wallet ETH balance: {bal_wei / 1e18:.5f} ETH")
    if bal_wei < int(0.002 * 1e18):
        logger.warning("Low ETH balance — may not cover gas for arb tx")

    # ── Build components ─────────────────────────────────────────────────────
    from multi_dex_router import MultiDexRouter
    from dex_arbitrage import ArbitrageScanner, ArbExecutor

    multi_dex = MultiDexRouter(w3, ARB_EXECUTOR_ADDR)
    scanner   = ArbitrageScanner(
        multi_dex     = multi_dex,
        shared_state  = None,   # no gas oracle — uses fallback 0.1 gwei
        price_reg     = None,   # no price registry — stablecoins default to $1, ETH to $1640
        min_profit_usd = 2.0,   # lower threshold for testing
    )
    executor  = ArbExecutor(
        w3                   = w3,
        arb_executor_address = ARB_EXECUTOR_ADDR,
        wallet               = WALLET_ADDR,
        private_key          = PRIVATE_KEY,
        shared_state         = None,
    )

    logger.info("Warming up executor (TCP sessions + nonce)...")
    await executor.warmup()

    # ── Scan loop ────────────────────────────────────────────────────────────
    logger.info(f"\nRunning {ROUNDS} scan rounds (1s apart)...\n")
    executed = 0

    for i in range(1, ROUNDS + 1):
        t0 = time.monotonic()
        opp = await scanner.scan_once()
        elapsed = (time.monotonic() - t0) * 1000

        if opp is None:
            logger.info(f"[{i:2d}/{ROUNDS}] No opportunity ({elapsed:.0f}ms)")
        else:
            logger.info(
                f"[{i:2d}/{ROUNDS}] Opportunity: {opp.token_in[:10]}→{opp.token_out[:10]} "
                f"buy={opp.buy_dex} sell={opp.sell_dex} "
                f"spread={opp.spread_pct:.3f}% "
                f"gross=${opp.gross_profit_usd:.4f} "
                f"gas=${opp.gas_cost_usd:.4f} "
                f"net=${opp.net_profit_usd:.4f} "
                f"profitable={opp.is_profitable()} "
                f"({elapsed:.0f}ms)"
            )
            logger.info(
                f"           buy_amount_out={opp.buy_amount_out} "
                f"buy_fee={opp.buy_fee} "
                f"sell_fee_tier={opp.sell_fee_tier}"
            )

            if opp.is_profitable():
                logger.info(f"  → PROFITABLE — {'dry run, skipping' if DRY_RUN else 'executing...'}")
                if not DRY_RUN:
                    tx_hash = await executor.execute(opp, multi_dex, dry_run=False)
                    if tx_hash:
                        executed += 1
                        logger.info(f"  ✓ TX submitted: {tx_hash}")
                    else:
                        logger.warning("  ✗ Execute returned None")

        await asyncio.sleep(1)

    # ── Summary ──────────────────────────────────────────────────────────────
    s = scanner.stats
    e = executor.stats
    logger.info(
        f"\nDone. scans={s['scans']} opps_found={s['opportunities_found']} "
        f"executed={e['executed']} failed={e['failed']} "
        f"dry_run={DRY_RUN}"
    )

    await session.close()


if __name__ == "__main__":
    asyncio.run(main())
