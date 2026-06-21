#!/usr/bin/env python3
"""
gTrade Keeper Bot — Main Pipeline

Monitors gTrade Diamond on Arbitrum for:
  1. Limit open orders (tradeType=0) whose price has crossed their trigger
  2. Market orders that have exceeded the 200-block timeout

Actions:
  - Sends Telegram alerts for every near-trigger and timeout
  - Attempts cancelOrderAfterTimeout(uint32) for timed-out market orders
  - (Limit order execution requires GNS oracle network membership — see trigger_executor.py)

Usage:
  python3 -m services.gTrade.pipeline            # dry-run (simulate, alert)
  python3 -m services.gTrade.pipeline --live      # real execution (needs private key)
  python3 -m services.gTrade.pipeline --scan-only # price + order monitor, no execution

Environment (from .env):
  BOT_PRIVATE_KEY    — hot wallet private key (required for live mode)
  BOT_ADDRESS        — hot wallet address (required for live mode)
  TELEGRAM_BOT_TOKEN — optional, Telegram alerts
  TELEGRAM_CHAT_ID   — optional, Telegram chat target
  READ_RPC_PRIMARY   — Arbitrum RPC URL (fallback: arb1.arbitrum.io/rpc)
"""

import asyncio
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set

import aiohttp
from dotenv import load_dotenv
from web3 import Web3

load_dotenv(os.path.expanduser("~/defi_flash_bot/.env"))

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)-8s %(name)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("gtrade.pipeline")

# ── Config ─────────────────────────────────────────────────────────────────────
RPC_URL = (
    os.getenv("READ_RPC_PRIMARY")
    or os.getenv("QUICKNODE_HTTP_URL")
    or "https://arb1.arbitrum.io/rpc"
)
GTRADE_DIAMOND   = "0xFF162c694eAA571f685030649814282eA457f169"
GTRADE_BACKEND   = "https://backend-arbitrum.gains.trade"

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
PRIVATE_KEY      = os.getenv("BOT_PRIVATE_KEY", "")
WALLET_ADDR      = os.getenv("BOT_ADDRESS", "")

# Scan intervals
PRICE_SCAN_INTERVAL   = 3.0    # seconds between price checks
ORDER_REFRESH_INTERVAL = 30.0  # seconds between /open-trades API calls
EVENT_SCAN_INTERVAL   = 15.0   # seconds between pending-order event scans

# Alert when limit order within this % of trigger
ALERT_PROXIMITY_PCT = 0.5    # 0.5 %
EXECUTE_PROXIMITY_PCT = 0.05  # 0.05% — consider it crossed (allows for tiny price drift)

# Min reward (USD) to bother executing; avoid spending more gas than reward
MIN_KEEPER_REWARD_USD = float(os.getenv("GTRADE_MIN_REWARD_USD", "0.50"))

# ── Startup guard ──────────────────────────────────────────────────────────────
def _check_env(live: bool) -> None:
    if not live:
        return
    missing = []
    if not PRIVATE_KEY:
        missing.append("BOT_PRIVATE_KEY")
    if not WALLET_ADDR:
        missing.append("BOT_ADDRESS")
    if missing:
        logger.critical("FATAL: live mode requires: %s", ", ".join(missing))
        sys.exit(1)


# ── Telegram helper ────────────────────────────────────────────────────────────
async def send_telegram(session: aiohttp.ClientSession, message: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logger.info("[TG] %s", message)
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        async with session.post(url, json={
            "chat_id":    TELEGRAM_CHAT_ID,
            "text":       message,
            "parse_mode": "Markdown",
        }) as resp:
            if resp.status != 200:
                logger.warning("[TG] send failed: %s", await resp.text())
    except Exception as e:
        logger.warning("[TG] send exception: %s", e)


# ── Pipeline ───────────────────────────────────────────────────────────────────
class GtradeKeeperPipeline:

    def __init__(self, dry_run: bool = True):
        self.dry_run  = dry_run
        self.running  = False
        self._started = datetime.now(timezone.utc)
        self._stats   = {
            "scans": 0,
            "alerts_sent": 0,
            "cancels_attempted": 0,
            "cancels_ok": 0,
        }

        # Scanner (sync; called from async via run_in_executor)
        from services.gTrade.order_scanner import OrderScanner
        self._scanner = OrderScanner(rpc_url=RPC_URL, backend_url=GTRADE_BACKEND)

        # Executor (sync)
        from services.gTrade.trigger_executor import TriggerExecutor
        self._executor = TriggerExecutor(rpc_url=RPC_URL)

        # Per-order de-dup: don't alert the same order twice in the same direction
        self._alerted_crossed: Set[str] = set()   # order keys that already fired "CROSSED"
        self._alerted_near:    Dict[str, float] = {}  # order key → last alert time

        # Cached limit orders (refreshed every 30s)
        self._limit_orders = []
        self._orders_last_refresh: float = 0.0

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _order_key(self, user: str, index: int) -> str:
        return f"{user.lower()}:{index}"

    def _fmt_price(self, price_1e10: int) -> str:
        if price_1e10 == 0:
            return "—"
        return f"${price_1e10 / 1e10:,.4f}"

    async def _refresh_orders(self) -> None:
        """Reload /open-trades if cache is stale."""
        now = time.monotonic()
        if now - self._orders_last_refresh < ORDER_REFRESH_INTERVAL:
            return
        loop = asyncio.get_event_loop()
        self._limit_orders = await loop.run_in_executor(
            None, self._scanner.fetch_limit_orders
        )
        self._orders_last_refresh = now
        logger.debug("[Pipeline] Refreshed %d limit orders", len(self._limit_orders))

    async def _get_prices(self, pair_indices) -> Dict[int, int]:
        """Fetch Chainlink prices for the given pair indices."""
        loop = asyncio.get_event_loop()
        prices = {}
        for pair_idx in pair_indices:
            price = await loop.run_in_executor(
                None, self._scanner.get_chainlink_price_1e10, pair_idx
            )
            if price:
                prices[pair_idx] = price
        return prices

    # ── Alert logic ────────────────────────────────────────────────────────────

    async def _maybe_alert_limit_order(
        self,
        session:      aiohttp.ClientSession,
        near_trigger,
    ) -> None:
        from services.gTrade.order_scanner import NearTrigger
        nt: NearTrigger = near_trigger

        order  = nt.order
        key    = self._order_key(order.user, order.index)
        side   = "LONG" if order.long else "SHORT"
        pair   = self._scanner.pair_name(order.pair_index)
        now_ts = time.monotonic()

        if nt.is_crossed:
            # Only alert once per crossing (until un-crossed and re-crossed)
            if key in self._alerted_crossed:
                return
            self._alerted_crossed.add(key)
            emoji  = "🚨"
            status = "*CROSSED* — limit order is triggerable NOW"
        else:
            # Alert nearby at most once per 5 minutes
            last = self._alerted_near.get(key, 0)
            if now_ts - last < 300:
                return
            self._alerted_near[key] = now_ts
            emoji  = "⚠️"
            status = f"NEAR trigger ({nt.distance_pct:.3f}% away)"

        leverage_x = order.leverage / 1000
        msg = (
            f"{emoji} *gTrade Keeper Alert*\n"
            f"Status: {status}\n"
            f"Pair: `{pair}` (pairIndex={order.pair_index})\n"
            f"Side: {side}  Leverage: {leverage_x:.1f}x\n"
            f"Trigger:  `{self._fmt_price(order.open_price_1e10)}`\n"
            f"Current:  `{self._fmt_price(nt.current_price_1e10)}`\n"
            f"Distance: `{nt.distance_pct:.3f}%`\n"
            f"Trader: `{order.user[:20]}...`  index={order.index}\n"
        )
        if order.sl_1e10:
            msg += f"SL: `{self._fmt_price(order.sl_1e10)}`   "
        if order.tp_1e10:
            msg += f"TP: `{self._fmt_price(order.tp_1e10)}`\n"

        await send_telegram(session, msg)
        self._stats["alerts_sent"] += 1
        logger.info("[Pipeline] ALERT sent: %s %s dist=%.3f%% crossed=%s",
                    pair, side, nt.distance_pct, nt.is_crossed)

    async def _maybe_alert_uncrossed(self, key: str) -> None:
        """Remove a key from crossed set when price moves away again."""
        if key in self._alerted_crossed:
            self._alerted_crossed.discard(key)

    # ── Timeout cancellation ───────────────────────────────────────────────────

    async def _try_cancel_timeout(
        self,
        session:  aiohttp.ClientSession,
        pending,
    ) -> None:
        from services.gTrade.order_scanner import PendingMarketOrder
        pmo: PendingMarketOrder = pending

        pid  = pmo.pending_order_id
        pair = self._scanner.pair_name(pmo.pair_index)
        action = "open" if pmo.is_open else "close"

        logger.info("[Pipeline] Timed-out market %s order: ID=%d trader=%s pair=%s",
                    action, pid, pmo.trader[:14], pair)

        await send_telegram(session,
            f"⏰ *gTrade Market Order Timeout*\n"
            f"Order ID: `{pid}`\n"
            f"Action: market {action}  Pair: `{pair}`\n"
            f"Trader: `{pmo.trader[:20]}...`\n"
            f"Created block: {pmo.created_block}\n"
            f"{'Attempting cancelOrderAfterTimeout...' if not self.dry_run else '(dry-run, not executing)'}"
        )
        self._stats["alerts_sent"] += 1

        if self.dry_run:
            return

        self._stats["cancels_attempted"] += 1
        loop = asyncio.get_event_loop()
        try:
            result = await loop.run_in_executor(
                None, self._cancel_on_chain, pid
            )
            if result.success:
                self._stats["cancels_ok"] += 1
                await send_telegram(session,
                    f"✅ cancelOrderAfterTimeout({pid}) succeeded!\n"
                    f"TX: `{result.tx_hash}`"
                )
                logger.info("[Pipeline] Cancel OK: pendingOrderId=%d tx=%s", pid, result.tx_hash)
            else:
                logger.warning("[Pipeline] Cancel failed: %s", result.error)
        except Exception as e:
            logger.error("[Pipeline] Cancel exception: %s", e)

    def _cancel_on_chain(self, pending_order_id: int):
        """Sync: call cancelOrderAfterTimeout(uint32 pendingOrderId)."""
        from eth_abi import encode as abi_encode
        from services.gTrade.trigger_executor import TriggerResult

        if not PRIVATE_KEY or not WALLET_ADDR:
            return TriggerResult(success=False, error="no wallet configured")

        w3 = self._executor.w3
        wallet = Web3.to_checksum_address(WALLET_ADDR)

        # cancelOrderAfterTimeout(uint32) selector: 0xb6919540
        calldata = bytes.fromhex("b6919540") + abi_encode(["uint32"], [pending_order_id])

        try:
            # Simulate first
            w3.eth.call({"to": self._executor.diamond, "from": wallet, "data": calldata})

            gas_price = int(w3.eth.gas_price * 1.15)
            nonce     = w3.eth.get_transaction_count(wallet)
            tx = {
                "from":     wallet,
                "to":       self._executor.diamond,
                "data":     calldata,
                "gas":      300_000,
                "gasPrice": gas_price,
                "nonce":    nonce,
                "chainId":  42161,
            }
            signed  = w3.eth.account.sign_transaction(tx, PRIVATE_KEY)
            tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
            receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
            from services.gTrade.trigger_executor import TriggerResult
            if receipt.status == 1:
                return TriggerResult(success=True, tx_hash=tx_hash.hex(), gas_used=receipt.gasUsed)
            else:
                return TriggerResult(success=False, tx_hash=tx_hash.hex(), error="reverted")
        except Exception as e:
            from services.gTrade.trigger_executor import TriggerResult
            return TriggerResult(success=False, error=str(e))

    # ── Main scan loop ─────────────────────────────────────────────────────────

    async def scan_loop(self, session: aiohttp.ClientSession) -> None:
        logger.info("[Pipeline] Starting scan loop (interval=%.0fs)", PRICE_SCAN_INTERVAL)

        last_event_scan  = 0.0
        scan_count       = 0
        log_every        = 60  # log a status line every 60 scans (~3 min)

        while self.running:
            try:
                # Refresh limit orders from API (lazy, every 30s)
                await self._refresh_orders()

                # Get unique pairs from active limit orders
                pair_indices = list({o.pair_index for o in self._limit_orders
                                     if o.pair_index in self._scanner._pair_oracles})

                # Fetch Chainlink prices
                prices = await self._get_prices(pair_indices)

                # Find near-trigger orders
                loop = asyncio.get_event_loop()
                from services.gTrade.order_scanner import OrderScanner
                near_triggers = await loop.run_in_executor(
                    None,
                    self._scanner.find_near_triggers,
                    self._limit_orders,
                    prices,
                    ALERT_PROXIMITY_PCT,
                )

                for nt in near_triggers:
                    await self._maybe_alert_limit_order(session, nt)

                # Clean up crossed set for orders that have moved away from trigger
                near_keys = {self._order_key(nt.order.user, nt.order.index)
                             for nt in near_triggers}
                for k in list(self._alerted_crossed):
                    if k not in near_keys:
                        self._alerted_crossed.discard(k)

                scan_count += 1
                self._stats["scans"] += 1

                # Periodic log
                if scan_count % log_every == 0:
                    n_pairs  = len(prices)
                    n_orders = len(self._limit_orders)
                    runtime  = (datetime.now(timezone.utc) - self._started).total_seconds()
                    logger.info(
                        "[Pipeline] scan=%d runtime=%.0fs orders=%d pairs_priced=%d "
                        "near_trigger=%d alerts=%d",
                        scan_count, runtime, n_orders, n_pairs,
                        len(near_triggers), self._stats["alerts_sent"],
                    )

                # Scan for timed-out market orders (less frequently)
                now = time.monotonic()
                if now - last_event_scan > EVENT_SCAN_INTERVAL:
                    timed_out = await loop.run_in_executor(
                        None,
                        self._scanner.scan_pending_market_orders,
                        300,
                    )
                    for pmo in timed_out:
                        await self._try_cancel_timeout(session, pmo)
                    last_event_scan = now

                await asyncio.sleep(PRICE_SCAN_INTERVAL)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("[Pipeline] Scan error: %s", e, exc_info=True)
                await asyncio.sleep(10)

    # ── Start / stop ───────────────────────────────────────────────────────────

    async def start(self) -> None:
        self.running = True
        _check_env(not self.dry_run)

        logger.info("=" * 70)
        logger.info(" gTrade Keeper Bot")
        logger.info("  Diamond:   %s", GTRADE_DIAMOND)
        logger.info("  Backend:   %s", GTRADE_BACKEND)
        logger.info("  Mode:      %s", "DRY-RUN" if self.dry_run else "LIVE")
        logger.info("  Wallet:    %s", WALLET_ADDR[:14] + "..." if WALLET_ADDR else "—")
        logger.info("  Telegram:  %s", "configured" if TELEGRAM_TOKEN else "NOT configured")
        logger.info("  Scan:      %.0fs prices / %.0fs orders", PRICE_SCAN_INTERVAL, ORDER_REFRESH_INTERVAL)
        logger.info("=" * 70)

        # Load pair oracles from the diamond
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self._scanner.load_pair_oracles, 100)

        async with aiohttp.ClientSession() as session:
            # Startup notification
            await send_telegram(session,
                f"🤖 *gTrade Keeper Bot started*\n"
                f"Mode: {'DRY-RUN' if self.dry_run else 'LIVE'}\n"
                f"Pairs loaded: {len(self._scanner._pair_oracles)}\n"
                f"Alert threshold: {ALERT_PROXIMITY_PCT}% from trigger"
            )
            await self.scan_loop(session)

    async def stop(self) -> None:
        self.running = False
        runtime = (datetime.now(timezone.utc) - self._started).total_seconds()
        logger.info(
            "[Pipeline] Stopped. Runtime=%.0fs scans=%d alerts=%d cancels=%d/%d",
            runtime,
            self._stats["scans"],
            self._stats["alerts_sent"],
            self._stats["cancels_ok"],
            self._stats["cancels_attempted"],
        )


# ── Entry point ────────────────────────────────────────────────────────────────
async def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="gTrade Keeper Bot")
    parser.add_argument("--live",      action="store_true", help="Enable live execution")
    parser.add_argument("--scan-only", action="store_true", help="Only log, no Telegram")
    args = parser.parse_args()

    if args.scan_only:
        global TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
        TELEGRAM_TOKEN = ""
        TELEGRAM_CHAT_ID = ""

    pipeline = GtradeKeeperPipeline(dry_run=not args.live)

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(pipeline.stop()))

    try:
        await pipeline.start()
    except KeyboardInterrupt:
        await pipeline.stop()


if __name__ == "__main__":
    asyncio.run(main())
