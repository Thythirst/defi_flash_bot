#!/usr/bin/env python3
"""
CEX Deviation Monitor — Standalone Service (Strategy #2)

Monitors Binance + Coinbase prices via WebSocket and compares against
on-chain Chainlink prices. Alerts when CEX price deviates beyond a
threshold below Chainlink's own trigger — providing pre-computation
time before the oracle update hits Arbitrum.

Requires:
  pip install aiohttp python-dotenv

Environment:
  ARBITRUM_HTTP_URL   — RPC for Chainlink price reads
  TELEGRAM_BOT_TOKEN  — Bot token for alerts
  TELEGRAM_CHAT_ID    — Chat ID for alerts

Usage:
  python3 scripts/cex_deviation_service.py
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(dotenv_path=project_root / ".env")

from scripts.oracle_guard import CexDeviationMonitor, DeviationAlert

# ─── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | CEX | %(message)s",
)
logger = logging.getLogger("cex_service")

# ─── Telegram Alerting ────────────────────────────────────────

class TelegramAlerter:
    """Simple async Telegram message sender."""

    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"

    async def send(self, text: str) -> bool:
        import aiohttp
        try:
            url = f"{self.base_url}/sendMessage"
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json={
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": "Markdown",
                }, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    return resp.status == 200
        except Exception as e:
            logger.error("Telegram send failed: %s", e)
            return False


# ─── Deviation Alert Callback ─────────────────────────────────

async def on_deviation_alert(alert: DeviationAlert, alerter: TelegramAlerter):
    """
    Called when CEX price deviates from Chainlink beyond our threshold.
    This gives us pre-computation time before the oracle updates on-chain.
    """
    direction = "📈 ABOVE" if alert.cex_price > alert.chainlink_price else "📉 BELOW"
    gap = abs(alert.cex_price - alert.chainlink_price)

    msg = (
        f"⚠️ *CEX DEVIATION ALERT*\n"
        f"Asset: `{alert.symbol}`\n"
        f"CEX Price: `${alert.cex_price:,.2f}`\n"
        f"Chainlink: `${alert.chainlink_price:,.2f}`\n"
        f"Deviation: `{alert.deviation_pct*100:.3f}%` {direction}\n"
        f"CL Threshold: `{alert.chainlink_threshold*100:.1f}%`\n"
        f"Alert at: `{alert.cex_threshold*100:.3f}%`\n"
        f"\n⏱️ Pre-compute window: Chainlink hasn't updated yet!"
    )

    success = await alerter.send(msg)
    if success:
        logger.info("Alert sent for %s (%.3f%% deviation)", alert.symbol, alert.deviation_pct * 100)


# ─── Main ─────────────────────────────────────────────────────

async def main():
    rpc_url = os.getenv("ARBITRUM_HTTP_URL") or os.getenv("ALCHEMY_HTTP_URL")
    if not rpc_url:
        logger.error("ARBITRUM_HTTP_URL not set")
        return

    tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
    tg_chat = os.getenv("TELEGRAM_CHAT_ID")
    if not tg_token or not tg_chat:
        logger.warning("Telegram not configured — alerts will only log")
        alerter = None
    else:
        alerter = TelegramAlerter(tg_token, tg_chat)
        logger.info("Telegram alerter ready (chat: %s)", tg_chat)

    # Create the callback that sends Telegram alerts
    async def alert_callback(alert: DeviationAlert):
        logger.info(
            "DEVIATION: %s CEX=%.2f CL=%.2f (%.3f%%)",
            alert.symbol, alert.cex_price, alert.chainlink_price,
            alert.deviation_pct * 100,
        )
        if alerter:
            await on_deviation_alert(alert, alerter)

    monitor = CexDeviationMonitor(
        rpc_url=rpc_url,
        alert_callback=alert_callback,
    )

    logger.info("Starting CEX Deviation Monitor...")
    logger.info("Assets: ETH (1.0%% threshold), BTC (1.0%%), LINK (1.5%%)")
    logger.info("RPC: %s", rpc_url[:50])

    await monitor.start()

    try:
        # Run forever
        while True:
            await asyncio.sleep(60)
            # Periodic health log
            cl_prices = {k: f"${v:,.2f}" for k, v in monitor.chainlink_prices.items()}
            if cl_prices:
                logger.info("Chainlink prices: %s", cl_prices)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await monitor.stop()


if __name__ == "__main__":
    asyncio.run(main())
