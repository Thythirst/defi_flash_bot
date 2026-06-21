#!/usr/bin/env python3
"""
wallet_alert.py — Hourly wallet balance monitor
Alerts via Telegram + log when ETH balance drops below threshold.

Cron entry (every hour):
    0 * * * * /home/ubuntu/defi_flash_bot/venv/bin/python3 \
        /home/ubuntu/defi_flash_bot/scripts/wallet_alert.py \
        >> /home/ubuntu/defi_flash_bot/logs/wallet_alert.log 2>&1

Required env vars:
    BOT_ADDRESS          — wallet to monitor
    TELEGRAM_BOT_TOKEN   — for alerts (optional, logs only if not set)
    TELEGRAM_CHAT_ID     — recipient chat ID
    QUICKNODE_HTTP_URL   — Arbitrum RPC
    BASE_RPC_URL         — Base RPC (optional)

Thresholds:
    CRITICAL: < 0.005 ETH — next tx may fail, top up immediately
    WARNING:  < 0.020 ETH — ~5 txs remaining, plan top-up
    INFO:     < 0.050 ETH — monitoring, no action needed
"""

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path

import aiohttp
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROD_DIR     = Path(__file__).resolve().parent.parent
STATE_FILE   = PROD_DIR / "logs" / "wallet_state.json"

WALLET       = os.getenv("BOT_ADDRESS", "")
TG_TOKEN     = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT      = os.getenv("TELEGRAM_CHAT_ID", "")
ARB_RPC      = os.getenv("QUICKNODE_HTTP_URL", "https://arb1.arbitrum.io/rpc")
BASE_RPC     = os.getenv("BASE_RPC_URL", "https://mainnet.base.org")

ETH_WEI      = 10 ** 18

THRESHOLDS = {
    "CRITICAL": 0.005,   # < 0.005 ETH — ~1-2 txs
    "WARNING":  0.020,   # < 0.020 ETH — ~5 txs
    "INFO":     0.050,   # < 0.050 ETH — ~12 txs
}

# Don't re-alert the same severity within this many seconds
ALERT_COOLDOWN = {
    "CRITICAL": 1800,    # 30 min
    "WARNING":  7200,    # 2 hours
    "INFO":     86400,   # 24 hours
}

# ---------------------------------------------------------------------------
# Balance checker
# ---------------------------------------------------------------------------

@dataclass
class ChainBalance:
    chain:       str
    balance_eth: float
    balance_usd: float
    severity:    str    # CRITICAL | WARNING | INFO | OK
    rpc_ok:      bool = True


async def get_balance(rpc_url: str, wallet: str, chain: str) -> ChainBalance:
    """Fetch ETH balance for wallet on given chain."""
    try:
        w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
        balance_wei = await w3.eth.get_balance(
            AsyncWeb3.to_checksum_address(wallet)
        )
        balance_eth = balance_wei / ETH_WEI

        # Rough USD estimate (uses hardcoded price — replace with PriceRegistry if accessible)
        eth_price_usd = 1640.0  # update manually or fetch from oracle
        balance_usd   = balance_eth * eth_price_usd

        # Determine severity
        severity = "OK"
        for level, threshold in THRESHOLDS.items():
            if balance_eth < threshold:
                severity = level
                break

        return ChainBalance(
            chain       = chain,
            balance_eth = balance_eth,
            balance_usd = balance_usd,
            severity    = severity,
        )
    except Exception as e:
        logger.error(f"[WalletAlert] {chain} balance check failed: {e}")
        return ChainBalance(
            chain       = chain,
            balance_eth = 0.0,
            balance_usd = 0.0,
            severity    = "CRITICAL",
            rpc_ok      = False,
        )


# ---------------------------------------------------------------------------
# State management
# ---------------------------------------------------------------------------

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {"last_alerts": {}, "balance_history": []}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def should_alert(severity: str, chain: str, state: dict) -> bool:
    """Respect cooldown — don't spam the same alert repeatedly."""
    if severity == "OK":
        return False
    key      = f"{chain}:{severity}"
    last     = state["last_alerts"].get(key, 0)
    cooldown = ALERT_COOLDOWN.get(severity, 3600)
    return time.time() - last > cooldown


def mark_alerted(severity: str, chain: str, state: dict) -> None:
    key = f"{chain}:{severity}"
    state["last_alerts"][key] = time.time()


# ---------------------------------------------------------------------------
# Telegram sender
# ---------------------------------------------------------------------------

async def send_telegram(message: str) -> bool:
    """Send Telegram alert. Returns True on success."""
    if not TG_TOKEN or not TG_CHAT:
        logger.info(f"[WalletAlert] No Telegram config — log only: {message}")
        return False
    try:
        url = f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage"
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json={
                "chat_id":    TG_CHAT,
                "text":       message,
                "parse_mode": "Markdown",
            }, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                return resp.status == 200
    except Exception as e:
        logger.error(f"[WalletAlert] Telegram send failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main():
    if not WALLET:
        logger.error("[WalletAlert] BOT_ADDRESS not set")
        return

    state = load_state()

    # Check balances on both chains
    arb_bal, base_bal = await asyncio.gather(
        get_balance(ARB_RPC,  WALLET, "Arbitrum"),
        get_balance(BASE_RPC, WALLET, "Base"),
    )

    # Log current state
    logger.info(
        f"[WalletAlert] "
        f"Arbitrum={arb_bal.balance_eth:.4f}ETH (${arb_bal.balance_usd:.0f}) [{arb_bal.severity}]  "
        f"Base={base_bal.balance_eth:.4f}ETH (${base_bal.balance_usd:.0f}) [{base_bal.severity}]"
    )

    # Track history (last 24 readings)
    state["balance_history"].append({
        "timestamp":   time.time(),
        "arb_eth":     arb_bal.balance_eth,
        "base_eth":    base_bal.balance_eth,
    })
    state["balance_history"] = state["balance_history"][-24:]

    # Compute trend (declining?)
    history = state["balance_history"]
    trend   = ""
    if len(history) >= 3:
        recent = history[-1]["arb_eth"]
        older  = history[-3]["arb_eth"]
        if recent < older * 0.9:
            trend = f"\n📉 Trend: dropped {(older-recent):.4f} ETH in last 3h"

    # Send alerts if needed
    for bal in [arb_bal, base_bal]:
        if bal.severity != "OK" and should_alert(bal.severity, bal.chain, state):
            icons = {"CRITICAL": "🚨", "WARNING": "⚠️", "INFO": "ℹ️"}
            icon  = icons.get(bal.severity, "")

            msg = (
                f"{icon} *Wallet Balance Alert — {bal.chain}*\n\n"
                f"Balance: `{bal.balance_eth:.4f} ETH` (${bal.balance_usd:.0f})\n"
                f"Severity: *{bal.severity}*\n"
                f"Wallet: `{WALLET[:10]}...{WALLET[-6:]}`\n"
                f"{trend}\n\n"
            )

            if bal.severity == "CRITICAL":
                msg += "⚡ *Action required: top up immediately*\n"
                msg += f"~{bal.balance_eth / 0.005:.0f} txs remaining at current gas"
            elif bal.severity == "WARNING":
                msg += f"Plan top-up soon. ~{bal.balance_eth / 0.005:.0f} txs remaining."

            sent = await send_telegram(msg)
            if sent or not TG_TOKEN:
                mark_alerted(bal.severity, bal.chain, state)

            logger.warning(f"[WalletAlert] ALERT sent: {bal.chain} {bal.severity} {bal.balance_eth:.4f}ETH")

    save_state(state)


if __name__ == "__main__":
    asyncio.run(main())

