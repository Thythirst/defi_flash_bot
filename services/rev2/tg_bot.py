#!/usr/bin/env python3
"""
tg_bot.py — Telegram command bot for the liquidation pipeline.

Commands:
  /status  — pipeline service status + last block + wallet balance
  /pnl     — win/loss summary and recent outcomes
  /hot     — hot positions currently being monitored
  /help    — list commands

Runs as a standalone asyncio service with Telegram long-polling.
Start via systemd: tg-bot.service
"""
import asyncio
import logging
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

sys.path.insert(0, str(Path(__file__).resolve().parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("tg_bot")

TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID  = os.getenv("TELEGRAM_CHAT_ID", "")
BASE_URL = f"https://api.telegram.org/bot{TOKEN}"

PROJ_DIR = Path(__file__).resolve().parent.parent.parent
DB_PATH  = PROJ_DIR / "liquidations.db"
LOG_PATH = PROJ_DIR / "logs" / "pipeline.log"
ARB_RPC  = os.getenv("ARBITRUM_HTTP_URL", "https://arb1.arbitrum.io/rpc")
WALLET   = os.getenv("BOT_ADDRESS", "")

POLL_TIMEOUT = 30   # long-poll seconds


# ── Telegram API helpers ───────────────────────────────────────────────────

async def tg_get(session: aiohttp.ClientSession, method: str, **params) -> dict:
    url = f"{BASE_URL}/{method}"
    async with session.get(url, params=params,
                           timeout=aiohttp.ClientTimeout(total=POLL_TIMEOUT + 5)) as r:
        return await r.json()


async def tg_post(session: aiohttp.ClientSession, method: str, payload: dict) -> dict:
    url = f"{BASE_URL}/{method}"
    async with session.post(url, json=payload,
                            timeout=aiohttp.ClientTimeout(total=10)) as r:
        return await r.json()


async def reply(session: aiohttp.ClientSession, chat_id: int, text: str) -> None:
    await tg_post(session, "sendMessage", {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True,
    })


# ── Command handlers ───────────────────────────────────────────────────────

def _service_status() -> str:
    try:
        result = subprocess.run(
            ["systemctl", "--user", "is-active", "pipeline-v3.service"],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout.strip()
    except Exception:
        return "unknown"


def _last_log_lines(n: int = 5) -> str:
    try:
        lines = LOG_PATH.read_text().splitlines()
        return "\n".join(lines[-n:])
    except Exception:
        return "(log unavailable)"


async def _get_block_and_balance(session: aiohttp.ClientSession) -> tuple[int, float]:
    """Returns (latest_block, wallet_eth_balance)."""
    payload_block = {"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}
    payload_bal   = {"jsonrpc": "2.0", "id": 2, "method": "eth_getBalance",
                     "params": [WALLET, "latest"]}
    block, bal = 0, 0.0
    try:
        async with session.post(ARB_RPC, json=payload_block,
                                timeout=aiohttp.ClientTimeout(total=5)) as r:
            data = await r.json()
            block = int(data.get("result", "0x0"), 16)
    except Exception:
        pass
    try:
        async with session.post(ARB_RPC, json=payload_bal,
                                timeout=aiohttp.ClientTimeout(total=5)) as r:
            data = await r.json()
            bal = int(data.get("result", "0x0"), 16) / 1e18
    except Exception:
        pass
    return block, bal


async def cmd_status(session: aiohttp.ClientSession, chat_id: int) -> None:
    svc    = _service_status()
    icon   = "🟢" if svc == "active" else "🔴"
    block, bal = await _get_block_and_balance(session)
    recent = _last_log_lines(3)

    text = (
        f"{icon} *Pipeline Status*\n\n"
        f"Service: `{svc}`\n"
        f"Block: `{block:,}`\n"
        f"Wallet: `{bal:.5f} ETH`\n\n"
        f"*Recent log:*\n```\n{recent}\n```"
    )
    await reply(session, chat_id, text)


def _pnl_from_db() -> str:
    if not DB_PATH.exists():
        return "DB not found"
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        row = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN result='confirmed' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result='reverted'  THEN 1 ELSE 0 END) as reverts,
                SUM(CASE WHEN result='lost_race' THEN 1 ELSE 0 END) as lost,
                SUM(CASE WHEN result='confirmed' THEN actual_profit ELSE 0 END) as profit
            FROM outcomes
        """).fetchone()

        recent = conn.execute("""
            SELECT result, estimated_profit, actual_profit, borrower, submitted_at
            FROM outcomes
            WHERE result != 'lost_race_observed'
            ORDER BY submitted_at DESC LIMIT 5
        """).fetchall()

        conn.close()

        win_rate = (row["wins"] / row["total"] * 100) if row["total"] else 0
        lines = [
            f"*📊 PnL Summary*\n",
            f"Total attempts: `{row['total']}`",
            f"✅ Confirmed: `{row['wins']}` ({win_rate:.0f}%)",
            f"❌ Reverted:  `{row['reverts']}`",
            f"🏁 Lost race: `{row['lost']}`",
            f"💰 Profit:    `${row['profit'] or 0:.2f}`\n",
            f"*Recent outcomes:*",
        ]
        icons = {"confirmed": "✅", "reverted": "❌", "lost_race": "🏁", "pending": "⏳"}
        for r in recent:
            ts = time.strftime("%H:%M", time.localtime(r["submitted_at"])) if r["submitted_at"] else "?"
            ico = icons.get(r["result"], "•")
            p = r["actual_profit"] if r["result"] == "confirmed" else r["estimated_profit"]
            lines.append(f"{ico} `{ts}` {r['borrower'][:10]}… ${p:.0f}")

        return "\n".join(lines)
    except Exception as e:
        return f"DB error: {e}"


async def cmd_pnl(session: aiohttp.ClientSession, chat_id: int) -> None:
    await reply(session, chat_id, _pnl_from_db())


def _hot_positions_from_db() -> str:
    if not DB_PATH.exists():
        return "DB not found"
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row

        # Candidates with recent updates (last 10 min)
        cutoff = int(time.time()) - 600
        rows = conn.execute("""
            SELECT address, hf, collateral_usd, debt_usd, best_collateral, best_debt, last_updated
            FROM candidates
            WHERE hf < 1.05 AND last_updated > ?
            ORDER BY hf ASC LIMIT 15
        """, (cutoff,)).fetchall()

        in_flight = conn.execute("""
            SELECT borrower FROM outcomes WHERE result='pending'
        """).fetchall()
        conn.close()

        in_flight_set = {r["borrower"] for r in in_flight}

        if not rows:
            return "🔍 *Hot Positions*\n\nNo positions with HF < 1.05 in the last 10 min."

        lines = [f"🔥 *Hot Positions* (HF < 1.05, last 10min)\n"]
        for r in rows:
            flag = " 🚀" if r["address"] in in_flight_set else ""
            lines.append(
                f"`{r['address'][:12]}…` HF=`{r['hf']:.4f}` "
                f"col=${r['collateral_usd']:,.0f} debt=${r['debt_usd']:,.0f}{flag}"
            )

        if in_flight_set:
            lines.append(f"\n🚀 = tx in flight")

        return "\n".join(lines)
    except Exception as e:
        return f"DB error: {e}"


async def cmd_hot(session: aiohttp.ClientSession, chat_id: int) -> None:
    await reply(session, chat_id, _hot_positions_from_db())


async def cmd_help(session: aiohttp.ClientSession, chat_id: int) -> None:
    text = (
        "*Flash Liquidation Bot*\n\n"
        "/status — pipeline service + block + wallet balance\n"
        "/pnl — win/loss summary and recent outcomes\n"
        "/hot — positions currently near liquidation\n"
        "/help — this message"
    )
    await reply(session, chat_id, text)


HANDLERS = {
    "/status": cmd_status,
    "/pnl":    cmd_pnl,
    "/stats":  cmd_pnl,
    "/hot":    cmd_hot,
    "/help":   cmd_help,
    "/start":  cmd_help,
}


# ── Main poll loop ─────────────────────────────────────────────────────────

async def poll_loop() -> None:
    if not TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set — exiting")
        return

    offset = 0
    logger.info(f"[TGBot] Starting long-poll loop (chat_id={CHAT_ID})")

    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Announce startup
        try:
            await tg_post(session, "sendMessage", {
                "chat_id": CHAT_ID,
                "text": "🤖 *Bot online* — send /help for commands",
                "parse_mode": "Markdown",
            })
        except Exception:
            pass

        while True:
            try:
                data = await tg_get(session, "getUpdates",
                                    offset=offset, timeout=POLL_TIMEOUT,
                                    allowed_updates="message")
                updates = data.get("result", [])
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.warning(f"[TGBot] getUpdates error: {e} — retrying in 5s")
                await asyncio.sleep(5)
                continue

            for update in updates:
                offset = update["update_id"] + 1
                msg = update.get("message", {})
                text = msg.get("text", "").strip()
                chat_id = msg.get("chat", {}).get("id")

                if not text or not chat_id:
                    continue

                # Only respond to the configured chat
                if str(chat_id) != str(CHAT_ID):
                    logger.info(f"[TGBot] Ignored message from unknown chat {chat_id}")
                    continue

                # Strip bot username suffix from command (e.g. /status@mybot)
                cmd = text.split("@")[0].split()[0].lower()
                handler = HANDLERS.get(cmd)
                if handler:
                    logger.info(f"[TGBot] Handling {cmd} from chat {chat_id}")
                    try:
                        await handler(session, chat_id)
                    except Exception as e:
                        logger.error(f"[TGBot] Handler {cmd} error: {e}")
                        try:
                            await reply(session, chat_id, f"⚠️ Error: `{e}`")
                        except Exception:
                            pass


if __name__ == "__main__":
    asyncio.run(poll_loop())
