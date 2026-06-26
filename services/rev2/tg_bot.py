#!/usr/bin/env python3
"""
tg_bot.py — Telegram bot for the liquidation pipeline + Claude AI chat.

Commands:
  /status  — pipeline service status + last block + wallet balance
  /pnl     — win/loss summary and recent outcomes
  /hot     — positions currently near liquidation
  /help    — list commands

Any non-command message is routed to Claude (claude-haiku-4-5) with full
context about the pipeline — ask questions, analyse logs, debug issues.

Runs as tg-bot.service (systemd user unit).
"""
import asyncio
import logging
import os
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

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

TOKEN         = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID       = os.getenv("TELEGRAM_CHAT_ID", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
BASE_URL      = f"https://api.telegram.org/bot{TOKEN}"

PROJ_DIR  = Path(__file__).resolve().parent.parent.parent
DB_PATH   = PROJ_DIR / "liquidations.db"
LOG_PATH  = PROJ_DIR / "logs" / "pipeline.log"
ARB_RPC   = os.getenv("ARBITRUM_HTTP_URL", "https://arb1.arbitrum.io/rpc")
WALLET    = os.getenv("BOT_ADDRESS", "")
CONTRACT  = os.getenv("FLASH_EXECUTOR_V3", "")

POLL_TIMEOUT = 30

# Per-chat conversation history for Claude (keeps last N turns)
_CLAUDE_HISTORY: list[dict] = []
MAX_HISTORY = 20  # message pairs to keep

CLAUDE_SYSTEM = """You are an assistant embedded in a DeFi flash liquidation bot running on Arbitrum.
The bot monitors Aave V3 positions and executes flash loan liquidations for profit.

Key facts about this system:
- Pipeline: services/rev2/pipeline_v3.py — main liquidation loop
- Contract: FlashExecutorV3 on Arbitrum (flash loan + liquidate + swap in one tx)
- Flash loans: Balancer (0% fee) primary, Aave V3 (9bps) fallback
- RPC: Chainstack WSS + HTTP primary, DRPC/PublicNode fallback
- DB: SQLite liquidations.db — outcomes, competitors, candidates
- Wallet: """ + WALLET + """
- Contract: """ + CONTRACT + """

The user is the bot operator. They may ask you to:
- Explain log output or errors
- Debug revert reasons or tx failures
- Analyse PnL data and suggest improvements
- Review or suggest code changes
- Answer DeFi/Aave/Arbitrum questions

Be concise. Use plain text (Telegram renders Markdown). Max ~800 chars per reply unless detail is needed."""


# ── Telegram API helpers ───────────────────────────────────────────────────

async def tg_post(session: aiohttp.ClientSession, method: str, payload: dict) -> dict:
    url = f"{BASE_URL}/{method}"
    async with session.post(url, json=payload,
                            timeout=aiohttp.ClientTimeout(total=10)) as r:
        return await r.json()


async def tg_get(session: aiohttp.ClientSession, method: str, **params) -> dict:
    url = f"{BASE_URL}/{method}"
    async with session.get(url, params=params,
                           timeout=aiohttp.ClientTimeout(total=POLL_TIMEOUT + 5)) as r:
        return await r.json()


async def reply(session: aiohttp.ClientSession, chat_id: int, text: str,
                parse_mode: str = "Markdown") -> None:
    # Telegram message limit is 4096 chars
    chunks = [text[i:i+4000] for i in range(0, len(text), 4000)]
    for chunk in chunks:
        await tg_post(session, "sendMessage", {
            "chat_id": chat_id,
            "text": chunk,
            "parse_mode": parse_mode,
            "disable_web_page_preview": True,
        })


async def send_typing(session: aiohttp.ClientSession, chat_id: int) -> None:
    await tg_post(session, "sendChatAction", {"chat_id": chat_id, "action": "typing"})


# ── Claude AI handler ──────────────────────────────────────────────────────

def _build_context_snippet() -> str:
    """Inject live pipeline context into each Claude request."""
    lines = []
    try:
        svc = subprocess.run(
            ["systemctl", "--user", "is-active", "pipeline-v3.service"],
            capture_output=True, text=True, timeout=3,
        ).stdout.strip()
        lines.append(f"Pipeline service: {svc}")
    except Exception:
        pass
    try:
        log_tail = LOG_PATH.read_text().splitlines()[-8:]
        lines.append("Recent log:\n" + "\n".join(log_tail))
    except Exception:
        pass
    if DB_PATH.exists():
        try:
            conn = sqlite3.connect(str(DB_PATH))
            row = conn.execute(
                "SELECT COUNT(*) t, SUM(result='confirmed') w, SUM(result='reverted') r, "
                "SUM(result='lost_race') l, SUM(CASE WHEN result='confirmed' THEN actual_profit ELSE 0 END) p "
                "FROM outcomes"
            ).fetchone()
            conn.close()
            lines.append(f"DB: {row[0]} attempts, {row[1]} wins, {row[2]} reverts, {row[3]} lost, ${row[4] or 0:.2f} profit")
        except Exception:
            pass
    return "\n".join(lines)


async def ask_claude(user_message: str) -> str:
    if not ANTHROPIC_KEY:
        return "ANTHROPIC_API_KEY not set — add it to .env to enable AI chat."

    global _CLAUDE_HISTORY
    context = _build_context_snippet()
    full_message = f"{user_message}\n\n[Live context]\n{context}" if context else user_message

    _CLAUDE_HISTORY.append({"role": "user", "content": full_message})
    if len(_CLAUDE_HISTORY) > MAX_HISTORY * 2:
        _CLAUDE_HISTORY = _CLAUDE_HISTORY[-MAX_HISTORY * 2:]

    try:
        import anthropic
        client = anthropic.AsyncAnthropic(api_key=ANTHROPIC_KEY)
        response = await client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1024,
            system=CLAUDE_SYSTEM,
            messages=_CLAUDE_HISTORY,
        )
        answer = response.content[0].text
        _CLAUDE_HISTORY.append({"role": "assistant", "content": answer})
        return answer
    except Exception as e:
        logger.error(f"[Claude] API error: {e}")
        return f"Claude API error: {e}"


# ── Command handlers ───────────────────────────────────────────────────────

def _service_status() -> str:
    try:
        return subprocess.run(
            ["systemctl", "--user", "is-active", "pipeline-v3.service"],
            capture_output=True, text=True, timeout=5,
        ).stdout.strip()
    except Exception:
        return "unknown"


def _last_log_lines(n: int = 5) -> str:
    try:
        return "\n".join(LOG_PATH.read_text().splitlines()[-n:])
    except Exception:
        return "(log unavailable)"


async def _get_block_and_balance(session: aiohttp.ClientSession) -> tuple[int, float]:
    block, bal = 0, 0.0
    try:
        async with session.post(ARB_RPC,
                                json={"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]},
                                timeout=aiohttp.ClientTimeout(total=5)) as r:
            block = int((await r.json()).get("result","0x0"), 16)
    except Exception:
        pass
    try:
        async with session.post(ARB_RPC,
                                json={"jsonrpc":"2.0","id":2,"method":"eth_getBalance","params":[WALLET,"latest"]},
                                timeout=aiohttp.ClientTimeout(total=5)) as r:
            bal = int((await r.json()).get("result","0x0"), 16) / 1e18
    except Exception:
        pass
    return block, bal


async def cmd_status(session: aiohttp.ClientSession, chat_id: int) -> None:
    svc = _service_status()
    icon = "🟢" if svc == "active" else "🔴"
    block, bal = await _get_block_and_balance(session)
    recent = _last_log_lines(3)
    await reply(session, chat_id,
        f"{icon} *Pipeline Status*\n\n"
        f"Service: `{svc}`\n"
        f"Block: `{block:,}`\n"
        f"Wallet: `{bal:.5f} ETH`\n\n"
        f"*Recent log:*\n```\n{recent}\n```"
    )


def _pnl_text() -> str:
    if not DB_PATH.exists():
        return "DB not found"
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        row = conn.execute("""
            SELECT COUNT(*) total,
                SUM(result='confirmed') wins,
                SUM(result='reverted')  reverts,
                SUM(result='lost_race') lost,
                SUM(CASE WHEN result='confirmed' THEN actual_profit ELSE 0 END) profit
            FROM outcomes
        """).fetchone()
        recent = conn.execute("""
            SELECT result, estimated_profit, actual_profit, borrower, submitted_at
            FROM outcomes WHERE result != 'lost_race_observed'
            ORDER BY submitted_at DESC LIMIT 5
        """).fetchall()
        conn.close()

        wr = (row["wins"] / row["total"] * 100) if row["total"] else 0
        icons = {"confirmed":"✅","reverted":"❌","lost_race":"🏁","pending":"⏳"}
        lines = [
            "*📊 PnL Summary*\n",
            f"Total: `{row['total']}`  Wins: `{row['wins']}` ({wr:.0f}%)",
            f"Reverts: `{row['reverts']}`  Lost: `{row['lost']}`",
            f"Profit: `${row['profit'] or 0:.2f}`\n",
            "*Recent:*",
        ]
        for r in recent:
            ts = time.strftime("%H:%M", time.localtime(r["submitted_at"])) if r["submitted_at"] else "?"
            p  = r["actual_profit"] if r["result"] == "confirmed" else r["estimated_profit"]
            lines.append(f"{icons.get(r['result'],'•')} `{ts}` {r['borrower'][:10]}… ${p:.0f}")
        return "\n".join(lines)
    except Exception as e:
        return f"DB error: {e}"


async def cmd_pnl(session: aiohttp.ClientSession, chat_id: int) -> None:
    await reply(session, chat_id, _pnl_text())


def _hot_text() -> str:
    if not DB_PATH.exists():
        return "DB not found"
    try:
        conn = sqlite3.connect(str(DB_PATH))
        conn.row_factory = sqlite3.Row
        cutoff = int(time.time()) - 600
        rows = conn.execute("""
            SELECT address, hf, collateral_usd, debt_usd FROM candidates
            WHERE hf < 1.05 AND last_updated > ?
            ORDER BY hf ASC LIMIT 15
        """, (cutoff,)).fetchall()
        in_flight = {r["borrower"] for r in conn.execute(
            "SELECT borrower FROM outcomes WHERE result='pending'"
        ).fetchall()}
        conn.close()

        if not rows:
            return "🔍 *Hot Positions*\n\nNone with HF < 1.05 in last 10 min."

        lines = ["🔥 *Hot Positions* (HF < 1.05)\n"]
        for r in rows:
            flag = " 🚀" if r["address"] in in_flight else ""
            lines.append(
                f"`{r['address'][:12]}…` HF=`{r['hf']:.4f}` "
                f"col=${r['collateral_usd']:,.0f}{flag}"
            )
        return "\n".join(lines)
    except Exception as e:
        return f"DB error: {e}"


async def cmd_hot(session: aiohttp.ClientSession, chat_id: int) -> None:
    await reply(session, chat_id, _hot_text())


async def cmd_clear(session: aiohttp.ClientSession, chat_id: int) -> None:
    global _CLAUDE_HISTORY
    _CLAUDE_HISTORY = []
    await reply(session, chat_id, "🧹 Conversation history cleared.")


async def cmd_help(session: aiohttp.ClientSession, chat_id: int) -> None:
    await reply(session, chat_id,
        "*Flash Liquidation Bot*\n\n"
        "/status — pipeline service + block + wallet\n"
        "/pnl — win/loss summary and recent outcomes\n"
        "/hot — positions currently near liquidation\n"
        "/clear — reset Claude conversation history\n"
        "/help — this message\n\n"
        "💬 _Any other message is sent to Claude AI — ask about logs, errors, code, DeFi concepts, anything._"
    )


HANDLERS = {
    "/status": cmd_status,
    "/pnl":    cmd_pnl,
    "/stats":  cmd_pnl,
    "/hot":    cmd_hot,
    "/clear":  cmd_clear,
    "/help":   cmd_help,
    "/start":  cmd_help,
}


# ── Main poll loop ─────────────────────────────────────────────────────────

async def poll_loop() -> None:
    if not TOKEN:
        logger.error("TELEGRAM_BOT_TOKEN not set — exiting")
        return

    offset = 0
    ai_label = "✅" if ANTHROPIC_KEY else "❌ (no ANTHROPIC_API_KEY)"
    logger.info(f"[TGBot] Starting — chat_id={CHAT_ID}  Claude AI={ai_label}")

    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            await tg_post(session, "sendMessage", {
                "chat_id": CHAT_ID,
                "text": (
                    f"🤖 *Bot online*\n\n"
                    f"Commands: /status /pnl /hot /help\n"
                    f"Claude AI: {ai_label}\n\n"
                    f"Send any message to chat with Claude."
                ),
                "parse_mode": "Markdown",
            })
        except Exception as e:
            logger.warning(f"[TGBot] Startup message failed: {e}")

        while True:
            try:
                data = await tg_get(session, "getUpdates",
                                    offset=offset, timeout=POLL_TIMEOUT,
                                    allowed_updates="message")
                updates = data.get("result", [])
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.warning(f"[TGBot] getUpdates error: {e} — retry in 5s")
                await asyncio.sleep(5)
                continue

            for update in updates:
                offset = update["update_id"] + 1
                msg     = update.get("message", {})
                text    = msg.get("text", "").strip()
                chat_id = msg.get("chat", {}).get("id")

                if not text or not chat_id:
                    continue

                if str(chat_id) != str(CHAT_ID):
                    logger.info(f"[TGBot] Ignored message from unknown chat {chat_id}")
                    continue

                cmd = text.split("@")[0].split()[0].lower()

                if cmd in HANDLERS:
                    logger.info(f"[TGBot] Command: {cmd}")
                    try:
                        await HANDLERS[cmd](session, chat_id)
                    except Exception as e:
                        logger.error(f"[TGBot] Handler {cmd} error: {e}")
                        try:
                            await reply(session, chat_id, f"⚠️ Error: `{e}`")
                        except Exception:
                            pass
                else:
                    # Route to Claude
                    logger.info(f"[TGBot] Claude query: {text[:60]}")
                    await send_typing(session, chat_id)
                    try:
                        answer = await ask_claude(text)
                        await reply(session, chat_id, answer, parse_mode="")
                    except Exception as e:
                        logger.error(f"[TGBot] Claude error: {e}")
                        await reply(session, chat_id, f"⚠️ Claude error: {e}")


if __name__ == "__main__":
    asyncio.run(poll_loop())
