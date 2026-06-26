"""
tg_alerts.py — Fire-and-forget Telegram alert sender.
Imported by pipeline_v3.py and tg_bot.py.
"""
import asyncio
import logging
import os
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
_CHAT:  str = os.getenv("TELEGRAM_CHAT_ID", "")
_BASE   = "https://api.telegram.org"


async def send(text: str, parse_mode: str = "Markdown") -> bool:
    """Send a Telegram message. Returns True on success, False on failure."""
    if not _TOKEN or not _CHAT:
        return False
    url = f"{_BASE}/bot{_TOKEN}/sendMessage"
    payload = {"chat_id": _CHAT, "text": text, "parse_mode": parse_mode,
                "disable_web_page_preview": True}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post(url, json=payload,
                              timeout=aiohttp.ClientTimeout(total=8)) as r:
                ok = r.status == 200
                if not ok:
                    body = await r.text()
                    logger.warning(f"[TG] send failed {r.status}: {body[:120]}")
                return ok
    except Exception as e:
        logger.warning(f"[TG] send error: {e}")
        return False


def send_sync(text: str, parse_mode: str = "Markdown") -> None:
    """Fire-and-forget from a sync context (creates a new event loop if needed)."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.ensure_future(send(text, parse_mode))
        else:
            loop.run_until_complete(send(text, parse_mode))
    except Exception:
        pass
