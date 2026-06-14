#!/usr/bin/env python3
"""
scripts/liquidation_watchdog.py — Runs every ~12s, checks Aave v3 borrowers,
alerts to Telegram when health factor drops below threshold.
"""

import asyncio
import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from scanner.liquidation_monitor import LiquidationMonitor
from scanner.aave_v3 import format_hf_status

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
logger = logging.getLogger("liquidation_watchdog")

ALERT_THRESHOLD = 1.05


async def main():
    rpc_url = os.getenv("ALCHEMY_HTTP_URL")
    if not rpc_url:
        logger.error("ALCHEMY_HTTP_URL not set")
        sys.exit(1)

    monitor = LiquidationMonitor(
        rpc_url=rpc_url,
        alert_threshold=ALERT_THRESHOLD,
        poll_interval=0,  # single scan, managed by cron
    )

    # Bootstrap from last 200K blocks
    count = await monitor.bootstrap_borrowers(lookback_blocks=200_000)
    logger.info("Bootstrapped %d borrowers", count)

    # Scan all
    alerted = await monitor.scan_all()
    critical = [u for u in alerted if u.latest_hf is not None and u.latest_hf < 1.1]

    if critical:
        lines = [f"*🚨 AAVE LIQUIDATION ALERT* — {datetime.utcnow().isoformat()}Z\n"]
        lines.append(f"*{len(critical)} borrowers near liquidation*\n")
        for u in critical[:20]:
            _, hf, collat, debt = u.history[-1]
            status = format_hf_status(hf)
            lines.append(
                f"`{u.address[:20]}...`  HF={hf:.4f}  "
                f"Collat=${collat/1e8:.0f}  Debt=${debt/1e8:.0f}  {status}"
            )
        msg = "\n".join(lines)
        # Print for cron delivery
        print(msg)
    else:
        print(f"✅ All {count} borrowers safe. Lowest HF: {min(monitor.users.values(), key=lambda u: u.latest_hf or float('inf')).latest_hf:.4f}")


if __name__ == "__main__":
    asyncio.run(main())
