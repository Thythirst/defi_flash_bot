#!/usr/bin/env python3
"""
gas_calibration.py — Weekly: calibrate gas oracle from backtest data
Reads backtest_full2.db, computes optimal surge_buffer and percentile,
writes updated config for pipeline to pick up on next restart.

Cron entry (Sunday 11:00 UTC):
    0 11 * * 0 /root/defi_flash_bot/prod/venv/bin/python3 \\
        /root/defi_flash_bot/prod/scripts/gas_calibration.py \\
        >> /root/defi_flash_bot/prod/logs/gas_calibration.log 2>&1
"""

import json, logging, os, sqlite3
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

PROD_DIR    = Path("/root/defi_flash_bot/prod")
BACKTEST_DB = PROD_DIR / "backtest_full2.db"
GAS_CONFIG  = PROD_DIR / "config" / "gas_oracle.json"

def calibrate():
    if not BACKTEST_DB.exists():
        logger.warning("[GasCalib] backtest_full2.db not found — skipping")
        return

    conn = sqlite3.connect(str(BACKTEST_DB))

    # How much do winners pay vs base fee?
    rows = conn.execute("""
        SELECT
            AVG(CAST(winner_gas_price AS REAL) / base_fee) as avg_winner_mult,
            MAX(CAST(winner_gas_price AS REAL) / base_fee) as max_winner_mult,
            COUNT(*) as total
        FROM liquidations
        WHERE base_fee > 0 AND winner_gas_price > 0
    """).fetchone()

    if not rows or not rows[0]:
        logger.warning("[GasCalib] Insufficient data for calibration")
        conn.close()
        return

    avg_mult, max_mult, total = rows
    logger.info(f"[GasCalib] Winner gas: avg={avg_mult:.2f}x max={max_mult:.2f}x ({total:,} events)")

    # Current win rate
    won  = conn.execute("SELECT COUNT(*) FROM liquidations WHERE would_win_current=1").fetchone()[0]
    enr  = conn.execute("SELECT COUNT(*) FROM liquidations WHERE base_fee > 0").fetchone()[0]
    win_rate = won / enr if enr else 0
    logger.info(f"[GasCalib] Current win rate: {win_rate:.1%} ({won:,}/{enr:,})")

    # Races lost — what was their gas multiple?
    lost_rows = conn.execute("""
        SELECT AVG(CAST(winner_gas_price AS REAL) / base_fee)
        FROM liquidations
        WHERE base_fee > 0 AND would_win_current = 0
    """).fetchone()
    lost_mult = lost_rows[0] if lost_rows and lost_rows[0] else avg_mult

    # Recommended surge_buffer: cover the avg lost race multiple + 10% margin
    recommended_surge = min(round(lost_mult * 1.1, 1), 5.0)

    # Recommended percentile: if win rate < 80%, raise to P85
    if win_rate >= 0.85:
        recommended_pct = 0.75
    elif win_rate >= 0.75:
        recommended_pct = 0.80
    else:
        recommended_pct = 0.90

    config = {
        "surge_buffer":         recommended_surge,
        "percentile":           recommended_pct,
        "cascade_percentile":   min(recommended_pct + 0.10, 0.95),
        "calibrated_from":      str(BACKTEST_DB),
        "calibrated_at":        str(Path(__file__).stat().st_mtime),
        "win_rate":             round(win_rate, 4),
        "avg_winner_mult":      round(avg_mult, 2),
        "lost_race_mult":       round(lost_mult, 2),
        "sample_size":          total,
    }

    GAS_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    GAS_CONFIG.write_text(json.dumps(config, indent=2))
    conn.close()

    logger.info(
        f"[GasCalib] Config written: surge_buffer={recommended_surge} "
        f"percentile=P{int(recommended_pct*100)} "
        f"→ {GAS_CONFIG}"
    )
    logger.info("[GasCalib] Restart pipeline to apply new gas config")

if __name__ == "__main__":
    calibrate()
