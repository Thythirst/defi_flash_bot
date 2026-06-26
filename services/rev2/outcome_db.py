#!/usr/bin/env python3
"""
outcome_db.py — SQLite-backed outcome tracking.
Tracks confirmed/reverted/lost_race with Bayesian win rates.
Replaces shadow_candidates.csv and Redis outcome hashes.
"""
import logging
import sqlite3
import time
from contextlib import contextmanager
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

DB_PATH = "liquidations.db"


class OutcomeDB:
    def __init__(self, path: str = DB_PATH):
        self.path = path
        self._conn: Optional[sqlite3.Connection] = None

    def init(self):
        with self._get_conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS candidates (
                    address         TEXT PRIMARY KEY,
                    hf              REAL,
                    collateral_usd  REAL,
                    debt_usd        REAL,
                    best_collateral TEXT,
                    best_debt       TEXT,
                    estimated_ev    REAL,
                    last_updated    INTEGER
                );
                CREATE TABLE IF NOT EXISTS outcomes (
                    tx_hash             TEXT PRIMARY KEY,
                    borrower            TEXT,
                    collateral_asset    TEXT,
                    debt_asset          TEXT,
                    result              TEXT DEFAULT 'pending',
                    execution_path      TEXT,
                    estimated_profit    REAL,
                    actual_profit       REAL DEFAULT 0,
                    gas_used            INTEGER DEFAULT 0,
                    gas_cost_usd        REAL DEFAULT 0,
                    slippage_actual     REAL DEFAULT 0,
                    competitor_address  TEXT DEFAULT '',
                    competitor_tx       TEXT DEFAULT '',
                    block_number        INTEGER DEFAULT 0,
                    submitted_at        REAL,
                    confirmed_at        REAL DEFAULT 0,
                    latency_ms          REAL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS oracle_events (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset           TEXT,
                    old_price       REAL,
                    new_price       REAL,
                    deviation_pct   REAL,
                    triggered_rescore INTEGER DEFAULT 0,
                    timestamp       INTEGER
                );
                CREATE TABLE IF NOT EXISTS competitors (
                    address         TEXT PRIMARY KEY,
                    wins            INTEGER DEFAULT 0,
                    first_seen      INTEGER,
                    last_seen       INTEGER,
                    avg_gas_price   REAL DEFAULT 0,
                    notes           TEXT DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_outcomes_borrower ON outcomes(borrower);
                CREATE INDEX IF NOT EXISTS idx_outcomes_result   ON outcomes(result);
                CREATE INDEX IF NOT EXISTS idx_outcomes_submitted ON outcomes(submitted_at);
            """)
        logger.info(f"[DB] Initialized at {self.path}")

    @contextmanager
    def _get_conn(self):
        conn = sqlite3.connect(self.path, timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def upsert_candidate(self, address: str, hf: float, collateral_usd: float,
                          debt_usd: float, best_collateral: str, best_debt: str,
                          estimated_ev: float):
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO candidates (address, hf, collateral_usd, debt_usd,
                    best_collateral, best_debt, estimated_ev, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(address) DO UPDATE SET
                    hf=excluded.hf, collateral_usd=excluded.collateral_usd,
                    debt_usd=excluded.debt_usd, estimated_ev=excluded.estimated_ev,
                    last_updated=excluded.last_updated
            """, (address, hf, collateral_usd, debt_usd,
                  best_collateral, best_debt, estimated_ev, int(time.time())))

    def record_submission(self, tx_hash: str, borrower: str, collateral_asset: str,
                           debt_asset: str, execution_path: str, estimated_profit: float):
        with self._get_conn() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO outcomes
                    (tx_hash, borrower, collateral_asset, debt_asset,
                     execution_path, estimated_profit, submitted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (tx_hash, borrower, collateral_asset, debt_asset,
                  execution_path, estimated_profit, time.time()))

    def record_confirmation(self, tx_hash: str, actual_profit: float,
                             gas_used: int, gas_cost_usd: float,
                             slippage_actual: float, block_number: int):
        confirmed_at = time.time()
        with self._get_conn() as conn:
            conn.execute("""
                UPDATE outcomes SET result='confirmed', actual_profit=?,
                    gas_used=?, gas_cost_usd=?, slippage_actual=?,
                    block_number=?, confirmed_at=?,
                    latency_ms=(? - submitted_at)*1000
                WHERE tx_hash=?
            """, (actual_profit, gas_used, gas_cost_usd, slippage_actual,
                  block_number, confirmed_at, confirmed_at, tx_hash))

    def record_revert(self, tx_hash: str, block_number: int):
        t = time.time()
        with self._get_conn() as conn:
            conn.execute("""
                UPDATE outcomes SET result='reverted', block_number=?,
                    confirmed_at=?, latency_ms=(? - submitted_at)*1000
                WHERE tx_hash=?
            """, (block_number, t, t, tx_hash))

    def mark_reverted(self, tx_hash: str, block_number: int):
        """Alias for record_revert — used by ConfirmationTracker."""
        self.record_revert(tx_hash, block_number)

    def record_lost_race(self, borrower: str, competitor_address: str,
                          competitor_tx: str, block_number: int,
                          competitor_gas_price: int = 0):
        with self._get_conn() as conn:
            # Step 1: Mark our pending outcome as lost (if we submitted)
            conn.execute("""
                UPDATE outcomes SET result='lost_race', competitor_address=?,
                    competitor_tx=?, block_number=?
                WHERE borrower=? AND result='pending'
            """, (competitor_address, competitor_tx, block_number, borrower))

            # Step 2: Insert competitor observation even if we never submitted
            # This captures intelligence on all liquidations we watched
            conn.execute("""
                INSERT INTO outcomes (borrower, result, competitor_address,
                    competitor_tx, block_number, estimated_profit, actual_profit)
                VALUES (?, 'lost_race_observed', ?, ?, ?, 0, 0)
                ON CONFLICT DO NOTHING
            """, (borrower, competitor_address, competitor_tx, block_number))

            # Step 3: Upsert competitor with gas price tracking
            if competitor_gas_price > 0:
                conn.execute("""
                    INSERT INTO competitors (address, wins, first_seen, last_seen, avg_gas_price)
                    VALUES (?, 1, ?, ?, ?)
                    ON CONFLICT(address) DO UPDATE SET
                        wins         = wins + 1,
                        last_seen    = excluded.last_seen,
                        avg_gas_price = (avg_gas_price * wins + excluded.avg_gas_price)
                                        / (wins + 1)
                """, (competitor_address, int(time.time()), int(time.time()), competitor_gas_price))
            else:
                conn.execute("""
                    INSERT INTO competitors (address, wins, first_seen, last_seen)
                    VALUES (?, 1, ?, ?)
                    ON CONFLICT(address) DO UPDATE SET
                        wins=wins+1, last_seen=excluded.last_seen
                """, (competitor_address, int(time.time()), int(time.time())))

        logger.warning(
            f"[DB] LOST RACE: {borrower[:8]} → {competitor_address[:8]} "
            f"gas={competitor_gas_price/1e9:.4f}gwei block={block_number}"
        )

    def record_oracle_event(self, asset: str, old_price: float,
                             new_price: float, triggered_rescore: bool):
        dev = abs(new_price - old_price) / old_price * 100 if old_price else 0
        with self._get_conn() as conn:
            conn.execute("""
                INSERT INTO oracle_events (asset, old_price, new_price,
                    deviation_pct, triggered_rescore, timestamp)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (asset, old_price, new_price, dev, int(triggered_rescore), int(time.time())))

    def win_rates(self) -> Dict[str, dict]:
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT execution_path,
                       SUM(CASE WHEN result='confirmed' THEN 1 ELSE 0 END) as wins,
                       SUM(CASE WHEN result IN ('reverted','lost_race') THEN 1 ELSE 0 END) as losses
                FROM outcomes GROUP BY execution_path
            """).fetchall()
        results = {}
        for row in rows:
            alpha = 1 + row['wins']
            beta = 1 + row['losses']
            results[row['execution_path']] = {
                'wins': row['wins'], 'losses': row['losses'],
                'bayesian_win_rate': alpha / (alpha + beta),
                'alpha': alpha, 'beta': beta,
            }
        return results

    def top_competitors(self, limit: int = 10) -> List[dict]:
        with self._get_conn() as conn:
            rows = conn.execute(
                "SELECT address, wins, first_seen, last_seen, avg_gas_price FROM competitors "
                "ORDER BY wins DESC LIMIT ?", (limit,)
            ).fetchall()
        return [dict(r) for r in rows]

    def recent_outcomes(self, limit: int = 20) -> List[dict]:
        with self._get_conn() as conn:
            rows = conn.execute("""
                SELECT tx_hash, borrower, result, execution_path,
                       estimated_profit, actual_profit, latency_ms,
                       competitor_address, block_number, submitted_at
                FROM outcomes ORDER BY submitted_at DESC LIMIT ?
            """, (limit,)).fetchall()
        return [dict(r) for r in rows]

    def pnl_summary(self) -> dict:
        with self._get_conn() as conn:
            row = conn.execute("""
                SELECT COUNT(*) as total,
                       SUM(CASE WHEN result='confirmed' THEN 1 ELSE 0 END) as confirmed,
                       SUM(CASE WHEN result='lost_race' THEN 1 ELSE 0 END) as lost,
                       SUM(CASE WHEN result='reverted' THEN 1 ELSE 0 END) as reverted,
                       SUM(CASE WHEN result='confirmed' THEN actual_profit ELSE 0 END) as total_profit,
                       SUM(gas_cost_usd) as total_gas,
                       AVG(CASE WHEN result='confirmed' THEN latency_ms END) as avg_latency_ms
                FROM outcomes
            """).fetchone()
        return dict(row) if row else {}
