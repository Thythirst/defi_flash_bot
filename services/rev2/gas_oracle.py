"""
gas_oracle.py — Dynamic gas oracle for competitive liquidation bidding
Replaces static base_fee × multiplier with a trailing percentile approach.

Problem with current approach (base_fee × 4):
    - During gas spikes: base fee jumps 2× in one block → underbids
    - During quiet periods: 4× multiplier overpays unnecessarily
    - Priority fee is guessed (base × 0.5) not market-derived

This oracle:
    - Tracks last N blocks of actual priority fees paid by liquidation txs
    - Bids at configurable percentile (default P75) of recent tips
    - Adjusts maxFeePerGas to cover realistic base fee surges
    - Stays competitive without overbidding during quiet markets

Integration:
    oracle = GasOracle(shared_state=self.shared_state)

    # Feed it on every new block (already have on_new_block hook):
    oracle.update(block_number, base_fee_wei, priority_fee_wei)

    # Use in _get_competitive_gas_price():
    max_fee, priority = oracle.recommend()
"""

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Arbitrum base fee typical ranges
ARBITRUM_BASE_FEE_TYPICAL  = 10_000_000      # 0.01 gwei — normal
ARBITRUM_BASE_FEE_STRESSED = 1_000_000_000   # 1.0 gwei — congested

# Minimum priority fee — always tip at least this much
MIN_PRIORITY_FEE = 1_000_000   # 0.001 gwei

# Maximum priority fee cap — don't overpay even during spikes
MAX_PRIORITY_FEE = 5_000_000_000  # 5 gwei


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class GasSnapshot:
    """One block's gas data point."""
    block_number:    int
    base_fee_wei:    int
    priority_fee_wei:int   # tip actually paid in this block
    timestamp:       float = field(default_factory=time.time)


@dataclass
class GasRecommendation:
    """Output of GasOracle.recommend()."""
    max_fee_per_gas:         int   # EIP-1559 maxFeePerGas
    max_priority_fee_per_gas:int   # EIP-1559 maxPriorityFeePerGas
    base_fee_current:        int   # latest base fee from SharedState
    percentile_used:         float # which percentile was applied
    source:                  str   # "oracle" | "fallback"

    @property
    def max_fee_gwei(self) -> float:
        return self.max_fee_per_gas / 1e9

    @property
    def priority_gwei(self) -> float:
        return self.max_priority_fee_per_gas / 1e9

    def log_line(self) -> str:
        return (
            f"maxFee={self.max_fee_gwei:.4f}gwei "
            f"priority={self.priority_gwei:.4f}gwei "
            f"base={self.base_fee_current/1e9:.4f}gwei "
            f"p{int(self.percentile_used*100)} source={self.source}"
        )


# ---------------------------------------------------------------------------
# GasOracle
# ---------------------------------------------------------------------------

class GasOracle:
    """
    Trailing percentile gas oracle for EIP-1559 liquidation transactions.

    Maintains a rolling window of base fees and priority fees.
    Recommends gas parameters that are competitive for the current
    market conditions without overbidding during quiet periods.

    Strategy:
        priority_fee = P(percentile) of recent tips
                       — tracks what competitors are actually paying
        max_fee      = base_fee × surge_buffer + priority_fee
                       — covers realistic base fee increases between
                         now and when tx is mined (1-3 blocks on Arbitrum)

    Percentile guidance:
        P50 — median tip — competitive in quiet markets, may lose races
        P75 — default — wins most uncontested liquidations
        P90 — aggressive — wins contested cascades, costs more
        P95 — max — use only during known cascade events

    Usage:
        oracle = GasOracle(shared_state=shared_state)

        # In block handler (on_new_block):
        priority = await rpc.w3.eth.max_priority_fee
        oracle.update(block_number, base_fee_wei, priority)

        # In _get_competitive_gas_price():
        rec = oracle.recommend()
        return rec.max_fee_per_gas, rec.max_priority_fee_per_gas
    """

    def __init__(
        self,
        shared_state          = None,   # SharedState from hot_path_fix.py
        window:          int   = 50,    # blocks of history to track
        percentile:      float = 0.75,  # P75 default
        surge_buffer:    float = 2.0,   # maxFee = base × this + priority
        cascade_percentile: float = 0.90,  # used when cascade detected
        cascade_threshold:  int   = 5,     # N liquidations/block = cascade
    ):
        self._state              = shared_state
        self._window             = window
        self._percentile         = percentile
        self._surge_buffer       = surge_buffer
        self._cascade_percentile = cascade_percentile
        self._cascade_threshold  = cascade_threshold

        self._snapshots: deque[GasSnapshot] = deque(maxlen=window)
        self._cascade_mode   = False
        self._cascade_until  = 0.0    # timestamp when cascade mode expires
        self._cascade_ttl    = 60.0   # seconds to stay in cascade mode

        # Stats
        self._recommendations = 0
        self._cascade_activations = 0

    def update(
        self,
        block_number:     int,
        base_fee_wei:     int,
        priority_fee_wei: int = 0,
    ) -> None:
        """
        Feed a new block's gas data into the oracle.
        Call from on_new_block() after SharedState.on_new_block().

        If priority_fee_wei is 0 (not fetched), oracle uses base_fee
        history only and estimates tip from base fee ratio.
        """
        self._snapshots.append(GasSnapshot(
            block_number     = block_number,
            base_fee_wei     = base_fee_wei,
            priority_fee_wei = max(priority_fee_wei, MIN_PRIORITY_FEE),
        ))

        # Check if cascade mode should expire
        if self._cascade_mode and time.time() > self._cascade_until:
            self._cascade_mode = False
            logger.info("[GasOracle] Cascade mode expired — returning to normal")

    def activate_cascade(self, reason: str = "manual") -> None:
        """
        Switch to aggressive (P90) bidding.
        Call when cascade event detected (multiple liquidations in one block,
        large price move, or competitor activity spike).
        Automatically expires after cascade_ttl seconds.
        """
        self._cascade_mode  = True
        self._cascade_until = time.time() + self._cascade_ttl
        self._cascade_activations += 1
        logger.info(
            f"[GasOracle] CASCADE MODE activated ({reason}) — "
            f"switching to P{int(self._cascade_percentile*100)} for {self._cascade_ttl}s"
        )

    def recommend(self, force_cascade: bool = False) -> GasRecommendation:
        """
        Compute recommended EIP-1559 gas parameters.

        Returns GasRecommendation with maxFeePerGas and maxPriorityFeePerGas.
        Falls back to conservative defaults if insufficient history.
        """
        self._recommendations += 1

        # Get current base fee from SharedState (zero RPC, always fresh)
        current_base_fee = (
            self._state.base_fee_wei
            if self._state and self._state.base_fee_wei > 0
            else ARBITRUM_BASE_FEE_TYPICAL
        )

        # Determine which percentile to use
        in_cascade = force_cascade or self._cascade_mode
        pct = self._cascade_percentile if in_cascade else self._percentile

        # Need at least 5 data points for meaningful percentile
        if len(self._snapshots) < 5:
            return self._fallback(current_base_fee, pct)

        # Compute percentile of recent priority fees
        tips = sorted(s.priority_fee_wei for s in self._snapshots)
        idx  = int(len(tips) * pct)
        idx  = min(idx, len(tips) - 1)
        priority_fee = max(tips[idx], MIN_PRIORITY_FEE)
        priority_fee = min(priority_fee, MAX_PRIORITY_FEE)

        # maxFeePerGas must cover base fee surge + our tip
        # surge_buffer=2.0 covers 2× base fee spike (common on Arbitrum)
        max_fee = int(current_base_fee * self._surge_buffer) + priority_fee

        logger.debug(
            f"[GasOracle] {GasRecommendation(max_fee, priority_fee, current_base_fee, pct, 'oracle').log_line()} "
            f"history={len(self._snapshots)}"
        )

        return GasRecommendation(
            max_fee_per_gas          = max_fee,
            max_priority_fee_per_gas = priority_fee,
            base_fee_current         = current_base_fee,
            percentile_used          = pct,
            source                   = "oracle",
        )

    def _fallback(self, base_fee: int, pct: float) -> GasRecommendation:
        """Conservative fallback when insufficient history."""
        priority = max(int(base_fee * 0.5), MIN_PRIORITY_FEE)
        max_fee  = int(base_fee * self._surge_buffer) + priority
        logger.debug(f"[GasOracle] Fallback (insufficient history) base={base_fee/1e9:.4f}gwei")
        return GasRecommendation(
            max_fee_per_gas          = max_fee,
            max_priority_fee_per_gas = priority,
            base_fee_current         = base_fee,
            percentile_used          = pct,
            source                   = "fallback",
        )

    @property
    def stats(self) -> dict:
        if not self._snapshots:
            return {"history": 0, "cascade_mode": self._cascade_mode}
        tips = sorted(s.priority_fee_wei for s in self._snapshots)
        return {
            "history":             len(self._snapshots),
            "cascade_mode":        self._cascade_mode,
            "cascade_activations": self._cascade_activations,
            "recommendations":     self._recommendations,
            "p50_tip_gwei":        tips[len(tips)//2] / 1e9,
            "p75_tip_gwei":        tips[int(len(tips)*0.75)] / 1e9,
            "p90_tip_gwei":        tips[int(len(tips)*0.90)] / 1e9,
            "current_base_gwei":   (self._state.base_fee_wei / 1e9
                                    if self._state else 0),
        }

    def log_stats(self) -> None:
        s = self.stats
        logger.info(
            f"[GasOracle] "
            f"history={s['history']} "
            f"p50={s['p50_tip_gwei']:.4f}gwei "
            f"p75={s['p75_tip_gwei']:.4f}gwei "
            f"p90={s['p90_tip_gwei']:.4f}gwei "
            f"cascade={'ON' if s['cascade_mode'] else 'off'} "
            f"activations={s['cascade_activations']}"
        )


# ---------------------------------------------------------------------------
# pipeline_v3.py integration
# ---------------------------------------------------------------------------
#
# 1. Import:
#       from gas_oracle import GasOracle
#
# 2. In setup():
#       self.gas_oracle = GasOracle(
#           shared_state       = self.shared_state,
#           window             = 50,      # 50 blocks = ~12.5s of history
#           percentile         = 0.75,    # P75 default — calibrate from backtest
#           surge_buffer       = 2.0,     # 2× base fee for maxFeePerGas
#           cascade_percentile = 0.90,    # P90 during cascade events
#       )
#
# 3. In block handler (on_new_block), AFTER shared_state.on_new_block():
#
#       # Feed oracle — fetch tip once per block
#       try:
#           priority_fee = await self.rpc.w3.eth.max_priority_fee
#       except Exception:
#           priority_fee = 0
#       self.gas_oracle.update(block_number, base_fee_wei, priority_fee)
#
# 4. Replace _get_competitive_gas_price():
#
#       async def _get_competitive_gas_price(self) -> tuple[int, int]:
#           rec = self.gas_oracle.recommend()
#           logger.debug(f"[Gas] {rec.log_line()}")
#           return rec.max_fee_per_gas, rec.max_priority_fee_per_gas
#
# 5. Activate cascade when multiple liquidations detected in one block:
#
#       # In _on_liquidatable() or on_new_block():
#       if liquidatable_count >= 3:
#           self.gas_oracle.activate_cascade(f"{liquidatable_count} positions underwater")
#
#       # Also activate when competitor liquidation detected:
#       # In handle_liquidation_log() when event.is_competitor:
#       self.gas_oracle.activate_cascade("competitor liquidation detected")
#
# 6. In stats loop:
#       self.gas_oracle.log_stats()
#
# 7. In shutdown(): nothing needed — no background task
#
# ---------------------------------------------------------------------------
#
# Calibration after backtest:
#
#   The backtest will show winner_gas_price / base_fee ratio distribution.
#   Use that to set the right percentile:
#
#   sqlite3 backtest_full2.db "
#     SELECT
#       AVG(CAST(winner_gas_price AS REAL) / base_fee) as avg_mult,
#       MAX(CAST(winner_gas_price AS REAL) / base_fee) as max_mult
#     FROM liquidations WHERE base_fee > 0 AND would_win_current = 0
#   "
#
#   If avg_mult for races you're LOSING is 3.5×, raise surge_buffer to 4.0
#   If avg_mult is 1.8×, current 2.0 is fine.
#
#   For percentile: check what % of races your current P75 wins.
#   If win rate is 83% (from 1K sample), P75 is calibrated correctly.
#   If win rate drops on full 51K dataset, raise to P85 or P90.
#
# ---------------------------------------------------------------------------
