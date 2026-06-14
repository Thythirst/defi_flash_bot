"""
Deviation Velocity Tracker.

Tracks how fast Chainlink → market price deviation changes over time.
Computes velocity (percent-per-second) and direction to improve
confidence scoring for liquidation signals.

Fast-converging deviations = higher confidence the feed will update soon.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, List, Optional

import asyncpg

logger = logging.getLogger(__name__)


@dataclass
class DeviationPoint:
    symbol: str
    cl_price: Decimal
    market_price: Decimal
    deviation_pct: Decimal
    timestamp: float  # monotonic


@dataclass
class VelocityResult:
    symbol: str
    deviation_pct: Decimal
    velocity_pps: Decimal          # percent-per-second
    direction: str                 # toward_threshold | away_from_threshold | stable
    confidence: Decimal            # 0.0 - 1.0
    estimated_departure_sec: Optional[float]  # seconds until CL deviates enough to trigger update


class DeviationVelocity:
    """
    Tracks deviation snapshots and computes velocity.
    
    Velocity is computed as: (deviation_now - deviation_prev) / (time_now - time_prev)
    
    Direction interpretation:
    - If deviation is positive (market > CL) and velocity is positive → 
      deviation growing → moving TOWARD threshold → high confidence
    - If deviation is positive and velocity is negative →
      deviation shrinking → moving AWAY from threshold → low confidence
    - If deviation is negative (market < CL) and velocity is negative →
      |deviation| growing → moving TOWARD threshold → high confidence
    - If |velocity| < 0.0001 → stable
    
    Threshold reference: Chainlink aggregators typically trigger at
    0.5% (500_000_000 ppb) for ETH/BTC, 1.0% for LINK, 1.5% for ARB.
    """

    # Deviation thresholds per symbol (in percent)
    THRESHOLDS: Dict[str, float] = {
        "ETH": 0.5,
        "WBTC": 0.5,
        "LINK": 1.0,
        "ARB": 1.5,
        "USDC": 0.5,
        "USDT": 0.5,
        "DAI": 0.5,
    }

    MIN_SAMPLES = 3
    MAX_HISTORY = 20

    def __init__(self, pg_pool):
        self.pg = pg_pool
        self._history: Dict[str, List[DeviationPoint]] = {}
        self._last_persist: Dict[str, float] = {}

    async def record(
        self, symbol: str, cl_price: Decimal, market_price: Decimal
    ) -> Optional[VelocityResult]:
        """
        Record a new deviation data point and compute velocity.
        Returns velocity result, or None if insufficient data.
        """
        if cl_price <= 0 or market_price <= 0:
            return None

        deviation_pct = (market_price - cl_price) / cl_price * 100
        now = time.monotonic()

        point = DeviationPoint(
            symbol=symbol,
            cl_price=cl_price,
            market_price=market_price,
            deviation_pct=deviation_pct,
            timestamp=now,
        )

        if symbol not in self._history:
            self._history[symbol] = []
        self._history[symbol].append(point)

        # Trim old
        if len(self._history[symbol]) > self.MAX_HISTORY:
            self._history[symbol] = self._history[symbol][-self.MAX_HISTORY:]

        # Persist to PG every ~30s per symbol
        if now - self._last_persist.get(symbol, 0) > 30:
            await self._persist(point)
            self._last_persist[symbol] = now

        if len(self._history[symbol]) < self.MIN_SAMPLES:
            return None

        return self._compute_velocity(symbol)

    def _compute_velocity(self, symbol: str) -> VelocityResult:
        """Compute velocity from recent history points."""
        points = self._history[symbol]
        now = points[-1].timestamp
        latest = points[-1].deviation_pct

        # Use weighted regression on last N points
        if len(points) >= 3:
            # Simple linear regression on last 5 points (or all if < 5)
            subset = points[-min(5, len(points)):]
            n = len(subset)
            sum_t = Decimal("0")
            sum_d = Decimal("0")
            sum_tt = Decimal("0")
            sum_td = Decimal("0")

            t0 = Decimal(str(subset[0].timestamp))
            for p in subset:
                t = Decimal(str(p.timestamp)) - t0
                d = p.deviation_pct
                sum_t += t
                sum_d += d
                sum_tt += t * t
                sum_td += t * d

            denom = (Decimal(str(n)) * sum_tt - sum_t * sum_t)
            if denom > 0:
                velocity = (Decimal(str(n)) * sum_td - sum_t * sum_d) / denom
            else:
                velocity = Decimal("0")
        else:
            # Simple delta
            prev = points[-2]
            dt = Decimal(str(now - prev.timestamp))
            if dt > 0:
                velocity = (latest - prev.deviation_pct) / dt
            else:
                velocity = Decimal("0")

        # Direction
        threshold = Decimal(str(self.THRESHOLDS.get(symbol, 1.0)))
        abs_dev = abs(latest)

        if abs(velocity) < Decimal("0.0001"):
            direction = "stable"
        elif latest > 0 and velocity > 0:
            direction = "toward_threshold"  # market rising above CL = more deviation
        elif latest < 0 and velocity < 0:
            direction = "toward_threshold"  # market dropping below CL = more deviation
        else:
            direction = "away_from_threshold"

        # Confidence score
        confidence = self._compute_confidence(abs_dev, threshold, velocity, direction)

        # Estimated time until threshold breach
        remaining = threshold - abs_dev
        if velocity != 0 and remaining > 0 and direction == "toward_threshold":
            eta = float(remaining / abs(velocity))
        else:
            eta = None

        return VelocityResult(
            symbol=symbol,
            deviation_pct=latest,
            velocity_pps=velocity,
            direction=direction,
            confidence=confidence,
            estimated_departure_sec=eta,
        )

    def _compute_confidence(
        self, abs_dev: Decimal, threshold: Decimal, velocity: Decimal, direction: str
    ) -> Decimal:
        """
        Compute confidence (0-1) that this feed will trigger soon.
        
        Factors:
        - Proximity to threshold: closer = higher
        - Velocity toward threshold: faster = higher
        - Direction: toward = bonus, away = penalty
        """
        # Proximity factor: 0 (far) to 0.5 (at threshold)
        if threshold > 0:
            proximity = min(abs_dev / threshold, Decimal("1"))
            proximity_score = proximity * Decimal("0.5")
        else:
            proximity_score = Decimal("0")

        # Velocity factor: 0 to 0.3
        abs_vel = abs(velocity)
        if abs_vel > Decimal("0.1"):
            vel_score = Decimal("0.3")
        elif abs_vel > Decimal("0.01"):
            vel_score = Decimal("0.2")
        elif abs_vel > Decimal("0.001"):
            vel_score = Decimal("0.1")
        else:
            vel_score = Decimal("0")

        # Direction factor: toward = +0.2, away = +0.0, stable = +0.05
        if direction == "toward_threshold":
            dir_score = Decimal("0.2")
        elif direction == "stable":
            dir_score = Decimal("0.05")
        else:
            dir_score = Decimal("0")

        confidence = proximity_score + vel_score + dir_score
        return min(confidence, Decimal("1.0")).quantize(Decimal("0.01"))

    async def _persist(self, point: DeviationPoint):
        """Write deviation point to PostgreSQL."""
        try:
            async with self.pg.acquire() as conn:
                await conn.execute(
                    """INSERT INTO deviation_snapshots 
                       (symbol, cl_price, market_price, deviation_pct)
                       VALUES ($1,$2,$3,$4)""",
                    point.symbol, point.cl_price, point.market_price, point.deviation_pct,
                )
        except Exception:
            logger.debug("Failed to persist deviation snapshot for %s", point.symbol, exc_info=True)

    def get_summary(self) -> List[VelocityResult]:
        """Get current velocity for all tracked symbols."""
        results = []
        for symbol in self._history:
            if len(self._history[symbol]) >= self.MIN_SAMPLES:
                result = self._compute_velocity(symbol)
                if result:
                    results.append(result)
        return sorted(results, key=lambda r: float(r.confidence), reverse=True)
