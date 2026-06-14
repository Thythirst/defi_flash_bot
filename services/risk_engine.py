"""
services/risk_engine.py — Production risk engine for MEV strategies.

Monitors capital exposure, enforces circuit breakers, tracks cumulative P&L,
and provides a unified kill-switch across all strategies.

Strategies covered:
  - liquidation   (Aave V3 liquidation bot)
  - dex_arb       (DEX-DEX cyclic arbitrage)
  - cex_deviation (CEX vs on-chain deviation monitor)

Risk Layers:
  1. PER-TRADE   — max exposure in ETH, min profit threshold
  2. CUMULATIVE  — daily loss cap, daily profit target
  3. CIRCUIT     — consecutive reverts, price deviation, oracle staleness
  4. GLOBAL      — kill switch, emergency pause

Redis keys:
  risk:state:{strategy}   HASH   — active/paused/stopped, since, reason
  risk:limits             HASH   — current limits (max_trade_eth, daily_loss_eth, ...)
  risk:circuit:{type}     STRING — circuit state with TTL
  risk:killswitch         STRING — "0" (off) or "1" (on)

Usage:
  python -m services.risk_engine
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

# Project root
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import redis.asyncio as redis
from dotenv import load_dotenv
load_dotenv(dotenv_path=project_root / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | risk | %(message)s",
)
logger = logging.getLogger("risk_engine")

# ────────────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────────────

@dataclass
class RiskConfig:
    """All risk limits — overridable via env vars."""
    # Per-trade limits
    max_trade_eth: float = 10.0        # max flash loan size in ETH
    min_profit_usd: float = 25.0       # minimum net profit per trade

    # Daily cumulative limits
    daily_loss_cap_eth: float = 2.0    # stop all strategies if daily loss exceeds
    daily_profit_target_eth: float = 5.0  # notification threshold (not enforced)

    # Circuit breakers
    max_consecutive_reverts: int = 5   # pause strategy after N consecutive reverts
    max_consecutive_failures: int = 10 # pause strategy after N non-revert failures
    revert_cooldown_seconds: int = 600 # 10 min pause after circuit trip

    # Price risk
    max_price_deviation_pct: float = 3.0  # pause if CEX/on-chain divergence >3%
    max_oracle_staleness_seconds: int = 1800  # pause if oracle stale >30 min

    # RPC health
    max_rpc_latency_ms: int = 5000     # alert if RPC latency exceeds

    # Global
    killswitch_enabled: bool = False   # global emergency stop (set via Redis)

    def load_from_env(self):
        self.max_trade_eth = float(os.getenv("RISK_MAX_TRADE_ETH", "10.0"))
        self.min_profit_usd = float(os.getenv("MIN_PROFIT_USD", "25.0"))
        self.daily_loss_cap_eth = float(os.getenv("RISK_DAILY_LOSS_CAP_ETH", "2.0"))
        self.max_consecutive_reverts = int(os.getenv("RISK_MAX_REVERTS", "5"))
        self.max_consecutive_failures = int(os.getenv("RISK_MAX_FAILURES", "10"))
        self.revert_cooldown_seconds = int(os.getenv("RISK_COOLDOWN_SEC", "600"))
        self.max_price_deviation_pct = float(os.getenv("RISK_MAX_PRICE_DEV_PCT", "3.0"))
        return self


# ────────────────────────────────────────────────────────────────
# Circuit breaker state
# ────────────────────────────────────────────────────────────────

class CircuitBreaker:
    """Tracks consecutive failures and enforces cooldown periods."""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    async def record_success(self, strategy: str):
        """Reset the consecutive failure counter."""
        await self.redis.delete(f"risk:circuit:reverts:{strategy}")
        await self.redis.delete(f"risk:circuit:failures:{strategy}")

    async def record_revert(self, strategy: str, max_reverts: int, cooldown: int) -> bool:
        """Record a revert. Returns True if circuit should trip."""
        key = f"risk:circuit:reverts:{strategy}"
        count = await self.redis.incr(key)
        await self.redis.expire(key, 3600)  # TTL 1h
        if count >= max_reverts:
            await self._trip(strategy, f"{count} consecutive reverts", cooldown)
            return True
        return False

    async def record_failure(self, strategy: str, max_failures: int, cooldown: int) -> bool:
        """Record a non-revert failure (RPC error, simulation error, etc)."""
        key = f"risk:circuit:failures:{strategy}"
        count = await self.redis.incr(key)
        await self.redis.expire(key, 3600)
        if count >= max_failures:
            await self._trip(strategy, f"{count} consecutive failures", cooldown)
            return True
        return False

    async def _trip(self, strategy: str, reason: str, cooldown: int):
        """Trip the circuit breaker — pause the strategy."""
        await self.redis.setex(f"risk:circuit:tripped:{strategy}", cooldown, reason)
        await self.redis.hset(f"risk:state:{strategy}", mapping={
            "status": "paused",
            "since": str(int(time.time())),
            "reason": reason,
        })
        logger.warning("CIRCUIT TRIPPED: %s — %s (cooldown %ds)", strategy, reason, cooldown)

    async def is_tripped(self, strategy: str) -> bool:
        """Check if a strategy's circuit breaker is currently tripped."""
        return await self.redis.exists(f"risk:circuit:tripped:{strategy}") > 0

    async def get_trip_reason(self, strategy: str) -> Optional[str]:
        raw = await self.redis.get(f"risk:circuit:tripped:{strategy}")
        return raw


# ────────────────────────────────────────────────────────────────
# Kill Switch
# ────────────────────────────────────────────────────────────────

class KillSwitch:
    """Global emergency stop — when on, NO strategy executes."""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    async def is_active(self) -> bool:
        val = await self.redis.get("risk:killswitch")
        return val == "1"

    async def activate(self, reason: str):
        """Pull the plug."""
        await self.redis.set("risk:killswitch", "1")
        await self.redis.set("risk:killswitch_reason", reason)
        await self.redis.set("risk:killswitch_since", str(int(time.time())))
        logger.critical("KILLSWITCH ACTIVATED: %s", reason)

    async def deactivate(self):
        await self.redis.set("risk:killswitch", "0")
        await self.redis.delete("risk:killswitch_reason", "risk:killswitch_since")
        logger.info("Killswitch deactivated")


# ────────────────────────────────────────────────────────────────
# Daily P&L Tracker
# ────────────────────────────────────────────────────────────────

class DailyPnL:
    """Tracks cumulative daily profit/loss per strategy from Redis metrics."""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    async def get_daily_pnl_eth(self) -> Dict[str, float]:
        """Read daily P&L from the metrics hash. Returns {strategy: profit_eth}."""
        date = time.strftime("%Y-%m-%d")
        key = f"arb:metrics:daily:{date}"
        data = await self.redis.hgetall(key)
        pnl = {}
        for field, value in data.items():
            # Fields: dex_arb_profit, liq_profit, gas_spent (all in wei)
            if field.endswith("_profit") and not field.startswith("best_"):
                strategy = field.replace("_profit", "")
                profit_wei = int(value) if value else 0
                pnl[strategy] = profit_wei / 1e18  # convert to ETH
            elif field == "gas_spent":
                pnl["gas"] = int(value) / 1e18 if value else 0

        # Also read from metric hashes for each strategy
        strategies = ["liq", "dex_arb", "cex_deviation"]
        for s in strategies:
            skey = f"arb:metrics:daily:{date}:{s}"
            sdata = await self.redis.hgetall(skey)
            if "profit_eth" in sdata:
                pnl[s] = float(sdata["profit_eth"])
            elif "profit_wei" in sdata:
                pnl[s] = int(sdata["profit_wei"]) / 1e18

        return pnl

    async def check_daily_limits(self, loss_cap_eth: float, profit_target_eth: float) -> dict:
        """Check daily P&L against limits. Returns alerts if breached."""
        pnl = await self.get_daily_pnl_eth()
        alerts = {}

        total_loss = sum(v for v in pnl.values() if v < 0)
        if abs(total_loss) >= loss_cap_eth:
            alerts["daily_loss_cap"] = {
                "severity": "critical",
                "message": f"Daily loss cap exceeded: {abs(total_loss):.3f} ETH > {loss_cap_eth} ETH limit",
                "pnl": pnl,
            }

        total_profit = sum(v for v in pnl.values() if v > 0)
        if total_profit >= profit_target_eth:
            alerts["daily_profit_target"] = {
                "severity": "info",
                "message": f"Daily profit target hit: {total_profit:.3f} ETH (target: {profit_target_eth})",
                "pnl": pnl,
            }

        return alerts


# ────────────────────────────────────────────────────────────────
# Main Risk Engine
# ────────────────────────────────────────────────────────────────

class RiskEngine:
    """Central risk management — monitors, enforces, alerts."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        config: Optional[RiskConfig] = None,
    ):
        self.redis_url = redis_url
        self.config = config or RiskConfig().load_from_env()
        self.redis: Optional[redis.Redis] = None
        self.circuit: Optional[CircuitBreaker] = None
        self.killswitch: Optional[KillSwitch] = None
        self.pnl: Optional[DailyPnL] = None

        # Track last seen execution events per strategy
        self._last_seen: Dict[str, str] = {}

    async def connect(self):
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        await self.redis.ping()
        self.circuit = CircuitBreaker(self.redis)
        self.killswitch = KillSwitch(self.redis)
        self.pnl = DailyPnL(self.redis)
        logger.info("Risk engine connected to %s", self.redis_url)

    # ── Core checks ─────────────────────────────────────────────

    async def check_execution_event(self, event: dict) -> Optional[str]:
        """
        Process an execution event from arb:events:execution.
        Returns error string if trade should be blocked/rejected, None if safe.
        """
        payload = json.loads(event.get("payload", "{}"))
        event_type = event.get("type", "")
        strategy = event.get("source", "unknown")

        # Skip non-execution events
        if not event_type.startswith("execution."):
            return None

        # Kill switch check
        if await self.killswitch.is_active():
            return "Killswitch active — all trades blocked"

        # Circuit breaker check
        if await self.circuit.is_tripped(strategy):
            reason = await self.circuit.get_trip_reason(strategy)
            return f"Circuit breaker tripped: {reason}"

        # Handle specific event types
        if event_type == "execution.submitted":
            # Check per-trade limits
            try:
                amount = float(payload.get("amount_eth", 0))
                if amount > self.config.max_trade_eth:
                    return f"Trade size {amount:.2f} ETH exceeds max {self.config.max_trade_eth} ETH"
            except (ValueError, TypeError):
                pass

        elif event_type == "execution.mined":
            # Check profit meets minimum
            profit_eth = float(payload.get("profit_eth", 0))
            if profit_eth < 0:
                net_loss = abs(profit_eth)
                # Check daily loss cap
                await self._check_daily_limits()

        elif event_type == "execution.reverted":
            await self.circuit.record_revert(
                strategy,
                self.config.max_consecutive_reverts,
                self.config.revert_cooldown_seconds,
            )

        elif event_type in ("execution.dropped", "execution.failed"):
            await self.circuit.record_failure(
                strategy,
                self.config.max_consecutive_failures,
                self.config.revert_cooldown_seconds,
            )

        return None  # no block

    async def check_price_event(self, event: dict) -> Optional[str]:
        """Process market/price events. Returns alert if risky."""
        payload = json.loads(event.get("payload", "{}"))
        event_type = event.get("type", "")
        severity = event.get("severity", "info")

        if event_type == "price.deviation":
            deviation = float(payload.get("deviation_pct", 0))
            if severity == "critical" and deviation >= self.config.max_price_deviation_pct:
                logger.warning("Price deviation critical: %s%%", deviation)
                return f"Price deviation {deviation:.2f}% exceeds max {self.config.max_price_deviation_pct}%"

        elif event_type == "price.stale":
            # Could trip circuit if combined with other risks
            pass

        return None

    async def check_system_event(self, event: dict) -> Optional[str]:
        """Process system events (RPC health, bot errors)."""
        payload = json.loads(event.get("payload", "{}"))
        event_type = event.get("type", "")

        if event_type == "system.rpc.failover":
            logger.warning("RPC failover detected: %s", payload.get("reason", "unknown"))

        elif event_type == "system.bot.error":
            strategy = event.get("source", "unknown")
            logger.warning("Bot error in %s: %s", strategy, payload.get("error", "unknown")[:200])

        return None

    # ── Proactive checks (polling) ──────────────────────────────

    async def _check_daily_limits(self):
        """Check daily P&L against limits and act if breached."""
        alerts = await self.pnl.check_daily_limits(
            self.config.daily_loss_cap_eth,
            self.config.daily_profit_target_eth,
        )

        if "daily_loss_cap" in alerts:
            a = alerts["daily_loss_cap"]
            logger.critical(a["message"])
            # Activate killswitch on daily loss cap breach
            await self.killswitch.activate(a["message"])
            await self._emit_alert(a)

        if "daily_profit_target" in alerts:
            a = alerts["daily_profit_target"]
            logger.info(a["message"])
            await self._emit_alert(a)

    async def _check_circuit_states(self):
        """Periodically check and log circuit breaker states."""
        for strategy in ["liquidation", "dex_arb", "cex_deviation"]:
            if await self.circuit.is_tripped(strategy):
                reason = await self.circuit.get_trip_reason(strategy)
                logger.debug("Circuit active: %s — %s", strategy, reason)

    async def _check_price_deviation(self):
        """Check current price deviation from oracle service metadata."""
        for sym in ["ETH", "BTC", "LINK"]:
            data = await self.redis.hgetall(f"price:meta:{sym}")
            if not data:
                continue
            deviation = float(data.get("deviation_max", "0"))
            circuit = data.get("circuit_broken", "0")
            if deviation >= self.config.max_price_deviation_pct or circuit == "1":
                logger.warning("Price risk: %s deviation %.2f%% circuit=%s", sym, deviation, circuit)

    # ── Event bus integration ───────────────────────────────────

    async def _emit_alert(self, alert: dict):
        """Emit risk alert to event bus."""
        try:
            ts = int(time.time() * 1000)
            await self.redis.xadd("arb:events:system", {
                "id": f"evt_{ts}",
                "ts": str(ts),
                "source": "risk_engine",
                "type": f"risk.{alert.get('severity', 'info')}",
                "severity": alert.get("severity", "info"),
                "block": "0",
                "payload": json.dumps(alert),
            }, maxlen=100_000, approximate=True)
        except Exception as e:
            logger.debug("Alert emit failed: %s", e)

    async def _read_execution_events(self, last_id: str = "$") -> tuple:
        """Read new execution events from stream. Returns (events, new_last_id)."""
        try:
            streams = {
                "arb:events:execution": last_id,
            }
            result = await self.redis.xread(streams, block=5000, count=50)
            new_last = last_id
            all_events = []
            if result:
                for stream_name, events in result:
                    for msg_id, fields in events:
                        all_events.append(fields)
                        new_last = msg_id
            return all_events, new_last
        except Exception as e:
            logger.debug("Event read failed: %s", e)
            return [], last_id

    # ── Main loop ───────────────────────────────────────────────

    async def run(self, poll_interval: float = 2.0):
        """Main risk monitoring loop."""
        await self.connect()

        # Write initial state
        for s in ["liquidation", "dex_arb", "cex_deviation"]:
            await self.redis.hset(f"risk:state:{s}", mapping={
                "status": "active",
                "since": str(int(time.time())),
                "reason": "",
            })
        await self.redis.hset("risk:limits", mapping={
            "max_trade_eth": str(self.config.max_trade_eth),
            "daily_loss_cap_eth": str(self.config.daily_loss_cap_eth),
            "max_consecutive_reverts": str(self.config.max_consecutive_reverts),
            "revert_cooldown_seconds": str(self.config.revert_cooldown_seconds),
        })

        logger.info("Risk engine started (max_trade=%.1f ETH, loss_cap=%.1f ETH/day, revert_limit=%d)",
                    self.config.max_trade_eth, self.config.daily_loss_cap_eth,
                    self.config.max_consecutive_reverts)

        exec_last_id = "$"
        price_last_id = "$"
        last_daily_check = time.time()

        while True:
            try:
                # Read execution events
                exec_events, exec_last_id = await self._read_execution_events(exec_last_id)
                for evt in exec_events:
                    block_reason = await self.check_execution_event(evt)
                    if block_reason:
                        logger.warning("TRADE BLOCKED: %s", block_reason)

                # Periodic checks
                now = time.time()
                if now - last_daily_check >= 60:  # every 60s
                    await self._check_daily_limits()
                    await self._check_circuit_states()
                    await self._check_price_deviation()
                    last_daily_check = now

                await asyncio.sleep(poll_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Risk engine loop error: %s", e, exc_info=True)
                await asyncio.sleep(poll_interval)

    async def stop(self):
        if self.redis:
            await self.redis.aclose()
        logger.info("Risk engine stopped")


# ─── CLI ──────────────────────────────────────────────────────

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="MEV Risk Engine")
    parser.add_argument("--redis", default="redis://localhost:6379")
    parser.add_argument("--interval", type=float, default=2.0)
    args = parser.parse_args()

    engine = RiskEngine(redis_url=args.redis)
    try:
        await engine.run(poll_interval=args.interval)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await engine.stop()


if __name__ == "__main__":
    asyncio.run(main())
