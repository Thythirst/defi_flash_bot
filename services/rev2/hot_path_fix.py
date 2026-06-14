"""
hot_path_fix.py — Eliminate redundant RPC calls from the liquidation hot path
Fixes ~100ms of unnecessary latency caused by two RPC calls that could be
served from in-memory state already updated every 68 seconds.

Problem:
    Step 3: gas_guard.check() calls eth.getBalance() + eth.gasPrice()  → 50-100ms
    Step 5: presigner calls get_base_fee() → eth.getBlock('latest')    → 50ms
    Both fire on EVERY liquidation attempt, in sequence, before blast_submit().

    The data already exists in memory:
    - ETH balance: _wallet_balance_loop updates self.wallet_balances every 68s
    - Base fee:    _block_watch_loop sees every new block — base fee is in the header

Fix:
    1. SharedState — single in-memory object holding ETH balance + base fee
       Updated by existing loops, read on hot path with zero RPC cost.
    2. FastGasGuard — reads SharedState instead of calling eth.getBalance()
    3. CachedBaseFeePresigner — reads SharedState instead of get_base_fee()
    4. LatencyTracker — real p50/p95/p99 latency from actual submissions

Result:
    Hot path: ~20-80ms (just blast_submit())
    Removes: 2 sequential RPC calls (~100ms) from every liquidation attempt
"""

import asyncio
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

ETH_WEI = 10 ** 18


# ---------------------------------------------------------------------------
# SharedState — single source of truth for hot-path reads
# ---------------------------------------------------------------------------

class SharedState:
    """
    In-memory cache of chain state updated by existing background loops.
    Hot path reads from here — zero RPC cost.

    Updated by:
        on_new_block()     — called from _block_watch_loop on every block
        on_balance_update()— called from _wallet_balance_loop every 68s

    Read by:
        FastGasGuard.check()
        CachedBaseFeePresigner.get_base_fee()
    """

    def __init__(self):
        self._base_fee_wei: int   = 0
        self._eth_balance_wei: int= 0
        self._last_block: int     = 0
        self._last_block_time: float = 0.0
        self._last_balance_time: float = 0.0
        self._base_fee_history: deque = deque(maxlen=20)  # last 20 blocks

    # -- Writers (called by existing background loops) ---------------------

    def on_new_block(self, block_number: int, base_fee_wei: int) -> None:
        """
        Call from _block_watch_loop whenever a new block arrives.
        Arbitrum blocks every ~250ms — base fee stays current.

        In pipeline.py block handler:
            shared_state.on_new_block(block['number'], block.get('baseFeePerGas', 0))
        """
        self._base_fee_wei    = base_fee_wei
        self._last_block      = block_number
        self._last_block_time = time.time()
        self._base_fee_history.append(base_fee_wei)

    def on_balance_update(self, eth_balance_wei: int) -> None:
        """
        Call from _wallet_balance_loop after each balance refresh.
        Updates ETH balance so gas_guard doesn't need a fresh RPC call.

        In pipeline.py wallet loop:
            shared_state.on_balance_update(eth_balance_wei)
        """
        self._eth_balance_wei  = eth_balance_wei
        self._last_balance_time= time.time()

    # -- Readers (called on hot path) -------------------------------------

    @property
    def base_fee_wei(self) -> int:
        return self._base_fee_wei

    @property
    def eth_balance_wei(self) -> int:
        return self._eth_balance_wei

    @property
    def eth_balance_eth(self) -> float:
        return self._eth_balance_wei / ETH_WEI

    @property
    def base_fee_age_seconds(self) -> float:
        return time.time() - self._last_block_time if self._last_block_time else float("inf")

    @property
    def balance_age_seconds(self) -> float:
        return time.time() - self._last_balance_time if self._last_balance_time else float("inf")

    @property
    def p95_base_fee(self) -> int:
        """
        95th percentile base fee over last 20 blocks.
        Use as gas price floor — more robust than single latest value.
        """
        if not self._base_fee_history:
            return self._base_fee_wei
        sorted_fees = sorted(self._base_fee_history)
        idx = int(len(sorted_fees) * 0.95)
        return sorted_fees[min(idx, len(sorted_fees) - 1)]

    def is_ready(self) -> bool:
        """True if both base fee and balance have been populated."""
        return self._base_fee_wei > 0 and self._eth_balance_wei > 0

    def status_line(self) -> str:
        return (
            f"base_fee={self._base_fee_wei/1e9:.4f}gwei "
            f"eth_balance={self.eth_balance_eth:.4f}ETH "
            f"block={self._last_block} "
            f"base_fee_age={self.base_fee_age_seconds:.0f}s "
            f"balance_age={self.balance_age_seconds:.0f}s"
        )


# ---------------------------------------------------------------------------
# FastGasGuard — reads SharedState instead of calling eth.getBalance()
# ---------------------------------------------------------------------------

class FastGasGuard:
    """
    Drop-in replacement for GasReserveGuard that reads from SharedState.
    Eliminates eth.getBalance() + eth.gasPrice() RPC calls from hot path.

    Saves ~50-100ms per liquidation attempt.

    Fallback: if SharedState hasn't been populated yet (startup),
    falls back to a single RPC call. After first wallet loop cycle
    (68s), all hot path calls are zero-RPC.
    """

    def __init__(
        self,
        shared_state: SharedState,
        rpc=None,                       # AsyncRPCClient — only for fallback
        wallet: str = "",
        min_eth: float = 0.005,
        safety_mult: float = 3.0,
        gas_limit: int = 400_000,
    ):
        self._state       = shared_state
        self._rpc         = rpc
        self._wallet      = wallet
        self._min_wei     = int(min_eth * ETH_WEI)
        self._safety_mult = safety_mult
        self._gas_limit   = gas_limit
        self._checks_ok   = 0
        self._checks_fail = 0
        self._rpc_fallbacks = 0

    async def check(self) -> tuple[bool, str]:
        """
        Check ETH balance and gas reserve. Zero RPC calls if SharedState is ready.
        Returns (ok, reason).
        """
        t0 = time.perf_counter()

        # Get balance — from SharedState if available, else RPC fallback
        if self._state.eth_balance_wei > 0:
            balance_wei = self._state.eth_balance_wei
        elif self._rpc and self._wallet:
            self._rpc_fallbacks += 1
            logger.debug("[FastGasGuard] SharedState not ready — falling back to RPC")
            try:
                from web3 import AsyncWeb3
                balance_wei = await self._rpc.w3.eth.get_balance(
                    AsyncWeb3.to_checksum_address(self._wallet)
                )
                self._state.on_balance_update(balance_wei)
            except Exception as e:
                logger.error(f"[FastGasGuard] RPC fallback failed: {e}")
                return False, "balance_check_failed"
        else:
            return False, "shared_state_not_ready"

        # Get gas price — from SharedState (updated every block)
        gas_price = self._state.base_fee_wei or 100_000_000  # 0.1 gwei default
        estimated_gas_cost = self._gas_limit * gas_price

        dynamic_floor = int(estimated_gas_cost * self._safety_mult)
        required_wei  = max(self._min_wei, dynamic_floor)

        ms = (time.perf_counter() - t0) * 1000
        logger.debug(f"[FastGasGuard] check() took {ms:.2f}ms (rpc_fallbacks={self._rpc_fallbacks})")

        if balance_wei < required_wei:
            self._checks_fail += 1
            reason = (
                f"balance {balance_wei/ETH_WEI:.4f}ETH < "
                f"required {required_wei/ETH_WEI:.4f}ETH"
            )
            if balance_wei < int(0.002 * ETH_WEI):
                logger.error(f"[FastGasGuard] CRITICAL low balance: {reason}")
            else:
                logger.warning(f"[FastGasGuard] Skip: {reason}")
            return False, reason

        self._checks_ok += 1
        return True, "ok"

    @property
    def stats(self) -> dict:
        total = self._checks_ok + self._checks_fail
        return {
            "passed":        self._checks_ok,
            "failed":        self._checks_fail,
            "rpc_fallbacks": self._rpc_fallbacks,
            "rpc_hit_rate":  1 - (self._rpc_fallbacks / total) if total else 0,
        }


# ---------------------------------------------------------------------------
# CachedBaseFeePresigner — reads SharedState instead of get_base_fee() RPC
# ---------------------------------------------------------------------------

class CachedBaseFeeChecker:
    """
    Replaces the get_base_fee() RPC call in presigner staleness check.
    Reads base fee from SharedState — updated every block (~250ms on Arbitrum).

    Saves ~50ms per cached tx staleness check.

    Usage:
        checker = CachedBaseFeeChecker(shared_state)

        # In presigner.fire(), replace:
        #   current_base_fee = await rpc_client.get_base_fee()  # 50ms RPC
        # With:
        #   current_base_fee = checker.get_base_fee()           # 0ms RAM read
    """

    def __init__(self, shared_state: SharedState, max_age_seconds: float = 5.0):
        self._state    = shared_state
        self._max_age  = max_age_seconds

    def get_base_fee(self) -> int:
        """
        Returns current base fee in wei. Zero RPC cost.
        Falls back to p95 if current value is stale.
        """
        age = self._state.base_fee_age_seconds

        if age > self._max_age:
            logger.debug(
                f"[CachedBaseFee] Base fee is {age:.1f}s old — "
                f"using p95 as conservative estimate"
            )
            return self._state.p95_base_fee

        return self._state.base_fee_wei

    def is_fresh(self) -> bool:
        return self._state.base_fee_age_seconds < self._max_age


# ---------------------------------------------------------------------------
# LatencyTracker — real p50/p95/p99 from actual submissions
# ---------------------------------------------------------------------------

class LatencyTracker:
    """
    Tracks real end-to-end latency from opportunity detection to blast_submit().
    Replaces the fake p50_latency=0ms default in SQLite stats.

    Measures two intervals:
        detection_to_submit: time from HF trigger to blast_submit() call
        submit_to_confirm:   time from blast_submit() to tx confirmation

    Usage:
        tracker = LatencyTracker()

        # When opportunity detected:
        token = tracker.start("0xborrower...")

        # Just before blast_submit():
        tracker.mark_submitted(token)

        # When confirmation received:
        tracker.mark_confirmed(token, success=True)

        # In stats loop:
        logger.info(tracker.summary())
    """

    def __init__(self, window: int = 100):
        self._window    = window
        self._detection_to_submit: deque = deque(maxlen=window)
        self._submit_to_confirm:   deque = deque(maxlen=window)
        self._in_flight: dict[str, float] = {}   # token → detection_time
        self._submit_times: dict[str, float] = {}
        self._attempts  = 0
        self._confirmed = 0
        self._reverted  = 0

    def start(self, token: str) -> str:
        """Mark opportunity detection time. Returns token for subsequent calls."""
        self._in_flight[token]   = time.perf_counter()
        self._attempts          += 1
        return token

    def mark_submitted(self, token: str) -> float:
        """
        Mark blast_submit() call time.
        Returns detection→submit latency in ms.
        """
        if token not in self._in_flight:
            return 0.0
        ms = (time.perf_counter() - self._in_flight[token]) * 1000
        self._detection_to_submit.append(ms)
        self._submit_times[token] = time.perf_counter()
        return ms

    def mark_confirmed(self, token: str, success: bool) -> float:
        """
        Mark confirmation time.
        Returns submit→confirm latency in ms.
        """
        if token not in self._submit_times:
            return 0.0
        ms = (time.perf_counter() - self._submit_times[token]) * 1000
        self._submit_to_confirm.append(ms)
        self._in_flight.pop(token, None)
        self._submit_times.pop(token, None)
        if success:
            self._confirmed += 1
        else:
            self._reverted += 1
        return ms

    def _percentile(self, data: deque, pct: float) -> float:
        if not data:
            return 0.0
        sorted_data = sorted(data)
        idx = int(len(sorted_data) * pct)
        return sorted_data[min(idx, len(sorted_data) - 1)]

    @property
    def p50_submit_ms(self) -> float:
        return self._percentile(self._detection_to_submit, 0.50)

    @property
    def p95_submit_ms(self) -> float:
        return self._percentile(self._detection_to_submit, 0.95)

    @property
    def p50_confirm_ms(self) -> float:
        return self._percentile(self._submit_to_confirm, 0.50)

    @property
    def p95_confirm_ms(self) -> float:
        return self._percentile(self._submit_to_confirm, 0.95)

    def summary(self) -> str:
        return (
            f"attempts={self._attempts} "
            f"confirmed={self._confirmed} "
            f"reverted={self._reverted} "
            f"p50_submit={self.p50_submit_ms:.0f}ms "
            f"p95_submit={self.p95_submit_ms:.0f}ms "
            f"p50_confirm={self.p50_confirm_ms:.0f}ms "
            f"p95_confirm={self.p95_confirm_ms:.0f}ms"
        )

    def to_dict(self) -> dict:
        return {
            "attempts":        self._attempts,
            "confirmed":       self._confirmed,
            "reverted":        self._reverted,
            "p50_submit_ms":   round(self.p50_submit_ms, 1),
            "p95_submit_ms":   round(self.p95_submit_ms, 1),
            "p50_confirm_ms":  round(self.p50_confirm_ms, 1),
            "p95_confirm_ms":  round(self.p95_confirm_ms, 1),
        }


# ---------------------------------------------------------------------------
# pipeline.py integration
# ---------------------------------------------------------------------------
#
# 1. Import:
#       from hot_path_fix import SharedState, FastGasGuard, CachedBaseFeeChecker, LatencyTracker
#
# 2. In setup():
#       self.shared_state   = SharedState()
#       self.fast_gas_guard = FastGasGuard(
#           shared_state = self.shared_state,
#           rpc          = self.rpc,           # fallback only
#           wallet       = WALLET_ADDR,
#           min_eth      = 0.005,
#       )
#       self.base_fee_checker = CachedBaseFeeChecker(self.shared_state)
#       self.latency_tracker  = LatencyTracker()
#
# 3. In _block_watch_loop(), on every new block:
#       block = await self.rpc.w3.eth.get_block('latest')
#       self.shared_state.on_new_block(
#           block_number  = block['number'],
#           base_fee_wei  = block.get('baseFeePerGas', 0),
#       )
#
# 4. In _wallet_balance_loop(), after fetching ETH balance:
#       self.shared_state.on_balance_update(eth_balance_wei)
#
# 5. In attempt_liquidation(), replace:
#
#       OLD:
#           ok, reason = await self.gas_guard.check(...)   # 50-100ms RPC
#           ...
#           current_base_fee = await rpc_client.get_base_fee()  # 50ms RPC
#           stale, reason = guard.is_stale(snapshot, current_base_fee)
#
#       NEW:
#           token = self.latency_tracker.start(borrower)
#
#           ok, reason = await self.fast_gas_guard.check()  # 0ms RAM
#           if not ok:
#               return None
#
#           current_base_fee = self.base_fee_checker.get_base_fee()  # 0ms RAM
#           stale, reason = guard.is_stale(snapshot, current_base_fee)
#
#           tx_hash = await blast_submit(raw_tx)
#           submit_ms = self.latency_tracker.mark_submitted(token)
#           logger.debug(f"[Latency] detection→submit={submit_ms:.0f}ms")
#
# 6. In ConfirmationTracker, when tx confirms:
#       confirm_ms = self.latency_tracker.mark_confirmed(token, success=True)
#
# 7. In stats loop, replace fake p50_latency=0ms:
#       OLD: avg_latency = db.fetch_one("SELECT avg_latency_ms FROM outcomes")
#       NEW: logger.info(f"[Stats] {self.latency_tracker.summary()}")
#            # Also write to SQLite for persistence:
#            db.execute("INSERT INTO latency_stats ... VALUES ...",
#                       self.latency_tracker.to_dict())
#
# Expected result in wallet log:
#       [Wallet] ETH=0.0830 candidates=0 prices_fresh=8/8
#       [Stats]  attempts=0 confirmed=0 p50_submit=0ms p95_submit=0ms
#       (zeros until first real attempt — but now correctly zero, not fake zero)
#
# Once live:
#       [Stats]  attempts=3 confirmed=1 reverted=1 p50_submit=45ms p95_submit=78ms
#
# ---------------------------------------------------------------------------
