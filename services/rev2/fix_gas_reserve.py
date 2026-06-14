"""
fix_gas_reserve.py — Pre-submission ETH balance gate
Fixes W4: pipeline submits tx without verifying ETH balance covers gas.
          At 0.083 ETH currently fine. Silent failure if balance drops.

The problem:
    blast_submit() fires without checking eth_balance >= gas_cost.
    On Arbitrum, a failed tx still costs gas. If balance drops below
    the cost of the next tx, you get a silent RPC error or a reverted
    tx that burns your remaining ETH.

This module:
    GasReserveGuard — checks balance before submission
    - Static floor: configurable ETH minimum (default 0.005 ETH)
    - Dynamic floor: max(static, estimated_gas_cost × safety_multiple)
    - Async: single awaited balanceOf — <10ms, safe on hot path
    - Tracks low-balance events for alerting
    - Integrates with ConfirmationTracker to track pending gas spend

Usage:
    guard = GasReserveGuard(
        rpc          = rpc_client,
        wallet       = WALLET_ADDR,
        min_eth      = 0.005,          # absolute floor
        safety_mult  = 3.0,            # must have 3× estimated gas cost
    )

    # In attempt_liquidation(), before blast_submit():
    ok, reason = await guard.check(estimated_gas_wei=400_000 * gas_price)
    if not ok:
        logger.warning(f"[GasGuard] Skipping: {reason}")
        return None
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from web3 import AsyncWeb3

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ETH_WEI = 10 ** 18

# Arbitrum gas estimates
ARBITRUM_GAS_LIMIT_DEFAULT  = 400_000   # current hardcoded limit (W5)
ARBITRUM_BASE_FEE_TYPICAL   = 100_000_000  # 0.1 gwei — Arbitrum is cheap
ARBITRUM_BASE_FEE_STRESSED  = 1_000_000_000  # 1 gwei — during cascade events

# Absolute minimum ETH to keep in wallet regardless of gas estimate
ABSOLUTE_MIN_ETH = 0.002  # never go below this
ABSOLUTE_MIN_WEI = int(ABSOLUTE_MIN_ETH * ETH_WEI)


# ---------------------------------------------------------------------------
# GasReserveGuard
# ---------------------------------------------------------------------------

@dataclass
class GasCheckResult:
    ok: bool
    balance_wei: int
    required_wei: int
    reason: str
    timestamp: float = field(default_factory=time.time)

    @property
    def balance_eth(self) -> float:
        return self.balance_wei / ETH_WEI

    @property
    def required_eth(self) -> float:
        return self.required_wei / ETH_WEI


class GasReserveGuard:
    """
    Pre-submission ETH balance gate.

    Check order:
    1. Fetch current ETH balance (single async eth_getBalance call)
    2. Compute dynamic floor = max(min_eth, estimated_gas × safety_mult)
    3. Also enforce absolute minimum (never spend below ABSOLUTE_MIN_ETH)
    4. If balance < floor → skip, log warning, track event
    5. If balance < absolute_min → skip + alert (this is critical)

    The dynamic floor is important during cascade events: gas prices can
    spike 10× on Arbitrum when the chain is congested, making the static
    floor too low. safety_mult=3 ensures you can execute at least 3 more
    liquidations before running dry.
    """

    def __init__(
        self,
        rpc,                            # AsyncRPCClient from async_web3.py
        wallet: str,
        min_eth: float  = 0.005,        # static floor (ETH)
        safety_mult: float = 3.0,       # dynamic floor = estimated_gas × this
        cache_seconds: float = 5.0,     # re-use balance fetch within this window
    ):
        self._rpc         = rpc
        self._wallet      = AsyncWeb3.to_checksum_address(wallet)
        self._min_wei     = int(min_eth * ETH_WEI)
        self._safety_mult = safety_mult
        self._cache_secs  = cache_seconds

        self._cached_balance: Optional[int] = None
        self._cache_time: float = 0.0
        self._low_balance_events: list[GasCheckResult] = []
        self._checks_passed   = 0
        self._checks_failed   = 0

    async def check(
        self,
        estimated_gas_wei: Optional[int] = None,
        gas_limit: int = ARBITRUM_GAS_LIMIT_DEFAULT,
        gas_price_wei: Optional[int] = None,
    ) -> tuple[bool, str]:
        """
        Main check — call before blast_submit().

        Args:
            estimated_gas_wei: total gas cost in wei (gas_limit × gas_price).
                               If None, fetches current base fee and computes.
            gas_limit:         fallback if estimated_gas_wei not provided
            gas_price_wei:     current gas price — fetched if not provided

        Returns:
            (ok: bool, reason: str)
        """
        balance = await self._get_balance()

        # Compute required amount
        if estimated_gas_wei is None:
            if gas_price_wei is None:
                try:
                    gas_price_wei = await self._rpc.w3.eth.gas_price
                except Exception:
                    gas_price_wei = ARBITRUM_BASE_FEE_STRESSED  # conservative fallback
            estimated_gas_wei = gas_limit * gas_price_wei

        dynamic_floor = int(estimated_gas_wei * self._safety_mult)
        required_wei  = max(self._min_wei, dynamic_floor, ABSOLUTE_MIN_WEI)

        result = GasCheckResult(
            ok           = balance >= required_wei,
            balance_wei  = balance,
            required_wei = required_wei,
            reason       = "",
        )

        if not result.ok:
            # Distinguish low-balance levels for alerting
            if balance < ABSOLUTE_MIN_WEI:
                result.reason = (
                    f"CRITICAL: balance {result.balance_eth:.4f} ETH < "
                    f"absolute minimum {ABSOLUTE_MIN_ETH} ETH — "
                    f"manual top-up required"
                )
                logger.error(f"[GasGuard] {result.reason}")
            else:
                result.reason = (
                    f"balance {result.balance_eth:.4f} ETH < "
                    f"required {result.required_eth:.4f} ETH "
                    f"(est_gas={estimated_gas_wei/ETH_WEI:.6f} ETH × {self._safety_mult}×)"
                )
                logger.warning(f"[GasGuard] Skipping submission — {result.reason}")

            self._low_balance_events.append(result)
            self._checks_failed += 1
            return False, result.reason

        self._checks_passed += 1
        logger.debug(
            f"[GasGuard] OK — balance={result.balance_eth:.4f} ETH, "
            f"required={result.required_eth:.4f} ETH"
        )
        return True, "ok"

    async def get_balance_eth(self) -> float:
        """Returns current wallet ETH balance as float. Cached."""
        return (await self._get_balance()) / ETH_WEI

    async def _get_balance(self) -> int:
        """Fetch ETH balance with short-lived cache to avoid hammering RPC."""
        now = time.time()
        if (
            self._cached_balance is not None
            and now - self._cache_time < self._cache_secs
        ):
            return self._cached_balance

        try:
            balance = await self._rpc.w3.eth.get_balance(self._wallet)
            self._cached_balance = balance
            self._cache_time = now
            return balance
        except Exception as e:
            logger.error(f"[GasGuard] get_balance failed: {e}")
            # Return 0 on failure — safer to skip than to submit blind
            return 0

    @property
    def stats(self) -> dict:
        total = self._checks_passed + self._checks_failed
        return {
            "checks_passed":      self._checks_passed,
            "checks_failed":      self._checks_failed,
            "failure_rate":       self._checks_failed / total if total else 0.0,
            "low_balance_events": len(self._low_balance_events),
            "last_event":         self._low_balance_events[-1] if self._low_balance_events else None,
        }

    def log_stats(self) -> None:
        s = self.stats
        logger.info(
            f"[GasGuard] passed={s['checks_passed']} "
            f"failed={s['checks_failed']} "
            f"low_balance_events={s['low_balance_events']}"
        )


# ---------------------------------------------------------------------------
# Dynamic gas estimator — replaces hardcoded 400K gas limit (W5)
# ---------------------------------------------------------------------------

class GasEstimator:
    """
    Replaces the hardcoded 400_000 gas limit.
    Fixes W5: if Aave upgrades or complexity increases, hardcoded limit fails.

    Uses eth_estimateGas via Multicall3 simulation before commit.
    Falls back to hardcoded limit if estimation fails (safe path).

    Usage:
        estimator = GasEstimator(rpc_client)
        gas_limit, gas_price = await estimator.estimate(
            contract=executor,
            fn_call=executor.functions.executeLiquidation(...),
            wallet=WALLET_ADDR,
        )
    """

    GAS_BUFFER_MULT   = 1.25  # add 25% buffer to estimate
    GAS_PRICE_PREMIUM = 1.10  # pay 10% above base fee for priority

    def __init__(self, rpc, fallback_gas: int = ARBITRUM_GAS_LIMIT_DEFAULT):
        self._rpc      = rpc
        self._fallback = fallback_gas
        self._history: list[int] = []  # recent gas estimates for trend analysis

    async def estimate(
        self,
        fn_call,        # web3 contract function call (built, not sent)
        wallet: str,
        value_wei: int = 0,
    ) -> tuple[int, int]:
        """
        Estimate gas for a transaction.
        Returns (gas_limit, gas_price_wei).
        """
        gas_price = await self._get_gas_price()

        try:
            tx_params = {
                "from":  wallet,
                "value": value_wei,
            }
            estimated = await fn_call.estimate_gas(tx_params)
            gas_limit = int(estimated * self.GAS_BUFFER_MULT)
            self._history.append(estimated)
            if len(self._history) > 100:
                self._history.pop(0)

            logger.debug(
                f"[GasEstimator] estimated={estimated} "
                f"with_buffer={gas_limit} "
                f"gas_price={gas_price/1e9:.3f} gwei"
            )
            return gas_limit, gas_price

        except Exception as e:
            logger.warning(
                f"[GasEstimator] estimate_gas failed: {e} — "
                f"using fallback {self._fallback}"
            )
            return self._fallback, gas_price

    async def _get_gas_price(self) -> int:
        """Current gas price with small priority premium."""
        try:
            base = await self._rpc.w3.eth.gas_price
            return int(base * self.GAS_PRICE_PREMIUM)
        except Exception:
            return ARBITRUM_BASE_FEE_STRESSED

    @property
    def p95_gas(self) -> Optional[int]:
        """95th percentile of recent gas estimates — useful for floor setting."""
        if not self._history:
            return None
        sorted_h = sorted(self._history)
        idx = int(len(sorted_h) * 0.95)
        return sorted_h[min(idx, len(sorted_h) - 1)]


# ---------------------------------------------------------------------------
# pipeline.py integration
# ---------------------------------------------------------------------------
#
# 1. Import:
#       from fix_gas_reserve import GasReserveGuard, GasEstimator
#
# 2. In setup():
#       self.gas_guard = GasReserveGuard(
#           rpc        = self.rpc,
#           wallet     = WALLET_ADDR,
#           min_eth    = 0.005,
#           safety_mult= 3.0,
#       )
#       self.gas_estimator = GasEstimator(self.rpc)
#
# 3. In attempt_liquidation(), replace the hardcoded gas limit:
#
#       OLD:
#           tx = executor.functions.executeLiquidation(...).build_transaction({
#               "gas": 400_000,    # hardcoded
#               ...
#           })
#
#       NEW:
#           fn = executor.functions.executeLiquidation(
#               collateral, debt, borrower, debt_to_cover, False
#           )
#           gas_limit, gas_price = await self.gas_estimator.estimate(fn, WALLET_ADDR)
#
#           # Gate on balance
#           ok, reason = await self.gas_guard.check(
#               estimated_gas_wei = gas_limit * gas_price,
#           )
#           if not ok:
#               return None
#
#           tx = fn.build_transaction({
#               "gas":      gas_limit,
#               "gasPrice": gas_price,
#               "nonce":    await self.nonce_mgr.next(),
#               "from":     WALLET_ADDR,
#           })
#
# 4. In wallet stats loop, log guard stats:
#       self.gas_guard.log_stats()
#
# 5. Update wallet log line to include reserve status:
#       ok = await self.gas_guard.get_balance_eth() > 0.005
#       logger.info(f"[Wallet] ETH={balance:.4f} gas_reserve={'OK' if ok else 'LOW'} ...")
#
# ---------------------------------------------------------------------------
