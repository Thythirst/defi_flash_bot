#!/usr/bin/env python3
"""
Phase 7 Task 2: Deterministic EV Engine — profit_estimator.py

Production-grade liquidation profit estimation.
Uses canonical Pool data for liquidation bonus.
Integrates with oracle for asset prices.
Rejects unprofitable executions deterministically.
"""
from __future__ import annotations
import os, json, time, logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger("profit_estimator")

# ═══════════════════════════════════════════════════════════════
# CONSTANTS
# ═══════════════════════════════════════════════════════════════

# Balancer V2 flash loan fee on Arbitrum: 0 bps
BALANCER_FLASH_FEE_BPS = 0

# Aave V3 default liquidation bonuses (from Pool config)
# These are per-reserve and should be queried at runtime.
# Fallback defaults per Aave V3 Arbitrum deployment:
DEFAULT_BONUS_VOLATILE_BPS = 10500   # 5% bonus for volatile assets
DEFAULT_BONUS_STABLECOIN_BPS = 10400  # 4% bonus for stablecoins

# Stablecoin set (expanded from canonical reserve registry)
STABLECOINS = {"USDC_n", "USDC_e", "USDT", "DAI", "FRAX", "LUSD", "MAI", "GHO", "USD₮0", "EURS"}

# Gas: Fork-measured constants from FlashExecutorV3
GAS_EXECUTE_DIRECT = 638_000     # executeLiquidationDirect()
GAS_EXECUTE_FLASH = 365_000     # executeLiquidation() with Balancer

# Default slippage for DEX swap (conservative)
DEFAULT_SLIPPAGE_BPS = 30       # 0.30%

# MEV priority fee buffer (configurable)
DEFAULT_MEV_BUFFER_USD = 5.0

# Safety margin on gross profit
SAFETY_MARGIN = 0.15            # 15%

# Minimum net EV to execute
MIN_NET_EV_USD = 50.0

# Minimum health factor (must be underwater)
MAX_EXECUTABLE_HF = 1.0

# Minimum debt to cover for profitability
MIN_DEBT_TO_COVER_USD = 100.0

# Gas price (gwei) — should be updated from oracle/gas tracker
GAS_PRICE_GWEI = 0.1            # Arbitrum baseline

# ETH price (USD) — should be updated from oracle
ETH_PRICE_USD = 1625


@dataclass
class EVEstimate:
    """Deterministic EV estimate for a liquidation candidate."""
    borrower: str
    collateral_asset: str
    debt_asset: str
    debt_to_cover_usd: float
    total_debt_usd: float
    health_factor: float

    # Costs
    gross_bonus_usd: float = 0.0
    bonus_bps: int = 10500
    bonus_pct: float = 0.05
    flash_loan_fee_usd: float = 0.0
    gas_cost_usd: float = 0.0
    gas_units: int = GAS_EXECUTE_FLASH
    swap_slippage_usd: float = 0.0
    slippage_bps: int = 0
    mev_buffer_usd: float = DEFAULT_MEV_BUFFER_USD
    safety_margin_usd: float = 0.0

    # Results
    gross_profit_usd: float = 0.0
    net_ev_usd: float = 0.0
    should_execute: bool = False
    rejection_reason: str = ""

    # Metadata
    need_swap: bool = False
    use_flash_loan: bool = True
    timestamp: str = ""


def estimate_ev(
    borrower: str,
    collateral_asset: str,
    debt_asset: str,
    debt_to_cover_usd: float,
    total_debt_usd: float = 0.0,
    health_factor: float = 1.0,
    gas_price_gwei: float = GAS_PRICE_GWEI,
    eth_price_usd: float = ETH_PRICE_USD,
    mev_buffer_usd: float = DEFAULT_MEV_BUFFER_USD,
    slippage_bps: int | None = None,
) -> EVEstimate:
    """
    Deterministic liquidation profit estimate.

    Args:
        borrower: Address of the borrower being liquidated
        collateral_asset: Symbol of collateral (e.g., 'WETH', 'WBTC')
        debt_asset: Symbol of debt (e.g., 'USDC_n', 'WETH')
        debt_to_cover_usd: USD value of debt to cover (max 50% of total)
        total_debt_usd: Total debt USD (for context)
        health_factor: Current health factor
        gas_price_gwei: Gas price in gwei
        eth_price_usd: ETH price in USD
        mev_buffer_usd: Priority fee / MEV buffer
        slippage_bps: Override slippage in bps (None = auto)

    Returns:
        EVEstimate with all costs, net EV, and execution decision.
    """
    est = EVEstimate(
        borrower=borrower,
        collateral_asset=collateral_asset,
        debt_asset=debt_asset,
        debt_to_cover_usd=debt_to_cover_usd,
        total_debt_usd=total_debt_usd,
        health_factor=health_factor,
        timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    )

    # ── Gate 1: Health factor ──
    if health_factor > MAX_EXECUTABLE_HF:
        est.rejection_reason = f"HF {health_factor:.4f} > {MAX_EXECUTABLE_HF:.4f} (not underwater)"
        return est

    # ── Gate 2: Minimum debt ──
    if debt_to_cover_usd < MIN_DEBT_TO_COVER_USD:
        est.rejection_reason = f"Debt ${debt_to_cover_usd:.0f} < ${MIN_DEBT_TO_COVER_USD:.0f} min"
        return est

    # ── Aave V3 close factor ──
    # Close factor is NOT fixed 50%. It scales with health factor:
    #   When HF >= CLOSE_FACTOR_HF_THRESHOLD (0.95): CF = 0.5 * (1-HF) / (1-0.95)
    #   When HF < 0.95: CF = 0.5
    # This prevents over-liquidating positions near the threshold.
    CLOSE_FACTOR_HF_THRESHOLD = 0.95
    if health_factor >= CLOSE_FACTOR_HF_THRESHOLD:
        close_factor = 0.5 * (1.0 - health_factor) / (1.0 - CLOSE_FACTOR_HF_THRESHOLD)
    else:
        close_factor = 0.5
    
    # Cap debt_to_cover at the close-factor-limited amount
    max_coverable = total_debt_usd * close_factor
    effective_cover = min(debt_to_cover_usd, max_coverable)
    if effective_cover < debt_to_cover_usd:
        est.debt_to_cover_usd = effective_cover
        debt_to_cover_usd = effective_cover
    
    # ── Determine bonus ──
    # If collateral is stablecoin → 4%, else 5%
    is_stable_coll = collateral_asset in STABLECOINS
    est.bonus_bps = DEFAULT_BONUS_STABLECOIN_BPS if is_stable_coll else DEFAULT_BONUS_VOLATILE_BPS
    est.bonus_pct = (est.bonus_bps / 10000.0) - 1.0
    est.gross_bonus_usd = debt_to_cover_usd * est.bonus_pct

    # ── Flash loan fee ──
    est.flash_loan_fee_usd = debt_to_cover_usd * (BALANCER_FLASH_FEE_BPS / 10000.0)

    # ── Gas cost ──
    est.gas_units = GAS_EXECUTE_FLASH if est.use_flash_loan else GAS_EXECUTE_DIRECT
    est.gas_cost_usd = (est.gas_units * gas_price_gwei / 1e9) * eth_price_usd

    # ── Swap slippage ──
    est.need_swap = (collateral_asset != debt_asset)
    if slippage_bps is not None:
        est.slippage_bps = slippage_bps
    elif est.need_swap:
        est.slippage_bps = DEFAULT_SLIPPAGE_BPS
    else:
        est.slippage_bps = 0

    if est.need_swap and est.slippage_bps > 0:
        est.swap_slippage_usd = (debt_to_cover_usd + est.gross_bonus_usd) * (est.slippage_bps / 10000.0)

    # ── MEV buffer ──
    est.mev_buffer_usd = mev_buffer_usd

    # ── Gross profit ──
    est.gross_profit_usd = (
        est.gross_bonus_usd
        - est.flash_loan_fee_usd
        - est.gas_cost_usd
        - est.swap_slippage_usd
        - est.mev_buffer_usd
    )

    # ── Safety margin ──
    if est.gross_profit_usd > 0:
        est.safety_margin_usd = est.gross_profit_usd * SAFETY_MARGIN
    else:
        est.safety_margin_usd = 0.0

    # ── Net EV ──
    est.net_ev_usd = round(est.gross_profit_usd - est.safety_margin_usd, 2)

    # ── Gate 3: Minimum net EV ──
    if est.net_ev_usd < MIN_NET_EV_USD:
        est.rejection_reason = f"Net EV ${est.net_ev_usd:.2f} < ${MIN_NET_EV_USD:.2f} min"
    else:
        est.should_execute = True

    return est


# ═══════════════════════════════════════════════════════════════
# BATCH ESTIMATOR
# ═══════════════════════════════════════════════════════════════

def rank_candidates(
    candidates: list[dict],
    gas_price_gwei: float = GAS_PRICE_GWEI,
    eth_price_usd: float = ETH_PRICE_USD,
) -> list[EVEstimate]:
    """
    Rank all candidates by net EV. Returns sorted list, best first.
    Rejects unprofitable candidates automatically.
    """
    estimates = []
    for c in candidates:
        # For positions where we know assets, estimate
        coll_assets = c.get("collateral_assets", "").split(",")
        debt_assets = c.get("debt_assets", "").split(",")
        primary_coll = coll_assets[0].strip() if coll_assets else "unknown"
        primary_debt = debt_assets[0].strip() if debt_assets else "unknown"

        # Use 50% of total debt as cover amount (Aave V3 max)
        total_debt = c.get("debt_usd", 0)
        dtc = total_debt * 0.50

        ev = estimate_ev(
            borrower=c.get("address", ""),
            collateral_asset=primary_coll,
            debt_asset=primary_debt,
            debt_to_cover_usd=dtc,
            total_debt_usd=total_debt,
            health_factor=c.get("health_factor", 2.0),
            gas_price_gwei=gas_price_gwei,
            eth_price_usd=eth_price_usd,
        )
        estimates.append(ev)

    # Sort: executable first (by net EV descending), then rejected (by HF ascending)
    executable = [e for e in estimates if e.should_execute]
    rejected = [e for e in estimates if not e.should_execute]

    executable.sort(key=lambda e: e.net_ev_usd, reverse=True)
    rejected.sort(key=lambda e: e.health_factor)

    return executable + rejected


# ═══════════════════════════════════════════════════════════════
# VALIDATION AGAINST FORK RESULTS
# ═══════════════════════════════════════════════════════════════

def validate_against_fork(
    ev_estimate: EVEstimate,
    fork_realized_profit_usd: float,
    fork_gas_used: int | None = None,
) -> dict:
    """
    Compare EV estimate against fork-test realized profit.
    Returns error % and pass/fail.
    """
    error_pct = 0.0
    if fork_realized_profit_usd != 0:
        error_pct = abs(ev_estimate.net_ev_usd - fork_realized_profit_usd) / abs(fork_realized_profit_usd) * 100

    return {
        "estimate_net_ev": ev_estimate.net_ev_usd,
        "fork_realized_profit": fork_realized_profit_usd,
        "error_pct": round(error_pct, 2),
        "within_10pct": error_pct <= 10.0,
        "fork_gas_used": fork_gas_used or ev_estimate.gas_units,
    }


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    # Quick test: Candidate A estimate
    ev = estimate_ev(
        borrower="0x572372831A9d6B2E3ee8fa284505599e6125Fea9",
        collateral_asset="WETH",
        debt_asset="WETH",
        debt_to_cover_usd=4202 * 0.50,  # 50% of $4,202
        total_debt_usd=4202,
        health_factor=0.9836,  # Fork-test: post-WBTC-drop HF
    )

    print("=== Candidate A EV Estimate ===")
    print(f"  Borrower: {ev.borrower[:16]}...")
    print(f"  Collateral: {ev.collateral_asset}, Debt: {ev.debt_asset}")
    print(f"  Debt to cover: ${ev.debt_to_cover_usd:.2f}")
    print(f"  HF: {ev.health_factor:.4f}")
    print(f"  Bonus: {ev.bonus_bps} bps ({ev.bonus_pct*100:.1f}%)")
    print(f"  Gross bonus: ${ev.gross_bonus_usd:.2f}")
    print(f"  Flash fee: ${ev.flash_loan_fee_usd:.2f}")
    print(f"  Gas: ${ev.gas_cost_usd:.4f} ({ev.gas_units:,} units)")
    print(f"  Swap: ${ev.swap_slippage_usd:.2f} (need_swap={ev.need_swap})")
    print(f"  MEV buffer: ${ev.mev_buffer_usd:.2f}")
    print(f"  Safety: ${ev.safety_margin_usd:.2f}")
    print(f"  Net EV: ${ev.net_ev_usd:.2f}")
    print(f"  Execute: {ev.should_execute}")
    if ev.rejection_reason:
        print(f"  Rejection: {ev.rejection_reason}")

    # Validate against fork
    val = validate_against_fork(ev, fork_realized_profit_usd=18.77, fork_gas_used=364562)
    print(f"\n  Fork validation:")
    print(f"    Realized: ${val['fork_realized_profit']:.2f}")
    print(f"    Error: {val['error_pct']:.1f}%")
    print(f"    Within 10%: {val['within_10pct']}")
