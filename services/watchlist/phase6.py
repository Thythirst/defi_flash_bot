#!/usr/bin/env python3
"""
Phase 6 — Same-Asset Filter, Heterogeneous Scan, and Profit Estimator.

Tasks 1-3 implemented as a single integrated pipeline.
Loads canonical registry, scans watchlist for heterogeneous positions,
and computes deterministic profit estimates.

Usage:
  python services/watchlist/phase6.py              # Full pipeline
  python services/watchlist/phase6.py --top 50     # Top N heterogeneous candidates
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
import csv
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import redis
from dotenv import load_dotenv
load_dotenv(dotenv_path=project_root / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | phase6 | %(message)s",
)
logger = logging.getLogger("phase6")

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
SELECTOR_ACCOUNT = "0xbf92857c"       # getUserAccountData(address)
SELECTOR_CONFIG  = "0xc44b11f7"       # getUserConfiguration(address)

RPC_URL = os.getenv("VALIDATOR_RPC_URL", "https://arb1.arbitrum.io/rpc")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

MIN_DEBT_USD = 1000.0
MAX_HF = 2.0
DUST_THRESHOLD = 100.0  # USD — ignore positions below this

# Balancer flash loan fee on Arbitrum: 0
FLASH_LOAN_FEE_BPS = 0

# Default slippage estimate for collateral→debt swap
DEFAULT_SLIPPAGE_BPS = 30  # 0.30%

# MEV cost buffer (configurable)
MEV_COST_USD = 5.0

# Safety buffer as fraction of gross profit
SAFETY_BUFFER = 0.15

# Minimum net EV to consider a candidate
MIN_NET_EV_USD = 50.0

# Gas: rolling median of 50 successful liquidations from competitor
# Default until benchmarked
DEFAULT_GAS_UNITS = 365_000   # from FlashExecutorV3 flash loan path
DEFAULT_GAS_PRICE_GWEI = 0.1
GAS_COST_ETH = DEFAULT_GAS_UNITS * DEFAULT_GAS_PRICE_GWEI / 1e9
ETH_PRICE_USD = 1625  # approximate, updated below


# ═══════════════════════════════════════════════════════════════
# CANONICAL REGISTRY
# ═══════════════════════════════════════════════════════════════

def load_registry(path: str = None) -> dict:
    """Load canonical reserve registry from JSON."""
    if path is None:
        path = str(project_root / "reports" / "reserve_registry.json")
    with open(path) as f:
        reserves = json.load(f)
    # Build lookup maps
    registry = {
        "by_underlying": {},
        "by_aToken": {},
        "by_vDebt": {},
        "reserves": reserves,
    }
    for r in reserves:
        ua = r["underlying"].lower()
        registry["by_underlying"][ua] = r
        registry["by_aToken"][r["aToken"].lower()] = r
        registry["by_vDebt"][r["varDebt"].lower()] = r
    return registry


# ═══════════════════════════════════════════════════════════════
# RPC
# ═══════════════════════════════════════════════════════════════

def rpc_call(method: str, params: list, timeout: float = 10.0) -> Optional[dict]:
    body = json.dumps({"jsonrpc": "2.0", "method": method, "params": params, "id": 1})
    req = urllib.request.Request(RPC_URL, data=body.encode(),
        headers={"Content-Type": "application/json", "User-Agent": "phase6/1.0"})
    try:
        resp = urllib.request.urlopen(req, timeout=timeout)
        return json.loads(resp.read())
    except Exception as e:
        return None


def safe_int(hex_str, default=0):
    if not hex_str or hex_str == "0x" or len(hex_str) < 3:
        return default
    try:
        return int(hex_str, 16)
    except (ValueError, TypeError):
        return default


def get_user_account_data(address: str) -> Optional[dict]:
    padded = address[2:].lower().rjust(64, "0")
    resp = rpc_call("eth_call", [{"to": AAVE_POOL, "data": SELECTOR_ACCOUNT + padded}, "latest"])
    if not resp or "result" not in resp:
        return None
    result = resp["result"]
    if len(result) < 386:
        return None
    return {
        "address": address,
        "collateral_usd": safe_int(result[2:66]) / 1e8,
        "debt_usd": safe_int(result[66:130]) / 1e8,
        "available_borrow_usd": safe_int(result[130:194]) / 1e8,
        "liq_threshold_bps": safe_int(result[194:258]),
        "ltv_bps": safe_int(result[258:322]),
        "health_factor": safe_int(result[322:386]) / 1e18,
    }


def get_token_balance(token_addr: str, user_addr: str) -> int:
    """ERC20 balanceOf. Returns raw balance or 0."""
    padded = user_addr[2:].lower().rjust(64, "0")
    call_data = "0x70a08231" + padded
    resp = rpc_call("eth_call", [{"to": token_addr, "data": call_data}, "latest"])
    return safe_int(resp.get("result", "0x") if resp else "0x")


def get_user_configuration(address: str) -> int:
    """Returns the user's configuration bitmask."""
    padded = address[2:].lower().rjust(64, "0")
    resp = rpc_call("eth_call", [{"to": AAVE_POOL, "data": SELECTOR_CONFIG + padded}, "latest"])
    return safe_int(resp.get("result", "0x") if resp else "0x")


# ═══════════════════════════════════════════════════════════════
# ASSET DETECTION
# ═══════════════════════════════════════════════════════════════

def detect_assets(user_addr: str, registry: dict) -> dict:
    """
    Detect which collateral and debt assets a user holds.
    
    Returns:
      {"collateral_assets": [(symbol, raw_balance, decimals, usd_value), ...],
       "debt_assets": [(symbol, raw_balance, decimals, usd_value), ...],
       "is_same_asset": bool,
       "has_multiple_collateral": bool}
    """
    reserves = registry["reserves"]
    
    collateral_assets = []
    debt_assets = []
    
    for r in reserves:
        a_token = r["aToken"]
        v_token = r["varDebt"]
        sym = r["symbol"]
        dec = r["dec"]
        
        # Query aToken (collateral)
        coll_raw = get_token_balance(a_token, user_addr)
        # Query variableDebtToken (debt)
        debt_raw = get_token_balance(v_token, user_addr)
        
        if coll_raw > 0:
            # Approximate USD: not exact without oracle price, use 1.0 for stablecoins
            # For precise USD, we'd query oracle. For filtering, raw balance suffices.
            collateral_assets.append((sym, coll_raw, dec, a_token))
        
        if debt_raw > 0:
            debt_assets.append((sym, debt_raw, dec, v_token))
    
    # Determine same-asset status
    coll_symbols = {c[0] for c in collateral_assets}
    debt_symbols = {d[0] for d in debt_assets}
    
    is_same_asset = False
    if coll_symbols and debt_symbols:
        # Same-asset if ALL debt symbols are also collateral symbols
        is_same_asset = debt_symbols.issubset(coll_symbols)
    
    return {
        "collateral_assets": collateral_assets,
        "debt_assets": debt_assets,
        "is_same_asset": is_same_asset,
        "coll_symbols": list(coll_symbols),
        "debt_symbols": list(debt_symbols),
    }


# ═══════════════════════════════════════════════════════════════
# PROFIT ESTIMATOR (Task 3)
# ═══════════════════════════════════════════════════════════════

def estimate_liquidation_profit(
    debt_usd: float,
    collateral_usd: float,
    liq_threshold_bps: int,
    debt_assets: list,
    collateral_assets: list,
) -> dict:
    """
    Deterministic profit estimate for a liquidation candidate.
    
    Uses:
      - 50% debt coverage (Aave V3 max per liquidationCall)
      - 5% liquidation bonus (default for volatile assets, 4% for stablecoins)
      - Balancer 0% flash loan fee
      - Rolling-median gas cost
      - Configurable slippage and MEV buffer
    """
    # Debt to cover: 50% of total debt (Aave V3 max per call)
    debt_to_cover = debt_usd * 0.50
    
    # Determine liquidation bonus
    # If all collateral is stablecoins → 4% bonus, else 5%
    stablecoins = {"USDC", "USDT", "DAI", "FRAX", "LUSD", "MAI", "GHO", "USD₮0", "EURS"}
    coll_symbols = {c[0] for c in collateral_assets}
    all_stable = coll_symbols.issubset(stablecoins)
    bonus_bps = 10400 if all_stable else 10500  # 4% or 5%
    bonus_pct = (bonus_bps / 10000) - 1.0
    
    # Gross bonus
    gross_bonus_usd = debt_to_cover * bonus_pct
    
    # Flash loan fee (Balancer = 0 on Arbitrum)
    flash_loan_fee_usd = debt_to_cover * (FLASH_LOAN_FEE_BPS / 10000)
    
    # Gas cost
    gas_cost_eth = DEFAULT_GAS_UNITS * DEFAULT_GAS_PRICE_GWEI / 1e9
    gas_cost_usd = gas_cost_eth * ETH_PRICE_USD
    
    # Swap slippage (collateral → debt asset swap)
    # Only applies if collateral asset != debt asset
    need_swap = coll_symbols != {d[0] for d in debt_assets} if debt_assets else False
    swap_slippage_usd = (debt_to_cover + gross_bonus_usd) * (DEFAULT_SLIPPAGE_BPS / 10000) if need_swap else 0.0
    
    # MEV / priority fee
    mev_cost_usd = MEV_COST_USD
    
    # Gross profit
    gross_profit_usd = gross_bonus_usd - flash_loan_fee_usd - gas_cost_usd - swap_slippage_usd - mev_cost_usd
    
    # Safety buffer
    safety_buffer_usd = gross_profit_usd * SAFETY_BUFFER if gross_profit_usd > 0 else 0.0
    
    # Net EV
    net_ev_usd = round(gross_profit_usd - safety_buffer_usd, 2)
    
    return {
        "debt_to_cover_usd": round(debt_to_cover, 2),
        "bonus_bps": bonus_bps,
        "bonus_pct": round(bonus_pct * 100, 2),
        "gross_bonus_usd": round(gross_bonus_usd, 2),
        "flash_loan_fee_usd": round(flash_loan_fee_usd, 2),
        "gas_cost_eth": round(gas_cost_eth, 6),
        "gas_cost_usd": round(gas_cost_usd, 2),
        "swap_slippage_usd": round(swap_slippage_usd, 2),
        "mev_cost_usd": round(mev_cost_usd, 2),
        "safety_buffer_usd": round(safety_buffer_usd, 2),
        "net_ev_usd": net_ev_usd,
        "passes_threshold": net_ev_usd >= MIN_NET_EV_USD,
        "need_swap": need_swap,
    }


# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════

def run_pipeline(top_n: int = 50):
    """Run Tasks 1-3: same-asset filter, heterogeneous scan, profit estimation."""
    t0 = time.time()
    
    # 1. Load canonical registry
    logger.info("Loading canonical registry...")
    registry = load_registry()
    logger.info("  %d reserves loaded", len(registry["reserves"]))
    
    # 2. Load watchlist from Redis
    logger.info("Loading watchlist from Redis...")
    r = redis.from_url(REDIS_URL, decode_responses=True)
    watchlist = r.zrange("arb:watchlist:active", 0, -1, withscores=True)
    logger.info("  %d borrowers in watchlist", len(watchlist))
    
    # 3. Scan each borrower for asset composition
    logger.info("=== PHASE 2b: Asset Detection & Same-Asset Filter ===")
    
    heterogeneous = []
    same_asset_filtered = []
    dust_filtered = []
    error_count = 0
    
    for idx, (addr, hf_score) in enumerate(watchlist):
        if idx > 0 and idx % 100 == 0:
            logger.info("  Progress: %d/%d | heterogeneous: %d | same-asset: %d",
                       idx, len(watchlist), len(heterogeneous), len(same_asset_filtered))
        
        # Get account totals
        data = get_user_account_data(addr)
        if not data:
            error_count += 1
            continue
        
        debt = data["debt_usd"]
        hf = data["health_factor"]
        
        # Skip dust
        if debt < DUST_THRESHOLD:
            dust_filtered.append(addr)
            continue
        
        # Detect assets (only for non-dust, in-range users)
        assets = detect_assets(addr, registry)
        
        if assets["is_same_asset"] and hf > 1.02:
            same_asset_filtered.append({
                "address": addr,
                "hf": hf,
                "debt_usd": debt,
                "coll_symbols": assets["coll_symbols"],
                "debt_symbols": assets["debt_symbols"],
            })
            continue
        
        # Heterogeneous candidate — compute profit estimate
        profit = estimate_liquidation_profit(
            debt_usd=debt,
            collateral_usd=data["collateral_usd"],
            liq_threshold_bps=data["liq_threshold_bps"],
            debt_assets=assets["debt_assets"],
            collateral_assets=assets["collateral_assets"],
        )
        
        heterogeneous.append({
            "address": addr,
            "health_factor": hf,
            "debt_usd": debt,
            "collateral_usd": data["collateral_usd"],
            "debt_assets": ",".join(assets["debt_symbols"]),
            "collateral_assets": ",".join(assets["coll_symbols"]),
            "liq_threshold_bps": data["liq_threshold_bps"],
            "distance_to_liq_pct": round((hf - 1.0) * 100, 2),
            "profit": profit,
        })
        
        time.sleep(0.02)  # Rate limit for RPC
    
    # 4. Sort by HF ascending
    heterogeneous.sort(key=lambda x: x["health_factor"])
    
    # 5. Export
    logger.info("\n=== RESULTS ===")
    logger.info("  Total scanned: %d", len(watchlist))
    logger.info("  Same-asset filtered: %d", len(same_asset_filtered))
    logger.info("  Dust filtered: %d", len(dust_filtered))
    logger.info("  Heterogeneous candidates: %d", len(heterogeneous))
    logger.info("  Errors: %d", error_count)
    
    # Export heterogeneous candidates
    csv_path = str(project_root / "reports" / "heterogeneous_candidates.csv")
    top = heterogeneous[:top_n]
    with open(csv_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "address", "health_factor", "debt_usd", "collateral_usd",
            "debt_assets", "collateral_assets", "liq_threshold_bps",
            "distance_to_liq_pct", "net_ev_usd", "passes_threshold",
            "gross_bonus_usd", "gas_cost_usd", "need_swap"
        ])
        w.writeheader()
        for c in top:
            w.writerow({
                "address": c["address"],
                "health_factor": f"{c['health_factor']:.4f}",
                "debt_usd": f"{c['debt_usd']:.2f}",
                "collateral_usd": f"{c['collateral_usd']:.2f}",
                "debt_assets": c["debt_assets"],
                "collateral_assets": c["collateral_assets"],
                "liq_threshold_bps": c["liq_threshold_bps"],
                "distance_to_liq_pct": f"{c['distance_to_liq_pct']:.2f}",
                "net_ev_usd": f"{c['profit']['net_ev_usd']:.2f}",
                "passes_threshold": str(c["profit"]["passes_threshold"]),
                "gross_bonus_usd": f"{c['profit']['gross_bonus_usd']:.2f}",
                "gas_cost_usd": f"{c['profit']['gas_cost_usd']:.2f}",
                "need_swap": str(c["profit"]["need_swap"]),
            })
    logger.info("  Exported top %d to %s", len(top), csv_path)
    
    # Export same-asset filtered
    sa_path = str(project_root / "reports" / "same_asset_filtered.csv")
    with open(sa_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["address", "hf", "debt_usd", "coll_symbols", "debt_symbols"])
        w.writeheader()
        for c in same_asset_filtered:
            w.writerow({
                "address": c["address"],
                "hf": f"{c['hf']:.4f}",
                "debt_usd": f"{c['debt_usd']:.2f}",
                "coll_symbols": ",".join(c["coll_symbols"]),
                "debt_symbols": ",".join(c["debt_symbols"]),
            })
    logger.info("  Exported %d same-asset to %s", len(same_asset_filtered), sa_path)
    
    # Candidate A verification
    ca = "0x572372831A9d6B2E3ee8fa284505599e6125Fea9".lower()
    a_in_het = any(c["address"].lower() == ca for c in heterogeneous)
    a_in_sa = any(c["address"].lower() == ca for c in same_asset_filtered)
    logger.info("\n=== ACCEPTANCE CHECK ===")
    logger.info("  Candidate A retained: %s", "PASS" if a_in_het else "FAIL")
    
    # Same-asset exclusions
    for label, addr in [("B", "0xF9D4FD46E2d1435e7BaC9BCee6fA9536e76e5101"),
                          ("C", "0x6f46C54D556FC8e040AC9226196605EeBDf334A1"),
                          ("D", "0x270d1C8C0f13fF925f710dFf38BF806BDbb4e6B2"),
                          ("E", "0x2406B3e14C2A2A7D394e24C5Dc0170F9Bc9f0166")]:
        addr_l = addr.lower()
        in_het = any(c["address"].lower() == addr_l for c in heterogeneous)
        in_sa = any(c["address"].lower() == addr_l for c in same_asset_filtered)
        logger.info("  Candidate %s: heterogeneous=%s, same_asset=%s", label, in_het, in_sa)
    
    elapsed = time.time() - t0
    logger.info("\n  Pipeline complete in %.0fs", elapsed)
    
    r.close()
    return {
        "heterogeneous": len(heterogeneous),
        "same_asset": len(same_asset_filtered),
        "dust": len(dust_filtered),
        "top_path": csv_path,
        "elapsed_s": round(elapsed, 1),
    }


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Phase 6 Pipeline")
    parser.add_argument("--top", type=int, default=50, help="Top N heterogeneous to export")
    args = parser.parse_args()
    result = run_pipeline(top_n=args.top)
    print(json.dumps(result, indent=2))
