"""
scanner/aave_v3.py — Aave v3 protocol constants & helpers (Arbitrum Mainnet).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from eth_abi import decode, encode
from eth_utils import keccak
from web3 import Web3

# ─────────────────────────────────────────────────────────────────────────────────────────────────
# Protocol Contracts (Arbitrum Mainnet)
# ─────────────────────────────────────────────────────────────────────────────────────────────────

POOL: str = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
POOL_DATA_PROVIDER: str = "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"
UI_POOL_DATA_PROVIDER: str = "0x5c5228dC8E3a47AeCf1b2eB5C152d024C705AcE6"
ORACLE: str = "0x81387c40C24a43cE66c44473D5317217351A9781"

# Aave v3 flash loan fee (bps)
FLASH_LOAN_FEE_BPS: int = 5  # 0.05%

# Liquidation close factor (max % of debt that can be liquidated in one call)
DEFAULT_CLOSE_FACTOR: int = 5000  # 50% in bps

# ─────────────────────────────────────────────────────────────────────────────────────────────────
# Event Signatures
# ─────────────────────────────────────────────────────────────────────────────────────────────────

LIQUIDATION_CALL_TOPIC: str = (
    "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286"
)
BORROW_TOPIC: str = (
    "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0"
)
SUPPLY_TOPIC: str = (
    "0x4de685973076ad95c50e605feebf2d025d2e947b4a5cd2701b1d6370884cd16e"
)
WITHDRAW_TOPIC: str = (
    "0x9f28b369ee5841d3e7e80d43dc36f61847d9d7e4f19a3b32b7a9e9db3d3b1f3a"
)
REPAY_TOPIC: str = (
    "0x9945c9c64a81a6f3540f8b3e38f5b3a0f8c8bee39519ba4bcf02b2"
)

# ─────────────────────────────────────────────────────────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────────────────────────────────────────────────────────


@dataclass
class UserAccountData:
    """Result of getUserAccountData(address)."""
    total_collateral_base: int  # base currency (USD), 8 decimals
    total_debt_base: int        # base currency (USD), 8 decimals
    available_borrows_base: int # base currency (USD), 8 decimals
    current_ltv: int            # in bps (e.g. 8000 = 80%)
    current_liquidation_threshold: int  # in bps
    health_factor: int          # RAY precision (1e27)

    @property
    def health_factor_float(self) -> float:
        return self.health_factor / 1e18

    @property
    def is_liquidatable(self) -> bool:
        return self.health_factor_float < 1.0 and self.total_debt_base > 0


@dataclass
class UserReserveData:
    """Per-asset position for a user."""
    asset: str
    symbol: str
    a_token_balance: int
    stable_debt: int
    variable_debt: int
    total_debt: int
    usage_as_collateral_enabled: bool
    decimals: int

    @property
    def is_collateral(self) -> bool:
        return self.a_token_balance > 0 and self.usage_as_collateral_enabled

    @property
    def is_debt(self) -> bool:
        return self.total_debt > 0


@dataclass
class LiquidationEvent:
    block: int
    tx_hash: str
    collateral_asset: str
    debt_asset: str
    user: str
    debt_to_cover: int
    liquidated_collateral_amount: int
    liquidator: str
    receive_a_token: bool
    log_index: int

    @property
    def liquidation_bonus_rate(self) -> float:
        """Implied liquidation bonus rate (gross profit)."""
        if self.debt_to_cover == 0:
            return 0.0
        return self.liquidated_collateral_amount / self.debt_to_cover - 1.0


@dataclass
class LiquidationProfitResult:
    block: int
    user: str
    collateral_asset: str
    debt_asset: str
    debt_to_cover: int
    liquidated_amount: int
    gross_profit: int           # collateral - debt, in debt asset terms
    flash_loan_fee: int         # 0.05% of debt
    gas_cost: int               # in wei (ETH)
    swap_slippage: int          # DEX swap fee to convert collateral to debt
    net_profit: int             # gross - all costs
    net_profit_pct: float       # net / debt_to_cover * 100


# ─────────────────────────────────────────────────────────────────────────────────────────────────
# Profit Math
# ─────────────────────────────────────────────────────────────────────────────────────────────────

# Arbitrum gas per liquidation transaction (approximate)
LIQUIDATION_GAS_LIMIT: int = 500_000

# Aave v3 flash loan premium (bps)
FLASH_LOAN_PREMIUM_BPS: int = 5  # 0.05%

# Swap fee to convert collateral back to debt asset (if different)
SWAP_FEE_BPS: int = 30  # 0.3% worst case (Uni V3 3000 pool)

# Gas price on Arbitrum (wei) — variable, use current
GAS_PRICE_GWEI: float = 0.1  # typical L2 gas price


def calculate_liquidation_profit(
    debt_to_cover: int,
    liquidated_collateral_amount: int,
    collateral_decimals: int = 18,
    debt_decimals: int = 18,
    collateral_price_usd: float = 1.0,
    debt_price_usd: float = 1.0,
    gas_price_wei: int = int(GAS_PRICE_GWEI * 1e9),
    swap_fee_bps: int = SWAP_FEE_BPS,
    flash_loan_bps: int = FLASH_LOAN_PREMIUM_BPS,
) -> LiquidationProfitResult:
    """
    Calculate net profit for a liquidation opportunity.

    Args:
        debt_to_cover: Amount of debt to repay (in debt asset base units)
        liquidated_collateral_amount: Amount of collateral received (in collateral asset base units)
        collateral_decimals: Decimals of collateral asset
        debt_decimals: Decimals of debt asset
        collateral_price_usd: Price of collateral in USD
        debt_price_usd: Price of debt asset in USD
        gas_price_wei: Current gas price in wei
        swap_fee_bps: Fee to swap collateral back to debt asset (0 if same)
        flash_loan_bps: Aave flash loan premium in bps

    Returns:
        LiquidationProfitResult with all cost breakdowns
    """
    # Normalize to USD for profit calculation
    debt_usd = debt_to_cover / (10 ** debt_decimals) * debt_price_usd
    collateral_usd = liquidated_collateral_amount / (10 ** collateral_decimals) * collateral_price_usd

    # Gross profit in USD
    gross_usd = collateral_usd - debt_usd

    # Costs
    flash_loan_usd = debt_usd * (flash_loan_bps / 10_000)
    gas_cost_eth = (LIQUIDATION_GAS_LIMIT * gas_price_wei) / 1e18
    gas_cost_usd = gas_cost_eth * debt_price_usd  # approximate: use debt price as ETH proxy
    swap_slippage_usd = collateral_usd * (swap_fee_bps / 10_000)

    total_costs_usd = flash_loan_usd + gas_cost_usd + swap_slippage_usd
    net_usd = gross_usd - total_costs_usd

    # Convert back to debt asset units
    net_debt_units = int(net_usd / debt_price_usd * (10 ** debt_decimals))
    gross_debt_units = int(gross_usd / debt_price_usd * (10 ** debt_decimals))

    return LiquidationProfitResult(
        block=0,
        user="",
        collateral_asset="",
        debt_asset="",
        debt_to_cover=debt_to_cover,
        liquidated_amount=liquidated_collateral_amount,
        gross_profit=gross_debt_units,
        flash_loan_fee=int(flash_loan_usd / debt_price_usd * (10 ** debt_decimals)),
        gas_cost=int(gas_cost_usd / debt_price_usd * (10 ** debt_decimals)),
        swap_slippage=int(swap_slippage_usd / debt_price_usd * (10 ** debt_decimals)),
        net_profit=net_debt_units,
        net_profit_pct=(net_usd / debt_usd * 100) if debt_usd > 0 else 0.0,
    )


def format_hf_status(hf: float) -> str:
    """Return status label from health factor."""
    if hf < 1.0:
        return "❌ LIQUIDATABLE"
    if hf < 1.05:
        return "🚨 CRITICAL"
    if hf < 1.1:
        return "⚠️ WARNING"
    if hf < 1.5:
        return "⚠️ ELEVATED"
    return "✅ SAFE"


# ─────────────────────────────────────────────────────────────────────────────────────────────────
# Reserve Discovery
# ─────────────────────────────────────────────────────────────────────────────────────────────────

# Common Aave V3 assets on Arbitrum (address, symbol, decimals)
# Extend this list as needed; these cover >95% of liquidatable volume.
KNOWN_ASSETS: List[Tuple[str, str, int]] = [
    ("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", "WETH", 18),
    ("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "USDC", 6),
    ("0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", "USDT", 6),
    ("0x912CE59144191C1204E64559FE8253a0e49E6548", "ARB", 18),
    ("0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f", "WBTC", 8),
    ("0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1", "DAI", 18),
    ("0xf97f4df75117a78c1A5a0DBb814Af92458539FB4", "LINK", 18),
    ("0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8", "USDC.e", 6),  # bridged USDC
]


def fetch_user_reserves(w3: Web3, user: str) -> List[UserReserveData]:
    """
    Fetch per-asset reserve data for a user across KNOWN_ASSETS.

    Returns list of UserReserveData where user has either collateral or debt.
    """
    selector = keccak(text="getUserReserveData(address,address)")[:4]
    results: List[UserReserveData] = []

    for asset, symbol, decimals in KNOWN_ASSETS:
        calldata = "0x" + selector.hex() + encode(
            ["address", "address"],
            [asset, user],
        ).hex()

        try:
            raw = w3.eth.call({"to": POOL_DATA_PROVIDER, "data": calldata})
            if len(raw) < 64:
                continue

            decoded = decode(
                ["uint256", "uint256", "uint256", "uint256", "uint256", "uint256", "uint256", "uint40", "bool"],
                raw,
            )
            a_token_balance = int(decoded[0])
            stable_debt = int(decoded[1])
            variable_debt = int(decoded[2])
            total_debt = stable_debt + variable_debt
            usage_as_collateral = bool(decoded[8])

            if a_token_balance > 0 or total_debt > 0:
                results.append(UserReserveData(
                    asset=asset,
                    symbol=symbol,
                    a_token_balance=a_token_balance,
                    stable_debt=stable_debt,
                    variable_debt=variable_debt,
                    total_debt=total_debt,
                    usage_as_collateral_enabled=usage_as_collateral,
                    decimals=decimals,
                ))
        except Exception:
            continue

    return results


def pick_liquidation_target(reserves: List[UserReserveData]) -> Optional[Tuple[UserReserveData, UserReserveData, int]]:
    """
    Pick the best collateral/debt pair for liquidation.

    Returns (collateral_reserve, debt_reserve, debt_to_cover) or None if no valid pair.
    debt_to_cover is in debt token base units (capped at 50% close factor).
    """
    collateral_candidates = [r for r in reserves if r.is_collateral]
    debt_candidates = [r for r in reserves if r.is_debt]

    if not collateral_candidates or not debt_candidates:
        return None

    # Pick largest debt
    debt_reserve = max(debt_candidates, key=lambda r: r.total_debt)

    # Pick largest collateral (prefer same asset if possible, else largest)
    same_asset = [r for r in collateral_candidates if r.asset.lower() == debt_reserve.asset.lower()]
    if same_asset:
        collateral_reserve = same_asset[0]
    else:
        collateral_reserve = max(collateral_candidates, key=lambda r: r.a_token_balance)

    # Aave V3 close factor: max 50% of debt in one tx
    close_factor_bps = 5000
    debt_to_cover = (debt_reserve.total_debt * close_factor_bps) // 10000

    # Ensure at least some dust remains if debt is tiny
    if debt_to_cover == 0 and debt_reserve.total_debt > 0:
        debt_to_cover = debt_reserve.total_debt

    return collateral_reserve, debt_reserve, debt_to_cover
