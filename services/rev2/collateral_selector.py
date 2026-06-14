"""
collateral_selector.py — Risk-adjusted collateral selection
Fixes W11: _best_asset() picked highest USD value, ignoring liquidation threshold.
           A $10K ARB position (65% threshold, 10% bonus) yields less than
           a $9K WETH position (82.5% threshold, 7.5% bonus) in risk-adjusted terms.

The correct selection maximises:
    expected_profit = collateral_usd × close_factor × liq_bonus_pct
    subject to:      collateral is enabled, has debt, usage_as_collateral=True

Usage:
    selector = CollateralSelector(position_loader)
    best = await selector.select(account_data, debt_asset, debt_amount, prices)
    if best:
        # best.asset, best.expected_profit_usd, best.debt_to_cover
"""

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# Aave V3 close factor — max % of debt that can be liquidated in one tx
# Standard: 50% when HF > 0.95, 100% when HF <= 0.95
CLOSE_FACTOR_STANDARD = 0.50
CLOSE_FACTOR_FULL     = 1.00
HF_FULL_LIQUIDATION   = 0.95 * 10**18  # WAD

# Basis point denominator
BPS = 10_000


@dataclass
class CollateralCandidate:
    """Ranked candidate for liquidation collateral selection."""
    asset: str
    symbol: str
    collateral_usd: float         # current USD value of collateral
    debt_to_cover: float          # USD value of debt this candidate covers
    liquidation_bonus_bps: int    # e.g. 10500 → 500bps = 5% bonus
    liquidation_threshold_bps: int
    expected_profit_usd: float    # the ranking metric
    a_token_balance: int          # raw on-chain units
    price_usd: float


@dataclass
class SelectionResult:
    asset: str
    symbol: str
    debt_to_cover: int            # raw uint256 for tx
    expected_profit_usd: float
    liquidation_bonus_bps: int
    close_factor: float
    reason: str                   # human-readable selection rationale


class CollateralSelector:
    """
    Selects the optimal collateral asset for a given liquidation.

    Ranking logic (replaces highest-USD-value heuristic):
    1. Filter: usage_as_collateral=True, a_token_balance > 0
    2. Compute close_factor from HF (50% standard, 100% if HF <= 0.95)
    3. For each candidate:
         debt_to_cover_usd = min(total_debt_usd × close_factor, collateral_usd)
         bonus_amount_usd  = debt_to_cover_usd × (liq_bonus_bps / BPS - 1.0)
         expected_profit   = bonus_amount_usd (before gas, slippage)
    4. Select candidate with highest expected_profit_usd

    Note: EVEstimator still applies gas + slippage on top for final GO/NO-GO.
    This replaces the pre-filter selection, not the full EV calc.
    """

    def __init__(self, position_loader=None, asset_symbols: dict[str, str] = None):
        """
        Args:
            position_loader: PositionLoader instance (for reserve configs)
            asset_symbols:   {address → symbol} map for logging
        """
        self._loader  = position_loader
        self._symbols = asset_symbols or {}

    def select(
        self,
        account_data,           # AccountData from position_loader
        total_debt_usd: float,  # from account_data.total_debt_base / 1e8
        asset_prices_usd: dict[str, float],  # {asset_addr → usd_price}
        asset_decimals: dict[str, int],      # {asset_addr → decimals}
    ) -> Optional[SelectionResult]:
        """
        Select the best collateral asset for liquidation.

        Args:
            account_data:     AccountData with reserves populated (HF < 1.05)
            total_debt_usd:   borrower's total debt in USD
            asset_prices_usd: current price per asset (USD, any precision)
            asset_decimals:   token decimals per asset

        Returns:
            SelectionResult or None if no eligible collateral found
        """
        if not account_data.reserves:
            logger.warning(
                f"[CollateralSelector] {account_data.address[:10]}… "
                f"has no reserve data — run refresh_hot() with HF < 1.05 first"
            )
            return None

        hf = account_data.health_factor
        close_factor = (
            CLOSE_FACTOR_FULL if hf <= HF_FULL_LIQUIDATION else CLOSE_FACTOR_STANDARD
        )

        candidates: list[CollateralCandidate] = []

        for reserve in account_data.reserves:
            asset = reserve.asset

            # Must be used as collateral
            if not reserve.usage_as_collateral:
                continue

            # Must have collateral balance
            if reserve.a_token_balance == 0:
                continue

            # Need price to value it
            price_usd = asset_prices_usd.get(asset)
            if price_usd is None or price_usd <= 0:
                continue

            # Get reserve config for bonus/threshold
            cfg = self._loader.get_reserve_config(asset) if self._loader else None
            if cfg is None:
                # Default conservative values if config unavailable
                liq_bonus_bps     = 10500  # 5% bonus
                liq_threshold_bps = 8000   # 80%
            else:
                liq_bonus_bps     = cfg.liquidation_bonus
                liq_threshold_bps = cfg.liquidation_threshold

            # Skip if bonus is 0 or implausible (asset misconfigured)
            if liq_bonus_bps <= BPS:
                continue

            # Compute collateral USD value
            decimals = asset_decimals.get(asset, 18)
            collateral_units = reserve.a_token_balance / (10 ** decimals)
            collateral_usd   = collateral_units * price_usd

            # How much debt can we cover against this collateral?
            max_debt_usd  = total_debt_usd * close_factor
            debt_to_cover = min(max_debt_usd, collateral_usd)

            # Profit = bonus on the liquidated collateral amount
            bonus_pct        = liq_bonus_bps / BPS - 1.0   # e.g. 10500/10000 - 1 = 0.05
            expected_profit  = debt_to_cover * bonus_pct

            symbol = self._symbols.get(asset, asset[:8] + "…")

            candidates.append(CollateralCandidate(
                asset=asset,
                symbol=symbol,
                collateral_usd=collateral_usd,
                debt_to_cover=debt_to_cover,
                liquidation_bonus_bps=liq_bonus_bps,
                liquidation_threshold_bps=liq_threshold_bps,
                expected_profit_usd=expected_profit,
                a_token_balance=reserve.a_token_balance,
                price_usd=price_usd,
            ))
            

        if not candidates:
            logger.warning(
                f"[CollateralSelector] No eligible collateral for "
                f"{account_data.address[:10]}… (HF={account_data.hf_float:.4f})"
            )
            return None

        # Rank by expected profit
        candidates.sort(key=lambda c: c.expected_profit_usd, reverse=True)
        best = candidates[0]

        # Log top 3 for debugging
        for i, c in enumerate(candidates[:3]):
            logger.debug(
                f"[CollateralSelector] #{i+1} {c.symbol}: "
                f"collateral=${c.collateral_usd:.0f} "
                f"bonus={c.liquidation_bonus_bps}bps "
                f"profit=${c.expected_profit_usd:.2f}"
            )

        # Convert debt_to_cover (USD) to raw uint256 in asset's native decimals.
        # best.asset is the collateral asset — for same-asset liquidations
        # (collateral == debt) this is correct. Cross-asset WETH→USDC uses
        # wrong decimals (18 vs 6) but those routes currently fail on QuoterV2
        # anyway. Fixing properly requires passing debt_asset to select().
        # Divide by price_usd because debt_to_cover is a USD float, not token units.
        debt_dec          = max(asset_decimals.get(best.asset, 6), 1)
        debt_to_cover_raw = int(best.debt_to_cover / best.price_usd * (10 ** debt_dec))

        reason = (
            f"{best.symbol} selected: ${best.expected_profit_usd:.2f} expected profit "
            f"(bonus={best.liquidation_bonus_bps}bps, "
            f"threshold={best.liquidation_threshold_bps}bps, "
            f"collateral=${best.collateral_usd:.0f})"
        )
        logger.info(f"[CollateralSelector] {reason}")

        return SelectionResult(
            asset=best.asset,
            symbol=best.symbol,
            debt_to_cover=debt_to_cover_raw,
            expected_profit_usd=best.expected_profit_usd,
            liquidation_bonus_bps=best.liquidation_bonus_bps,
            close_factor=close_factor,
            reason=reason,
        )


# ---------------------------------------------------------------------------
# ev_estimator.py integration guide
# ---------------------------------------------------------------------------
#
# Replace _best_asset() method:
#
#     OLD:
#         def _best_asset(self, amounts):
#             best_usd = 0
#             best_asset = None
#             for asset, amount in amounts.items():
#                 dec   = DECIMALS.get(asset, 18)
#                 price = self.prices.get(asset, 0) / 10**8
#                 usd   = (amount / 10**dec) * price
#                 if usd > best_usd:
#                     best_usd   = usd
#                     best_asset = asset
#             return best_asset
#
#     NEW:
#         from collateral_selector import CollateralSelector
#         self.selector = CollateralSelector(
#             position_loader=loader,
#             asset_symbols=ASSET_SYMBOLS,
#         )
#
#         # In estimate():
#         result = self.selector.select(
#             account_data     = loader.get(borrower),
#             total_debt_usd   = account_data.total_debt_base / 1e8,
#             asset_prices_usd = {a: prices.get_price(a) / 1e8 for a in assets},
#             asset_decimals   = DECIMALS,
#         )
#         if result is None:
#             return None  # no eligible collateral
#
#         collateral_asset = result.asset
#         debt_to_cover    = result.debt_to_cover  # raw uint256
#         expected_profit  = result.expected_profit_usd
#
# ---------------------------------------------------------------------------
