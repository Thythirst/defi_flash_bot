#!/usr/bin/env python3
"""
ev_estimator.py — EV computation using Aave V3 close factor + Uni V3 real-time quotes.
Hard go/no-go gate before every tx submission. Min profit: $8.
"""
import logging
from dataclasses import dataclass
from typing import Dict, Optional

from web3 import Web3

logger = logging.getLogger(__name__)

UNI_V3_QUOTER = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"

HF_CLOSE_FACTOR_THRESHOLD = 0.95
HF_MAX_CLOSE_FACTOR = 0.5
HF_MIN_CLOSE_FACTOR_HF = 1.0

FLASH_GAS = 400_000
DIRECT_GAS = 680_000
MIN_PROFIT_USD = 8.0
MEV_SAFETY_MARGIN_USD = 5.0

# Per-asset liquidation bonuses (Aave V3 Arbitrum — from on-chain config)
ASSET_LIQ_BONUS: Dict[str, float] = {
    "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1": 0.075,  # WETH
    "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f": 0.075,  # WBTC
    "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8": 0.05,   # USDC.e
    "0xaf88d065e77c8cC2239327C5EDb3A432268e5831": 0.05,   # USDC native
    "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9": 0.05,   # USDT
    "0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1": 0.05,   # DAI
    "0x5979D7b546E38E414F7E9822514be443A4800529": 0.075,  # wstETH
    "0x912CE59144191C1204E64559FE8253a0e49E6548": 0.10,   # ARB
}
DEFAULT_LIQ_BONUS = 0.05


@dataclass
class EVResult:
    go: bool
    net_ev_usd: float
    gross_profit_usd: float
    gas_cost_usd: float
    slippage_cost_usd: float
    close_factor: float
    debt_to_cover_raw: int
    debt_to_cover_usd: float
    collateral_asset: str
    debt_asset: str
    execution_path: str
    reason: str


class EVEstimator:
    def __init__(self, w3: Web3, prices: Dict[str, int], decimals: Dict[str, int],
                 wallet_balances: Dict[str, int], quoter_abi: list):
        self.w3 = w3
        self.prices = prices
        self.decimals = decimals
        self.wallet_balances = wallet_balances
        self.quoter = w3.eth.contract(address=UNI_V3_QUOTER, abi=quoter_abi)

    def compute(self, address: str, hf: float, pos) -> EVResult:
        if hf >= 1.0:
            return self._nogo("HF >= 1.0, not liquidatable", address, pos)

        best_collateral = self._best_asset(pos.collateral, pos.collateral_assets)
        best_debt = self._best_asset(pos.debt, pos.debt_assets)
        if best_collateral is None or best_debt is None:
            return self._nogo("No valid collateral or debt with known price", address, pos)
        if best_collateral == best_debt:
            return self._nogo("Same-asset position (price-invariant)", address, pos)

        close_factor = self._close_factor(hf)
        if close_factor < 0.01:
            return self._nogo(f"Close factor too small ({close_factor:.4f})", address, pos)

        debt_raw = pos.debt.get(best_debt, 0)
        debt_to_cover_raw = int(debt_raw * close_factor)
        debt_dec = self.decimals.get(best_debt, 18)
        debt_price = self.prices.get(best_debt, 0)
        debt_to_cover_usd = (debt_to_cover_raw / 10**debt_dec) * (debt_price / 10**8)

        if debt_to_cover_usd < 50:
            return self._nogo(f"Debt to cover too small (${debt_to_cover_usd:.2f})", address, pos)

        bonus_pct = ASSET_LIQ_BONUS.get(best_collateral, DEFAULT_LIQ_BONUS)
        gross_profit_usd = debt_to_cover_usd * bonus_pct

        wallet_bal = self.wallet_balances.get(best_debt, 0)
        wallet_usd = (wallet_bal / 10**debt_dec) * (debt_price / 10**8)
        use_direct = wallet_usd >= debt_to_cover_usd * 1.05
        execution_path = 'direct' if use_direct else 'flash'
        gas_units = DIRECT_GAS if use_direct else FLASH_GAS

        base_fee = self._get_base_fee()
        gas_price_wei = int(base_fee * 1.5)
        eth_price = self.prices.get(
            "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", 200000_00000000
        ) / 10**8
        gas_cost_eth = (gas_units * gas_price_wei) / 10**18
        gas_cost_usd = gas_cost_eth * eth_price

        slippage_cost_usd = 0.0
        if best_collateral != best_debt:
            slippage_cost_usd = self._estimate_slippage(best_collateral, best_debt, debt_to_cover_usd)

        net_ev = gross_profit_usd - gas_cost_usd - slippage_cost_usd - MEV_SAFETY_MARGIN_USD

        if net_ev < MIN_PROFIT_USD:
            return EVResult(go=False, net_ev_usd=net_ev, gross_profit_usd=gross_profit_usd,
                            gas_cost_usd=gas_cost_usd, slippage_cost_usd=slippage_cost_usd,
                            close_factor=close_factor, debt_to_cover_raw=debt_to_cover_raw,
                            debt_to_cover_usd=debt_to_cover_usd, collateral_asset=best_collateral,
                            debt_asset=best_debt, execution_path=execution_path,
                            reason=f"Net EV ${net_ev:.2f} < ${MIN_PROFIT_USD}")

        return EVResult(go=True, net_ev_usd=net_ev, gross_profit_usd=gross_profit_usd,
                        gas_cost_usd=gas_cost_usd, slippage_cost_usd=slippage_cost_usd,
                        close_factor=close_factor, debt_to_cover_raw=debt_to_cover_raw,
                        debt_to_cover_usd=debt_to_cover_usd, collateral_asset=best_collateral,
                        debt_asset=best_debt, execution_path=execution_path, reason="GO")

    def _close_factor(self, hf: float) -> float:
        if hf >= HF_MIN_CLOSE_FACTOR_HF:
            return 0.0
        if hf <= HF_CLOSE_FACTOR_THRESHOLD:
            return HF_MAX_CLOSE_FACTOR    # 0.5 — max 50% close factor
        cf = (HF_MIN_CLOSE_FACTOR_HF - hf) / (HF_MIN_CLOSE_FACTOR_HF - HF_CLOSE_FACTOR_THRESHOLD)
        return max(0.0, min(cf, HF_MAX_CLOSE_FACTOR))

    def _best_asset(self, amounts: Dict[str, int], assets) -> Optional[str]:
        best = None
        best_usd = 0.0
        for asset in assets:
            price = self.prices.get(asset)
            if price is None:
                continue
            dec = self.decimals.get(asset, 18)
            usd = (amounts.get(asset, 0) / 10**dec) * (price / 10**8)
            if usd > best_usd:
                best_usd = usd
                best = asset
        return best

    def _estimate_slippage(self, token_in: str, token_out: str, amount_usd: float) -> float:
        try:
            dec_in = self.decimals.get(token_in, 18)
            price_in = self.prices.get(token_in, 0)
            amount_in = int((amount_usd / (price_in / 10**8)) * 10**dec_in)
            dec_out = self.decimals.get(token_out, 18)
            price_out = self.prices.get(token_out, 0)
            expected_out = int((amount_usd / (price_out / 10**8)) * 10**dec_out)
            best_out = 0
            for fee in [500, 3000, 10000]:
                try:
                    result = self.quoter.functions.quoteExactInputSingle({
                        'tokenIn': token_in, 'tokenOut': token_out,
                        'amountIn': amount_in, 'fee': fee,
                        'sqrtPriceLimitX96': 0,
                    }).call()
                    best_out = max(best_out, result[0])
                except Exception:
                    continue
            if best_out == 0 or expected_out == 0:
                return amount_usd * 0.005
            slippage_pct = 1 - (best_out / expected_out)
            return amount_usd * max(0, slippage_pct)
        except Exception as e:
            logger.warning(f"[EV] Slippage estimate failed: {e} — using 0.5%")
            return amount_usd * 0.005

    def _get_base_fee(self) -> int:
        try:
            return self.w3.eth.get_block('pending')['baseFeePerGas']
        except Exception:
            return 100_000_000

    def _nogo(self, reason: str, address: str, pos) -> EVResult:
        best_c = self._best_asset(pos.collateral, pos.collateral_assets) or ""
        best_d = self._best_asset(pos.debt, pos.debt_assets) or ""
        return EVResult(go=False, net_ev_usd=0, gross_profit_usd=0,
                        gas_cost_usd=0, slippage_cost_usd=0, close_factor=0,
                        debt_to_cover_raw=0, debt_to_cover_usd=0,
                        collateral_asset=best_c, debt_asset=best_d,
                        execution_path='flash', reason=reason)
