"""
Arbitrum Nitro gas estimation using ArbGasInfo precompile.
Replaces the dangerously inaccurate hardcoded ARB_L1_BASE_ESTIMATE.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from web3 import AsyncWeb3, Web3
from eth_utils import to_checksum_address

logger = logging.getLogger("gas")

# ArbGasInfo precompile on Arbitrum
ARBGASINFO_ADDRESS = to_checksum_address("0x000000000000000000000000000000000000006c")

ARBGASINFO_ABI = [
    {
        "inputs": [],
        "name": "getL1BaseFeeEstimate",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "getMinimumGasPrice",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "getPricesInWei",
        "outputs": [
            {"internalType": "uint256", "name": "l2BaseFee", "type": "uint256"},
            {"internalType": "uint256", "name": "l1BaseFeeEstimate", "type": "uint256"},
            {"internalType": "uint256", "name": "", "type": "uint256"},
            {"internalType": "uint256", "name": "", "type": "uint256"},
            {"internalType": "uint256", "name": "", "type": "uint256"},
            {"internalType": "uint256", "name": "", "type": "uint256"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]


class ArbitrumGasEstimator:
    """Estimates total tx cost on Arbitrum Nitro (L2 execution + L1 calldata)."""

    def __init__(self, safety_multiplier: float = 1.3):
        self.safety_multiplier = safety_multiplier

    async def estimate_total_cost(self, w3: AsyncWeb3, tx_dict: Dict[str, Any]) -> int:
        """Return total estimated cost in wei (L2 fee + L1 fee), with safety margin."""
        gas_price = await w3.eth.gas_price

        # L2 gas estimation
        try:
            l2_gas = await w3.eth.estimate_gas(tx_dict)
        except Exception:
            # Fallback: rough heuristic for FlashExecutor.executeFlashLoan
            l2_gas = 800_000

        l2_fee = l2_gas * gas_price

        # L1 fee via ArbGasInfo
        arb_gas = w3.eth.contract(address=ARBGASINFO_ADDRESS, abi=ARBGASINFO_ABI)
        try:
            prices = await arb_gas.functions.getPricesInWei().call()
            l1_base_fee = prices[1]  # l1BaseFeeEstimate
        except Exception as exc:
            logger.warning("ArbGasInfo call failed (%s), using fallback L1 fee", exc)
            l1_base_fee = gas_price * 10  # conservative fallback

        # Estimate L1 calldata gas units
        tx_data = tx_dict.get("data", "0x")
        data_bytes = bytes.fromhex(tx_data[2:]) if tx_data.startswith("0x") else b""
        zero_bytes = sum(1 for b in data_bytes if b == 0)
        non_zero_bytes = len(data_bytes) - zero_bytes

        # Arbitrum L1 gas formula approximation (similar to Ethereum but with per-tx overhead)
        l1_gas_units = 2100 + (non_zero_bytes * 16) + (zero_bytes * 4)
        l1_fee = l1_gas_units * l1_base_fee

        total = l2_fee + l1_fee
        safe_total = int(total * self.safety_multiplier)
        logger.debug(
            "Gas est: l2_gas=%d l2_fee=%d l1_fee=%d total=%d safe=%d",
            l2_gas, l2_fee, l1_fee, total, safe_total,
        )
        return safe_total
