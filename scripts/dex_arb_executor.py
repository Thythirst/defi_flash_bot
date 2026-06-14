"""
scripts/dex_arb_executor.py — DEX-DEX cyclic arbitrage executor.

Integrates with scanner/dex_arb.py. When the scanner finds a profitable
cyclic arb, this module broadcasts the executeArbitrage transaction
through DexArbExecutor + Balancer flash loans.

Usage:
    from scripts.dex_arb_executor import DexArbExecutor
    executor = DexArbExecutor(rpc_url, private_key, contract_address)
    tx_hash = executor.execute(opportunity)

Requires:
    DEX_ARB_EXECUTOR — deployed contract address (env var or default)
    BOT_PRIVATE_KEY   — hot wallet private key
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from eth_abi import encode
from eth_utils import keccak
from web3 import Web3
from web3.types import TxParams

logger = logging.getLogger("dex_arb_executor")

# Default deployed address (override via DEX_ARB_EXECUTOR env var)
DEFAULT_EXECUTOR = "0xdC8B7B7d33356a4dd72C44c2d8ff992eC086FbDc"

# ABI snippet for executeArbitrage + approveRouter
EXECUTOR_ABI = [
    {
        "type": "function",
        "name": "executeArbitrage",
        "inputs": [
            {"name": "tokenA", "type": "address"},
            {"name": "tokenB", "type": "address"},
            {"name": "amountA", "type": "uint256"},
            {"name": "routerFwd", "type": "address"},
            {"name": "feeFwd", "type": "uint24"},
            {"name": "routerRev", "type": "address"},
            {"name": "feeRev", "type": "uint24"},
            {"name": "amountOutMin", "type": "uint256"},
        ],
        "outputs": [],
        "stateMutability": "nonpayable",
    },
]

# Router address mapping (must match scanner/dex_arb.py ROUTERS)
ROUTER_ADDRESSES = {
    "UniV3":        "0xE592427A0AEce92De3Edee1F18E0157C05861564",
    "SushiV3":      "0x8A21F6768c1F8075791D08546dADF6daA0Be16eC",
    "PancakeSwapV3":"0x1b81D678ffb9C0263b24A97847620C99d213eB14",
}

# Reverse fee tiers for reverse swap — prefer same fee tier as forward
# but can use 0.3% as safe default if reverse fee unknown
DEFAULT_REV_FEE = 3000  # 0.3%


class DexArbExecutor:
    """Encodes and broadcasts DEX-DEX cyclic arbitrage transactions."""

    def __init__(
        self,
        rpc_url: str,
        private_key: str,
        contract_address: str = "",
    ):
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))
        self.account = self.w3.eth.account.from_key(private_key)
        self.contract_address = self.w3.to_checksum_address(
            contract_address or os.getenv("DEX_ARB_EXECUTOR", DEFAULT_EXECUTOR)
        )
        self.contract = self.w3.eth.contract(
            address=self.contract_address,
            abi=EXECUTOR_ABI,
        )

        # Slippage: require at least 99.5% of input back
        self.slippage_bps = 5  # 0.5%

        logger.info(
            "DexArbExecutor initialized: contract=%s account=%s",
            self.contract_address,
            self.account.address,
        )

    def encode_execute_arbitrage(
        self,
        token_a: str,
        token_b: str,
        amount_a: int,
        sell_router_name: str,  # forward: tokenA → tokenB
        buy_router_name: str,   # reverse: tokenB → tokenA
        fee_fwd: int,
        fee_rev: int = 0,
        amount_out_min: int = 0,
    ) -> str:
        """
        Encode executeArbitrage calldata for DexArbExecutor.

        Args:
            token_a: Flash-loaned token address
            token_b: Intermediate token address
            amount_a: Flash loan amount in token_a base units
            sell_router_name: Router name for forward swap (tokenA → tokenB)
            buy_router_name: Router name for reverse swap (tokenB → tokenA)
            fee_fwd: Forward swap fee tier (e.g., 3000 for 0.3%)
            fee_rev: Reverse swap fee tier (default: same as forward)
            amount_out_min: Minimum tokenA to receive from reverse swap
        """
        if fee_rev == 0:
            fee_rev = fee_fwd

        if amount_out_min == 0:
            amount_out_min = int(amount_a * (10000 - self.slippage_bps) / 10000)

        router_fwd = ROUTER_ADDRESSES[sell_router_name]
        router_rev = ROUTER_ADDRESSES[buy_router_name]

        return self.contract.encodeABI(
            fn_name="executeArbitrage",
            args=[
                token_a,
                token_b,
                amount_a,
                router_fwd,
                fee_fwd,
                router_rev,
                fee_rev,
                amount_out_min,
            ],
        )

    def execute(
        self,
        token_a: str,
        token_b: str,
        amount_a: int,
        sell_router_name: str,
        buy_router_name: str,
        fee_fwd: int,
        fee_rev: int = 0,
        amount_out_min: int = 0,
        gas_limit: int = 1_500_000,
        dry_run: bool = False,
    ) -> Optional[str]:
        """
        Execute a DEX-DEX cyclic arbitrage.

        Returns tx_hash on success, None on failure/dry_run.
        """
        calldata = self.encode_execute_arbitrage(
            token_a=token_a,
            token_b=token_b,
            amount_a=amount_a,
            sell_router_name=sell_router_name,
            buy_router_name=buy_router_name,
            fee_fwd=fee_fwd,
            fee_rev=fee_rev,
            amount_out_min=amount_out_min,
        )

        nonce = self.w3.eth.get_transaction_count(self.account.address)

        gas_price = self.w3.eth.gas_price
        # Add 20% buffer for L2 priority
        gas_price = int(gas_price * 1.2)

        tx: TxParams = {
            "from": self.account.address,
            "to": self.contract_address,
            "data": calldata,
            "nonce": nonce,
            "gas": gas_limit,
            "gasPrice": gas_price,
            "chainId": 42161,
        }

        if dry_run:
            logger.info(
                "DRY RUN: Would execute %s→%s→%s via %s/%s (amount=%d, fee=%d)",
                _addr_short(token_a),
                _addr_short(token_b),
                _addr_short(token_a),
                sell_router_name,
                buy_router_name,
                amount_a,
                fee_fwd,
            )
            return None

        logger.info(
            "Executing DEX arb: %s→%s→%s via %s/%s amount=%d",
            _addr_short(token_a),
            _addr_short(token_b),
            _addr_short(token_a),
            sell_router_name,
            buy_router_name,
            amount_a,
        )

        signed = self.account.sign_transaction(tx)
        tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)

        logger.info("TX broadcast: %s", tx_hash.hex())
        return tx_hash.hex()


def _addr_short(addr: str) -> str:
    return addr[:10] + "..." if len(addr) > 10 else addr
