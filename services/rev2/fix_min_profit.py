"""
fix_min_profit.py — MinProfitThreshold safe setter + unit verifier
Fixes W1: MinProfitThreshold = 1 wei causes dust liquidations where gas
          cost (~$0.07) exceeds profit on every marginal fill.

Problem:
    The contract's minProfitThreshold is in whatever unit executeLiquidation
    uses internally. Setting it wrong (e.g. 1M wei thinking it's "$1 USDC"
    when the contract actually measures ETH) either:
      - Leaves dust execution on (threshold too low)
      - Blocks all liquidations (threshold too high)

This module:
    1. Reads the contract to detect what unit threshold is measured in
    2. Computes the correct threshold value for a given USD floor
    3. Provides a safe setter with pre/post verification
    4. Adds runtime profit gate to pipeline.py (no contract call needed)

Usage:
    # One-time setup — run from CLI to set threshold correctly:
    python fix_min_profit.py --rpc $RPC --contract $EXECUTOR --pk $PK --usd 5

    # Runtime gate in pipeline.py (no contract interaction needed):
    from fix_min_profit import ProfitGate
    gate = ProfitGate(min_profit_usd=5.0, gas_cost_usd=0.10)
    if not gate.check(expected_profit_usd=ev_result.profit):
        return None  # skip dust liquidation
"""

import argparse
import asyncio
import logging
import os
import sys
from dataclasses import dataclass
from typing import Optional

from web3 import AsyncWeb3, Web3
from web3.providers import AsyncHTTPProvider

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Contract ABI fragments
# ---------------------------------------------------------------------------

EXECUTOR_ABI = [
    {
        "inputs": [],
        "name": "minProfitThreshold",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "uint256", "name": "_threshold", "type": "uint256"}],
        "name": "setMinProfitThreshold",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "owner",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    # Try to detect profit token — not all executors expose this
    {
        "inputs": [],
        "name": "profitToken",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "WETH",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function",
    },
]

ERC20_ABI = [
    {
        "inputs": [],
        "name": "decimals",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "symbol",
        "outputs": [{"internalType": "string", "name": "", "type": "string"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# Known token decimals on Arbitrum — fallback if contract doesn't expose profitToken
KNOWN_DECIMALS = {
    "ETH":  18,
    "WETH": 18,
    "USDC": 6,
    "USDT": 6,
    "DAI":  18,
    "WBTC": 8,
}

# Approximate ETH price for USD conversion — update or fetch dynamically
ETH_PRICE_USD_FALLBACK = 3500.0


# ---------------------------------------------------------------------------
# Unit detector
# ---------------------------------------------------------------------------

@dataclass
class ThresholdUnit:
    symbol: str
    decimals: int
    token_address: Optional[str]
    is_eth: bool


async def detect_threshold_unit(
    w3: AsyncWeb3,
    executor_address: str,
) -> ThresholdUnit:
    """
    Attempts to determine what unit the contract measures profit in.
    Tries profitToken() → WETH() → falls back to ETH assumption.
    """
    contract = w3.eth.contract(
        address=AsyncWeb3.to_checksum_address(executor_address),
        abi=EXECUTOR_ABI,
    )

    # Try profitToken()
    try:
        token_addr = await contract.functions.profitToken().call()
        if token_addr and token_addr != "0x0000000000000000000000000000000000000000":
            erc20 = w3.eth.contract(
                address=AsyncWeb3.to_checksum_address(token_addr),
                abi=ERC20_ABI,
            )
            decimals = await erc20.functions.decimals().call()
            symbol   = await erc20.functions.symbol().call()
            logger.info(f"[ProfitUnit] Detected via profitToken(): {symbol} ({decimals} decimals)")
            return ThresholdUnit(
                symbol=symbol,
                decimals=decimals,
                token_address=token_addr,
                is_eth=False,
            )
    except Exception:
        pass

    # Try WETH()
    try:
        weth_addr = await contract.functions.WETH().call()
        if weth_addr and weth_addr != "0x0000000000000000000000000000000000000000":
            logger.info(f"[ProfitUnit] Detected via WETH(): ETH/WETH (18 decimals)")
            return ThresholdUnit(
                symbol="WETH",
                decimals=18,
                token_address=weth_addr,
                is_eth=True,
            )
    except Exception:
        pass

    # Read current threshold — if it's in range of Aave oracle units (8 decimals)
    # it's likely USD-denominated. If it's 1 wei (the bug), we can't tell.
    try:
        current = await contract.functions.minProfitThreshold().call()
        logger.warning(
            f"[ProfitUnit] Cannot detect unit automatically. "
            f"Current threshold={current}. "
            f"Defaulting to ETH (18 decimals). "
            f"Verify manually against contract source."
        )
    except Exception:
        pass

    return ThresholdUnit(
        symbol="ETH",
        decimals=18,
        token_address=None,
        is_eth=True,
    )


# ---------------------------------------------------------------------------
# Threshold calculator
# ---------------------------------------------------------------------------

def compute_threshold(
    min_profit_usd: float,
    unit: ThresholdUnit,
    eth_price_usd: float = ETH_PRICE_USD_FALLBACK,
) -> int:
    """
    Convert a USD profit floor to the contract's native unit.

    Args:
        min_profit_usd: desired minimum profit in USD (e.g. 5.0)
        unit:           detected threshold unit from detect_threshold_unit()
        eth_price_usd:  current ETH price for ETH-denominated contracts

    Returns:
        Raw uint256 value to pass to setMinProfitThreshold()
    """
    if unit.symbol in ("ETH", "WETH"):
        # ETH-denominated: $5 / $3500 per ETH * 1e18
        eth_amount = min_profit_usd / eth_price_usd
        raw = int(eth_amount * (10 ** unit.decimals))
        logger.info(
            f"[ThresholdCalc] ${min_profit_usd} USD = "
            f"{eth_amount:.6f} ETH = {raw} wei "
            f"(at ETH=${eth_price_usd})"
        )
    elif unit.decimals == 8:
        # Aave oracle units (8 decimals, USD-based)
        raw = int(min_profit_usd * 1e8)
        logger.info(f"[ThresholdCalc] ${min_profit_usd} USD = {raw} (8-decimal oracle units)")
    elif unit.decimals == 6:
        # USDC/USDT
        raw = int(min_profit_usd * 1e6)
        logger.info(f"[ThresholdCalc] ${min_profit_usd} USD = {raw} ({unit.symbol} 6-decimal units)")
    elif unit.decimals == 18:
        # Other 18-decimal stable or token — treat as 1:1 USD if stablecoin
        raw = int(min_profit_usd * 1e18)
        logger.info(f"[ThresholdCalc] ${min_profit_usd} USD = {raw} ({unit.symbol} 18-decimal units)")
    else:
        raw = int(min_profit_usd * (10 ** unit.decimals))
        logger.info(f"[ThresholdCalc] ${min_profit_usd} USD = {raw} ({unit.decimals}-decimal units)")

    return raw


# ---------------------------------------------------------------------------
# Safe setter
# ---------------------------------------------------------------------------

async def set_min_profit_threshold(
    rpc_url: str,
    executor_address: str,
    private_key: str,
    min_profit_usd: float,
    eth_price_usd: float = ETH_PRICE_USD_FALLBACK,
    dry_run: bool = False,
) -> bool:
    """
    Safely sets minProfitThreshold with unit detection and pre/post verification.

    Returns True if successful, False if any check fails.
    """
    w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
    executor = w3.eth.contract(
        address=AsyncWeb3.to_checksum_address(executor_address),
        abi=EXECUTOR_ABI,
    )

    # Verify ownership
    wallet = w3.eth.account.from_key(private_key).address
    try:
        owner = await executor.functions.owner().call()
        if owner.lower() != wallet.lower():
            logger.error(
                f"[ProfitSetter] Wallet {wallet[:10]}… is not owner. "
                f"Owner is {owner[:10]}…"
            )
            return False
        logger.info(f"[ProfitSetter] Ownership confirmed: {wallet[:10]}…")
    except Exception as e:
        logger.warning(f"[ProfitSetter] Could not verify ownership: {e}")

    # Read current value
    current = await executor.functions.minProfitThreshold().call()
    logger.info(f"[ProfitSetter] Current threshold: {current}")

    # Detect unit and compute new value
    unit = await detect_threshold_unit(w3, executor_address)
    new_threshold = compute_threshold(min_profit_usd, unit, eth_price_usd)

    logger.info(
        f"[ProfitSetter] Setting threshold: {current} → {new_threshold} "
        f"(${min_profit_usd} in {unit.symbol} units)"
    )

    if dry_run:
        logger.info("[ProfitSetter] DRY RUN — no transaction sent")
        return True

    # Build and send transaction
    try:
        nonce    = await w3.eth.get_transaction_count(wallet, "pending")
        gas_price= await w3.eth.gas_price

        tx = await executor.functions.setMinProfitThreshold(new_threshold).build_transaction({
            "from":     wallet,
            "nonce":    nonce,
            "gasPrice": int(gas_price * 1.2),
            "gas":      100_000,
        })

        signed = w3.eth.account.sign_transaction(tx, private_key)
        tx_hash = await w3.eth.send_raw_transaction(signed.raw_transaction)
        logger.info(f"[ProfitSetter] Tx sent: {tx_hash.hex()}")

        receipt = await w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt["status"] != 1:
            logger.error(f"[ProfitSetter] Transaction reverted: {tx_hash.hex()}")
            return False

        # Post-verify
        confirmed = await executor.functions.minProfitThreshold().call()
        if confirmed != new_threshold:
            logger.error(
                f"[ProfitSetter] Post-verify failed: expected {new_threshold}, got {confirmed}"
            )
            return False

        logger.info(
            f"[ProfitSetter] ✓ Threshold set to {confirmed} "
            f"(${min_profit_usd} in {unit.symbol} units) "
            f"block={receipt['blockNumber']}"
        )
        return True

    except Exception as e:
        logger.error(f"[ProfitSetter] Transaction failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Runtime profit gate — used in pipeline.py, no contract interaction
# ---------------------------------------------------------------------------

class ProfitGate:
    """
    Lightweight runtime check before blast_submit().
    Prevents dust liquidations without a contract call on the hot path.

    Usage in pipeline.py (after EVEstimator returns):
        gate = ProfitGate(min_profit_usd=5.0, gas_cost_usd=0.10)

        # In attempt_liquidation():
        if not gate.check(ev_result.expected_profit_usd):
            logger.debug(f"Below profit floor — skip {borrower[:10]}")
            return None
    """

    def __init__(
        self,
        min_profit_usd: float = 5.0,
        gas_cost_usd: float   = 0.10,
        min_profit_multiple: float = 2.0,  # profit must be >= 2× gas cost
    ):
        self.min_profit_usd     = min_profit_usd
        self.gas_cost_usd       = gas_cost_usd
        self.min_profit_multiple= min_profit_multiple
        self._skipped           = 0
        self._passed            = 0

    def check(self, expected_profit_usd: float) -> bool:
        """
        Returns True if the trade clears the profit floor.
        Logs skip reason for diagnostics.
        """
        # Absolute floor
        if expected_profit_usd < self.min_profit_usd:
            self._skipped += 1
            logger.debug(
                f"[ProfitGate] SKIP — profit=${expected_profit_usd:.2f} < "
                f"floor=${self.min_profit_usd:.2f}"
            )
            return False

        # Gas multiple check
        min_by_gas = self.gas_cost_usd * self.min_profit_multiple
        if expected_profit_usd < min_by_gas:
            self._skipped += 1
            logger.debug(
                f"[ProfitGate] SKIP — profit=${expected_profit_usd:.2f} < "
                f"{self.min_profit_multiple}× gas=${min_by_gas:.2f}"
            )
            return False

        self._passed += 1
        return True

    @property
    def stats(self) -> dict:
        total = self._passed + self._skipped
        return {
            "passed":  self._passed,
            "skipped": self._skipped,
            "skip_rate": self._skipped / total if total else 0.0,
        }


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

async def _main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Set MinProfitThreshold safely")
    parser.add_argument("--rpc",      required=True, help="HTTP RPC URL")
    parser.add_argument("--contract", required=True, help="FlashExecutorV3 address")
    parser.add_argument("--pk",       default=os.getenv("PK"), help="Private key (or set $PK)")
    parser.add_argument("--usd",      type=float, default=5.0, help="Min profit in USD (default: 5.0)")
    parser.add_argument("--eth-price",type=float, default=ETH_PRICE_USD_FALLBACK)
    parser.add_argument("--dry-run",  action="store_true", help="Simulate only, no tx")
    args = parser.parse_args()

    if not args.pk:
        print("Error: provide --pk or set $PK environment variable")
        sys.exit(1)

    success = await set_min_profit_threshold(
        rpc_url          = args.rpc,
        executor_address = args.contract,
        private_key      = args.pk,
        min_profit_usd   = args.usd,
        eth_price_usd    = args.eth_price,
        dry_run          = args.dry_run,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    asyncio.run(_main())


# ---------------------------------------------------------------------------
# pipeline.py integration
# ---------------------------------------------------------------------------
#
# 1. One-time threshold setter (run from CLI):
#       python fix_min_profit.py \
#           --rpc   $QUICKNODE_HTTP \
#           --contract 0x4CdADEd4749FcB498e7E371EBF00C319674D3F8D \
#           --pk    $PK \
#           --usd   5.0
#
# 2. Runtime gate in pipeline.py:
#       from fix_min_profit import ProfitGate
#       self.profit_gate = ProfitGate(min_profit_usd=5.0, gas_cost_usd=0.10)
#
#       # In attempt_liquidation(), before blast_submit():
#       if not self.profit_gate.check(ev_result.expected_profit_usd):
#           return None
#
# 3. Log gate stats periodically:
#       logger.info(f"[ProfitGate] {self.profit_gate.stats}")
#
# ---------------------------------------------------------------------------
