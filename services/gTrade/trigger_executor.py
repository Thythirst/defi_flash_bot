"""
gTrade Keeper Bot — Trigger Executor

Submits on-chain transactions to GNSMultiCollatDiamond.

CONTRACT INTERFACE (verified 2026-06-21 against 0xFF162c694eAA571f685030649814282eA457f169):

  PERMISSIONLESS (our bot can call these directly):
    0xb6919540  cancelOrderAfterTimeout(uint32 pendingOrderId)
                — cancels market open/close orders after marketOrdersTimeoutBlocks (200)
    0xeb9359aa  triggerOrder(uint256 pendingOrderId)
                — Chainlink-based limit order trigger (slower path, may lose to GNS oracle network)

  RESTRICTED (require trader delegate approval — DelegateNotApproved() = 0x0cf0b6f5 if not set):
    0x737b84cd  delegatedTradingAction(address trader, bytes innerCalldata)
                — outer wrapper for trader-authorized actions
    0x36ce736b  closeTradeMarket(uint32 tradeIndex, uint64 price_1e10)
                — close an open position at market (inner; needs delegate approval)
    0x5bfcc4f8  openTrade(Trade, uint16 maxSlippage, address referrer)
                — open a new trade (inner; needs delegate approval)

  GNS ORACLE KEEPER PATH (requires signed oracle price proofs — closed network):
    0xc7e2b2a9  triggerOrderWithSignatures(uint256 pendingOrderId, sigs[])
                — active keeper path; requires GNS oracle network membership

  Price precision: GNS uses 1e10 (Chainlink 1e8 answer × 100).
"""

import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv
from web3 import Web3
from eth_abi import encode as abi_encode

load_dotenv(os.path.expanduser("~/defi_flash_bot/.env"))

logger = logging.getLogger("gtrade.executor")

# ── Contract addresses ─────────────────────────────────────────────────────────
GTRADE_DIAMOND = "0xFF162c694eAA571f685030649814282eA457f169"

# Chainlink ETH/USD on Arbitrum (for gas cost estimation)
CHAINLINK_ETH_USD = "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"

# ── Verified function selectors ────────────────────────────────────────────────
#
# Permissionless:
CANCEL_TIMEOUT_SEL     = bytes.fromhex("b6919540")   # cancelOrderAfterTimeout(uint32)
TRIGGER_ORDER_SEL      = bytes.fromhex("eb9359aa")   # triggerOrder(uint256)

# Restricted (needs trader delegate approval):
DELEGATED_ACTION_SEL   = bytes.fromhex("737b84cd")   # delegatedTradingAction(address, bytes)
CLOSE_TRADE_MARKET_SEL = bytes.fromhex("36ce736b")   # closeTradeMarket(uint32, uint64)

# Chainlink latestRoundData()
LATEST_ROUND_DATA_SEL  = "0xfeaf968c"

# ── Runtime config ─────────────────────────────────────────────────────────────
RPC_URL = (
    os.getenv("READ_RPC_PRIMARY")
    or os.getenv("QUICKNODE_HTTP_URL")
    or "https://lb.drpc.org/ogrpc?network=arbitrum&dkey=AtjbSqJ_xEnQ"
)
WALLET_ADDR  = os.getenv("BOT_ADDRESS", "")
PRIVATE_KEY  = os.getenv("BOT_PRIVATE_KEY", "")

MIN_KEEPER_REWARD_USD = float(os.getenv("GTRADE_MIN_REWARD_USD", "1.0"))
GAS_LIMIT             = 500_000
GAS_PRICE_MULTIPLIER  = 1.15      # 15% above current base fee


@dataclass
class TriggerResult:
    success:  bool
    tx_hash:  Optional[str] = None
    error:    Optional[str] = None
    gas_used: int = 0


class TriggerExecutor:
    """
    Executes permissionless gTrade keeper actions.

    Primary action: cancel_timed_out_order(pending_order_id)
      — calls cancelOrderAfterTimeout(uint32) after the 200-block window.

    Secondary action: trigger_order(pending_order_id)
      — calls triggerOrder(uint256) on the Chainlink path; this competes with
        the GNS oracle network and will often lose on speed, but is still valid.
    """

    CHAINLINK_TO_GNS_PRICE = 100   # Chainlink 1e8 × 100 = GNS 1e10

    def __init__(
        self,
        rpc_url:     str = RPC_URL,
        wallet_addr: str = WALLET_ADDR,
        private_key: str = PRIVATE_KEY,
        diamond:     str = GTRADE_DIAMOND,
    ):
        self.diamond     = Web3.to_checksum_address(diamond)
        self.wallet      = Web3.to_checksum_address(wallet_addr) if wallet_addr else None
        self.private_key = private_key

        self.w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))
        if not self.w3.is_connected():
            raise RuntimeError(f"Cannot connect to {rpc_url}")

        logger.info("[TriggerExecutor] Connected: %s", rpc_url[:60])

    # ── Public API ─────────────────────────────────────────────────────────────

    def cancel_timed_out_order(self, pending_order_id: int) -> TriggerResult:
        """
        Call cancelOrderAfterTimeout(uint32 pendingOrderId).
        Permissionless — any EOA can cancel a market order after 200 blocks.
        """
        calldata = CANCEL_TIMEOUT_SEL + abi_encode(["uint32"], [pending_order_id])
        return self._submit(calldata, f"cancelTimeout id={pending_order_id}")

    def simulate_cancel(self, pending_order_id: int) -> TriggerResult:
        """Dry-run cancelOrderAfterTimeout without broadcasting."""
        calldata = CANCEL_TIMEOUT_SEL + abi_encode(["uint32"], [pending_order_id])
        return self._simulate(calldata, f"cancelTimeout id={pending_order_id}")

    def trigger_order(self, pending_order_id: int) -> TriggerResult:
        """
        Call triggerOrder(uint256 pendingOrderId) — Chainlink keeper path.
        Permissionless but races GNS oracle network. Use for limit orders.
        """
        calldata = TRIGGER_ORDER_SEL + abi_encode(["uint256"], [pending_order_id])
        return self._submit(calldata, f"triggerOrder id={pending_order_id}")

    def simulate_trigger_order(self, pending_order_id: int) -> TriggerResult:
        """Dry-run triggerOrder without broadcasting."""
        calldata = TRIGGER_ORDER_SEL + abi_encode(["uint256"], [pending_order_id])
        return self._simulate(calldata, f"triggerOrder id={pending_order_id}")

    def get_chainlink_price(self, feed_addr: str = CHAINLINK_ETH_USD) -> Optional[int]:
        """
        Read latestRoundData() from a Chainlink feed.
        Returns raw 1e8 answer (multiply by 100 for GNS 1e10 format), or None.
        """
        try:
            result = self.w3.eth.call({
                "to":   Web3.to_checksum_address(feed_addr),
                "data": LATEST_ROUND_DATA_SEL,
            })
            from eth_abi import decode as abi_decode
            _, answer, _, updated_at, _ = abi_decode(
                ["uint80", "int256", "uint256", "uint256", "uint80"], result
            )
            age = int(time.time()) - updated_at
            if age > 3600:
                logger.warning("[TriggerExecutor] Chainlink price stale (%ds old)", age)
                return None
            return int(answer)
        except Exception as e:
            logger.warning("[TriggerExecutor] Chainlink read failed (%s): %s", feed_addr[:10], e)
            return None

    def chainlink_to_gns_price(self, chainlink_answer: int) -> int:
        return chainlink_answer * self.CHAINLINK_TO_GNS_PRICE

    # ── Execution internals ────────────────────────────────────────────────────

    def _simulate(self, calldata: bytes, label: str) -> TriggerResult:
        try:
            self.w3.eth.call({
                "from": self.wallet or "0x0000000000000000000000000000000000000001",
                "to":   self.diamond,
                "data": calldata,
            })
            logger.info("[TriggerExecutor] sim OK: %s", label)
            return TriggerResult(success=True)
        except Exception as e:
            err = str(e)
            logger.debug("[TriggerExecutor] sim FAIL: %s — %s", label, err[:120])
            return TriggerResult(success=False, error=err)

    def _submit(self, calldata: bytes, label: str) -> TriggerResult:
        if not self.private_key:
            return TriggerResult(success=False, error="no private key")
        if not self.wallet:
            return TriggerResult(success=False, error="no wallet address")

        try:
            gas_price = self._gas_price()
            nonce     = self.w3.eth.get_transaction_count(self.wallet)

            tx = {
                "from":     self.wallet,
                "to":       self.diamond,
                "data":     calldata,
                "gas":      GAS_LIMIT,
                "gasPrice": gas_price,
                "nonce":    nonce,
                "chainId":  42161,
            }

            signed  = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
            logger.info("[TriggerExecutor] submitted %s: %s", label, tx_hash.hex())

            receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
            if receipt.status == 1:
                logger.info("[TriggerExecutor] confirmed block=%s gas=%s: %s",
                            receipt.blockNumber, receipt.gasUsed, label)
                return TriggerResult(success=True, tx_hash=tx_hash.hex(), gas_used=receipt.gasUsed)
            else:
                logger.warning("[TriggerExecutor] reverted: %s tx=%s", label, tx_hash.hex())
                return TriggerResult(success=False, tx_hash=tx_hash.hex(), error="transaction reverted")
        except Exception as e:
            logger.error("[TriggerExecutor] submit failed: %s — %s", label, e)
            return TriggerResult(success=False, error=str(e))

    def _gas_price(self) -> int:
        try:
            return int(self.w3.eth.gas_price * GAS_PRICE_MULTIPLIER)
        except Exception:
            return 100_000_000  # 0.1 gwei fallback for Arbitrum
