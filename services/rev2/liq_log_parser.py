"""
liq_log_parser.py — Correct Aave V3 LiquidationCall log parsing
Fixes W3: topics[2] was read as borrower (it's debtAsset).
          data[26:66] was read as liquidator (it's mid-debtToCover).

LiquidationCall event signature:
    LiquidationCall(
        address indexed collateralAsset,   → topics[1]
        address indexed debtAsset,         → topics[2]
        address indexed user,              → topics[3]  ← borrower
        uint256 debtToCover,               → data[0:32]
        uint256 liquidatedCollateralAmount,→ data[32:64]
        address liquidator,                → data[64:96] (padded to 32 bytes)
        bool receiveAToken                 → data[96:128]
    )

Usage:
    from liq_log_parser import parse_liquidation_log, is_our_wallet

    event = parse_liquidation_log(log, our_wallet_address)
    if event:
        if event.is_competitor:
            # lost race — update OutcomeDB
        else:
            # our own confirmed liquidation
"""

import logging
from dataclasses import dataclass
from typing import Optional

logger = logging.getLogger(__name__)

# Aave V3 LiquidationCall topic0 (keccak256 of event signature)
LIQUIDATION_CALL_TOPIC = (
    "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286"
)


@dataclass
class LiquidationEvent:
    collateral_asset: str    # checksummed address
    debt_asset: str          # checksummed address
    borrower: str            # checksummed address (the liquidated user)
    liquidator: str          # checksummed address (who won the race)
    debt_to_cover: int       # raw uint256
    collateral_amount: int   # raw uint256
    receive_a_token: bool
    tx_hash: str
    block_number: int
    is_competitor: bool      # True if liquidator != our wallet
    gas_price: int = 0      # effective gas price from tx (0 if unknown)


def _strip_topic_to_address(topic: str) -> str:
    """
    ABI-encoded address in a topic is right-padded to 32 bytes.
    Extract the rightmost 20 bytes (40 hex chars).
    """
    clean = topic.removeprefix("0x")
    return "0x" + clean[-40:]


def _decode_address_from_data(data_hex: str, slot: int) -> str:
    """
    Extract an address from a specific 32-byte slot in ABI-encoded data.
    slot=0 → bytes 0-31, slot=1 → bytes 32-63, slot=2 → bytes 64-95, etc.
    Address occupies the rightmost 20 bytes of each 32-byte slot.

    Args:
        data_hex: hex string of the log data field (with or without 0x prefix)
        slot:     0-indexed slot number
    """
    clean = data_hex.removeprefix("0x")
    start = slot * 64          # each 32-byte slot = 64 hex chars
    end   = start + 64
    slot_hex = clean[start:end]
    if len(slot_hex) < 64:
        raise ValueError(f"data too short for slot {slot}: got {len(clean)} chars")
    return "0x" + slot_hex[-40:]  # rightmost 20 bytes


def _decode_uint256_from_data(data_hex: str, slot: int) -> int:
    """Extract a uint256 from a specific 32-byte slot in ABI-encoded data."""
    clean = data_hex.removeprefix("0x")
    start = slot * 64
    end   = start + 64
    slot_hex = clean[start:end]
    if len(slot_hex) < 64:
        raise ValueError(f"data too short for slot {slot}")
    return int(slot_hex, 16)


def _decode_bool_from_data(data_hex: str, slot: int) -> bool:
    return _decode_uint256_from_data(data_hex, slot) != 0


def parse_liquidation_log(
    log: dict,
    our_wallet: str,
) -> Optional[LiquidationEvent]:
    """
    Parse a raw eth_getLogs / WebSocket log dict into a LiquidationEvent.
    Returns None if the log is not a LiquidationCall or decoding fails.

    Fixes W3 in pipeline.py:
        OLD: borrower   = "0x" + topics[2][-40:]   ← debtAsset, not user
             liquidator = "0x" + log['data'][26:66] ← wrong offset

        NEW: borrower   = topics[3] rightmost 40 chars
             liquidator = data slot 2 rightmost 40 chars

    Args:
        log:        raw log dict with 'topics', 'data', 'transactionHash',
                    'blockNumber' fields
        our_wallet: checksummed wallet address to detect competitor events
    """
    topics = log.get("topics", [])

    # Verify this is a LiquidationCall event
    if not topics or topics[0].lower() != LIQUIDATION_CALL_TOPIC.lower():
        return None

    if len(topics) < 4:
        logger.warning(
            f"[LiqParser] LiquidationCall with only {len(topics)} topics "
            f"(expected 4) in tx {log.get('transactionHash', '?')}"
        )
        return None

    data_hex = log.get("data", "0x")

    try:
        # --- Indexed parameters (topics) ---
        collateral_asset = _strip_topic_to_address(topics[1])  # topic[1]
        debt_asset       = _strip_topic_to_address(topics[2])  # topic[2]
        borrower         = _strip_topic_to_address(topics[3])  # topic[3] ← THE FIX

        # --- Non-indexed parameters (data, ABI-encoded) ---
        # Slot 0: uint256 debtToCover
        # Slot 1: uint256 liquidatedCollateralAmount
        # Slot 2: address liquidator           ← THE FIX (was data[26:66])
        # Slot 3: bool    receiveAToken
        debt_to_cover     = _decode_uint256_from_data(data_hex, slot=0)
        collateral_amount = _decode_uint256_from_data(data_hex, slot=1)
        liquidator        = _decode_address_from_data(data_hex, slot=2)  # bytes 64-95
        receive_a_token   = _decode_bool_from_data(data_hex, slot=3)

        tx_hash      = log.get("transactionHash", "0x")
        block_number = int(log.get("blockNumber", 0), 16) if isinstance(
            log.get("blockNumber"), str
        ) else log.get("blockNumber", 0)

        # Extract gas price if available (from tx receipt or full tx object)
        gas_price = int(log.get("effectiveGasPrice", 0), 16) if isinstance(
            log.get("effectiveGasPrice"), str
        ) else log.get("effectiveGasPrice", 0)
        if gas_price == 0:
            gas_price = int(log.get("gasPrice", 0), 16) if isinstance(
                log.get("gasPrice"), str
            ) else log.get("gasPrice", 0)

        is_competitor = liquidator.lower() != our_wallet.lower()

        event = LiquidationEvent(
            collateral_asset=collateral_asset,
            debt_asset=debt_asset,
            borrower=borrower,
            liquidator=liquidator,
            debt_to_cover=debt_to_cover,
            collateral_amount=collateral_amount,
            receive_a_token=receive_a_token,
            tx_hash=tx_hash,
            block_number=block_number,
            is_competitor=is_competitor,
            gas_price=gas_price,
        )

        if is_competitor:
            logger.info(
                f"[LiqParser] COMPETITOR liquidation — "
                f"borrower={borrower[:10]}… "
                f"liquidator={liquidator[:10]}… "
                f"block={block_number}"
            )
        else:
            logger.info(
                f"[LiqParser] OUR liquidation confirmed — "
                f"borrower={borrower[:10]}… "
                f"block={block_number}"
            )

        return event

    except Exception as e:
        logger.error(
            f"[LiqParser] Decode failed for tx "
            f"{log.get('transactionHash', '?')}: {e}"
        )
        return None


def is_our_wallet(event: LiquidationEvent, our_wallet: str) -> bool:
    return event.liquidator.lower() == our_wallet.lower()


# ---------------------------------------------------------------------------
# pipeline.py patch
# ---------------------------------------------------------------------------
#
# BEFORE (~line 368):
#     borrower   = "0x" + topics[2][-40:]      # WRONG: debtAsset
#     liquidator = "0x" + log['data'][26:66]   # WRONG: mid-debtToCover
#
# AFTER:
#     from liq_log_parser import parse_liquidation_log
#
#     event = parse_liquidation_log(log, WALLET_ADDR)
#     if event is None:
#         return
#     borrower   = event.borrower
#     liquidator = event.liquidator
#     if event.is_competitor:
#         outcome_db.record_loss(borrower, liquidator, event.block_number)
#     else:
#         outcome_db.record_win(borrower, event.block_number)
#
# ---------------------------------------------------------------------------
