"""
tests/test_aave_indexer.py — Comprehensive test suite for the Aave V3 indexer.

Tests cover:
  1. Event decoding — all 10 event types
  2. State machine — supply/withdraw/borrow/repay/liquidation/collateral/emode
  3. Health factor computation
  4. Position scaling on index update
  5. RedisLiqReader integration

Uses fakeredis for zero-dependency Redis mocking.
"""

from __future__ import annotations

import json
import math
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

import pytest
import pytest_asyncio
from eth_utils import keccak

# Ensure the project root is importable
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from indexers.aave_indexer import (
    AaveIndexer,
    AAVE_POOL,
    ALL_TOPICS,
    # Event decoders
    decode_supply,
    decode_withdraw,
    decode_borrow,
    decode_repay,
    decode_liquidation,
    decode_reserve_data_updated,
    decode_collateral_toggle,
    decode_emode,
    # Event data classes
    SupplyEvent,
    WithdrawEvent,
    BorrowEvent,
    RepayEvent,
    LiquidationCallEvent,
    ReserveDataUpdatedEvent,
    CollateralToggledEvent,
    UserEModeSetEvent,
    # Event topics
    TOPIC_SUPPLY,
    TOPIC_WITHDRAW,
    TOPIC_BORROW,
    TOPIC_REPAY,
    TOPIC_LIQUIDATION,
    TOPIC_RESERVE_DATA_UPDATED,
    TOPIC_COLLATERAL_ENABLED,
    TOPIC_COLLATERAL_DISABLED,
    TOPIC_EMODE_SET,
    # Helpers
    pad_address,
)
from indexers.redis_liq_reader import RedisLiqReader, LiquidationCandidate

# ────────────────────────────────────────────────────────────────
# Test Constants
# ────────────────────────────────────────────────────────────────

WETH = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
ALICE = "0x1111111111111111111111111111111111111111"
BOB = "0x2222222222222222222222222222222222222222"
CAROL = "0x3333333333333333333333333333333333333333"
LIQUIDATOR = "0x4444444444444444444444444444444444444444"

TX_HASH = "0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"
BLOCK = 20_000_000

# Realistic prices: WETH=$2000, USDC=$1
WETH_PRICE = 2000_00_000000  # 8 decimals
USDC_PRICE = 1_00_000000     # 8 decimals

# Reserve configs (matching real Aave V3 Arbitrum values)
WETH_CONFIG = {
    "symbol": "WETH",
    "decimals": "18",
    "a_token": "0xe50fA9b3c56FfB159cB0FCa9F52D5e80c8a1A09F",
    "variable_debt_token": "0x0c84331e39d6658Cd6e6b9ba04736cC4c4734351",
    "stable_debt_token": "0xdCA0B5bB5182319b3C6B0b1A7F5e23773dbC35b9",
    "liquidity_index": "1000000000000000000000000000",
    "variable_borrow_index": "1000000000000000000000000000",
    "liquidity_rate": "35200000000000000000000000",
    "variable_borrow_rate": "48700000000000000000000000",
    "stable_borrow_rate": "52100000000000000000000000",
    "ltv": "8000",
    "liquidation_threshold": "8400",
    "liquidation_bonus": "10500",
    "is_active": "1",
    "is_frozen": "0",
    "price": str(WETH_PRICE),
}

USDC_CONFIG = {
    "symbol": "USDC",
    "decimals": "6",
    "a_token": "0x724dc807b04555b71ed48a6896b6F41593b8C637",
    "variable_debt_token": "0xfccf3cAbbe80101232d343252614b6a3eE81C989",
    "stable_debt_token": "0xB4a4b569E7Bd00F62878A6cf2D0AceE59909de09",
    "liquidity_index": "1000000000000000000000000000",
    "variable_borrow_index": "1000000000000000000000000000",
    "liquidity_rate": "35200000000000000000000000",
    "variable_borrow_rate": "48700000000000000000000000",
    "stable_borrow_rate": "52100000000000000000000000",
    "ltv": "7500",
    "liquidation_threshold": "7800",
    "liquidation_bonus": "10500",
    "is_active": "1",
    "is_frozen": "0",
    "price": str(USDC_PRICE),
}


# ────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────

def make_log(topics: List[str], data: str, tx_hash: str = TX_HASH, block: int = BLOCK) -> dict:
    """Build a JSON-RPC log response dict."""
    return {
        "address": AAVE_POOL,
        "topics": topics,
        "data": data,
        "blockNumber": hex(block),
        "transactionHash": tx_hash,
        "logIndex": "0x0",
    }


def encode_data(types: List[str], values: List[Any]) -> str:
    """Encode event data using eth_abi."""
    from eth_abi import encode as abi_encode
    return "0x" + abi_encode(types, values).hex()


# ────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────

@pytest_asyncio.fixture
async def indexer():
    """Create an AaveIndexer with fakeredis and pre-loaded reserve configs."""
    import fakeredis.aioredis

    idx = AaveIndexer(
        rpc_url="http://noop",
        redis_url="redis://fake",
    )
    # Replace Redis with fakeredis (decode_responses=True gives string values)
    idx.redis = await fakeredis.aioredis.FakeRedis(decode_responses=True)
    idx._reserve_configs = {
        WETH.lower(): dict(WETH_CONFIG),
        USDC.lower(): dict(USDC_CONFIG),
    }
    # Also cache to Redis so the reader can resolve configs
    # Use individual hset calls (fakeredis pipeline has decode_responses issues)
    for addr, config in idx._reserve_configs.items():
        await idx.redis.hset(f"aave:reserve:{addr}", mapping=config)
    return idx

@pytest_asyncio.fixture
async def reader(indexer):
    """Create a RedisLiqReader backed by the same fakeredis."""
    r = RedisLiqReader(redis_url="redis://fake")
    r.redis = indexer.redis
    return r


# ────────────────────────────────────────────────────────────────
# 1. Event Decoding Tests
# ────────────────────────────────────────────────────────────────

class TestEventDecoding:
    """Verify all 10 Aave V3 event types decode correctly from raw logs."""

    def test_decode_supply(self):
        topics = [
            TOPIC_SUPPLY,
            "0x" + "0"*24 + WETH[2:],      # reserve (indexed)
            "0x" + "0"*24 + ALICE[2:],      # user (indexed)
            "0x" + "0"*24 + ALICE[2:],      # onBehalfOf (indexed)
        ]
        data = encode_data(["uint256", "uint16"], [1_000_000_000_000_000_000, 0])
        log = make_log(topics, data)
        idx = AaveIndexer(rpc_url="http://x", redis_url="redis://x")
        evt = idx._parse_log(log)
        assert isinstance(evt, SupplyEvent)
        assert evt.reserve.lower() == WETH.lower()
        assert evt.user.lower() == ALICE.lower()
        assert evt.on_behalf_of.lower() == ALICE.lower()
        assert evt.amount == 1_000_000_000_000_000_000

    def test_decode_supply_on_behalf(self):
        """onBehalfOf different from msg.sender."""
        topics = [
            TOPIC_SUPPLY,
            "0x" + "0"*24 + WETH[2:],
            "0x" + "0"*24 + ALICE[2:],
            "0x" + "0"*24 + BOB[2:],        # BOB is beneficiary
        ]
        data = encode_data(["uint256", "uint16"], [500_000_000_000_000_000, 0])
        log = make_log(topics, data)
        idx = AaveIndexer(rpc_url="http://x", redis_url="redis://x")
        evt = idx._parse_log(log)
        assert evt.on_behalf_of.lower() == BOB.lower()
        assert evt.user.lower() == ALICE.lower()  # msg.sender

    def test_decode_withdraw(self):
        topics = [
            TOPIC_WITHDRAW,
            "0x" + "0"*24 + USDC[2:],
            "0x" + "0"*24 + ALICE[2:],
            "0x" + "0"*24 + BOB[2:],         # recipient
        ]
        data = encode_data(["uint256"], [1000_000000])
        log = make_log(topics, data)
        idx = AaveIndexer(rpc_url="http://x", redis_url="redis://x")
        evt = idx._parse_log(log)
        assert isinstance(evt, WithdrawEvent)
        assert evt.reserve.lower() == USDC.lower()
        assert evt.to_addr.lower() == BOB.lower()
        assert evt.amount == 1000_000000

    def test_decode_borrow(self):
        topics = [
            TOPIC_BORROW,
            "0x" + "0"*24 + USDC[2:],
            "0x" + "0"*24 + ALICE[2:],
            "0x" + "0"*24 + ALICE[2:],       # onBehalfOf
        ]
        data = encode_data(["uint256", "uint8", "uint256", "uint16"],
                          [5000_000000, 2, 48700000000000000000000000, 0])
        log = make_log(topics, data)
        idx = AaveIndexer(rpc_url="http://x", redis_url="redis://x")
        evt = idx._parse_log(log)
        assert isinstance(evt, BorrowEvent)
        assert evt.amount == 5000_000000
        assert evt.interest_rate_mode == 2  # variable
        assert evt.borrow_rate == 48700000000000000000000000

    def test_decode_repay(self):
        topics = [
            TOPIC_REPAY,
            "0x" + "0"*24 + USDC[2:],
            "0x" + "0"*24 + ALICE[2:],
            "0x" + "0"*24 + BOB[2:],          # repayer
        ]
        data = encode_data(["uint256", "bool"], [2000_000000, False])
        log = make_log(topics, data)
        idx = AaveIndexer(rpc_url="http://x", redis_url="redis://x")
        evt = idx._parse_log(log)
        assert isinstance(evt, RepayEvent)
        assert evt.repayer.lower() == BOB.lower()
        assert evt.use_a_tokens is False

    def test_decode_liquidation(self):
        topics = [
            TOPIC_LIQUIDATION,
            "0x" + "0"*24 + WETH[2:],         # collateralAsset
            "0x" + "0"*24 + USDC[2:],         # debtAsset
            "0x" + "0"*24 + ALICE[2:],        # user (debtor)
        ]
        data = encode_data(["uint256", "uint256", "address", "bool"],
                          [5000_000000, 2_500_000_000_000_000_000,
                           LIQUIDATOR, False])
        log = make_log(topics, data)
        idx = AaveIndexer(rpc_url="http://x", redis_url="redis://x")
        evt = idx._parse_log(log)
        assert isinstance(evt, LiquidationCallEvent)
        assert evt.collateral_asset.lower() == WETH.lower()
        assert evt.debt_asset.lower() == USDC.lower()
        assert evt.liquidated_collateral_amount == 2_500_000_000_000_000_000
        assert evt.liquidator.lower() == LIQUIDATOR.lower()
        assert evt.receive_a_token is False

    def test_decode_reserve_data_updated(self):
        topics = [
            TOPIC_RESERVE_DATA_UPDATED,
            "0x" + "0"*24 + WETH[2:],
        ]
        data = encode_data(
            ["uint256", "uint256", "uint256", "uint256", "uint256"],
            [35200000000000000000000000, 52100000000000000000000000,
             48700000000000000000000000, 1048230000000000000000000000,
             1052180000000000000000000000],
        )
        log = make_log(topics, data)
        idx = AaveIndexer(rpc_url="http://x", redis_url="redis://x")
        evt = idx._parse_log(log)
        assert isinstance(evt, ReserveDataUpdatedEvent)
        assert evt.liquidity_index == 1048230000000000000000000000
        assert evt.variable_borrow_index == 1052180000000000000000000000
        assert evt.liquidity_rate == 35200000000000000000000000

    def test_decode_collateral_enabled(self):
        topics = [
            TOPIC_COLLATERAL_ENABLED,
            "0x" + "0"*24 + WETH[2:],
            "0x" + "0"*24 + ALICE[2:],
        ]
        data = "0x"
        log = make_log(topics, data)
        idx = AaveIndexer(rpc_url="http://x", redis_url="redis://x")
        evt = idx._parse_log(log)
        assert isinstance(evt, CollateralToggledEvent)
        assert evt.enabled is True

    def test_decode_collateral_disabled(self):
        topics = [
            TOPIC_COLLATERAL_DISABLED,
            "0x" + "0"*24 + WETH[2:],
            "0x" + "0"*24 + ALICE[2:],
        ]
        data = "0x"
        log = make_log(topics, data)
        idx = AaveIndexer(rpc_url="http://x", redis_url="redis://x")
        evt = idx._parse_log(log)
        assert isinstance(evt, CollateralToggledEvent)
        assert evt.enabled is False

    def test_decode_emode(self):
        topics = [
            TOPIC_EMODE_SET,
            "0x" + "0"*24 + ALICE[2:],
        ]
        data = encode_data(["uint8"], [1])
        log = make_log(topics, data)
        idx = AaveIndexer(rpc_url="http://x", redis_url="redis://x")
        evt = idx._parse_log(log)
        assert isinstance(evt, UserEModeSetEvent)
        assert evt.category_id == 1

    def test_parse_log_in_indexer(self):
        """Verify _parse_log dispatches correctly for each event type."""
        idx = AaveIndexer(rpc_url="http://x", redis_url="redis://x")

        # Supply
        topics = [TOPIC_SUPPLY] + ["0x" + "0"*24 + WETH[2:], "0x" + "0"*24 + ALICE[2:], "0x" + "0"*24 + ALICE[2:]]
        data = encode_data(["uint256", "uint16"], [1_000_000_000_000_000_000, 0])
        log = make_log(topics, data)
        assert isinstance(idx._parse_log(log), SupplyEvent)

        # Liquidation
        topics = [TOPIC_LIQUIDATION] + ["0x" + "0"*24 + WETH[2:], "0x" + "0"*24 + USDC[2:], "0x" + "0"*24 + ALICE[2:]]
        data = encode_data(["uint256", "uint256", "address", "bool"], [4000_000000, 2_000_000_000_000_000_000, LIQUIDATOR, False])
        log = make_log(topics, data)
        assert isinstance(idx._parse_log(log), LiquidationCallEvent)

        # Borrow
        topics = [TOPIC_BORROW] + ["0x" + "0"*24 + USDC[2:], "0x" + "0"*24 + ALICE[2:], "0x" + "0"*24 + ALICE[2:]]
        data = encode_data(["uint256", "uint8", "uint256", "uint16"], [1000_000000, 2, 0, 0])
        log = make_log(topics, data)
        assert isinstance(idx._parse_log(log), BorrowEvent)

    def test_parse_log_unknown_topic(self):
        """Unknown topics should return None gracefully."""
        idx = AaveIndexer(rpc_url="http://x", redis_url="redis://x")
        log = make_log(["0xdeadbeef00000000000000000000000000000000000000000000000000000000"], "0x")
        assert idx._parse_log(log) is None


# ────────────────────────────────────────────────────────────────
# 2. State Machine Tests
# ────────────────────────────────────────────────────────────────

class TestStateMachine:
    """Verify that events correctly modify Redis state."""

    async def test_supply_adds_collateral(self, indexer):
        """Supply creates a position with collateral."""
        evt = SupplyEvent(
            reserve=WETH, user=ALICE, on_behalf_of=ALICE,
            amount=5_000_000_000_000_000_000,
            referral_code=0, tx_hash=TX_HASH, block=BLOCK, log_index=0,
        )
        await indexer.handle_supply(evt)

        positions = await indexer._get_user_positions(ALICE)
        assert WETH.lower() in positions
        assert positions[WETH.lower()]["collateral"] == 5_000_000_000_000_000_000
        assert positions[WETH.lower()]["is_collateral"] is True

        # User should be tracked in reserve's user set (fakeredis returns int)
        is_member = await indexer.redis.sismember(f"aave:reserve:{WETH.lower()}:users", ALICE.lower())
        assert is_member == 1

    async def test_supply_on_behalf_of(self, indexer):
        """Supply where msg.sender != beneficiary."""
        evt = SupplyEvent(
            reserve=WETH, user=ALICE, on_behalf_of=BOB,
            amount=3_000_000_000_000_000_000,
            referral_code=0, tx_hash=TX_HASH, block=BLOCK, log_index=0,
        )
        await indexer.handle_supply(evt)

        alice_pos = await indexer._get_user_positions(ALICE)
        assert alice_pos == {}

        bob_pos = await indexer._get_user_positions(BOB)
        assert WETH.lower() in bob_pos
        assert bob_pos[WETH.lower()]["collateral"] == 3_000_000_000_000_000_000

    async def test_withdraw_removes_collateral(self, indexer):
        """Withdraw reduces collateral."""
        await indexer._add_collateral(ALICE, WETH, 5_000_000_000_000_000_000)

        evt = WithdrawEvent(
            reserve=WETH, user=ALICE, to_addr=ALICE,
            amount=2_000_000_000_000_000_000,
            tx_hash=TX_HASH, block=BLOCK, log_index=1,
        )
        await indexer.handle_withdraw(evt)

        positions = await indexer._get_user_positions(ALICE)
        assert positions[WETH.lower()]["collateral"] == 3_000_000_000_000_000_000

    async def test_withdraw_all_cleans_up(self, indexer):
        """Withdrawing everything removes the reserve from user's positions."""
        await indexer._add_collateral(ALICE, WETH, 5_000_000_000_000_000_000)
        evt = WithdrawEvent(
            reserve=WETH, user=ALICE, to_addr=ALICE,
            amount=5_000_000_000_000_000_000,
            tx_hash=TX_HASH, block=BLOCK, log_index=1,
        )
        await indexer.handle_withdraw(evt)

        positions = await indexer._get_user_positions(ALICE)
        assert WETH.lower() not in positions

        is_member = await indexer.redis.sismember(f"aave:reserve:{WETH.lower()}:users", ALICE.lower())
        assert is_member == 0

    async def test_borrow_adds_debt(self, indexer):
        """Borrow creates a debt position."""
        evt = BorrowEvent(
            reserve=USDC, user=ALICE, on_behalf_of=ALICE,
            amount=1000_000000,
            interest_rate_mode=2,
            borrow_rate=48700000000000000000000000,
            referral_code=0, tx_hash=TX_HASH, block=BLOCK, log_index=0,
        )
        await indexer.handle_borrow(evt)

        positions = await indexer._get_user_positions(ALICE)
        assert USDC.lower() in positions
        assert positions[USDC.lower()]["debt"] == 1000_000000
        assert positions[USDC.lower()]["borrow_mode"] == "variable"

    async def test_borrow_stable_mode(self, indexer):
        """Borrow in stable mode."""
        evt = BorrowEvent(
            reserve=USDC, user=ALICE, on_behalf_of=ALICE,
            amount=500_000000,
            interest_rate_mode=1,
            borrow_rate=52100000000000000000000000,
            referral_code=0, tx_hash=TX_HASH, block=BLOCK, log_index=0,
        )
        await indexer.handle_borrow(evt)

        positions = await indexer._get_user_positions(ALICE)
        assert positions[USDC.lower()]["borrow_mode"] == "stable"

    async def test_repay_reduces_debt(self, indexer):
        """Repay reduces outstanding debt."""
        await indexer._add_debt(ALICE, USDC, 1000_000000, 2)

        evt = RepayEvent(
            reserve=USDC, user=ALICE, repayer=ALICE,
            amount=400_000000,
            use_a_tokens=False, tx_hash=TX_HASH, block=BLOCK, log_index=0,
        )
        await indexer.handle_repay(evt)

        positions = await indexer._get_user_positions(ALICE)
        assert positions[USDC.lower()]["debt"] == 600_000000

    async def test_repay_full_cleans_up(self, indexer):
        """Full repayment removes the debt reserve."""
        await indexer._add_collateral(ALICE, WETH, 1_000_000_000_000_000_000)
        await indexer._add_debt(ALICE, USDC, 1000_000000, 2)

        evt = RepayEvent(
            reserve=USDC, user=ALICE, repayer=ALICE,
            amount=1000_000000,
            use_a_tokens=False, tx_hash=TX_HASH, block=BLOCK, log_index=0,
        )
        await indexer.handle_repay(evt)

        positions = await indexer._get_user_positions(ALICE)
        assert WETH.lower() in positions
        assert USDC.lower() not in positions

    async def test_liquidation_removes_debt_and_collateral(self, indexer):
        """Liquidation reduces debtor's debt AND collateral."""
        await indexer._add_collateral(ALICE, WETH, 5_000_000_000_000_000_000)
        await indexer._add_debt(ALICE, USDC, 8000_000000, 2)

        evt = LiquidationCallEvent(
            collateral_asset=WETH, debt_asset=USDC,
            user=ALICE,
            debt_to_cover=4000_000000,
            liquidated_collateral_amount=2_100_000_000_000_000_000,
            liquidator=LIQUIDATOR,
            receive_a_token=False,
            tx_hash=TX_HASH, block=BLOCK, log_index=0,
        )
        await indexer.handle_liquidation(evt)

        positions = await indexer._get_user_positions(ALICE)
        assert positions[WETH.lower()]["collateral"] == 2_900_000_000_000_000_000
        assert positions[USDC.lower()]["debt"] == 4000_000000

    async def test_liquidation_full_cleanup(self, indexer):
        """Full liquidation removes all positions."""
        await indexer._add_collateral(ALICE, WETH, 1_000_000_000_000_000_000)
        await indexer._add_debt(ALICE, USDC, 1000_000000, 2)

        evt = LiquidationCallEvent(
            collateral_asset=WETH, debt_asset=USDC,
            user=ALICE,
            debt_to_cover=1000_000000,
            liquidated_collateral_amount=1_000_000_000_000_000_000,
            liquidator=LIQUIDATOR,
            receive_a_token=False,
            tx_hash=TX_HASH, block=BLOCK, log_index=0,
        )
        await indexer.handle_liquidation(evt)

        positions = await indexer._get_user_positions(ALICE)
        assert positions == {}

    async def test_collateral_toggle(self, indexer):
        """Enabling/disabling collateral via toggle events."""
        await indexer._add_collateral(ALICE, WETH, 1_000_000_000_000_000_000)

        evt = CollateralToggledEvent(
            reserve=WETH, user=ALICE, enabled=False,
            tx_hash=TX_HASH, block=BLOCK, log_index=0,
        )
        await indexer.handle_collateral_toggle(evt)
        positions = await indexer._get_user_positions(ALICE)
        assert positions[WETH.lower()]["is_collateral"] is False

        evt2 = CollateralToggledEvent(
            reserve=WETH, user=ALICE, enabled=True,
            tx_hash=TX_HASH, block=BLOCK+1, log_index=0,
        )
        await indexer.handle_collateral_toggle(evt2)
        positions = await indexer._get_user_positions(ALICE)
        assert positions[WETH.lower()]["is_collateral"] is True

    async def test_emode_set(self, indexer):
        evt = UserEModeSetEvent(
            user=ALICE, category_id=1,
            tx_hash=TX_HASH, block=BLOCK, log_index=0,
        )
        await indexer.handle_emode_set(evt)
        stored = await indexer.redis.hget(f"aave:user:{ALICE.lower()}", "eMode")
        assert stored == "1"


# ────────────────────────────────────────────────────────────────
# 4. Health Factor Tests
# ────────────────────────────────────────────────────────────────

class TestHealthFactor:
    """Verify HF computation and liquidatable ZSET management."""

    async def test_healthy_position(self, indexer):
        """Supply WETH, borrow some USDC — should be healthy."""
        # 5 WETH @ $2000 = $10,000, liq threshold 84% → max borrow $8,400
        await indexer._add_collateral(ALICE, WETH, 5_000_000_000_000_000_000)
        await indexer._add_debt(ALICE, USDC, 4000_000000, 2)

        hf = await indexer.recalc_health_factor(ALICE)
        # HF = 5*2000*0.84 / 4000 = 8400/4000 = 2.1
        assert hf > 1.0
        assert 2.0 < hf < 2.2

        score = await indexer.redis.zscore("aave:liquidatable", ALICE.lower())
        assert score is None

    async def test_liquidatable_position(self, indexer):
        """Heavy borrowing — should be liquidatable."""
        await indexer._add_collateral(ALICE, WETH, 5_000_000_000_000_000_000)
        await indexer._add_debt(ALICE, USDC, 9000_000000, 2)

        hf = await indexer.recalc_health_factor(ALICE)
        assert hf < 1.0
        assert hf > 0.5

        score = await indexer.redis.zscore("aave:liquidatable", ALICE.lower())
        assert score is not None
        assert float(score) < 1.0

    async def test_no_debt_infinite_hf(self, indexer):
        """Position with only collateral = infinite HF."""
        await indexer._add_collateral(ALICE, WETH, 5_000_000_000_000_000_000)

        hf = await indexer.recalc_health_factor(ALICE)
        assert hf == float('inf')

        score = await indexer.redis.zscore("aave:liquidatable", ALICE.lower())
        assert score is None

    async def test_collateral_not_enabled(self, indexer):
        """Disabled collateral doesn't count toward HF."""
        await indexer._add_collateral(ALICE, WETH, 5_000_000_000_000_000_000)
        await indexer._toggle_collateral(ALICE, WETH, False)
        await indexer._add_debt(ALICE, USDC, 1000_000000, 2)

        hf = await indexer.recalc_health_factor(ALICE)
        # No effective collateral → 0 / 1000 = 0
        assert hf == 0.0

    async def test_liquidatable_sorted_by_hf(self, indexer):
        """Most at-risk users should come first in ZSET."""
        # Alice: 2 WETH, 4000 USDC debt → HF = 2*2000*0.84/4000 = 3360/4000 = 0.84
        await indexer._add_collateral(ALICE, WETH, 2_000_000_000_000_000_000)
        await indexer._add_debt(ALICE, USDC, 4000_000000, 2)
        await indexer.recalc_health_factor(ALICE)

        # Bob: 2 WETH, 3500 USDC debt → HF = 3360/3500 = 0.96
        await indexer._add_collateral(BOB, WETH, 2_000_000_000_000_000_000)
        await indexer._add_debt(BOB, USDC, 3500_000000, 2)
        await indexer.recalc_health_factor(BOB)

        # Carol: 1 WETH, 1500 USDC debt → HF = 1*2000*0.84/1500 = 1680/1500 = 1.12 (healthy)
        await indexer._add_collateral(CAROL, WETH, 1_000_000_000_000_000_000)
        await indexer._add_debt(CAROL, USDC, 1500_000000, 2)
        await indexer.recalc_health_factor(CAROL)

        results = await indexer.redis.zrange("aave:liquidatable", 0, -1, withscores=True)
        assert len(results) == 2  # Carol is healthy
        users, scores = zip(*results)
        assert users[0].lower() == ALICE.lower()  # Alice most at-risk
        assert float(scores[0]) < float(scores[1])

    async def test_repay_removes_from_liquidatable(self, indexer):
        """Repaying debt removes from liquidatable set."""
        await indexer._add_collateral(ALICE, WETH, 5_000_000_000_000_000_000)
        await indexer._add_debt(ALICE, USDC, 9000_000000, 2)
        await indexer.recalc_health_factor(ALICE)

        score = await indexer.redis.zscore("aave:liquidatable", ALICE.lower())
        assert score is not None

        await indexer._remove_debt(ALICE, USDC, 7000_000000)
        await indexer.recalc_health_factor(ALICE)

        score = await indexer.redis.zscore("aave:liquidatable", ALICE.lower())
        assert score is None

    async def test_unknown_reserve_graceful(self, indexer):
        """Unknown reserves are skipped in HF calc, not crashing."""
        unknown = "0x9999999999999999999999999999999999999999"
        positions = {
            WETH.lower(): {"collateral": 5_000_000_000_000_000_000, "debt": 1000_000000, "borrow_mode": "variable", "is_collateral": True},
            unknown: {"collateral": 1000, "debt": 500, "borrow_mode": "variable", "is_collateral": True},
        }
        await indexer._set_user_positions(ALICE, positions)

        hf = indexer._compute_health_factor(positions)
        # Only WETH counts → 5*2000*0.84 / (1000*1) = 8400/1000 = 8.4
        assert hf > 1.0
        assert hf != float('inf')


# ────────────────────────────────────────────────────────────────
# 5. Position Scaling Tests
# ────────────────────────────────────────────────────────────────

class TestPositionScaling:
    """Verify positions are scaled correctly when reserve indexes update."""

    async def test_collateral_scales_with_liquidity_index(self, indexer):
        """Liquidity index increase scales collateral proportionally."""
        await indexer._add_collateral(ALICE, WETH, 1_000_000_000_000_000_000)
        await indexer._add_collateral(ALICE, USDC, 1000_000000)

        old_liq = 1_000_000_000_000_000_000_000_000_000
        new_liq = 1_100_000_000_000_000_000_000_000_000  # 10% increase
        old_bor = 1_000_000_000_000_000_000_000_000_000
        new_bor = 1_050_000_000_000_000_000_000_000_000

        await indexer._scale_positions_for_reserve(WETH, old_liq, new_liq, old_bor, new_bor)

        positions = await indexer._get_user_positions(ALICE)
        # 1 WETH * 1.1 = 1.1 WETH. Float math may have small rounding — use int cast.
        expected = int(1_000_000_000_000_000_000 * (new_liq / old_liq))
        assert positions[WETH.lower()]["collateral"] == expected
        assert positions[USDC.lower()]["collateral"] == 1000_000000  # unchanged

    async def test_debt_scales_with_borrow_index(self, indexer):
        """Borrow index increase scales debt proportionally."""
        await indexer._add_debt(ALICE, USDC, 1000_000000, 2)

        old_liq = 1_000_000_000_000_000_000_000_000_000
        new_liq = 1_000_000_000_000_000_000_000_000_000
        old_bor = 1_000_000_000_000_000_000_000_000_000
        new_bor = 1_200_000_000_000_000_000_000_000_000  # 20% increase

        await indexer._scale_positions_for_reserve(USDC, old_liq, new_liq, old_bor, new_bor)

        positions = await indexer._get_user_positions(ALICE)
        expected = int(1000_000000 * (new_bor / old_bor))
        assert positions[USDC.lower()]["debt"] == expected

    async def test_zero_index_noop(self, indexer):
        """Zero old index should skip scaling (defensive)."""
        await indexer._add_collateral(ALICE, WETH, 1_000_000_000_000_000_000)

        await indexer._scale_positions_for_reserve(
            WETH, 0, 1_000_000_000_000_000_000_000_000_000,
            0, 1_000_000_000_000_000_000_000_000_000,
        )

        positions = await indexer._get_user_positions(ALICE)
        assert positions[WETH.lower()]["collateral"] == 1_000_000_000_000_000_000


# ────────────────────────────────────────────────────────────────
# 6. RedisLiqReader Tests
# ────────────────────────────────────────────────────────────────

class TestRedisLiqReader:
    """Verify the liquidation reader correctly fetches candidates from Redis."""

    async def test_no_liquidatable_returns_empty(self, reader):
        candidates = await reader.get_liquidatable_positions()
        assert candidates == []

    async def test_count_returns_zero_initially(self, reader):
        assert await reader.get_liquidatable_count() == 0

    async def test_returns_liquidatable_candidates(self, indexer, reader):
        """Setup liquidatable positions and verify reader returns them."""
        # Alice: 2 WETH, 4000 USDC → HF = 3360/4000 = 0.84 (liquidatable)
        await indexer._add_collateral(ALICE, WETH, 2_000_000_000_000_000_000)
        await indexer._add_debt(ALICE, USDC, 4000_000000, 2)
        await indexer.recalc_health_factor(ALICE)

        # Bob: 2 WETH, 3500 USDC → HF = 3360/3500 = 0.96 (liquidatable)
        await indexer._add_collateral(BOB, WETH, 2_000_000_000_000_000_000)
        await indexer._add_debt(BOB, USDC, 3500_000000, 2)
        await indexer.recalc_health_factor(BOB)

        # Carol: 10 WETH, 5000 USDC → HF = 16800/5000 = 3.36 (healthy)
        await indexer._add_collateral(CAROL, WETH, 10_000_000_000_000_000_000)
        await indexer._add_debt(CAROL, USDC, 5000_000000, 2)
        await indexer.recalc_health_factor(CAROL)

        assert await reader.get_liquidatable_count() == 2

        candidates = await reader.get_liquidatable_positions(limit=10)
        assert len(candidates) == 2

        # Alice should come first (lower HF = more urgent)
        assert candidates[0].user.lower() == ALICE.lower()
        assert candidates[0].health_factor < 0.9
        assert candidates[0].debt_usd > 0
        assert candidates[0].coll_usd > 0
        assert candidates[0].debt_symbol == "USDC"
        assert candidates[0].coll_symbol == "WETH"
        assert candidates[0].bonus_bps == 10500

        assert candidates[1].user.lower() == BOB.lower()
        assert candidates[1].health_factor > candidates[0].health_factor

    async def test_limit_parameter(self, indexer, reader):
        """Reader respects the limit parameter."""
        for user in [ALICE, BOB, CAROL]:
            # 2 WETH, 5000 USDC → HF = 3360/5000 = 0.672 (all liquidatable)
            await indexer._add_collateral(user, WETH, 2_000_000_000_000_000_000)
            await indexer._add_debt(user, USDC, 5000_000000, 2)
            await indexer.recalc_health_factor(user)

        candidates = await reader.get_liquidatable_positions(limit=1)
        assert len(candidates) == 1

    async def test_is_liquidatable_check(self, indexer, reader):
        """Per-user liquidatable check."""
        await indexer._add_collateral(ALICE, WETH, 2_000_000_000_000_000_000)
        await indexer._add_debt(ALICE, USDC, 4000_000000, 2)
        await indexer.recalc_health_factor(ALICE)

        assert await reader.is_liquidatable(ALICE) is True
        assert await reader.is_liquidatable(BOB) is False

    async def test_get_user_hf(self, indexer, reader):
        """Fetch a specific user's health factor."""
        await indexer._add_collateral(ALICE, WETH, 5_000_000_000_000_000_000)
        await indexer._add_debt(ALICE, USDC, 9000_000000, 2)
        await indexer.recalc_health_factor(ALICE)

        hf = await reader.get_user_health_factor(ALICE)
        assert hf is not None
        assert 0 < hf < 1.0

        assert await reader.get_user_health_factor(BOB) is None

    async def test_candidate_has_positions(self, indexer, reader):
        """Candidate includes full position data."""
        await indexer._add_collateral(ALICE, WETH, 5_000_000_000_000_000_000)
        await indexer._add_collateral(ALICE, USDC, 10000_000000)
        # 5 WETH * $2000 * 0.84 + 10000 USDC * $1 * 0.78 = 8400 + 7800 = 16200 max
        # Need > 16200 debt to be liquidatable
        await indexer._add_debt(ALICE, USDC, 18000_000000, 2)
        await indexer.recalc_health_factor(ALICE)

        candidates = await reader.get_liquidatable_positions(limit=1)
        assert len(candidates) == 1
        c = candidates[0]
        assert len(c.positions) >= 2
        assert WETH.lower() in c.positions
        assert USDC.lower() in c.positions


# ────────────────────────────────────────────────────────────────
# 7. Integration / End-to-End
# ────────────────────────────────────────────────────────────────

class TestIntegration:
    """End-to-end: events → state machine → liquidatable detection."""

    async def test_full_borrower_lifecycle(self, indexer, reader):
        """Alice supplies, borrows, goes underwater, gets liquidated."""
        indexer._reserve_configs[WETH.lower()]["price"] = str(2000_00_000000)

        # 1. Supply 10 WETH
        await indexer.handle_supply(SupplyEvent(
            reserve=WETH, user=ALICE, on_behalf_of=ALICE,
            amount=10_000_000_000_000_000_000,
            referral_code=0, tx_hash=TX_HASH, block=100, log_index=0,
        ))
        # 2. Borrow 8200 USDC (close to max)
        await indexer.handle_borrow(BorrowEvent(
            reserve=USDC, user=ALICE, on_behalf_of=ALICE,
            amount=8200_000000, interest_rate_mode=2,
            borrow_rate=48700000000000000000000000,
            referral_code=0, tx_hash=TX_HASH, block=101, log_index=0,
        ))
        await indexer.recalc_health_factor(ALICE)
        assert await reader.is_liquidatable(ALICE) is False

        # 3. Borrow more — 8000 additional → 16200 total
        await indexer.handle_borrow(BorrowEvent(
            reserve=USDC, user=ALICE, on_behalf_of=ALICE,
            amount=8000_000000, interest_rate_mode=2,
            borrow_rate=48700000000000000000000000,
            referral_code=0, tx_hash=TX_HASH, block=102, log_index=0,
        ))
        await indexer.recalc_health_factor(ALICE)
        # HF = 10 * 2000 * 0.84 / 16200 = 16800 / 16200 ≈ 1.037 (borderline but > 1)
        assert await reader.is_liquidatable(ALICE) is False  # still healthy by Aave rules

        # 4. Drop WETH price to $1500 → HF = 10*1500*0.84 / 16200 = 12600/16200 ≈ 0.78
        indexer._reserve_configs[WETH.lower()]["price"] = str(1500_00_000000)
        await indexer.recalc_health_factor(ALICE)
        assert await reader.is_liquidatable(ALICE) is True
        candidates = await reader.get_liquidatable_positions(limit=1)
        assert len(candidates) == 1
        assert candidates[0].user.lower() == ALICE.lower()

        # 5. Liquidation at 50% close factor
        await indexer.handle_liquidation(LiquidationCallEvent(
            collateral_asset=WETH, debt_asset=USDC,
            user=ALICE,
            debt_to_cover=8100_000000,
            liquidated_collateral_amount=5_670_000_000_000_000_000,
            liquidator=LIQUIDATOR,
            receive_a_token=False,
            tx_hash=TX_HASH, block=103, log_index=0,
        ))
        await indexer.recalc_health_factor(ALICE)

        # Remaining: 4.33 WETH, 8100 USDC → HF = 4.33*1500*0.84/8100 = 5455/8100 ≈ 0.67 (still liquidatable)
        # Actually still < 1 because we removed a big chunk. For simplicity, just check the update worked.
        positions = await indexer._get_user_positions(ALICE)
        assert positions[WETH.lower()]["collateral"] < 10_000_000_000_000_000_000
        assert positions[USDC.lower()]["debt"] < 16200_000000

    async def test_batch_hf_recalc(self, indexer, reader):
        """Full batch recalculation covers all users."""
        # Alice: 2 WETH, 4000 USDC → HF = 0.84 (liquidatable)
        await indexer._add_collateral(ALICE, WETH, 2_000_000_000_000_000_000)
        await indexer._add_debt(ALICE, USDC, 4000_000000, 2)
        await indexer.recalc_health_factor(ALICE)

        # Bob: 2 WETH, 3500 USDC → HF = 0.96 (liquidatable)
        await indexer._add_collateral(BOB, WETH, 2_000_000_000_000_000_000)
        await indexer._add_debt(BOB, USDC, 3500_000000, 2)
        await indexer.recalc_health_factor(BOB)

        # Carol: 10 WETH, 5000 USDC → HF = 3.36 (healthy)
        await indexer._add_collateral(CAROL, WETH, 10_000_000_000_000_000_000)
        await indexer._add_debt(CAROL, USDC, 5000_000000, 2)
        await indexer.recalc_health_factor(CAROL)

        assert await reader.get_liquidatable_count() == 2

        # Run full recalculation
        await indexer.recalc_all_health_factors()
        assert await reader.get_liquidatable_count() == 2


# ────────────────────────────────────────────────────────────────
# Run with:  pytest tests/test_aave_indexer.py -v
#            pytest tests/test_aave_indexer.py -v -k "TestEventDecoding"
#            pytest tests/test_aave_indexer.py -v --tb=short
# ────────────────────────────────────────────────────────────────
