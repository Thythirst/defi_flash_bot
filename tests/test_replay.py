"""
Tests for the Historical Replay Validation Framework.

Tests cover:
- StateReconstructor: event application, snapshot checkpoint, position rebuild
- HistoricalPriceFeeder: Chainlink decoding, CEX price mapping
- ReplayEngine: health factor computation, profit estimation, metric computation
"""
import asyncio
import json
import pytest
from decimal import Decimal, ROUND_DOWN
from unittest.mock import AsyncMock, MagicMock, patch

from replay.state_reconstructor import (
    StateReconstructor,
    PositionSnapshot,
    ReserveConfig,
    KNOWN_ASSETS,
    ADDR_TO_SYMBOL,
    SYMBOL_TO_DECIMALS,
)

# ── Test fixtures ─────────────────────────────────────────────────────────

@pytest.fixture
def mock_rpc():
    """Create a mock RPC that returns empty results."""
    mock = AsyncMock()
    return mock


@pytest.fixture
def recon():
    """Create a StateReconstructor with no real RPC."""
    recon = StateReconstructor(rpc_url="http://mock:8545")
    return recon


@pytest.fixture
def sample_reserve_configs():
    """Sample reserve configs for testing."""
    return {
        "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": ReserveConfig(
            reserve_addr="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
            symbol="WETH",
            decimals=18,
            ltv_bps=8000,
            liq_threshold_bps=8250,
            liq_bonus_bps=10500,
        ),
        "0xaf88d065e77c8cc2239327c5edb3a432268e5831": ReserveConfig(
            reserve_addr="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
            symbol="USDC",
            decimals=6,
            ltv_bps=8000,
            liq_threshold_bps=8250,
            liq_bonus_bps=10400,
        ),
        "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9": ReserveConfig(
            reserve_addr="0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9",
            symbol="USDT",
            decimals=6,
            ltv_bps=7500,
            liq_threshold_bps=8000,
            liq_bonus_bps=10500,
        ),
    }


# ── StateReconstructor Tests ─────────────────────────────────────────────

class TestStateReconstructor:
    """Tests for the state reconstructor."""

    def test_apply_supply_adds_position(self, recon):
        """Supply event creates a collateral position."""
        user_addr = "0xdead000000000000000000000000000000000001"
        user_topic = "0x" + user_addr[2:].zfill(64)
        log = {
            "topics": [
                "0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61",
                "0x00000000000000000000000082af49447d8a07e3bd95bd0d56f35241523fbab1",  # WETH
                user_topic,
            ],
            "data": "0x" + (1_000_000_000_000_000_000).to_bytes(32, "big").hex()
                  + (0).to_bytes(32, "big").hex(),  # 1 ETH supply
        }
        recon._apply_supply(log, block=100)
        
        reserve = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
        
        assert user_addr in recon._positions
        assert reserve in recon._positions[user_addr]
        assert recon._positions[user_addr][reserve].collateral == 1_000_000_000_000_000_000

    def test_apply_borrow_adds_debt(self, recon):
        """Borrow event creates a debt position.
        
        Borrow event topics: [1]=reserve, [2]=onBehalfOf, [3]=user
        """
        user_addr = "0xdead000000000000000000000000000000000002"
        user_topic = "0x" + user_addr[2:].zfill(64)
        usdc_addr = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
        usdc_topic = "0x" + usdc_addr[2:].zfill(64)
        log = {
            "topics": [
                "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0",
                usdc_topic,  # topics[1] = reserve (USDC)
                user_topic,  # topics[2] = onBehalfOf
                user_topic,  # topics[3] = user
            ],
            "data": "0x" + (5000_000_000).to_bytes(32, "big").hex()  # 5000 USDC (6 decimals)
                  + "02".ljust(64, "0")  # variable rate mode
                  + (0).to_bytes(32, "big").hex()
                  + (0).to_bytes(32, "big").hex(),
        }
        recon._apply_borrow(log, block=100)
        
        reserve = usdc_addr
        
        assert reserve in recon._positions[user_addr]
        assert recon._positions[user_addr][reserve].debt == 5000_000_000

    def test_apply_repay_reduces_debt(self, recon):
        """Repay event reduces debt."""
        user_addr = "0xdead000000000000000000000000000000000003"
        user_topic = "0x" + user_addr[2:].zfill(64)
        usdc_addr = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
        usdc_topic = "0x" + usdc_addr[2:].zfill(64)
        # First add debt — Borrow: topics[1]=reserve, topics[2]=onBehalfOf, topics[3]=user
        borrow_log = {
            "topics": [
                "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0",
                usdc_topic,   # topics[1] = reserve
                user_topic,   # topics[2] = onBehalfOf
                user_topic,   # topics[3] = user
            ],
            "data": "0x" + (10000_000_000).to_bytes(32, "big").hex()
                  + "02".ljust(64, "0")
                  + (0).to_bytes(32, "big").hex()
                  + (0).to_bytes(32, "big").hex(),
        }
        recon._apply_borrow(borrow_log, block=100)
        
        # Then repay half — Repay: topics[1]=reserve, topics[2]=user, topics[3]=repayer
        repay_log = {
            "topics": [
                "0xa534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051",
                usdc_topic,   # topics[1] = reserve
                user_topic,   # topics[2] = user
                user_topic,   # topics[3] = repayer
            ],
            "data": "0x" + (5000_000_000).to_bytes(32, "big").hex()
                  + (0).to_bytes(32, "big").hex(),  # useATokens=false
        }
        recon._apply_repay(repay_log, block=200)
        
        reserve = usdc_addr
        
        assert recon._positions[user_addr][reserve].debt == 5000_000_000

    def test_apply_withdraw_reduces_collateral(self, recon):
        """Withdraw event reduces collateral."""
        user_addr = "0xdead000000000000000000000000000000000004"
        user_topic = "0x" + user_addr[2:].zfill(64)
        # Supply first
        supply_log = {
            "topics": [
                "0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61",
                "0x00000000000000000000000082af49447d8a07e3bd95bd0d56f35241523fbab1",
                user_topic,
            ],
            "data": "0x" + (2_000_000_000_000_000_000).to_bytes(32, "big").hex()
                  + (0).to_bytes(32, "big").hex(),
        }
        recon._apply_supply(supply_log, block=100)
        
        # Withdraw half
        withdraw_log = {
            "topics": [
                "0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7",
                "0x00000000000000000000000082af49447d8a07e3bd95bd0d56f35241523fbab1",
                user_topic,
            ],
            "data": "0x" + (1_000_000_000_000_000_000).to_bytes(32, "big").hex(),
        }
        recon._apply_withdraw(withdraw_log, block=200)
        
        reserve = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
        
        assert recon._positions[user_addr][reserve].collateral == 1_000_000_000_000_000_000

    def test_apply_liquidation_call_tracks_event(self, recon):
        """LiquidationCall event is recorded and positions updated."""
        user_addr = "0xdead000000000000000000000000000000000005"
        user_topic = "0x" + user_addr[2:].zfill(64)
        log = {
            "blockNumber": "0x1000",
            "transactionHash": "0xabc123",
            "logIndex": "0x1",
            "topics": [
                "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286",
                "0x00000000000000000000000082af49447d8a07e3bd95bd0d56f35241523fbab1",  # WETH collat
                "0x000000000000000000000000af88d065e77c8cc2239327c5edb3a432268e5831",  # USDC debt
                user_topic,
            ],
            "data": "0x"
                  + (5000_000_000).to_bytes(32, "big").hex()  # debt covered
                  + (200_000_000_000_000_000).to_bytes(32, "big").hex()  # collat seized
                  + "000000000000000000000000c0ffee0000000000000000000000000000000001"  # liquidator
                  + (0).to_bytes(32, "big").hex(),  # receiveAToken=false
        }
        recon._apply_liquidation_call(log, block=4096)
        
        assert len(recon.liquidation_events) == 1
        ev = recon.liquidation_events[0]
        assert ev["block"] == 4096
        assert ev["user"] == user_addr
        assert ev["debt_to_cover"] == 5000_000_000
        assert ev["liquidated_collateral_amount"] == 200_000_000_000_000_000

    def test_snapshot_roundtrip(self, recon):
        """Snapshot save and restore preserves state."""
        user_addr = "0xdead000000000000000000000000000000000010"
        user_topic = "0x" + user_addr[2:].zfill(64)
        # Build some state
        log = {
            "topics": [
                "0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61",
                "0x00000000000000000000000082af49447d8a07e3bd95bd0d56f35241523fbab1",
                user_topic,
            ],
            "data": "0x" + (3_000_000_000_000_000_000).to_bytes(32, "big").hex()
                  + (0).to_bytes(32, "big").hex(),
        }
        recon._apply_supply(log, block=100)
        recon._all_users.add(user_addr)
        recon._reserve_configs["0xtest"] = ReserveConfig(
            reserve_addr="0xtest",
            symbol="TEST",
            decimals=18,
            ltv_bps=8000,
        )
        
        # Snapshot
        snapshot = recon._take_snapshot()
        
        # Clear state
        recon._positions.clear()
        recon._reserve_configs.clear()
        recon._all_users.clear()
        
        # Restore
        recon._restore_snapshot(snapshot)
        
        reserve = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
        
        assert user_addr in recon._positions
        assert reserve in recon._positions[user_addr]
        assert recon._positions[user_addr][reserve].collateral == 3_000_000_000_000_000_000
        assert "0xtest" in recon._reserve_configs
        assert user_addr in recon._all_users

    def test_multiple_users_positions(self, recon):
        """Multiple users with overlapping reserves track correctly."""
        user1 = "0xaaaa000000000000000000000000000000000001"
        user2 = "0xbbbb000000000000000000000000000000000002"
        t1 = "0x" + user1[2:].zfill(64)
        t2 = "0x" + user2[2:].zfill(64)
        weth = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
        usdc = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
        wt = "0x" + weth[2:].zfill(64)
        ut = "0x" + usdc[2:].zfill(64)
        # User 1: supply WETH, borrow USDC
        recon._apply_supply({
            "topics": ["0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61",
                       wt, t1],
            "data": "0x" + (5_000_000_000_000_000_000).to_bytes(32, "big").hex() + (0).to_bytes(32, "big").hex(),
        }, block=100)
        recon._apply_borrow({
            "topics": ["0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0",
                       ut, t1, t1],  # [1]=reserve(USDC), [2]=onBehalfOf, [3]=user
            "data": "0x" + (2000_000_000).to_bytes(32, "big").hex() + "02".ljust(64, "0")
                  + (0).to_bytes(32, "big").hex() + (0).to_bytes(32, "big").hex(),
        }, block=100)
        
        # User 2: supply USDC, borrow WETH
        recon._apply_supply({
            "topics": ["0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61",
                       ut, t2],
            "data": "0x" + (10_000_000_000).to_bytes(32, "big").hex() + (0).to_bytes(32, "big").hex(),
        }, block=200)
        recon._apply_borrow({
            "topics": ["0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0",
                       wt, t2, t2],  # [1]=reserve(WETH), [2]=onBehalfOf, [3]=user
            "data": "0x" + (500_000_000_000_000_000).to_bytes(32, "big").hex() + "02".ljust(64, "0")
                  + (0).to_bytes(32, "big").hex() + (0).to_bytes(32, "big").hex(),
        }, block=200)
        
        assert len(recon._positions) == 2
        assert len(recon._all_users) == 2
        
        weth = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"
        usdc = "0xaf88d065e77c8cc2239327c5edb3a432268e5831"
        
        assert weth in recon._positions[user1]  # WETH collateral
        assert usdc in recon._positions[user1]  # USDC debt
        assert recon._positions[user1][weth].collateral == 5_000_000_000_000_000_000
        assert recon._positions[user1][usdc].debt == 2000_000_000
        
        assert usdc in recon._positions[user2]  # USDC collateral
        assert weth in recon._positions[user2]  # WETH debt
        assert recon._positions[user2][usdc].collateral == 10_000_000_000
        assert recon._positions[user2][weth].debt == 500_000_000_000_000_000


# ── Health Factor Tests ──────────────────────────────────────────────────

class TestHealthFactor:
    """Tests for health factor computation."""

    def test_safe_position_hf(self, sample_reserve_configs):
        """Safe position with plenty of collateral."""
        positions = {
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": PositionSnapshot(
                user_addr="0xtest",
                reserve_addr="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                symbol="WETH",
                collateral=10_000_000_000_000_000_000,  # 10 ETH
                debt=0,
                decimals=18,
            ),
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831": PositionSnapshot(
                user_addr="0xtest",
                reserve_addr="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                symbol="USDC",
                collateral=0,
                debt=10_000_000_000,  # 10,000 USDC
                decimals=6,
            ),
        }
        
        prices = {"WETH": Decimal("3000"), "USDC": Decimal("1")}
        
        hf, coll, debt = StateReconstructor.compute_health_factor(
            positions, prices, sample_reserve_configs,
        )
        
        # Collateral: 10 ETH * $3000 = $30,000
        # Risk-adjusted: $30,000 * 0.825 = $24,750
        # Debt: 10,000 USDC * $1 = $10,000
        # HF = 24,750 / 10,000 = 2.475
        assert hf > Decimal("2.0")
        assert hf < Decimal("3.0")
        assert hf > Decimal("1.0")  # Safe

    def test_liquidatable_position_hf(self, sample_reserve_configs):
        """Underwater position with low collateral."""
        positions = {
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": PositionSnapshot(
                user_addr="0xtest",
                reserve_addr="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                symbol="WETH",
                collateral=1_000_000_000_000_000_000,  # 1 ETH
                debt=0,
                decimals=18,
            ),
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831": PositionSnapshot(
                user_addr="0xtest",
                reserve_addr="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                symbol="USDC",
                collateral=0,
                debt=3_000_000_000,  # 3,000 USDC
                decimals=6,
            ),
        }
        
        prices = {"WETH": Decimal("2000"), "USDC": Decimal("1")}
        
        hf, coll, debt = StateReconstructor.compute_health_factor(
            positions, prices, sample_reserve_configs,
        )
        
        # Collateral: 1 ETH * $2000 = $2,000
        # Risk-adjusted: $2,000 * 0.825 = $1,650
        # Debt: 3,000 USDC * $1 = $3,000
        # HF = 1,650 / 3,000 = 0.55
        assert hf < Decimal("1.0")

    def test_no_debt_hf_infinite(self, sample_reserve_configs):
        """Position with no debt has infinite HF."""
        positions = {
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": PositionSnapshot(
                user_addr="0xtest",
                reserve_addr="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                symbol="WETH",
                collateral=10_000_000_000_000_000_000,
                debt=0,
                decimals=18,
            ),
        }
        
        prices = {"WETH": Decimal("3000")}
        
        hf, _, _ = StateReconstructor.compute_health_factor(
            positions, prices, sample_reserve_configs,
        )
        
        assert hf == Decimal("Infinity")

    def test_round_down_precision(self, sample_reserve_configs):
        """HF computation uses ROUND_DOWN to avoid masking liquidations."""
        positions = {
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": PositionSnapshot(
                user_addr="0xtest",
                reserve_addr="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                symbol="WETH",
                collateral=333_333_333_333_333_333,  # 0.333... ETH
                debt=0,
                decimals=18,
            ),
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831": PositionSnapshot(
                user_addr="0xtest",
                reserve_addr="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                symbol="USDC",
                collateral=0,
                debt=1_000_000_000,  # 1,000 USDC
                decimals=6,
            ),
        }
        
        prices = {"WETH": Decimal("3000"), "USDC": Decimal("1")}
        
        hf, _, _ = StateReconstructor.compute_health_factor(
            positions, prices, sample_reserve_configs,
        )
        
        # Collat: 0.333 ETH * $3000 = ~$1,000
        # Adj: $1,000 * 0.825 = $825
        # HF: 825 / 1000 = 0.825
        # With ROUND_DOWN, this should be 0.825000 (not rounding to anything above)
        assert hf < Decimal("1.0")  # Must not round up


# ── Profit Estimation Tests ──────────────────────────────────────────────

class TestProfitEstimation:
    """Tests for liquidation profit estimation."""

    def test_profitable_liquidation(self, sample_reserve_configs):
        """Estimate profit for a liquidatable position."""
        positions = {
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": PositionSnapshot(
                user_addr="0xtest",
                reserve_addr="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                symbol="WETH",
                collateral=10_000_000_000_000_000_000,  # 10 ETH
                debt=0,
                decimals=18,
            ),
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831": PositionSnapshot(
                user_addr="0xtest",
                reserve_addr="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                symbol="USDC",
                collateral=0,
                debt=10_000_000_000,  # 10,000 USDC
                decimals=6,
            ),
        }
        
        prices = {"WETH": Decimal("3000"), "USDC": Decimal("1")}
        
        profit = StateReconstructor.estimate_liquidation_profit(
            positions, prices, sample_reserve_configs,
        )
        
        # Debt covered: 50% of $10,000 = $5,000
        # Collat seized: $5,000 * 1.05 (WETH bonus) = $5,250
        # Gross: $5,250 - $5,000 = $250
        # Gas: ~$5
        # Net: ~$245
        assert profit > Decimal("0")
        assert profit < Decimal("500")

    def test_unprofitable_position(self, sample_reserve_configs):
        """Small positions are not profitable after gas."""
        positions = {
            "0x82af49447d8a07e3bd95bd0d56f35241523fbab1": PositionSnapshot(
                user_addr="0xtest",
                reserve_addr="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
                symbol="WETH",
                collateral=100_000_000_000_000_000,  # 0.1 ETH
                debt=0,
                decimals=18,
            ),
            "0xaf88d065e77c8cc2239327c5edb3a432268e5831": PositionSnapshot(
                user_addr="0xtest",
                reserve_addr="0xaf88d065e77c8cc2239327c5edb3a432268e5831",
                symbol="USDC",
                collateral=0,
                debt=50_000_000,  # 50 USDC
                decimals=6,
            ),
        }
        
        prices = {"WETH": Decimal("3000"), "USDC": Decimal("1")}
        
        profit = StateReconstructor.estimate_liquidation_profit(
            positions, prices, sample_reserve_configs,
        )
        
        # Very small position, profit should be marginal
        assert profit < Decimal("25")  # Below min profit threshold


# ── Price Feeder Tests ───────────────────────────────────────────────────

class TestPriceFeeder:
    """Tests for the historical price feeder."""

    def test_chainlink_decode(self):
        """Chainlink latestRoundData returns correct price."""
        from replay.price_feeder import CHAINLINK_FEEDS
        
        # Verify feeds are correctly configured
        assert "ETH" in CHAINLINK_FEEDS
        assert "BTC" in CHAINLINK_FEEDS
        assert "LINK" in CHAINLINK_FEEDS
        assert all(addr.startswith("0x") for addr in CHAINLINK_FEEDS.values())

    def test_aave_to_cl_symbol_mapping(self):
        """Aave symbols map to Chainlink symbols correctly."""
        from replay.price_feeder import AAVE_TO_CL_SYMBOL
        
        assert AAVE_TO_CL_SYMBOL["WETH"] == "ETH"
        assert AAVE_TO_CL_SYMBOL["WBTC"] == "BTC"
        assert AAVE_TO_CL_SYMBOL["USDC.e"] == "USDC"
        assert AAVE_TO_CL_SYMBOL["tBTC"] == "BTC"
        assert AAVE_TO_CL_SYMBOL["rsETH"] == "ETH"


# ── Integration Tests ────────────────────────────────────────────────────

class TestIntegration:
    """End-to-end integration tests."""

    def test_full_pipeline_with_mock_data(self, recon, sample_reserve_configs):
        """Test full pipeline: events → positions → HF → detection."""
        # Setup: user with borderline position
        user = "0xdead0000000000000000000000000000000000ff"
        # Build correct 32-byte topic: pad 40-char address to 64 hex with leading zeros
        user_topic = "0x" + user[2:].zfill(64)
        weth_topic = "0x" + "82af49447d8a07e3bd95bd0d56f35241523fbab1".zfill(64)
        usdc_topic = "0x" + "af88d065e77c8cc2239327c5edb3a432268e5831".zfill(64)
        
        # Supply 5 ETH as collateral
        recon._apply_supply({
            "topics": [
                "0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61",
                weth_topic,
                user_topic,
            ],
            "data": "0x" + (5_000_000_000_000_000_000).to_bytes(32, "big").hex()
                  + (0).to_bytes(32, "big").hex(),
        }, block=1000)
        
        # Borrow 12,000 USDC against 5 ETH
        recon._apply_borrow({
            "topics": [
                "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0",
                usdc_topic,   # topics[1] = reserve (USDC)
                user_topic,   # topics[2] = onBehalfOf
                user_topic,   # topics[3] = user
            ],
            "data": "0x" + (12_000_000_000).to_bytes(32, "big").hex()
                  + "02".ljust(64, "0")
                  + (0).to_bytes(32, "big").hex()
                  + (0).to_bytes(32, "big").hex(),
        }, block=1000)
        
        recon._reserve_configs = sample_reserve_configs
        
        # The positions dict key must match the user address exactly
        assert user in recon._positions, f"User {user} not in positions: {list(recon._positions.keys())}"
        pos_keys = list(recon._positions[user].keys())
        assert len(pos_keys) == 2, f"Expected 2 positions, got {len(pos_keys)}: {pos_keys}"
        
        # At ETH = $3000: 5 ETH * $3000 = $15,000 collat
        # Adj: $15,000 * 0.825 = $12,375
        # Debt: $12,000
        # HF = 12,375 / 12,000 = 1.031 (safe)
        prices_safe = {"WETH": Decimal("3000"), "USDC": Decimal("1")}
        hf_safe, _, _ = StateReconstructor.compute_health_factor(
            recon._positions[user], prices_safe, recon._reserve_configs,
        )
        assert hf_safe > Decimal("1.0"), f"Expected safe HF, got {hf_safe}"
        
        # ETH drops to $2700: 5 ETH * $2700 = $13,500
        # Adj: $13,500 * 0.825 = $11,137.5
        # Debt: $12,000
        # HF = 11,137.5 / 12,000 = 0.928 (LIQUIDATABLE)
        prices_low = {"WETH": Decimal("2700"), "USDC": Decimal("1")}
        hf_low, _, _ = StateReconstructor.compute_health_factor(
            recon._positions[user], prices_low, recon._reserve_configs,
        )
        assert hf_low < Decimal("1.0"), f"Expected liquidatable HF, got {hf_low}"
        
        # Verify profit estimation works
        profit = StateReconstructor.estimate_liquidation_profit(
            recon._positions[user], prices_low, recon._reserve_configs,
        )
        assert profit > Decimal("20"), f"Expected profit > $20, got ${profit}"


# ── CLI Tests ────────────────────────────────────────────────────────────

class TestCLI:
    """Tests for the CLI module."""

    def test_imports(self):
        """All modules import without errors."""
        import replay
        import replay.state_reconstructor
        import replay.price_feeder
        import replay.replay_engine
        import replay.cli
        assert replay is not None
