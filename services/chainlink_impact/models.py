"""
SQLAlchemy ORM models for the Chainlink Impact Simulator.

Mirrors the DDL in sql/schema.sql. Uses async SQLAlchemy 2.0 style.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


# ── Reserve Configs ──────────────────────────────────────────────────────

class ReserveConfig(Base):
    __tablename__ = "reserve_configs"

    reserve_addr: Mapped[str] = mapped_column(Text, primary_key=True)
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    decimals: Mapped[int] = mapped_column(Integer, nullable=False)
    ltv_bps: Mapped[int] = mapped_column(Integer, nullable=False)
    liq_threshold_bps: Mapped[int] = mapped_column(Integer, nullable=False)
    liq_bonus_bps: Mapped[int] = mapped_column(Integer, nullable=False)
    reserve_factor_bps: Mapped[int] = mapped_column(Integer, nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    is_frozen: Mapped[bool] = mapped_column(Boolean, default=False)
    aave_price_raw: Mapped[Optional[int]] = mapped_column(BigInteger)
    price_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    # Relationships
    positions: Mapped[list[BorrowPosition]] = relationship(back_populates="reserve")


# ── Borrower Positions ───────────────────────────────────────────────────

class BorrowPosition(Base):
    __tablename__ = "borrow_positions"
    __table_args__ = (
        UniqueConstraint("user_addr", "reserve_addr"),
        Index("idx_borrow_user", "user_addr"),
        Index("idx_borrow_hf", "health_factor"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_addr: Mapped[str] = mapped_column(Text, nullable=False)
    reserve_addr: Mapped[str] = mapped_column(
        Text, ForeignKey("reserve_configs.reserve_addr")
    )
    symbol: Mapped[str] = mapped_column(Text, nullable=False)
    collateral: Mapped[Decimal] = mapped_column(Numeric(36, 18), default=Decimal("0"))
    debt: Mapped[Decimal] = mapped_column(Numeric(36, 18), default=Decimal("0"))
    collateral_usd: Mapped[Decimal] = mapped_column(Numeric(24, 8), default=Decimal("0"))
    debt_usd: Mapped[Decimal] = mapped_column(Numeric(24, 8), default=Decimal("0"))
    is_collateral: Mapped[bool] = mapped_column(Boolean, default=True)
    is_isolated: Mapped[bool] = mapped_column(Boolean, default=False)
    e_mode_category: Mapped[int] = mapped_column(Integer, default=0)
    health_factor: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6))
    snapshot_ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    reserve: Mapped[ReserveConfig] = relationship(back_populates="positions")


# ── Chainlink Feeds ──────────────────────────────────────────────────────

class ChainlinkFeed(Base):
    __tablename__ = "chainlink_feeds"

    symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    feed_addr: Mapped[str] = mapped_column(Text, nullable=False)
    round_id: Mapped[Optional[int]] = mapped_column(BigInteger)
    price_raw: Mapped[Optional[int]] = mapped_column(BigInteger)
    price_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    decimals: Mapped[int] = mapped_column(Integer, default=8)
    updated_at_ts: Mapped[Optional[int]] = mapped_column(BigInteger)
    heartbeat_sec: Mapped[int] = mapped_column(Integer, nullable=False)
    deviation_ppb: Mapped[Optional[int]] = mapped_column(BigInteger)
    age_seconds: Mapped[Optional[int]] = mapped_column(Integer)
    heartbeat_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(6, 2))
    snapshot_ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


# ── Market Prices ────────────────────────────────────────────────────────

class MarketPrice(Base):
    __tablename__ = "market_prices"

    symbol: Mapped[str] = mapped_column(Text, primary_key=True)
    binance_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    coinbase_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    mid_price: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    spread_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4))
    cl_deviation_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4))
    snapshot_ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


# ── Simulation Runs ──────────────────────────────────────────────────────

class SimulationRun(Base):
    __tablename__ = "simulation_runs"
    __table_args__ = (
        Index("idx_sim_runs_status", "status"),
        Index("idx_sim_runs_started", "started_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), unique=True, default=uuid.uuid4
    )
    run_type: Mapped[str] = mapped_column(Text, nullable=False)
    scenario_name: Mapped[Optional[str]] = mapped_column(Text)
    feed_symbols: Mapped[Optional[list[str]]] = mapped_column(ARRAY(Text))
    price_shocks: Mapped[Optional[dict]] = mapped_column(JSONB)
    min_profit_usd: Mapped[Decimal] = mapped_column(Numeric(12, 2), default=Decimal("25"))
    total_borrowers: Mapped[Optional[int]] = mapped_column(Integer)
    newly_liquidatable: Mapped[Optional[int]] = mapped_column(Integer)
    total_opportunities: Mapped[Optional[int]] = mapped_column(Integer)
    estimated_profit: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 2))
    top_opportunity_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 2))
    status: Mapped[str] = mapped_column(Text, default="running")
    elapsed_ms: Mapped[Optional[int]] = mapped_column(Integer)
    error_msg: Mapped[Optional[str]] = mapped_column(Text)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    results: Mapped[list[SimulationResult]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )
    signals: Mapped[list[LiquidationSignal]] = relationship(
        back_populates="run", cascade="all, delete-orphan"
    )


# ── Simulation Results ───────────────────────────────────────────────────

class SimulationResult(Base):
    __tablename__ = "simulation_results"
    __table_args__ = (
        Index("idx_sim_res_liquidatable", "run_id", "is_liquidatable"),
        Index("idx_sim_res_profit", "run_id", "net_profit_usd"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("simulation_runs.run_id", ondelete="CASCADE")
    )
    user_addr: Mapped[str] = mapped_column(Text, nullable=False)
    hf_before: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6))
    total_debt_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    total_coll_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    hf_after: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6))
    hf_delta_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 4))
    is_liquidatable: Mapped[bool] = mapped_column(Boolean, default=False)
    debt_asset: Mapped[Optional[str]] = mapped_column(Text)
    debt_asset_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    coll_asset: Mapped[Optional[str]] = mapped_column(Text)
    coll_asset_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    close_factor: Mapped[Optional[Decimal]] = mapped_column(Numeric(7, 4))
    liq_bonus_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(7, 4))
    gross_profit_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    gas_estimate_gwei: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    net_profit_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    profit_rank: Mapped[Optional[int]] = mapped_column(Integer)
    details_json: Mapped[Optional[dict]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )

    run: Mapped[SimulationRun] = relationship(back_populates="results")


# ── Liquidation Signals ──────────────────────────────────────────────────

class LiquidationSignal(Base):
    __tablename__ = "liquidation_signals"
    __table_args__ = (
        Index("idx_sig_trigger", "trigger_feed"),
        Index("idx_sig_priority", "priority", "net_profit_usd"),
        Index("idx_sig_expires", "expires_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    signal_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), unique=True, default=uuid.uuid4
    )
    user_addr: Mapped[str] = mapped_column(Text, nullable=False)
    run_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("simulation_runs.run_id", ondelete="CASCADE")
    )
    trigger_feed: Mapped[str] = mapped_column(Text, nullable=False)
    trigger_shock_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(8, 4))
    debt_asset: Mapped[str] = mapped_column(Text, nullable=False)
    debt_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    coll_asset: Mapped[str] = mapped_column(Text, nullable=False)
    coll_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    hf_before: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6))
    hf_after: Mapped[Optional[Decimal]] = mapped_column(Numeric(12, 6))
    net_profit_usd: Mapped[Optional[Decimal]] = mapped_column(Numeric(24, 8))
    priority: Mapped[Optional[int]] = mapped_column(Integer)
    confidence: Mapped[Optional[float]] = mapped_column(Numeric(4, 2))
    published_to_redis: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc).replace(hour=(
            datetime.now(timezone.utc).hour + 1
        ) % 24),
    )

    run: Mapped[SimulationRun] = relationship(back_populates="signals")
