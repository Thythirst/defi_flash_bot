"""
scanner/config_loader.py — Parse the new pairs.yaml schema.

Maps YAML → typed Strategy dataclasses so the backtest engine stays agnostic
to the config file layout.
"""

from __future__ import annotations

import os
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import yaml

logger = logging.getLogger("config")


@dataclass
class Pool:
    name: str
    dex: str
    pool_type: str  # concentrated_v3 | concentrated_algebra
    address: str
    fee_tier: Optional[int]  # 500, 3000, 10000, or None for dynamic-fee Algebra pools
    tick_spacing: int
    token0: str
    token1: str
    direction_for_swap0: bool

    def checksum_address(self) -> str:
        # Use web3.utils if available; otherwise naive lower
        return self.address.lower()


@dataclass
class Strategy:
    key: str
    enabled: bool
    pair: List[str]
    loan_size_wei: int
    min_profit_threshold_wei: int
    slippage_tolerance_bps: int
    flash_loan_premium_bps: int
    pools: List[Pool]
    execution: str
    max_tick_cross_allowed: int
    math_module: str


@dataclass
class RPCConfig:
    http_url: str
    wss_url: str
    chain_id: int
    chunk_size: int
    max_concurrent_requests: int
    request_delay_ms: float
    max_retries: int
    backoff_base_ms: int
    cache_dir: str

    def resolved_http_url(self) -> str:
        """Expand environment variables like ${ALCHEMY_HTTP_URL}."""
        return os.path.expandvars(self.http_url)


@dataclass
class BacktestConfig:
    from_block: int
    to_block: int
    event_signatures: Dict[str, str] = field(default_factory=dict)
    blocks_per_batch: int = 20000
    log_batch_size: int = 500


@dataclass
class Config:
    rpc: RPCConfig
    tokens: Dict[str, Dict[str, any]]
    strategies: Dict[str, Strategy]
    backtest: BacktestConfig


def load_config(path: str = "pairs.yaml") -> Config:
    """Load and validate the full pairs.yaml into typed Config."""
    raw = yaml.safe_load(Path(path).read_text())

    # ── RPC ──────────────────────────────────────────────────────────
    rpc_raw = raw.get("rpc", {})
    fetcher_raw = rpc_raw.get("fetcher", {})
    rpc = RPCConfig(
        http_url=rpc_raw.get("http_url", "${ALCHEMY_HTTP_URL}"),
        wss_url=rpc_raw.get("wss_url", "${ALCHEMY_WSS_URL}"),
        chain_id=int(rpc_raw.get("chain_id", 42161)),
        chunk_size=int(fetcher_raw.get("chunk_size", 20000)),
        max_concurrent_requests=int(fetcher_raw.get("max_concurrent_requests", 8)),
        request_delay_ms=float(fetcher_raw.get("request_delay_ms", 120)),
        max_retries=int(fetcher_raw.get("max_retries", 3)),
        backoff_base_ms=int(fetcher_raw.get("backoff_base_ms", 250)),
        cache_dir=fetcher_raw.get("cache_dir", "~/.defi_flash_bot/cache"),
    )

    # ── Tokens ───────────────────────────────────────────────────────
    tokens = raw.get("tokens", {})

    # ── Strategies ───────────────────────────────────────────────────
    strategies_raw = raw.get("strategies", {})
    strategies: Dict[str, Strategy] = {}
    for key, sraw in strategies_raw.items():
        if not isinstance(sraw, dict):
            logger.warning("Strategy %s malformed, skipping", key)
            continue
        pools = []
        for prow in sraw.get("pools", []):
            ft = prow.get("fee_tier")
            pools.append(
                Pool(
                    name=prow["name"],
                    dex=prow["dex"],
                    pool_type=prow["type"],
                    address=prow["address"],
                    fee_tier=int(ft) if ft is not None else None,
                    tick_spacing=int(prow["tick_spacing"]),
                    token0=prow["token0"],
                    token1=prow["token1"],
                    direction_for_swap0=prow["direction_for_swap0"],
                )
            )
        strategies[key] = Strategy(
            key=key,
            enabled=bool(sraw.get("enabled", True)),
            pair=sraw.get("pair", []),
            loan_size_wei=int(sraw.get("loan_size_wei", 0)),
            min_profit_threshold_wei=int(sraw.get("min_profit_threshold_wei", 10**15)),
            slippage_tolerance_bps=int(sraw.get("slippage_tolerance_bps", 50)),
            flash_loan_premium_bps=int(sraw.get("flash_loan_premium_bps", 5)),
            pools=pools,
            execution=sraw.get("execution", "sequential_leg"),
            max_tick_cross_allowed=int(sraw.get("max_tick_cross_allowed", 0)),
            math_module=sraw.get("math_module", "scanner.v3_math"),
        )

    # ── Backtest section ─────────────────────────────────────────────
    bt_raw = raw.get("backtest", {})
    backtest = BacktestConfig(
        from_block=int(bt_raw.get("from_block", 0)),
        to_block=int(bt_raw.get("to_block", 0)),
        event_signatures=bt_raw.get("event_signatures", {}),
        blocks_per_batch=int(bt_raw.get("blocks_per_batch", 20000)),
        log_batch_size=int(bt_raw.get("log_batch_size", 500)),
    )

    return Config(rpc=rpc, tokens=tokens, strategies=strategies, backtest=backtest)
