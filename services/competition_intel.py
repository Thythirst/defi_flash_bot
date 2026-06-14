"""
services/competition_intel.py — Mempool Competition Intelligence Module.

Subscribes to competitor liquidation signals from mempool_intel,
decodes full liquidation parameters, tracks outcomes on-chain,
matches to our detected opportunities, and computes empirical
competition statistics.

Replaces estimated competition assumptions with measured market behavior.

Data flow:
  mempool_intel ──pubsub──> arb:signals:competitor_liquidation
                                   │
  competition_intel.py ◄───────────┘
       ├── Decodes liquidation call parameters
       ├── Stores pending detection in Redis
       ├── Polls confirmed blocks for receipts
       ├── Matches to preliq opportunities by borrower
       └── Computes competition statistics

Redis schema:
  comp:detection:{tx_hash}     HASH    liquidator, borrower, collateral, debt, gas, tip, etc.
  comp:pending                 ZSET    score=detected_at, member=tx_hash
  comp:resolved                ZSET    score=resolved_at, member=tx_hash
  comp:opportunity:{opp_id}    HASH    matched competition data per opportunity
  comp:stats:daily:{date}      HASH    aggregate daily stats
  comp:stats:global            HASH    running totals

Usage:
  python -m services.competition_intel
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import redis.asyncio as redis
from dotenv import load_dotenv
from eth_utils import keccak, to_checksum_address
from web3 import Web3
from web3.exceptions import TransactionNotFound

load_dotenv(dotenv_path=project_root / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | comp_intel | %(message)s",
)
logger = logging.getLogger("comp_intel")

# ────────────────────────────────────────────────────────────────
# Constants
# ────────────────────────────────────────────────────────────────

AVERAGE_BLOCK_TIME = 0.25      # Arbitrum block time in seconds
BLOCK_CONFIRMATION_WINDOW = 20  # blocks to wait before marking as lost/uncontested
DETECTION_TTL = 300             # seconds to keep pending detections
STATS_FLUSH_INTERVAL = 60       # seconds between stats writes
ARBITRUM_CHAIN_ID = 42161

# Aave V3 Pool liquidationCall signature:
#   liquidationCall(address,address,address,uint256,bool)
LIQUIDATION_SELECTOR = "0xab9c4b5d"
LIQUIDATION_TOPIC = "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286"
AAVE_POOL = "0x794a61358d6845594f94dc1db02a252b5b4814ad"

# Gas constants (for tip estimation when receipt unavailable)
DEFAULT_BASE_FEE = 0.1          # gwei estimate
PRIORITY_FEE_ESTIMATE = 0.05    # gwei estimate


# ────────────────────────────────────────────────────────────────
# Data Classes
# ────────────────────────────────────────────────────────────────

@dataclass
class CompetitorDetection:
    """A liquidation transaction detected in the mempool."""
    tx_hash: str
    liquidator: str
    borrower: str
    collateral_asset: str
    debt_asset: str
    debt_to_cover: int
    receive_a_token: bool
    detected_at: float
    # Gas params (from pending tx)
    max_fee_per_gas: int = 0
    max_priority_fee_per_gas: int = 0
    gas_limit: int = 0
    # Outcome (populated after confirmation)
    confirmed: bool = False
    confirmation_block: int = 0
    confirmation_time: float = 0.0
    gas_used: int = 0
    effective_gas_price: int = 0
    status: int = 0           # 1=success, 0=fail
    tip_wei: int = 0
    tip_gwei: float = 0.0
    builder: str = ""
    # Matched opportunity
    matched_opp_id: str = ""


@dataclass
class CompetitionStats:
    """Rolling competition statistics."""
    total_detections: int = 0
    total_resolved: int = 0
    total_matched: int = 0     # matched to our opportunities
    # Competitor counts per opportunity  
    opps_with_0_comp: int = 0
    opps_with_1_comp: int = 0
    opps_with_2_comp: int = 0
    opps_with_3plus_comp: int = 0
    # Tip economics (gwei)
    total_tip_gwei: float = 0.0
    tip_count: int = 0
    min_tip_gwei: float = float("inf")
    max_tip_gwei: float = 0.0
    # Gas used
    total_gas_used: int = 0
    # Builder distribution
    builder_wins: Dict[str, int] = field(default_factory=dict)
    # Liquidator distribution
    liquidator_wins: Dict[str, int] = field(default_factory=dict)

    @property
    def avg_tip_gwei(self) -> float:
        if self.tip_count == 0:
            return 0.0
        return self.total_tip_gwei / self.tip_count

    @property
    def avg_tip_wei(self) -> int:
        return int(self.avg_tip_gwei * 1e9)

    @property
    def p_comp_ge_1(self) -> float:
        total = self.total_matched
        if total == 0:
            return 0.0
        return (self.opps_with_1_comp + self.opps_with_2_comp + self.opps_with_3plus_comp) / total

    @property
    def p_comp_ge_2(self) -> float:
        total = self.total_matched
        if total == 0:
            return 0.0
        return (self.opps_with_2_comp + self.opps_with_3plus_comp) / total

    @property
    def p_comp_ge_3(self) -> float:
        total = self.total_matched
        if total == 0:
            return 0.0
        return self.opps_with_3plus_comp / total


# ────────────────────────────────────────────────────────────────
# Liquidation Call Decoder
# ────────────────────────────────────────────────────────────────

class LiquidationDecoder:
    """Decodes Aave V3 liquidationCall parameters from tx input data."""

    @staticmethod
    def decode(input_data: str) -> Optional[Dict]:
        """
        Decode liquidationCall(address,address,address,uint256,bool).

        Input: 0x + selector(4B) + collateralAsset(32B) + debtAsset(32B)
               + user(32B) + debtToCover(32B) + receiveAToken(32B)
        Returns dict or None if invalid.
        """
        if not input_data or len(input_data) < 10:
            return None
        if not input_data.startswith("0x"):
            input_data = "0x" + input_data

        selector = input_data[:10].lower()
        if selector != LIQUIDATION_SELECTOR:
            return None

        data = input_data[10:]  # strip 0x + selector
        if len(data) < 320:     # 5 × 64 hex chars = 320
            return None

        try:
            # Each param is 64 hex chars (32 bytes)
            collateral_asset = "0x" + data[24:64]     # address: last 20 bytes
            debt_asset = "0x" + data[88:128]           # address: last 20 bytes
            user = "0x" + data[152:192]                # address: last 20 bytes (borrower)
            debt_to_cover = int(data[192:256], 16)     # uint256
            receive_a_token = int(data[318:320], 16) == 1  # bool: last byte

            return {
                "collateral_asset": to_checksum_address(collateral_asset),
                "debt_asset": to_checksum_address(debt_asset),
                "borrower": to_checksum_address(user),
                "debt_to_cover": debt_to_cover,
                "receive_a_token": receive_a_token,
            }
        except Exception:
            return None

    @staticmethod
    def decode_from_tx(tx_data: dict) -> Optional[Dict]:
        """Decode from a full eth_getTransactionByHash response."""
        input_data = tx_data.get("input", "0x")
        result = LiquidationDecoder.decode(input_data)
        if result is None:
            return None

        result["liquidator"] = tx_data.get("from", "")
        result["max_fee_per_gas"] = int(tx_data.get("maxFeePerGas", "0x0"), 16)
        result["max_priority_fee_per_gas"] = int(tx_data.get("maxPriorityFeePerGas", "0x0"), 16)
        result["gas_limit"] = int(tx_data.get("gas", "0x0"), 16)
        result["tx_hash"] = tx_data.get("hash", "")
        return result


# ────────────────────────────────────────────────────────────────
# Competition Intelligence Service
# ────────────────────────────────────────────────────────────────

class CompetitionIntel:
    """Tracks competitor liquidations, resolves outcomes, computes stats."""

    def __init__(
        self,
        rpc_url: Optional[str] = None,
        redis_url: str = "redis://localhost:6379",
    ):
        self.rpc_url = rpc_url or os.getenv(
            "QUICKNODE_HTTP_URL",
            os.getenv("ARBITRUM_HTTP_URL", ""),
        )
        self.redis_url = redis_url
        self.redis: Optional[redis.Redis] = None
        self.w3: Optional[Web3] = None
        self.stats = CompetitionStats()

        # In-memory state
        self._pending: Dict[str, CompetitorDetection] = {}
        self._last_stats_flush = 0.0
        self._last_block_poll = 0.0
        self._current_block = 0

        # Reserve config cache (symbol → address mapping)
        self._reserve_symbols: Dict[str, str] = {}  # address → symbol

    async def connect(self):
        """Connect to Redis and Web3."""
        self.redis = redis.from_url(
            self.redis_url, decode_responses=True,
            socket_timeout=30, socket_connect_timeout=10,
        )
        await self.redis.ping()
        self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))
        self._current_block = self.w3.eth.block_number
        logger.info("Redis connected: %s", self.redis_url)
        logger.info("RPC connected: block %d", self._current_block)

        # Load reserve configs for symbol resolution
        await self._load_reserves()

        # Load persisted stats from Redis
        await self._load_stats()

    async def _load_reserves(self):
        """Load Aave reserve configs for symbol resolution."""
        keys = await self.redis.keys("aave:reserve:*")
        for key in keys:
            addr = key.replace("aave:reserve:", "")
            if ":" in addr:
                continue
            try:
                data = await self.redis.hgetall(key)
                symbol = data.get("symbol", "")
                if symbol:
                    self._reserve_symbols[to_checksum_address(addr)] = symbol
            except Exception:
                pass
        logger.info("Loaded %d reserve symbols", len(self._reserve_symbols))

    async def _load_stats(self):
        """Restore in-memory stats from Redis after restart."""
        try:
            data = await self.redis.hgetall("comp:stats:global")
            if data:
                self.stats.total_detections = int(data.get("total_detections", "0"))
                self.stats.total_resolved = int(data.get("total_resolved", "0"))
                self.stats.total_matched = int(data.get("total_matched", "0"))
                self.stats.tip_count = int(data.get("tip_count", "0"))
                self.stats.total_tip_gwei = float(data.get("total_tip_gwei", "0"))
                self.stats.total_gas_used = int(data.get("total_gas_used", "0"))
                logger.info("Restored stats: %d detections, %d resolved, %d matched",
                           self.stats.total_detections, self.stats.total_resolved,
                           self.stats.total_matched)
        except Exception:
            pass

    # ── PubSub Listener ─────────────────────────────────────────

    async def listen(self):
        """Main loop — subscribe to competitor signals and poll outcomes.

        Uses non-blocking pubsub.get_message() to avoid socket timeout
        on idle channels (no competitor liquidations for long periods).
        """
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("arb:signals:competitor_liquidation")
        logger.info("Subscribed to arb:signals:competitor_liquidation")

        self._last_stats_flush = time.time()
        self._last_block_poll = time.time()

        try:
            while True:
                # Non-blocking poll for pubsub messages
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message["type"] == "message":
                    try:
                        payload = json.loads(message["data"])
                        await self._handle_competitor_signal(payload)
                    except Exception as e:
                        logger.debug("Signal processing error: %s", e)

                now = time.time()

                # Flush stats every 60s
                if now - self._last_stats_flush >= STATS_FLUSH_INTERVAL:
                    await self._flush_stats()
                    self._last_stats_flush = now

                # Poll for block confirmations
                if now - self._last_block_poll >= 2.0:
                    await self._poll_confirmations()
                    self._last_block_poll = now

                # Yield to event loop
                await asyncio.sleep(0.25)

        except asyncio.CancelledError:
            pass
        finally:
            await pubsub.unsubscribe()
            await self.redis.aclose()
            logger.info("Competition Intel stopped")

    async def _handle_competitor_signal(self, payload: dict):
        """Process a competitor liquidation signal from mempool_intel."""
        tx_hash = payload.get("tx_hash", "")
        detail = payload.get("detail", "")

        if not tx_hash:
            return

        # Try to decode if input_data is available
        input_data = payload.get("input_data", "")
        decoded = None
        if input_data:
            decoded = LiquidationDecoder.decode(input_data)

        # If pubsub payload lacks input_data, try reading from mempool Redis cache
        if decoded is None:
            cached = await self.redis.hgetall(f"mempool:tx:{tx_hash}")
            if cached:
                input_data = cached.get("input_data", "")
                decoded = LiquidationDecoder.decode(input_data)

        # If still no decode, try fetching from RPC
        if decoded is None and self.w3:
            decoded = await self._fetch_and_decode_tx(tx_hash)

        if decoded is None:
            logger.debug("Could not decode liquidation params for %s", tx_hash[:16])
            return

        detection = CompetitorDetection(
            tx_hash=tx_hash,
            liquidator=decoded.get("liquidator", payload.get("from", "")),
            borrower=decoded["borrower"],
            collateral_asset=decoded["collateral_asset"],
            debt_asset=decoded["debt_asset"],
            debt_to_cover=decoded["debt_to_cover"],
            receive_a_token=decoded.get("receive_a_token", False),
            max_fee_per_gas=decoded.get("max_fee_per_gas", 0),
            max_priority_fee_per_gas=decoded.get("max_priority_fee_per_gas", 0),
            gas_limit=decoded.get("gas_limit", 0),
            detected_at=time.time(),
        )

        # Dedup
        if tx_hash in self._pending:
            return

        self._pending[tx_hash] = detection
        self.stats.total_detections += 1

        # Store in Redis
        pipe = self.redis.pipeline()
        pipe.hset(f"comp:detection:{tx_hash}", mapping={
            "tx_hash": tx_hash,
            "liquidator": detection.liquidator,
            "borrower": detection.borrower,
            "collateral_asset": detection.collateral_asset,
            "debt_asset": detection.debt_asset,
            "debt_to_cover": str(detection.debt_to_cover),
            "max_fee_per_gas": str(detection.max_fee_per_gas),
            "max_priority_fee_per_gas": str(detection.max_priority_fee_per_gas),
            "gas_limit": str(detection.gas_limit),
            "detected_at": str(detection.detected_at),
            "status": "pending",
        })
        pipe.zadd("comp:pending", {tx_hash: detection.detected_at})
        pipe.expire(f"comp:detection:{tx_hash}", DETECTION_TTL)
        pipe.expire("comp:pending", DETECTION_TTL)
        await pipe.execute()

        coll_sym = self._reserve_symbols.get(detection.collateral_asset, detection.collateral_asset[:10])
        debt_sym = self._reserve_symbols.get(detection.debt_asset, detection.debt_asset[:10])
        logger.info(
            "🏴 Comp liq: %s | liquidator=%s | borrower=%s | "
            "%s→%s | debt=%d | tip=%.1f gwei",
            tx_hash[:16], detection.liquidator[:10],
            detection.borrower[:10],
            coll_sym, debt_sym, detection.debt_to_cover,
            detection.max_priority_fee_per_gas / 1e9,
        )

    async def _fetch_and_decode_tx(self, tx_hash: str) -> Optional[Dict]:
        """Fetch full transaction by hash from RPC and decode."""
        try:
            tx = self.w3.eth.get_transaction(tx_hash)
            return LiquidationDecoder.decode_from_tx(tx)
        except TransactionNotFound:
            return None
        except Exception as e:
            logger.debug("RPC fetch failed for %s: %s", tx_hash[:16], e)
            return None

    # ── Outcome Resolution ──────────────────────────────────────

    async def _poll_confirmations(self):
        """Poll confirmed blocks for pending detection outcomes."""
        self._current_block = self.w3.eth.block_number
        now = time.time()

        to_remove = []
        for tx_hash, detection in list(self._pending.items()):
            age = now - detection.detected_at
            # Give up after TTL
            if age > DETECTION_TTL:
                # Mark as expired in Redis before dropping
                try:
                    await self.redis.hset(f"comp:detection:{tx_hash}", "status", "expired")
                    await self.redis.zrem("comp:pending", tx_hash)
                except Exception:
                    pass
                to_remove.append(tx_hash)
                continue

            try:
                receipt = self.w3.eth.get_transaction_receipt(tx_hash)
                if receipt is None:
                    continue

                await self._resolve_detection(detection, receipt)
                to_remove.append(tx_hash)

            except TransactionNotFound:
                continue
            except Exception:
                continue

        for tx_hash in to_remove:
            self._pending.pop(tx_hash, None)

    async def _resolve_detection(self, detection: CompetitorDetection, receipt: dict):
        """Resolve a confirmed detection from its receipt."""
        detection.confirmed = True
        detection.confirmation_block = receipt.get("blockNumber", 0)
        detection.confirmation_time = time.time()
        detection.gas_used = receipt.get("gasUsed", 0)
        detection.effective_gas_price = receipt.get("effectiveGasPrice", 0)
        detection.status = receipt.get("status", 0)

        # Calculate tip
        base_fee = 0
        try:
            block = self.w3.eth.get_block(detection.confirmation_block)
            base_fee = block.get("baseFeePerGas", 0)
        except Exception:
            base_fee = 0

        if detection.effective_gas_price > 0 and base_fee > 0:
            tip = detection.effective_gas_price - base_fee
        else:
            tip = detection.max_priority_fee_per_gas
        detection.tip_wei = max(0, tip)
        detection.tip_gwei = detection.tip_wei / 1e9

        # Update stats
        now = time.time()
        pipe = self.redis.pipeline()

        update = {
            "status": "confirmed" if detection.status == 1 else "reverted",
            "confirmation_block": str(detection.confirmation_block),
            "gas_used": str(detection.gas_used),
            "effective_gas_price": str(detection.effective_gas_price),
            "tip_gwei": f"{detection.tip_gwei:.2f}",
            "tip_wei": str(detection.tip_wei),
            "resolved_at": str(now),
        }
        pipe.hset(f"comp:detection:{detection.tx_hash}", mapping=update)
        pipe.zadd("comp:resolved", {detection.tx_hash: now})
        pipe.zrem("comp:pending", detection.tx_hash)
        pipe.expire("comp:resolved", 86400 * 7)  # 7 day TTL
        await pipe.execute()

        self.stats.total_resolved += 1

        # Tip stats (only for successful liquidations)
        if detection.status == 1:
            self.stats.total_tip_gwei += detection.tip_gwei
            self.stats.tip_count += 1
            if detection.tip_gwei < self.stats.min_tip_gwei:
                self.stats.min_tip_gwei = detection.tip_gwei
            if detection.tip_gwei > self.stats.max_tip_gwei:
                self.stats.max_tip_gwei = detection.tip_gwei
            self.stats.total_gas_used += detection.gas_used

        winner_status = "✅ WON" if detection.status == 1 else "❌ REVERTED"
        logger.info(
            "%s: %s | liquidator=%s | gas=%d | tip=%.2f gwei | block=%d",
            winner_status, detection.tx_hash[:16],
            detection.liquidator[:10], detection.gas_used,
            detection.tip_gwei, detection.confirmation_block,
        )

        # Try to match to our opportunities (best-effort, fire-and-forget)
        task = asyncio.create_task(self._match_to_opportunity(detection))
        task.add_done_callback(
            lambda t: logger.debug("Match task exception: %s", t.exception())
            if t.exception() else None
        )

    async def _match_to_opportunity(self, detection: CompetitorDetection):
        """Match a resolved competitor liquidation to our detected opportunities."""
        # Search preliq opportunities by borrower within a time window
        # Check shadow payloads and calibration DB
        window_start = detection.detected_at - 60  # 60s before
        window_end = detection.detected_at + 10    # 10s after

        try:
            # Check shadow payloads
            shadow_keys = await self.redis.keys("shadow:payload:*")
            for key in shadow_keys:
                data = await self.redis.hgetall(key)
                if data.get("borrower", "").lower() == detection.borrower.lower():
                    opp_ts = float(data.get("timestamp", "0"))
                    if window_start <= opp_ts <= window_end:
                        detection.matched_opp_id = key
                        await self._record_competition_for_opp(key, detection, data)
                        self.stats.total_matched += 1
                        logger.info(
                            "🎯 Matched: competitor %s → our opportunity %s | borrower=%s",
                            detection.tx_hash[:16], key.split(":")[-1][:16],
                            detection.borrower[:10],
                        )
                        return

            # Check calibration opportunities
            cal_keys = await self.redis.keys("cal:opportunity:*")
            for key in cal_keys:
                data = await self.redis.hgetall(key)
                if data.get("borrower", "").lower() == detection.borrower.lower():
                    opp_ts = float(data.get("timestamp", "0"))
                    if window_start <= opp_ts <= window_end:
                        detection.matched_opp_id = key
                        await self._record_competition_for_opp(key, detection, data)
                        self.stats.total_matched += 1
                        logger.info(
                            "🎯 Matched cal: competitor %s → opportunity %s",
                            detection.tx_hash[:16], key.split(":")[-1][:16],
                        )
                        return
        except Exception as e:
            logger.debug("Opportunity matching error: %s", e)

    async def _record_competition_for_opp(
        self, opp_key: str, detection: CompetitorDetection, opp_data: dict,
    ):
        """Record competition data for a matched opportunity."""
        opp_id = opp_key.split(":")[-1][:32]

        # Increment competitor count for this opportunity
        comp_key = f"comp:opportunity:{opp_id}"

        existing = await self.redis.hget(comp_key, "competitor_count")
        current_count = int(existing or 0) + 1

        entry = {
            f"comp_{current_count}_tx": detection.tx_hash,
            f"comp_{current_count}_liquidator": detection.liquidator,
            f"comp_{current_count}_tip_gwei": f"{detection.tip_gwei:.2f}",
            f"comp_{current_count}_gas": str(detection.gas_used),
            f"comp_{current_count}_won": "1" if detection.status == 1 else "0",
        }

        pipe = self.redis.pipeline()
        pipe.hset(comp_key, mapping={
            **entry,
            "competitor_count": str(current_count),
            "opp_key": opp_key,
            "borrower": detection.borrower,
            "trigger": opp_data.get("trigger", "unknown"),
            "last_detected_at": str(time.time()),
            # Persist our opportunity data for reference
            "our_debt": opp_data.get("debt_to_cover", opp_data.get("debt", "0")),
            "our_profit_usd": opp_data.get("profit_usd", opp_data.get("expected_profit", "0")),
            "our_ev_usd": opp_data.get("ev_usd", opp_data.get("expected_ev", "0")),
            "our_wp": opp_data.get("wp", opp_data.get("predicted_wp", "0")),
        })
        pipe.expire(comp_key, 86400 * 30)  # 30 day TTL
        await pipe.execute()

        # Store comprehensive resolved opportunity record for analytics
        await self._store_resolved_opp(
            opp_id=opp_id, opp_key=opp_key, opp_data=opp_data,
            detection=detection, competitor_count=current_count,
        )

        # Update competitor count histogram
        if current_count == 1:
            self.stats.opps_with_1_comp += 1
        elif current_count == 2:
            self.stats.opps_with_1_comp -= 1
            self.stats.opps_with_2_comp += 1
        elif current_count == 3:
            self.stats.opps_with_2_comp -= 1
            self.stats.opps_with_3plus_comp += 1
        elif current_count > 3:
            self.stats.opps_with_3plus_comp += 1

    # ── Statistics & Reporting ──────────────────────────────────

    async def _store_resolved_opp(
        self, opp_id: str, opp_key: str, opp_data: dict,
        detection: CompetitorDetection, competitor_count: int,
    ):
        """Store a comprehensive resolved opportunity record for analytics.

        Writes all fields needed by competition_analytics.py:
        timestamp, block, borrower, assets, debt USD, trigger, competitor count,
        winning liquidator, builder, tip, gas, bonus, profit, win status.
        """
        try:
            # Get USD prices from Redis
            coll_price = await self._get_asset_price(detection.collateral_asset)
            debt_price = await self._get_asset_price(detection.debt_asset)

            # Debt size in USD (debt_to_cover is in native units)
            # Determine decimals for debt asset (default to 6 for USDC/USDT, 18 for ETH)
            debt_decimals = 6  # default USDC
            debt_sym = self._reserve_symbols.get(detection.debt_asset, "")
            if debt_sym in ("ETH", "WETH", "BTC", "WBTC"):
                debt_decimals = 18
            elif debt_sym in ("DAI", "USDC", "USDT", "USDCe"):
                debt_decimals = 6

            debt_size_usd = (detection.debt_to_cover / (10 ** debt_decimals)) * debt_price

            # Liquidation bonus as fraction
            bonus_pct = await self._get_liquidation_bonus(detection.collateral_asset)
            liquidation_bonus_usd = debt_size_usd * bonus_pct

            # Our estimated profit from opp_data
            our_profit = float(opp_data.get("profit_usd", opp_data.get("our_profit_usd", "0") or "0"))
            our_ev = float(opp_data.get("ev_usd", opp_data.get("our_ev_usd", "0") or "0"))
            our_wp = float(opp_data.get("wp", opp_data.get("our_wp", "0") or "0"))

            # Our would-have-won: competitor won means we lost
            our_would_have_won = detection.status != 1

            # Detection hour
            detection_hour = int(time.strftime("%H", time.gmtime(detection.detected_at)))

            # Builder identification
            builder_name = self._identify_builder(detection.liquidator)

            resolved_key = f"comp:resolved_opp:{opp_id}"
            await self.redis.hset(resolved_key, mapping={
                "opp_id": opp_id,
                "opp_key": opp_key,
                "tx_hash": detection.tx_hash,
                "timestamp": str(detection.detected_at),
                "block_number": str(detection.confirmation_block),
                "borrower": detection.borrower,
                "collateral_asset": detection.collateral_asset,
                "collateral_symbol": self._reserve_symbols.get(detection.collateral_asset, ""),
                "debt_asset": detection.debt_asset,
                "debt_symbol": self._reserve_symbols.get(detection.debt_asset, ""),
                "debt_size_raw": str(detection.debt_to_cover),
                "debt_size_usd": f"{debt_size_usd:.2f}",
                "trigger_type": opp_data.get("trigger", "unknown"),
                "competitor_count": str(competitor_count),
                "winning_liquidator": detection.liquidator,
                "winning_builder": builder_name,
                "builder_tip_gwei": f"{detection.tip_gwei:.2f}",
                "gas_used": str(detection.gas_used),
                "effective_gas_price": str(detection.effective_gas_price),
                "liquidation_bonus_usd": f"{liquidation_bonus_usd:.2f}",
                "estimated_profit_usd": f"{our_profit:.2f}",
                "realized_profit_usd": f"{our_profit:.2f}" if not our_would_have_won else "0.00",
                "our_would_have_won": "1" if our_would_have_won else "0",
                "detection_hour": str(detection_hour),
                "resolved_at": str(time.time()),
            })
            await self.redis.expire(resolved_key, 86400 * 30)  # 30 day TTL
        except Exception as e:
            logger.debug("Failed to store resolved opp: %s", e)

    async def _get_asset_price(self, asset_address: str) -> float:
        """Look up USD price for an asset from Redis chainlink feeds."""
        try:
            sym = self._reserve_symbols.get(asset_address, "")
            if not sym:
                return 0.0
            sym = {"WETH": "ETH", "WBTC": "BTC"}.get(sym, sym)
            price_raw = await self.redis.hget(f"price:chainlink:{sym}", "price")
            if price_raw:
                return int(price_raw) / 1e8
        except Exception:
            pass
        return 0.0

    async def _get_liquidation_bonus(self, asset_address: str) -> float:
        """Return liquidation bonus as decimal (e.g., 0.05 for 5%)."""
        try:
            key = f"aave:reserve:{asset_address.lower()}"
            bonus_raw = await self.redis.hget(key, "liquidationBonus")
            if bonus_raw:
                bonus_bps = int(bonus_raw)
                return (bonus_bps - 10000) / 10000
        except Exception:
            pass
        return 0.05  # default 5%

    @staticmethod
    def _identify_builder(liquidator: str) -> str:
        """Identify known builder addresses, otherwise return liquidator prefix."""
        KNOWN = {
            "0x1f9090aae28b8a3dceadf281b0f12828e676c326": "MEV Blocker",
            "0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5": "beaverbuild",
            "0x4838b106fce9647bdf1e7877bf73ce8b0bad5f97": "rsync-builder",
            "0xa1d76a7ca91f398c0a533b92c0b2f2f5549d22a9": "Titan",
            "0x690b9a9e9aa1c9db991c7721a73d351ec4badb91": "JetBuilder",
        }
        return KNOWN.get(liquidator.lower(), liquidator[:12])

    async def _flush_stats(self):
        """Write aggregated stats to Redis."""
        date = time.strftime("%Y-%m-%d")
        await self.redis.hset(f"comp:stats:daily:{date}", mapping={
            "total_detections": str(self.stats.total_detections),
            "total_resolved": str(self.stats.total_resolved),
            "total_matched": str(self.stats.total_matched),
            "opps_with_0_comp": str(self.stats.opps_with_0_comp),
            "opps_with_1_comp": str(self.stats.opps_with_1_comp),
            "opps_with_2_comp": str(self.stats.opps_with_2_comp),
            "opps_with_3plus_comp": str(self.stats.opps_with_3plus_comp),
            "p_comp_ge_1": f"{self.stats.p_comp_ge_1:.4f}",
            "p_comp_ge_2": f"{self.stats.p_comp_ge_2:.4f}",
            "p_comp_ge_3": f"{self.stats.p_comp_ge_3:.4f}",
            "avg_tip_gwei": f"{self.stats.avg_tip_gwei:.2f}",
            "min_tip_gwei": f"{self.stats.min_tip_gwei:.2f}",
            "max_tip_gwei": f"{self.stats.max_tip_gwei:.2f}",
            "tip_count": str(self.stats.tip_count),
            "total_gas_used": str(self.stats.total_gas_used),
            "avg_gas_used": str(
                self.stats.total_gas_used // max(self.stats.tip_count, 1)
            ),
            "updated_at": str(time.time()),
        })
        await self.redis.expire(f"comp:stats:daily:{date}", 86400 * 30)

        # Also update global stats
        await self.redis.hset("comp:stats:global", mapping={
            "total_detections": str(self.stats.total_detections),
            "total_resolved": str(self.stats.total_resolved),
            "total_matched": str(self.stats.total_matched),
            "p_comp_ge_1": f"{self.stats.p_comp_ge_1:.4f}",
            "p_comp_ge_2": f"{self.stats.p_comp_ge_2:.4f}",
            "p_comp_ge_3": f"{self.stats.p_comp_ge_3:.4f}",
            "avg_tip_gwei": f"{self.stats.avg_tip_gwei:.2f}",
            "tip_count": str(self.stats.tip_count),
            "total_tip_gwei": f"{self.stats.total_tip_gwei:.2f}",
            "total_gas_used": str(self.stats.total_gas_used),
            "updated_at": str(time.time()),
        })

        logger.info(
            "Stats: %d detected | %d resolved | %d matched | "
            "P(≥1)=%.2f P(≥2)=%.2f P(≥3)=%.2f | tip avg=%.2f gwei",
            self.stats.total_detections,
            self.stats.total_resolved,
            self.stats.total_matched,
            self.stats.p_comp_ge_1,
            self.stats.p_comp_ge_2,
            self.stats.p_comp_ge_3,
            self.stats.avg_tip_gwei,
        )

    async def generate_report(self) -> str:
        """Generate a human-readable competition intelligence report."""
        s = self.stats
        lines = [
            "═══ COMPETITION INTELLIGENCE REPORT ═══",
            "",
            "── Activity ──",
            f"  Total detections: {s.total_detections}",
            f"  Total resolved:   {s.total_resolved}",
            f"  Matched to ours:  {s.total_matched}",
            "",
            "── Competition Rates (per opportunity) ──",
            f"  P(≥1 competitor): {s.p_comp_ge_1:.2%}",
            f"  P(≥2 competitors): {s.p_comp_ge_2:.2%}",
            f"  P(≥3 competitors): {s.p_comp_ge_3:.2%}",
            "",
            "── Competitor Distribution ──",
            f"  0 competitors:  {s.opps_with_0_comp}",
            f"  1 competitor:   {s.opps_with_1_comp}",
            f"  2 competitors:  {s.opps_with_2_comp}",
            f"  3+ competitors: {s.opps_with_3plus_comp}",
            "",
            "── Tip Economics ──",
            f"  Average tip:     {s.avg_tip_gwei:.2f} gwei",
            f"  Min tip:         {s.min_tip_gwei:.2f} gwei",
            f"  Max tip:         {s.max_tip_gwei:.2f} gwei",
            f"  Tip samples:     {s.tip_count}",
            f"  Average gas:     {s.total_gas_used // max(s.tip_count, 1):,} units",
            "",
            "── Impact on Model Assumptions ──",
        ]

        # Compare to current model assumptions
        # Current model: base/(1 + n*0.5), cap n=3, floor 30%
        # With real data we can validate or replace this
        if s.tip_count > 0:
            lines.append(f"  Avg winning tip as % of bonus: (need liquidation bonus data)")
            lines.append(f"  Model assumes multiplicative competition penalty")
            lines.append(f"  Real P(≥1)={s.p_comp_ge_1:.2%} vs assumed competition presence")

        return "\n".join(lines)

    async def get_win_probability(
        self, debt_size_usd: float, competitor_count: int, tip_gwei: float,
    ) -> float:
        """
        Empirical win probability estimate based on observed data.

        Currently returns model-based estimate. After sufficient data (>100
        resolved matched opportunities), this can be replaced with empirical
        logistic regression.
        """
        # Fallback to model-based until we have enough data
        base = 0.50
        comp_penalty = base / (1 + min(competitor_count, 3) * 0.5)
        return max(comp_penalty, base * 0.30)


# ────────────────────────────────────────────────────────────────
# CLI Entry Point
# ────────────────────────────────────────────────────────────────

async def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Mempool Competition Intelligence Module"
    )
    parser.add_argument("--redis", default="redis://localhost:6379")
    parser.add_argument("--rpc", default="")
    parser.add_argument("--report", nargs="?", const="summary", default=None,
                       choices=["summary", "full", "daily"],
                       help="Generate report and exit (summary/full/daily)")
    parser.add_argument("--days", type=int, default=30,
                       help="Lookback days for analytics report (default: 30)")
    args = parser.parse_args()

    intel = CompetitionIntel(
        rpc_url=args.rpc or os.getenv("QUICKNODE_HTTP_URL", os.getenv("ARBITRUM_HTTP_URL", "")),
        redis_url=args.redis,
    )
    await intel.connect()

    if args.report == "summary":
        report = await intel.generate_report()
        print(report)
        global_stats = await intel.redis.hgetall("comp:stats:global")
        if global_stats:
            print("\n═══ GLOBAL STATS (Redis) ═══")
            for k, v in sorted(global_stats.items()):
                print(f"  {k}: {v}")
        await intel.redis.aclose()
        return

    if args.report in ("full", "daily"):
        # Use the analytics engine for detailed reports
        from services.competition_analytics import CompetitionAnalyticsEngine
        analytics = CompetitionAnalyticsEngine(redis_url=args.redis)
        await analytics.connect()
        opportunities = await analytics.collect_resolved_opportunities(days=args.days)
        report = await analytics.generate_daily_report(opportunities)
        print(report)
        await analytics.redis.aclose()
        await intel.redis.aclose()
        return

    logger.info("Competition Intelligence Module started")
    await intel.listen()


if __name__ == "__main__":
    asyncio.run(main())
