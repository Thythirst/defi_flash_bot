"""
services/execution_engine.py — Unified on-chain execution engine.

Accepts opportunities from all detection sources (liquidation, DEX arb, CEX),
queues them by priority, runs pre-flight checks, simulates, broadcasts via
optimal RPC path, and tracks confirmation.

Architecture:
  Scanner → ExecutionRequest → [Risk Check] → [Simulate] → [Broadcast] → [Confirm]
                                    ↑              ↑            ↑
                              risk:state:*    eth_call    MEV Blocker
                                                           / Direct RPC / Flashbots

Redis state:
  engine:queue              ZSET     score=priority, member=request_id
  engine:pending:{id}       HASH     request details + status
  engine:active:{id}        HASH     in-flight (submitted, awaiting confirmation)
  engine:history:{hour}     ZSET     completed executions (last 24h)
  engine:nonce:{address}    STRING   current nonce tracker

Environment:
  BOT_PRIVATE_KEY           — hot wallet key
  FLASH_EXECUTOR_V3         — liquidation contract address
  DEX_ARB_EXECUTOR          — DEX arb contract address
  ARBITRUM_HTTP_URL         — primary RPC
  MEVBLOCKER_URL            — MEV Blocker endpoint (optional)

Usage:
  python -m services.execution_engine
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import aiohttp
import redis.asyncio as redis
from dotenv import load_dotenv
from eth_account import Account
from eth_account.signers.local import LocalAccount
from web3 import Web3
from web3.types import TxParams, TxReceipt

load_dotenv(dotenv_path=project_root / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | executor | %(message)s",
)
logger = logging.getLogger("executor")


# ────────────────────────────────────────────────────────────────
# Types
# ────────────────────────────────────────────────────────────────

class ExecutionType(str, Enum):
    LIQUIDATION = "liquidation"
    DEX_ARB = "dex_arb"
    CEX_ARB = "cex_arb"

class ExecutionStatus(str, Enum):
    QUEUED = "queued"
    CHECKING = "checking"
    SIMULATING = "simulating"
    BROADCASTING = "broadcasting"
    PENDING = "pending"
    CONFIRMED = "confirmed"
    FAILED = "failed"
    REJECTED = "rejected"
    REVERTED = "reverted"


@dataclass
class ExecutionRequest:
    """Standardized execution request from any detection source."""
    request_id: str
    exec_type: ExecutionType
    contract_address: str          # target contract to call
    calldata: str                  # encoded function call
    value_wei: int = 0
    expected_profit_usd: float = 0.0
    expected_profit_wei: int = 0
    priority: float = 0.0          # higher = execute first
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Filled during lifecycle
    status: ExecutionStatus = ExecutionStatus.QUEUED
    tx_hash: Optional[str] = None
    tx_receipt: Optional[dict] = None
    gas_used: int = 0
    gas_price_wei: int = 0
    actual_profit_wei: int = 0
    error: Optional[str] = None
    attempts: int = 0
    created_at: float = field(default_factory=time.time)


# ────────────────────────────────────────────────────────────────
# Execution Engine
# ────────────────────────────────────────────────────────────────

class ExecutionEngine:
    """Unified execution layer for all MEV strategies."""

    def __init__(
        self,
        rpc_url: Optional[str] = None,
        redis_url: str = "redis://localhost:6379",
        mevblocker_url: Optional[str] = None,
        dry_run: bool = False,
    ):
        self.rpc_url = rpc_url or os.getenv("ARBITRUM_HTTP_URL", "")
        self.redis_url = redis_url
        self.mevblocker_url = mevblocker_url or os.getenv("MEVBLOCKER_URL", "")
        self.mevblocker_backup_url = os.getenv("MEVBLOCKER_BACKUP_URL", "")
        self.dry_run = dry_run or os.getenv("DRY_RUN", "0") == "1"

        self.redis: Optional[redis.Redis] = None
        self.w3: Optional[Web3] = None
        self.mevblocker_w3: Optional[Web3] = None
        self.mevblocker_backup_w3: Optional[Web3] = None
        self.account: Optional[LocalAccount] = None
        self._session: Optional[aiohttp.ClientSession] = None

        # Active execution tracking
        self._active_executions: Dict[str, ExecutionRequest] = {}
        self._nonce: Optional[int] = None
        self._nonce_lock = asyncio.Lock()

        # Contract addresses
        self.flash_executor = os.getenv("FLASH_EXECUTOR_V3", "")
        self.dex_arb_executor = os.getenv("DEX_ARB_EXECUTOR", "")

    # ── Setup ───────────────────────────────────────────────────

    async def connect(self):
        # Redis with retry on transient connection failures
        for attempt in range(5):
            try:
                self.redis = redis.from_url(
                    self.redis_url, decode_responses=True,
                    socket_connect_timeout=10, socket_timeout=30,
                )
                await self.redis.ping()
                break
            except Exception as e:
                if attempt < 4:
                    logger.warning("Redis connect attempt %d failed: %s. Retrying...", attempt + 1, e)
                    await asyncio.sleep(2)
                else:
                    raise

        self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))
        if self.mevblocker_url:
            self.mevblocker_w3 = Web3(Web3.HTTPProvider(self.mevblocker_url))
        if self.mevblocker_backup_url:
            self.mevblocker_backup_w3 = Web3(Web3.HTTPProvider(self.mevblocker_backup_url))

        private_key = os.getenv("BOT_PRIVATE_KEY", "")
        if private_key:
            self.account = Account.from_key(private_key)
            await self._sync_nonce()
            logger.info("Account loaded: %s (nonce=%d)", self.account.address[:10], self._nonce)
        else:
            logger.warning("No BOT_PRIVATE_KEY set — signing disabled")

        if self.dry_run:
            logger.warning("DRY_RUN=1 — transactions will NOT be broadcast")
        logger.info("Execution engine connected (RPC=%s, MEV=%s)", self.rpc_url[:40],
                    "enabled" if self.mevblocker_w3 else "disabled")

    async def _sync_nonce(self):
        if self.account:
            self._nonce = self.w3.eth.get_transaction_count(self.account.address)

    async def _next_nonce(self) -> int:
        async with self._nonce_lock:
            nonce = self._nonce
            self._nonce += 1
            return nonce

    # ── Queue management ────────────────────────────────────────

    async def submit(self, request: ExecutionRequest) -> str:
        """Submit an execution request to the queue."""
        await self._store_request(request)
        # Add to priority queue (score = priority)
        await self.redis.zadd("engine:queue", {request.request_id: request.priority})
        logger.info("Queued: %s type=%s profit=$%.2f priority=%.2f",
                    request.request_id[:8], request.exec_type.value,
                    request.expected_profit_usd, request.priority)
        return request.request_id

    async def _store_request(self, req: ExecutionRequest):
        await self.redis.hset(f"engine:pending:{req.request_id}", mapping={
            "type": req.exec_type.value,
            "contract": req.contract_address,
            "calldata": req.calldata[:200],
            "expected_profit_usd": str(req.expected_profit_usd),
            "priority": str(req.priority),
            "status": req.status.value,
            "created_at": str(req.created_at),
            "metadata": json.dumps(req.metadata),
        })

    async def _update_status(self, req: ExecutionRequest):
        await self.redis.hset(f"engine:pending:{req.request_id}", "status", req.status.value)
        if req.tx_hash:
            await self.redis.hset(f"engine:pending:{req.request_id}", "tx_hash", req.tx_hash)

    # Required fields for a valid execution request
    _REQUIRED_FIELDS = ("type", "contract", "calldata", "expected_profit_usd", "priority")

    async def _pop_next(self) -> Optional[ExecutionRequest]:
        """Pop highest-priority request from queue. Skips malformed entries."""
        results = await self.redis.zpopmax("engine:queue", 1)
        if not results:
            return None
        req_id = results[0][0]
        data = await self.redis.hgetall(f"engine:pending:{req_id}")
        if not data:
            return None

        # Schema validation: reject malformed entries before they crash the loop
        missing = [f for f in self._REQUIRED_FIELDS if f not in data]
        if missing:
            logger.error(
                "❌ MALFORMED QUEUE ENTRY: req=%s missing fields=%s — "
                "skipping and cleaning up. Producer must be fixed.",
                req_id[:8], missing,
            )
            await self.redis.delete(f"engine:pending:{req_id}")
            return None

        return ExecutionRequest(
            request_id=req_id,
            exec_type=ExecutionType(data["type"]),
            contract_address=data["contract"],
            calldata=data["calldata"],
            expected_profit_usd=float(data["expected_profit_usd"]),
            priority=float(data["priority"]),
            metadata=json.loads(data.get("metadata", "{}")),
        )

    # ── Pre-flight checks ───────────────────────────────────────

    async def _check_risk(self, req: ExecutionRequest) -> Optional[str]:
        """Check risk engine constraints. Returns block reason or None."""
        # Killswitch
        if await self.redis.get("risk:killswitch") == "1":
            return "Global killswitch active"

        # Circuit breakers
        strategy = req.exec_type.value
        if await self.redis.exists(f"risk:circuit:tripped:{strategy}"):
            reason = await self.redis.get(f"risk:circuit:tripped:{strategy}")
            return f"Circuit breaker tripped: {reason}"

        # Daily loss cap
        limits = await self.redis.hgetall("risk:limits")
        if limits:
            max_trade = float(limits.get("max_trade_eth", "10"))
            value_eth = req.value_wei / 1e18
            if value_eth > max_trade:
                return f"Trade size {value_eth:.2f} ETH exceeds limit {max_trade} ETH"

        return None

    async def _check_gas(self) -> Optional[str]:
        """Check gas price is within acceptable range."""
        try:
            fee_history = self.w3.eth.fee_history(1, "latest")
            base_fee = fee_history.get("baseFeePerGas", [0])[0]
            if base_fee > 100 * 1e9:  # >100 gwei
                return f"Base fee too high: {base_fee/1e9:.0f} gwei"
        except Exception:
            pass
        return None

    # ── Simulation ──────────────────────────────────────────────

    async def _simulate(self, req: ExecutionRequest) -> Optional[str]:
        """Simulate execution via eth_call. Returns error string or None."""
        try:
            tx = {
                "from": self.account.address,
                "to": req.contract_address,
                "data": req.calldata,
                "value": hex(req.value_wei),
            }
            result = self.w3.eth.call(tx)
            # If call succeeds, return None (no error)
            return None
        except Exception as e:
            err_str = str(e)[:200]
            logger.warning("Simulation failed for %s: %s", req.request_id[:8], err_str)
            return err_str

    # ── Gas estimation ──────────────────────────────────────────

    async def _estimate_gas(self, req: ExecutionRequest) -> dict:
        """Estimate gas parameters for the transaction."""
        tx = {
            "from": self.account.address,
            "to": req.contract_address,
            "data": req.calldata,
            "value": hex(req.value_wei),
        }
        gas_limit = 0
        try:
            gas_limit = self.w3.eth.estimate_gas(tx)
        except Exception:
            gas_limit = 2_000_000  # safe default

        # Add 20% buffer
        gas_limit = int(gas_limit * 1.2)

        # Gas price: use EIP-1559
        try:
            fee_history = self.w3.eth.fee_history(1, "latest")
            base_fee = fee_history.get("baseFeePerGas", [0])[0]
            max_priority = self.w3.eth.max_priority_fee or 1_000_000_000  # 1 gwei default
            max_fee = base_fee * 2 + max_priority
        except Exception:
            base_fee = 1_000_000_000  # fallback 1 gwei
            max_priority = 1_000_000_000
            max_fee = 5_000_000_000

        return {
            "gas": gas_limit,
            "maxFeePerGas": max_fee,
            "maxPriorityFeePerGas": max_priority,
        }

    # ── Broadcast ───────────────────────────────────────────────

    async def _broadcast(self, req: ExecutionRequest) -> Optional[str]:
        """Build, sign, and broadcast transaction. Returns tx_hash or None."""
        if not self.account:
            return None

        gas = await self._estimate_gas(req)

        nonce = await self._next_nonce()
        tx: TxParams = {
            "from": self.account.address,
            "to": Web3.to_checksum_address(req.contract_address),
            "data": req.calldata,
            "value": req.value_wei,
            "gas": gas["gas"],
            "maxFeePerGas": gas["maxFeePerGas"],
            "maxPriorityFeePerGas": gas["maxPriorityFeePerGas"],
            "nonce": nonce,
            "chainId": 42161,  # Arbitrum
            "type": 2,         # EIP-1559
        }

        # Sign
        signed = self.account.sign_transaction(tx)

        if self.dry_run:
            logger.info("[DRY RUN] Would broadcast: %s (nonce=%d, gas=%d)",
                        signed.hash.hex()[:16], nonce, gas["gas"])
            req.tx_hash = signed.hash.hex()
            return req.tx_hash

        # Broadcast — try MEV Blocker primary → MEV Blocker backup → direct RPC
        tx_hex = signed.raw_transaction.hex()
        tx_hash = None

        if self.mevblocker_w3:
            try:
                tx_hash = self.mevblocker_w3.eth.send_raw_transaction(signed.raw_transaction)
                logger.info("Broadcast via MEV Blocker: %s", tx_hash.hex()[:16])
            except Exception as e:
                logger.warning("MEV Blocker primary broadcast failed: %s", e)

        if not tx_hash and self.mevblocker_backup_w3:
            try:
                tx_hash = self.mevblocker_backup_w3.eth.send_raw_transaction(signed.raw_transaction)
                logger.info("Broadcast via MEV Blocker backup: %s", tx_hash.hex()[:16])
            except Exception as e:
                logger.warning("MEV Blocker backup broadcast failed: %s", e)

        if not tx_hash:
            try:
                tx_hash = self.w3.eth.send_raw_transaction(signed.raw_transaction)
                logger.info("Broadcast via direct RPC: %s", tx_hash.hex()[:16])
            except Exception as e:
                logger.error("Broadcast FAILED: %s", e)
                return None

        req.tx_hash = tx_hash.hex()
        req.gas_price_wei = gas["maxFeePerGas"]
        return req.tx_hash

    # ── Confirmation tracking ───────────────────────────────────

    async def _wait_for_receipt(self, req: ExecutionRequest, timeout: float = 60.0) -> Optional[dict]:
        """Wait for transaction receipt."""
        if not req.tx_hash:
            return None

        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                receipt = self.w3.eth.get_transaction_receipt(req.tx_hash)
                if receipt:
                    return dict(receipt)
            except Exception:
                pass
            await asyncio.sleep(0.5)

        logger.warning("Receipt timeout for %s", req.tx_hash[:16])
        return None

    # ── Post-execution ──────────────────────────────────────────

    async def _record_result(self, req: ExecutionRequest, receipt: Optional[dict]):
        """Record execution result to Redis and event bus."""
        ts = int(time.time())
        hour = time.strftime("%Y-%m-%dT%H")

        status = "confirmed" if receipt and receipt.get("status") == 1 else "reverted"

        # History ZSET
        await self.redis.zadd(f"engine:history:{hour}", {req.request_id: ts})
        await self.redis.expire(f"engine:history:{hour}", 86400)

        # Clean up pending
        await self.redis.delete(f"engine:pending:{req.request_id}")

        # Event bus
        event_type = "execution.confirmed" if status == "confirmed" else "execution.reverted"
        await self._emit_event("arb:events:execution", {
            "type": event_type,
            "request_id": req.request_id,
            "exec_type": req.exec_type.value,
            "tx_hash": req.tx_hash or "",
            "gas_used": receipt.get("gasUsed", 0) if receipt else 0,
            "status": status,
            "expected_profit_usd": req.expected_profit_usd,
        })

        if status == "confirmed":
            # Notify risk engine of success
            await self.redis.delete(f"risk:circuit:reverts:{req.exec_type.value}")
            logger.info("✅ EXECUTED: %s type=%s tx=%s gas=%d",
                       req.request_id[:8], req.exec_type.value,
                       (req.tx_hash or "")[:16],
                       receipt.get("gasUsed", 0) if receipt else 0)
        else:
            # Record revert for circuit breaker
            await self.redis.incr(f"risk:circuit:reverts:{req.exec_type.value}")
            logger.warning("❌ REVERTED: %s type=%s tx=%s",
                          req.request_id[:8], req.exec_type.value,
                          (req.tx_hash or "")[:16])

    async def _emit_event(self, stream: str, payload: dict):
        try:
            ts = int(time.time() * 1000)
            await self.redis.xadd(stream, {
                "id": f"evt_{ts}",
                "ts": str(ts),
                "source": "execution_engine",
                "type": payload.get("type", "execution.event"),
                "severity": "info",
                "block": "0",
                "payload": json.dumps(payload),
            }, maxlen=100_000, approximate=True)
        except Exception as e:
            logger.debug("Event emit failed: %s", e)

    # ── Main lifecycle ──────────────────────────────────────────

    async def execute(self, req: ExecutionRequest) -> bool:
        """Full execution lifecycle for a single request."""
        req.status = ExecutionStatus.CHECKING
        await self._update_status(req)

        # Pre-flight checks
        block_reason = await self._check_risk(req)
        if block_reason:
            req.status = ExecutionStatus.REJECTED
            req.error = block_reason
            await self._update_status(req)
            logger.info("Rejected %s: %s", req.request_id[:8], block_reason)
            return False

        block_reason = await self._check_gas()
        if block_reason:
            req.status = ExecutionStatus.REJECTED
            req.error = block_reason
            await self._update_status(req)
            logger.info("Rejected %s: %s", req.request_id[:8], block_reason)
            return False

        # Simulate
        req.status = ExecutionStatus.SIMULATING
        await self._update_status(req)
        sim_error = await self._simulate(req)
        if sim_error:
            req.status = ExecutionStatus.REJECTED
            req.error = sim_error
            await self._update_status(req)
            logger.info("Simulation failed %s: %s", req.request_id[:8], sim_error[:100])
            return False

        # Broadcast
        req.status = ExecutionStatus.BROADCASTING
        await self._update_status(req)
        tx_hash = await self._broadcast(req)
        if not tx_hash:
            req.status = ExecutionStatus.FAILED
            req.error = "Broadcast failed"
            await self._update_status(req)
            return False

        # Wait for confirmation
        req.status = ExecutionStatus.PENDING
        await self._update_status(req)
        receipt = await self._wait_for_receipt(req)

        # Record
        await self._record_result(req, receipt)
        return receipt is not None and receipt.get("status") == 1

    # ── Main loop ───────────────────────────────────────────────

    async def run(self):
        """Main execution loop — processes queue continuously."""
        await self.connect()

        if not self.account:
            logger.error("No account configured. Set BOT_PRIVATE_KEY.")
            return

        logger.info("Execution engine running (dry_run=%s)", self.dry_run)

        while True:
            try:
                req = await self._pop_next()
                if not req:
                    await asyncio.sleep(0.1)
                    continue

                req.attempts += 1
                success = await self.execute(req)

                if not success and req.attempts < 3:
                    # Re-queue with slightly lower priority
                    req.priority *= 0.9
                    await self.redis.zadd("engine:queue", {req.request_id: req.priority})

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Execution loop error: %s", e, exc_info=True)
                await asyncio.sleep(1)

    async def stop(self):
        if self.redis:
            await self.redis.aclose()
        logger.info("Execution engine stopped")


# ─── CLI ──────────────────────────────────────────────────────

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Unified MEV Execution Engine")
    parser.add_argument("--rpc", default=None)
    parser.add_argument("--redis", default="redis://localhost:6379")
    parser.add_argument("--mevblocker", default=None)
    args = parser.parse_args()

    engine = ExecutionEngine(
        rpc_url=args.rpc,
        redis_url=args.redis,
        mevblocker_url=args.mevblocker,
    )
    try:
        await engine.run()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await engine.stop()


if __name__ == "__main__":
    asyncio.run(main())
