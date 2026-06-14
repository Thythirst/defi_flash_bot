"""
scripts/live_executor.py — Fully autonomous Aave v3 liquidation executor.

Watches Aave borrowers, identifies liquidatable positions, and automatically:
1. Simulates liquidation via eth_call (dry run)
2. Checks profit > $50 + gas costs
3. Signs and broadcasts FlashExecutorV3.executeLiquidation()
4. Sends Telegram alert with tx hash

Environment:
    BOT_PRIVATE_KEY          — Required. Hot wallet with ETH for gas.
    FLASH_EXECUTOR_V3        — Required. Deployed contract address.
    ALCHEMY_HTTP_URL         — Required. Arbitrum RPC endpoint.
    TELEGRAM_BOT_TOKEN       — Optional. For alerts.
    TELEGRAM_CHAT_ID         — Optional. Target chat.
    MIN_PROFIT_USD           — Optional. Override $50 default.

Usage:
    export BOT_PRIVATE_KEY=0x...
    export FLASH_EXECUTOR_V3=0x...
    export ALCHEMY_HTTP_URL=https://arb-mainnet.g.alchemy.com/v2/...
    python3 scripts/live_executor.py
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import aiohttp
from dotenv import load_dotenv
from eth_abi import encode
from eth_utils import keccak, to_hex
from web3 import Web3
from web3.types import TxParams

# ─── Local imports ──────────────────────────────────────────
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scanner.aave_v3 import (
    POOL,
    POOL_DATA_PROVIDER,
    ORACLE,
    UserAccountData,
    UserReserveData,
    format_hf_status,
    LIQUIDATION_GAS_LIMIT,
    fetch_user_reserves,
    pick_liquidation_target,
)
from scanner.liquidation_executor import encode_uni_v3_exact_input_single
from scripts.oracle_guard import OracleStalenessGuard

# ─── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("live_executor")

# ─── Constants ──────────────────────────────────────────────
ARBITRUM_CHAIN_ID = 42161
ALERT_HF_THRESHOLD = 1.05   # Alert when HF below this
EXECUTE_HF_THRESHOLD = 1.0  # Strict: only execute when HF < 1.0

BALANCER_VAULT = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"
AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"

# DEX routers (must be approved on FlashExecutorV3)
UNI_V3_SWAPROUTER = "0xE592427A0AEce92De3Edee1F18E0157C05861564"
SUSHI_V3_ROUTER = "0x8A21F6768c1F8075791D08546dADF6daA0Be16eC"
CAMELOT_V3_ROUTER = "0xf5f4496219F31dDB12b336056fE74D0bB8405239"
PANCAKESWAP_V3_ROUTER = "0x1b81D678ffb9C0263b24A97847620C99d213eB14"
UNI_V3_QUOTER = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"  # Quoter V2 on Arbitrum (Camelot/PancakeSwap use same quoter)
FEE_TIERS = [100, 500, 3000, 10000]  # 0.01%, 0.05%, 0.3%, 1%

# Min debt to consider (in USD) — positions smaller than this won't cover gas
MIN_DEBT_USD = 5000

# Poll interval (seconds) — fallback if sequencer feed disconnects
POLL_INTERVAL = 15  # ~1.25 Arbitrum blocks

# Arbitrum sequencer feed — FREE push-based block notifications (~100ms latency)
SEQUENCER_FEED_URL = "wss://arb1.arbitrum.io/feed"

# Chainstack — paid WebSocket fallback (eth_subscribe newHeads, more reliable)
CHAINSTACK_WS_URL = os.getenv(
    "CHAINSTACK_ARBITRUM_WS_URL",
    "wss://arbitrum-mainnet.core.chainstack.com/b718a2bff0d80347e010705841095295",
)

# MEV Blocker RPC — shields transactions from mempool front-running
# Routes txs through private builder network, splits MEV rebates
MEV_BLOCKER_RPC = "https://rpc.mevblocker.io"

# Multicall3 — batches N contract reads into 1 RPC call
MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"

# RPC batch size for borrower polling
BATCH_SIZE = 20


# ─── Dataclasses ────────────────────────────────────────────

@dataclass
class LiquidationOpportunity:
    borrower: str
    collateral_asset: str
    debt_asset: str
    debt_to_cover: int          # in wei
    health_factor: float
    collateral_usd: float
    debt_usd: float
    liquidation_bonus_bps: int  # fetched from Aave
    estimated_profit_wei: int
    estimated_gas_cost_wei: int


# ─── Balancer Flash Loan Calldata Helper ────────────────────

class FlashLoanCalldataBuilder:
    """Build calldata for FlashExecutorV3.executeLiquidation()"""

    @staticmethod
    def encode_execute_liquidation(
        collateral_asset: str,
        debt_asset: str,
        borrower: str,
        debt_to_cover: int,
        swap_router: str,          # address(0) if no swap
        swap_calldata: str,        # "0x" if no swap
        receive_a_token: bool = False,
    ) -> str:
        """
        Encode the executeLiquidation function call.
        Returns hex string calldata.
        """
        selector = keccak(
            text="executeLiquidation(address,address,address,uint256,bool,address,bytes)"
        )[:4]

        # Encode the parameters
        encoded = encode(
            ["address", "address", "address", "uint256", "bool", "address", "bytes"],
            [
                collateral_asset,
                debt_asset,
                borrower,
                debt_to_cover,
                receive_a_token,
                swap_router,
                bytes.fromhex(swap_calldata[2:]) if swap_calldata != "0x" else b"",
            ],
        )

        return "0x" + selector.hex() + encoded.hex()

    @staticmethod
    def encode_execute_liquidation_direct(
        collateral_asset: str,
        debt_asset: str,
        borrower: str,
        debt_to_cover: int,
        swap_router: str,          # address(0) if no swap
        swap_calldata: str,        # "0x" if no swap
        receive_a_token: bool = False,
    ) -> str:
        """
        Encode executeLiquidationDirect function call (pre-funded, no flash loan).
        Returns hex string calldata.
        """
        selector = keccak(
            text="executeLiquidationDirect(address,address,address,uint256,bool,address,bytes)"
        )[:4]

        encoded = encode(
            ["address", "address", "address", "uint256", "bool", "address", "bytes"],
            [
                collateral_asset,
                debt_asset,
                borrower,
                debt_to_cover,
                receive_a_token,
                swap_router,
                bytes.fromhex(swap_calldata[2:]) if swap_calldata != "0x" else b"",
            ],
        )

        return "0x" + selector.hex() + encoded.hex()


# ─── Live Executor ──────────────────────────────────────────

class AaveLiquidationExecutor:
    """
    Monitors Aave borrowers and auto-executes profitable liquidations
    via FlashExecutorV3 + Balancer flash loans.
    """

    def __init__(
        self,
        rpc_url: str,
        private_key: str,
        executor_address: str,
        min_profit_usd: float = 50.0,
    ):
        self._init_rpc_metrics()
        self.rpc_url = rpc_url
        self.w3 = Web3(Web3.HTTPProvider(rpc_url))  # reads
        self.mevblocker_w3 = Web3(Web3.HTTPProvider(MEV_BLOCKER_RPC))  # tx submission
        self.account = self.w3.eth.account.from_key(private_key)
        self.executor_address = self.w3.to_checksum_address(executor_address)
        self.min_profit_usd = min_profit_usd

        # RPC fallback chain (tried in order on failure)
        # QuickNode (Build, 50 req/s) → Chainstack → Public Arbitrum
        quicknode = os.getenv("QUICKNODE_HTTP_URL", "")
        chainstack = os.getenv("CHAINSTACK_ARBITRUM_HTTP_URL", "")
        self.rpc_urls = []
        if quicknode:
            self.rpc_urls.append(quicknode.strip())
            logger.info("QuickNode RPC: primary endpoint")
        if chainstack:
            self.rpc_urls.append(chainstack.strip())
            logger.info("Chainstack RPC: secondary endpoint")
        # Only add the constructor URL if it's not already in the chain
        # (rpc_url often resolves to ARBITRUM_HTTP_URL = QuickNode, causing duplicates)
        if rpc_url and rpc_url.strip() not in self.rpc_urls:
            self.rpc_urls.append(rpc_url.strip())
        # Public Arbitrum as final fallback
        public_arb = os.getenv("PUBLIC_ARBITRUM_RPC", "https://arb1.arbitrum.io/rpc")
        if public_arb not in self.rpc_urls:
            self.rpc_urls.append(public_arb)
            logger.info("Public Arbitrum RPC: final fallback")

        # Safety flags
        self.dry_run = os.getenv("DRY_RUN", "0") == "1"
        if self.dry_run:
            logger.warning("═" * 60)
            logger.warning("  DRY RUN MODE: Transactions will be SIMULATED only")
            logger.warning("  No real transactions will be broadcast.")
            logger.warning("═" * 60)

        # Stale-position guard + circuit breaker
        self.consecutive_reverts = 0
        self.max_consecutive_reverts = 3

        # Load executor ABI
        self.executor_abi = self._load_executor_abi()
        self.executor = self.w3.eth.contract(
            address=self.executor_address,
            abi=self.executor_abi,
        )

        # Track state
        self.monitored_users: Dict[str, Dict] = {}
        self.last_alerted: Dict[str, float] = {}  # address -> timestamp
        self.known_borrowers: set = set()

        # Gas escalation: track how many liquidatable positions we see per poll
        # 0=idle, 1+=escalating (more competition = higher priority fee)
        self.competition_level = 0

        # Sequencer feed: asyncio.Event signalled on each new block
        self._block_event = asyncio.Event()
        self._sequencer_enabled = (
            os.getenv("USE_SEQUENCER_FEED", "0") == "1"
        )
        self._sequencer_task = None

        # Block dedup: sequencer sends multiple messages per block; scan only once
        self._last_scanned_block = 0

        # Extract block number from sequencer messages (avoids get_latest_block RPC)
        self._latest_sequencer_block = 0

        # Rate limiter: minimum gap between scans (seconds)
        # Prevents flooding Alchemy with Multicall3 calls at block-speed
        self._min_scan_interval = 2.0  # scan every 2s max
        self._last_scan_time = 0.0

        # Telegram
        self.telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")

        # Oracle staleness guard (Strategy #1)
        self.oracle_guard = OracleStalenessGuard(self.w3, rpc_url)

        # Oracle update watcher (Strategy #3 — same-block liquidation)
        from scripts.oracle_monitor import OracleUpdateWatcher
        self.oracle_watcher = OracleUpdateWatcher(self.w3)

        # ETH price cascade alert state (prevents repeat alerts)
        self._eth_alert_fired: Dict[str, bool] = {}

    def _load_executor_abi(self) -> List[Dict]:
        """Load FlashExecutorV3 ABI from Forge output."""
        abi_path = (
            Path(__file__).parent.parent
            / "out"
            / "FlashExecutorV3.sol"
            / "FlashExecutorV3.json"
        )
        with open(abi_path) as f:
            artifact = json.load(f)
        return artifact["abi"]

    # ─── RPC Helpers ────────────────────────────────────────

    # ── RPC Metrics (per-session) ─────────────────────────────
    SERVICE_NAME: str = ""
    rpc_metrics: Dict[str, int] = {
        "total_requests": 0,
        "quicknode_429": 0,
        "chainstack_requests": 0,
        "public_arb_requests": 0,
        "fallback_successes": 0,
        "all_providers_failed": 0,
        "peak_req_s": 0.0,
        "sustained_req_s": 0.0,
    }
    _rpc_window: List[float] = []  # timestamps for rate calculation
    _rpc_redis_metrics_key: str = ""  # Redis key for cross-service aggregation

    def _init_rpc_metrics(self):
        """Detect service name and set up cross-service Redis RPC counters."""
        self.SERVICE_NAME = os.getenv("SERVICE_NAME", 
            os.path.basename(sys.argv[0]).replace(".py", "") if sys.argv else "unknown")
        self._rpc_redis_metrics_key = f"rpc:metrics:{self.SERVICE_NAME}"

    async def _rpc_call(self, method: str, params: list, retries: int = 2) -> dict:
        """Async RPC call with automatic fallback on failure.

        Handles QuickNode's text/plain 429 responses by falling back to
        raw text parsing when Content-Type is not application/json.
        """
        now = time.time()
        self._rpc_window.append(now)
        # Prune window older than 60s
        self._rpc_window = [t for t in self._rpc_window if t > now - 60]
        self.rpc_metrics["total_requests"] += 1
        # Update peak/sustained
        if len(self._rpc_window) > 1:
            span = self._rpc_window[-1] - self._rpc_window[0]
            if span > 0:
                rate = len(self._rpc_window) / min(span, 60.0)
                self.rpc_metrics["sustained_req_s"] = round(rate, 1)
                if rate > self.rpc_metrics["peak_req_s"]:
                    self.rpc_metrics["peak_req_s"] = round(rate, 1)

        last_error = None
        for attempt, url in enumerate(self.rpc_urls * (retries + 1)):
            if attempt > 0:
                logger.warning("RPC retry %d/%d → %s", attempt, retries + 1, url)
            # Track provider usage (counts actual attempts, succeeds or fails)
            if "chainstack" in url.lower():
                self.rpc_metrics["chainstack_requests"] += 1
            elif "arbitrum.io" in url.lower():
                self.rpc_metrics["public_arb_requests"] += 1

            try:
                async with aiohttp.ClientSession() as session:
                    payload = {
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": method,
                        "params": params,
                    }
                    async with session.post(
                        url,
                        json=payload,
                        headers={"Content-Type": "application/json"},
                        timeout=aiohttp.ClientTimeout(total=10),
                    ) as resp:
                        # ── Response Validation ──────────────────────────
                        # Three-layer defense against non-standard RPC errors:
                        # 1. HTTP 429 → always an error, regardless of body
                        # 2. Standard JSON-RPC "error" key
                        # 3. Bare error objects with "code"/"message" but no "result"
                        #    (QuickNode text/plain 429 returns: {"code":-32007,"message":"..."})
                        http_status = resp.status
                        raw_text = await resp.text()
                        try:
                            data = json.loads(raw_text)
                        except json.JSONDecodeError:
                            raise Exception(
                                f"Non-JSON response (HTTP {http_status}): {raw_text[:200]}"
                            )

                        # Layer 1: HTTP 429 is always a rate-limit error
                        if http_status == 429:
                            # Extract error detail from body if possible
                            err_code = data.get("code", 429) if isinstance(data, dict) else 429
                            err_msg = str(data.get("message", "rate limit")) if isinstance(data, dict) else "rate limit"
                            if isinstance(data, dict) and "error" in data and isinstance(data["error"], dict):
                                err_code = data["error"].get("code", err_code)
                                err_msg = str(data["error"].get("message", err_msg))
                            provider = self._provider_label(url)
                            if provider == "QuickNode":
                                self.rpc_metrics["quicknode_429"] += 1
                            raise Exception(
                                f"RATE_LIMIT(HTTP 429): [{provider}] code={err_code} {err_msg[:150]}"
                            )

                        # Must parse to a dict
                        if not isinstance(data, dict):
                            raise Exception(
                                f"Unexpected RPC response type {type(data).__name__}: "
                                f"{raw_text[:200]}"
                            )

                        # Layer 2: Standard JSON-RPC error envelope
                        if "error" in data:
                            err = data["error"]
                            if isinstance(err, dict):
                                err_code = err.get("code", 0)
                                err_msg = str(err.get("message", ""))[:200]
                            else:
                                err_code = -1
                                err_msg = str(err)[:200]

                            if err_code == -32007 or "request limit reached" in err_msg.lower():
                                provider = self._provider_label(url)
                                if provider == "QuickNode":
                                    self.rpc_metrics["quicknode_429"] += 1
                                raise Exception(
                                    f"RATE_LIMIT(json-rpc): [{provider}] code={err_code} {err_msg}"
                                )
                            raise Exception(f"RPC error: code={err_code} {err_msg}")

                        # Layer 3: Bare error object (QuickNode text/plain 429 pattern)
                        # {"code": -32007, "message": "50/second request limit reached..."}
                        # Has "code" and "message" keys but NO "result", NO "jsonrpc"
                        if "result" not in data:
                            bare_code = data.get("code")
                            bare_msg = str(data.get("message", ""))[:200]
                            if bare_code is not None:
                                provider = self._provider_label(url)
                                # Classify: -32007 with "request limit" → rate-limit
                                if bare_code == -32007 or "request limit" in bare_msg.lower():
                                    if provider == "QuickNode":
                                        self.rpc_metrics["quicknode_429"] += 1
                                    raise Exception(
                                        f"RATE_LIMIT(bare-error): [{provider}] code={bare_code} {bare_msg}"
                                    )
                                raise Exception(
                                    f"RPC error (bare): [{provider}] code={bare_code} {bare_msg}"
                                )
                            # No "result", no "code" → malformed response
                            raise Exception(
                                f"RPC response missing 'result': [{self._provider_label(url)}] "
                                f"{raw_text[:200]}"
                            )

                        # Success — track fallback if this was a retry
                        if attempt > 0:
                            self.rpc_metrics["fallback_successes"] += 1
                        return data
            except Exception as e:
                last_error = e
                err_type = type(e).__name__
                err_str = str(e)[:120]
                # Show rate-limit events more concisely
                if "RATE_LIMIT" in err_str:
                    logger.warning(
                        "RPC attempt %d RATE-LIMITED (%s): %s",
                        attempt + 1, self._provider_label(url), err_str,
                    )
                else:
                    logger.warning(
                        "RPC attempt %d failed (%s): %s",
                        attempt + 1, err_type, err_str,
                    )
                if attempt < len(self.rpc_urls) * (retries + 1) - 1:
                    await asyncio.sleep(0.5)
                continue

        self.rpc_metrics["all_providers_failed"] += 1
        raise last_error or Exception("All RPC endpoints exhausted")

    def _provider_label(self, url: str) -> str:
        """Human-readable provider name from URL."""
        url_lower = url.lower()
        if "quiknode" in url_lower or "quicknode" in url_lower:
            return "QuickNode"
        if "chainstack" in url_lower:
            return "Chainstack"
        if "alchemy" in url_lower:
            return "Alchemy"
        if "arbitrum.io" in url_lower:
            return "PublicArb"
        return "RPC"

    async def log_rpc_metrics(self) -> None:
        """Log current RPC metrics snapshot (call every 60s)."""
        m = self.rpc_metrics
        fallback_rate = (
            f"{m['fallback_successes']/max(m['total_requests'],1)*100:.1f}%"
        )
        qn_burst = " 🔥 BURST" if m["quicknode_429"] > 0 else ""
        logger.info(
            "📊 RPC METRICS [%s] | total=%d | peak=%.1f/s | sustained=%.1f/s | "
            "QuickNode_429=%d | Chainstack=%d | PublicArb=%d | "
            "fallback=%s | all_failed=%d%s",
            self.SERVICE_NAME,
            m["total_requests"], m["peak_req_s"], m["sustained_req_s"],
            m["quicknode_429"], m["chainstack_requests"], m["public_arb_requests"],
            fallback_rate, m["all_providers_failed"], qn_burst,
        )
        # Write per-service snapshot to Redis for cross-service visibility
        if hasattr(self, 'redis') and self.redis and self._rpc_redis_metrics_key:
            try:
                await self.redis.hset(
                    self._rpc_redis_metrics_key,
                    mapping={
                        "total_requests": str(m["total_requests"]),
                        "quicknode_429": str(m["quicknode_429"]),
                        "chainstack_requests": str(m["chainstack_requests"]),
                        "public_arb_requests": str(m["public_arb_requests"]),
                        "sustained_req_s": str(m["sustained_req_s"]),
                        "peak_req_s": str(m["peak_req_s"]),
                        "updated_at": str(time.time()),
                    },
                )
            except Exception:
                pass

    def get_rpc_metrics_snapshot(self) -> dict:
        """Return a copy of current metrics for external collection."""
        import copy
        snapshot = copy.deepcopy(self.rpc_metrics)
        snapshot["rpc_chain"] = [self._provider_label(u) for u in self.rpc_urls]
        return snapshot

    async def get_latest_block(self) -> int:
        """Fetch current block number with Redis cache (TTL 2s)."""
        cache_key = "rpc:latest_block"
        # Try Redis cache first (if connected)
        try:
            if hasattr(self, 'redis') and self.redis:
                cached = await self.redis.get(cache_key)
                if cached and time.time() - float(cached.decode().split(":")[1]) < 2.0:
                    return int(cached.decode().split(":")[0])
        except Exception:
            pass
        result = await self._rpc_call("eth_blockNumber", [])
        raw_result = result.get("result", "")
        if not raw_result:
            raise Exception(f"eth_blockNumber returned no result: {result}")
        block = int(raw_result, 16)
        # Cache in Redis (async fire-and-forget)
        try:
            if hasattr(self, 'redis') and self.redis:
                await self.redis.set(cache_key, f"{block}:{time.time()}", ex=5)
        except Exception:
            pass
        return block

    async def fetch_borrow_events(self, from_block: int, to_block: int) -> set:
        """Fetch unique borrower addresses from Aave Borrow events.

        Uses the primary RPC provider (QuickNode Build supports larger ranges).
        Falls back through provider chain on range-too-large errors.
        """
        borrowers = set()
        chunk_size = 2000
        borrow_topic = "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0"

        # Use the RPC chain — try primary, fall through on error
        primary_url = self.rpc_urls[0] if self.rpc_urls else ""

        for chunk_start in range(from_block, to_block + 1, chunk_size):
            chunk_end = min(chunk_start + chunk_size - 1, to_block)
            success = False
            for url in self.rpc_urls:
                try:
                    async with aiohttp.ClientSession() as session:
                        payload = {
                            "jsonrpc": "2.0", "id": 1, "method": "eth_getLogs",
                            "params": [{
                                "address": AAVE_POOL,
                                "topics": [borrow_topic],
                                "fromBlock": to_hex(chunk_start),
                                "toBlock": to_hex(chunk_end),
                            }]
                        }
                        async with session.post(
                            url,
                            json=payload,
                            headers={"Content-Type": "application/json"},
                            timeout=aiohttp.ClientTimeout(total=30),
                        ) as resp:
                            raw_text = await resp.text()
                            data = json.loads(raw_text)
                        if "error" in data:
                            err_msg = data["error"].get("message", "")
                            logger.debug("eth_getLogs chunk %d-%d [%s]: %s",
                                        chunk_start, chunk_end,
                                        self._provider_label(url),
                                        err_msg[:80])
                            continue
                        logs = data.get("result", [])
                        for log in logs:
                            user = "0x" + log["topics"][2][-40:]
                            borrowers.add(user)
                        success = True
                        break  # got it, move to next chunk
                except Exception as e:
                    logger.debug("eth_getLogs failed [%s] for %d-%d: %s",
                                self._provider_label(url), chunk_start, chunk_end, e)
                    continue
            if not success:
                logger.warning("eth_getLogs chunk %d-%d: all providers failed", chunk_start, chunk_end)
            await asyncio.sleep(0.3)  # Rate limit

        return borrowers

    async def fetch_user_account_data(self, user: str) -> Optional[UserAccountData]:
        """Call getUserAccountData on Aave Pool."""
        selector = keccak(text="getUserAccountData(address)")[:4]
        calldata = "0x" + selector.hex() + user[2:].rjust(64, "0")

        result = await self._rpc_call(
            "eth_call",
            [{"to": AAVE_POOL, "data": calldata}, "latest"],
        )

        raw = result.get("result", "0x")
        if len(raw) < 2:
            return None

        try:
            from eth_abi import decode
            decoded = decode(
                ["uint256", "uint256", "uint256", "uint256", "uint256", "uint256"],
                bytes.fromhex(raw[2:]),
            )
            return UserAccountData(
                total_collateral_base=decoded[0],
                total_debt_base=decoded[1],
                available_borrows_base=decoded[2],
                current_ltv=decoded[3],
                current_liquidation_threshold=decoded[4],
                health_factor=decoded[5],
            )
        except Exception as e:
            logger.debug("decode failed for %s: %s", user, e)
            return None

    async def get_liquidation_bonus(self, collateral_asset: str) -> int:
        """Fetch liquidation bonus (in bps) for an asset from Aave PoolDataProvider."""
        selector = keccak(text="getReserveConfigurationData(address)")[:4]
        calldata = "0x" + selector.hex() + collateral_asset[2:].rjust(64, "0")

        result = await self._rpc_call(
            "eth_call",
            [{"to": POOL_DATA_PROVIDER, "data": calldata}, "latest"],
        )

        raw = result.get("result", "0x")
        if len(raw) < 2:
            return 500  # default 5%

        try:
            from eth_abi import decode
            # Returns: ltv, liqThreshold, liqBonus, decimals, reserveFactor, (bool flags...)
            decoded = decode(
                ["uint256", "uint256", "uint256", "uint256", "uint256", "bool", "bool", "bool", "bool", "bool"],
                bytes.fromhex(raw[2:]),
            )
            # liquidation bonus is expressed as basis points above 100%
            # e.g. 10500 means 5% bonus (105% - 100%)
            return int(decoded[2])
        except Exception as e:
            logger.warning("Failed to fetch liquidation bonus for %s: %s", collateral_asset, e)
            return 500  # default 5%

    # ─── Batch Health Factor Fetch (Multicall3) ─────────────

    # Number of parallel Multicall3 chunks (splits borrowers for speed)
    BATCH_CHUNKS = 2

    async def batch_fetch_health_factors(
        self, addresses: List[str]
    ) -> Dict[str, Optional[UserAccountData]]:
        """
        Fetch health factors for ALL borrowers via parallel Multicall3 calls.

        Optimizations:
        - Splits borrowers into BATCH_CHUNKS parallel eth_call requests
        - ~250ms → ~120ms latency reduction
        """
        from eth_abi import encode as abi_encode, decode as abi_decode
        from eth_utils import keccak as _keccak

        if not addresses:
            return {}

        selector = _keccak(text="getUserAccountData(address)")[:4].hex()
        agg_selector = _keccak(text="aggregate3((address,bool,bytes)[])")[:4]

        # Split addresses into chunks for parallel execution
        chunk_size = max(1, len(addresses) // self.BATCH_CHUNKS)
        chunks = [
            addresses[i:i + chunk_size]
            for i in range(0, len(addresses), chunk_size)
        ]

        async def _fetch_chunk(chunk: List[str]) -> List[Tuple[str, Optional[bytes]]]:
            """Fetch one chunk of borrowers via Multicall3, return [(addr, raw_data)]."""
            calls = []
            for addr in chunk:
                user_calldata = selector + addr[2:].lower().rjust(64, "0")
                calls.append((AAVE_POOL, True, bytes.fromhex(user_calldata)))

            encoded_calls = abi_encode(["(address,bool,bytes)[]"], [calls])
            calldata = "0x" + agg_selector.hex() + encoded_calls.hex()

            result = await self._rpc_call(
                "eth_call",
                [{"to": MULTICALL3, "data": calldata}, "latest"]
            )
            raw = result.get("result", "0x")

            if len(raw) < 10:
                return [(addr, None) for addr in chunk]

            try:
                decoded = abi_decode(["(bool,bytes)[]"], bytes.fromhex(raw[2:]))[0]
            except Exception:
                return [(addr, None) for addr in chunk]

            return [
                (addr, ret_data if success and len(ret_data) >= 64 else None)
                for addr, (success, ret_data) in zip(chunk, decoded)
            ]

        # Fire all chunks in parallel
        try:
            chunk_results = await asyncio.gather(*[_fetch_chunk(c) for c in chunks])
        except Exception as e:
            logger.error("Parallel Multicall3 failed: %s", e)
            return {}

        # Merge and decode results
        output: Dict[str, Optional[UserAccountData]] = {}
        for chunk_result in chunk_results:
            for addr, ret_data in chunk_result:
                if ret_data is None:
                    output[addr] = None
                    continue
                try:
                    values = abi_decode(
                        ["uint256", "uint256", "uint256", "uint256", "uint256", "uint256"],
                        ret_data
                    )
                    output[addr] = UserAccountData(
                        total_collateral_base=values[0],
                        total_debt_base=values[1],
                        available_borrows_base=values[2],
                        current_ltv=values[3],
                        current_liquidation_threshold=values[4],
                        health_factor=values[5],
                    )
                except Exception:
                    output[addr] = None

        return output
    # ─── Sequencer Feed Listener ─────────────────────────────

    async def _listen_sequencer(self):
        """
        Connect to Arbitrum sequencer feed, signal _block_event on each new block.
        Falls back to Chainstack WSS eth_subscribe if primary feed fails 3 times.
        Auto-reconnects on disconnect. Runs as background task.
        """
        urls = [SEQUENCER_FEED_URL, CHAINSTACK_WS_URL]
        url_idx = 0
        consecutive_failures = 0

        while True:
            url = urls[url_idx]
            logger.info("Sequencer feed: connecting to %s...", url[:50])
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.ws_connect(url) as ws:
                        consecutive_failures = 0  # reset on successful connect
                        if url == CHAINSTACK_WS_URL:
                            # eth_subscribe protocol
                            sub_msg = json.dumps({
                                "jsonrpc": "2.0", "id": 1,
                                "method": "eth_subscribe",
                                "params": ["newHeads"],
                            })
                            await ws.send_str(sub_msg)
                            logger.info("Sequencer feed: connected (Chainstack eth_subscribe)")
                        else:
                            logger.info("Sequencer feed: connected (Arbitrum public)")

                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                try:
                                    data = json.loads(msg.data)
                                    if url == CHAINSTACK_WS_URL:
                                        # eth_subscription response
                                        block_num = data.get("params", {}).get("result", {}).get("number")
                                        if block_num:
                                            self._latest_sequencer_block = int(block_num, 16)
                                    else:
                                        # Sequencer feed message
                                        for m in data.get("messages", []):
                                            header = m.get("message", {}).get("header", {})
                                            if "number" in header:
                                                self._latest_sequencer_block = header["number"]
                                except Exception:
                                    pass
                                self._block_event.set()
                                self._block_event.clear()
                            elif msg.type == aiohttp.WSMsgType.CLOSED:
                                logger.warning("Sequencer feed: closed by server")
                                break
                            elif msg.type == aiohttp.WSMsgType.ERROR:
                                logger.error("Sequencer feed: error")
                                break
            except Exception as e:
                logger.warning("Sequencer feed disconnected: %s. Reconnecting in 5s...", e)

            consecutive_failures += 1
            # Switch to fallback after 3 consecutive failures
            if consecutive_failures >= 3:
                url_idx = (url_idx + 1) % len(urls)
                consecutive_failures = 0
                logger.warning("Sequencer feed: switching to %s...", urls[url_idx][:50])
            await asyncio.sleep(5)

    async def _rpc_call_race(self, method: str, params: list, timeout: float = 3.0) -> dict:
        """
        Fire the same RPC call to ALL endpoints in parallel.
        Returns the first successful response. Cuts latency by 2-3x.
        """
        async def call_one(url: str):
            async with aiohttp.ClientSession() as session:
                payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
                async with session.post(
                    url, json=payload,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as resp:
                    data = await resp.json()
                    if "error" in data:
                        raise Exception(f"RPC error from {url[-30:]}: {data['error']}")
                    return data

        tasks = {}
        for url in self.rpc_urls:
            task = asyncio.create_task(call_one(url))
            task.add_done_callback(lambda t, u=url: t.exception() if t.cancelled() else None)  # suppress leak
            tasks[task] = url
        pending = set(tasks.keys())
        last_error = None

        while pending:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED, timeout=timeout
            )
            for t in done:
                try:
                    result = t.result()
                    logger.debug("RPC race won by: %s", tasks[t][-30:])
                    # Cancel remaining
                    for p in pending:
                        p.cancel()
                    return result
                except Exception as e:
                    last_error = e
                    logger.debug("RPC race: %s failed (%s)", tasks[t][-30:], str(e)[:80])

        raise Exception("All %d RPC endpoints failed. Last: %s" % (len(self.rpc_urls), last_error))

    async def _wait_for_block(self) -> None:
        """
        Wait for next block notification. Uses sequencer feed if enabled,
        otherwise falls back to POLL_INTERVAL sleep.
        """
        if self._sequencer_enabled and self._sequencer_task:
            # Wait for sequencer message OR timeout (fallback to polling)
            try:
                await asyncio.wait_for(
                    self._block_event.wait(),
                    timeout=POLL_INTERVAL * 2
                )
            except asyncio.TimeoutError:
                logger.debug("Sequencer timeout — falling back to poll interval")
        else:
            await self._wait_for_block()

    # ─── Bootstrap ──────────────────────────────────────────

    async def bootstrap_borrowers(self, lookback_blocks: int = 200_000) -> int:
        """Build initial borrower set from recent events."""
        latest = await self.get_latest_block()
        from_block = max(latest - lookback_blocks, 0)

        logger.info("Bootstrapping borrowers from blocks %d-%d...", from_block, latest)
        borrowers = await self.fetch_borrow_events(from_block, latest)

        for borrower in borrowers:
            if borrower not in self.monitored_users:
                self.monitored_users[borrower] = {
                    "address": borrower,
                    "first_seen": latest,
                    "history": [],
                }

        self.known_borrowers.update(borrowers)
        logger.info("Bootstrapped %d borrowers", len(borrowers))
        return len(borrowers)

    # ─── Opportunity Assessment ─────────────────────────────

    async def assess_liquidation_opportunity(
        self,
        borrower: str,
        data: UserAccountData,
    ) -> Optional[LiquidationOpportunity]:
        """
        Determine if a borrower position is profitable to liquidate.
        Discovers actual collateral/debt reserves and computes real amounts.
        Returns None if not profitable or not liquidatable.
        """
        hf = data.health_factor_float

        # Must be strictly liquidatable
        if hf >= 1.0:
            return None

        # Must have meaningful debt
        debt_usd = data.total_debt_base / 1e8
        if debt_usd < MIN_DEBT_USD:
            return None

        collateral_usd = data.total_collateral_base / 1e8

        # Discover user's actual reserves
        reserves = fetch_user_reserves(self.w3, borrower)
        target = pick_liquidation_target(reserves)
        if target is None:
            logger.debug("No valid collateral/debt pair for %s", borrower)
            return None

        collateral_reserve, debt_reserve, debt_to_cover = target

        logger.info(
            "⬇️ LIQUIDATABLE: %s | HF=%.4f | Debt=$%.2f | Collat=$%.2f | Pair=%s/%s",
            borrower, hf, debt_usd, collateral_usd,
            collateral_reserve.symbol, debt_reserve.symbol
        )

        # Fetch actual liquidation bonus for this collateral
        liq_bonus_bps = await self.get_liquidation_bonus(collateral_reserve.asset)

        # Rough profit estimate in USD terms
        # debt_to_cover is in debt token units; convert to USD roughly
        debt_to_cover_usd = debt_to_cover / (10 ** debt_reserve.decimals)
        # Use a rough price from base currency
        if data.total_debt_base > 0:
            price_scale = debt_usd / (data.total_debt_base / 1e8)
            debt_to_cover_usd = (debt_to_cover / (10 ** debt_reserve.decimals)) * price_scale

        gross_profit_usd = debt_to_cover_usd * (liq_bonus_bps / 10000)
        flash_fee_usd = 0  # Balancer = 0%
        gas_cost_eth = (LIQUIDATION_GAS_LIMIT * int(0.1e9)) / 1e18
        gas_cost_usd = gas_cost_eth * 2000  # Approx ETH at $2000
        swap_fee_usd = gross_profit_usd * 0.003 if collateral_reserve.asset.lower() != debt_reserve.asset.lower() else 0

        net_profit_usd = gross_profit_usd - gas_cost_usd - swap_fee_usd

        if net_profit_usd < self.min_profit_usd:
            logger.info(
                "  Skipping: est_profit=$%.2f (gross=$%.2f, gas=$%.2f, swap=$%.2f) < min=$%.2f",
                net_profit_usd, gross_profit_usd, gas_cost_usd, swap_fee_usd, self.min_profit_usd
            )
            return None

        logger.info(
            "  ✅ PROFITABLE: est_net=$%.2f | gross=$%.2f | gas=$%.2f | swap=$%.2f | debt=%d %s",
            net_profit_usd, gross_profit_usd, gas_cost_usd, swap_fee_usd,
            debt_to_cover, debt_reserve.symbol
        )

        return LiquidationOpportunity(
            borrower=borrower,
            collateral_asset=collateral_reserve.asset,
            debt_asset=debt_reserve.asset,
            debt_to_cover=debt_to_cover,
            health_factor=hf,
            collateral_usd=collateral_usd,
            debt_usd=debt_usd,
            liquidation_bonus_bps=liq_bonus_bps,
            estimated_profit_wei=int(net_profit_usd * (10 ** debt_reserve.decimals)),
            estimated_gas_cost_wei=int(gas_cost_eth * 1e18),
        )

    # ─── Simulation ─────────────────────────────────────────

    async def get_asset_price(self, asset: str) -> int:
        """Fetch asset price from Aave Oracle (USD, 8 decimals)."""
        selector = keccak(text="getAssetPrice(address)")[:4]
        calldata = "0x" + selector.hex() + asset[2:].rjust(64, "0")
        result = await self._rpc_call(
            "eth_call",
            [{"to": ORACLE, "data": calldata}, "latest"],
        )
        raw = result.get("result", "0x")
        if len(raw) < 2:
            return 0
        try:
            from eth_abi import decode
            decoded = decode(["uint256"], bytes.fromhex(raw[2:]))
            return int(decoded[0])
        except Exception as e:
            logger.warning("Failed to fetch price for %s: %s", asset, e)
            return 0

    def estimate_collateral_amount(
        self,
        debt_to_cover: int,
        debt_decimals: int,
        collateral_decimals: int,
        debt_price: int,
        collateral_price: int,
        liq_bonus_bps: int,
    ) -> int:
        """
        Estimate collateral received from liquidation.
        Prices are Aave oracle prices (8 decimals).
        """
        if collateral_price == 0 or debt_price == 0:
            return 0
        # debt_value_usd = debt_to_cover / 10^debt_decimals * debt_price / 10^8
        # collateral_amount = debt_value_usd * (1 + bonus) / collateral_price * 10^collateral_decimals
        # = debt_to_cover * debt_price * (10000 + bonus) * 10^collateral_decimals / (10^debt_decimals * 10^8 * 10000)
        numerator = debt_to_cover * debt_price * (10000 + liq_bonus_bps) * (10 ** collateral_decimals)
        denominator = (10 ** debt_decimals) * (10 ** 8) * 10000
        return numerator // denominator

    # ─── Multi-DEX Swap Quoting ───────────────────────────────

    async def _quote_uni_v3_swap(
        self, token_in: str, token_out: str, amount_in: int, fee: int,
        router: str = None
    ) -> Optional[int]:
        """
        Quote a swap on Uni V3 / Sushi V3 via Quoter V2 eth_call.
        Returns amount_out in wei, or None if pool doesn't exist.
        """
        if router is None:
            router = UNI_V3_QUOTER
        if amount_in == 0:
            return None
        try:
            from eth_abi import encode as abi_encode
            from eth_utils import keccak as _keccak
            selector = _keccak(
                text="quoteExactInputSingle((address,address,uint256,uint24,uint160))"
            )[:4]
            encoded_params = abi_encode(
                ["address", "address", "uint256", "uint24", "uint160"],
                [token_in, token_out, amount_in, fee, 0]
            )
            calldata = "0x" + selector.hex() + encoded_params.hex()
            result = await self._rpc_call(
                "eth_call",
                [{"to": router, "data": calldata}, "latest"]
            )
            raw = result.get("result", "0x")
            if len(raw) < 10:
                return None
            from eth_abi import decode as abi_decode
            decoded = abi_decode(
                ["uint256", "uint160", "uint32", "uint256"],
                bytes.fromhex(raw[2:])
            )
            return decoded[0]  # amountOut
        except Exception:
            return None

    async def _best_swap_route(
        self, token_in: str, token_out: str, amount_in: int
    ) -> tuple:
        """
        Find best swap route across Uni V3 + Sushi V3, all fee tiers.
        Returns (router_address, best_fee, best_amount_out).
        Falls back to Uni V3 fee=500 if all quoting fails.
        """
        candidates = [
            (UNI_V3_SWAPROUTER, "UniV3"),
            (SUSHI_V3_ROUTER, "SushiV3"),
            (CAMELOT_V3_ROUTER, "CamelotV3"),
            (PANCAKESWAP_V3_ROUTER, "PancakeV3"),
        ]
        best_fee = 500
        best_out = 0
        best_router = UNI_V3_SWAPROUTER

        for router, name in candidates:
            for fee in FEE_TIERS:
                amount_out = await self._quote_uni_v3_swap(
                    token_in, token_out, amount_in, fee
                )
                if amount_out and amount_out > best_out:
                    best_out = amount_out
                    best_fee = fee
                    best_router = router
                    logger.debug(
                        "  Best swap so far: %s fee=%d → %d wei out",
                        name, fee, best_out
                    )

        if best_out == 0:
            logger.warning(
                "No swap quote available for %s→%s, using UniV3 fee=500 fallback",
                token_in[:10], token_out[:10]
            )
            return UNI_V3_SWAPROUTER, 500, 0

        logger.info(
            "Best swap: %s fee=%d (%d bps) → %d wei out",
            {UNI_V3_SWAPROUTER: "UniV3", SUSHI_V3_ROUTER: "SushiV3", CAMELOT_V3_ROUTER: "CamelotV3", PANCAKESWAP_V3_ROUTER: "PancakeV3"}[best_router],
            best_fee, best_fee // 50, best_out
        )
        return best_router, best_fee, best_out

    async def simulate_liquidation(
        self,
        opp: LiquidationOpportunity,
    ) -> Tuple[bool, str]:
        """
        Simulate the liquidation transaction via eth_call.
        Returns (success: bool, revert_reason: str).
        """
        logger.info("Simulating liquidation for %s...", opp.borrower)

        # Build swap calldata if collateral != debt
        swap_calldata = "0x"
        swap_router = "0x0000000000000000000000000000000000000000"

        if opp.collateral_asset.lower() != opp.debt_asset.lower():
            # Estimate collateral amount for swap
            collateral_price = await self.get_asset_price(opp.collateral_asset)
            debt_price = await self.get_asset_price(opp.debt_asset)
            # Need decimals - fetch from known list or assume 18
            debt_decimals = 18
            collateral_decimals = 18
            # Try to get from known assets
            from scanner.aave_v3 import KNOWN_ASSETS
            for addr, sym, dec in KNOWN_ASSETS:
                if addr.lower() == opp.debt_asset.lower():
                    debt_decimals = dec
                if addr.lower() == opp.collateral_asset.lower():
                    collateral_decimals = dec

            est_collateral = self.estimate_collateral_amount(
                opp.debt_to_cover, debt_decimals, collateral_decimals,
                debt_price, collateral_price, opp.liquidation_bonus_bps
            )
            # Use 95% of estimate to avoid overestimation revert
            amount_in = int(est_collateral * 0.95)
            if amount_in == 0:
                amount_in = 1  # minimum non-zero

            # Slippage protection: 2% max deviation from oracle-implied rate
            expected_out = (
                amount_in * collateral_price * (10 ** debt_decimals)
            ) // (debt_price * (10 ** collateral_decimals))
            amount_out_minimum = max(int(expected_out * 0.98), 1)

            # Find best swap route across all fee tiers + DEXs
            best_router, best_fee, best_quoted_out = await self._best_swap_route(
                opp.collateral_asset, opp.debt_asset, amount_in
            )
            # If quoted output is lower than minimum, tighten minimum to 95% of quote
            if best_quoted_out > 0:
                amount_out_minimum = max(amount_out_minimum, int(best_quoted_out * 0.95))

            deadline = int(time.time()) + 60
            swap_calldata = encode_uni_v3_exact_input_single(
                token_in=opp.collateral_asset,
                token_out=opp.debt_asset,
                fee=best_fee,
                recipient=self.executor_address,  # contract receives output
                deadline=deadline,
                amount_in=amount_in,
                amount_out_minimum=amount_out_minimum,
            )
            swap_router = best_router

        # Build executeLiquidation calldata
        tx_calldata = FlashLoanCalldataBuilder.encode_execute_liquidation(
            collateral_asset=opp.collateral_asset,
            debt_asset=opp.debt_asset,
            borrower=opp.borrower,
            debt_to_cover=opp.debt_to_cover,
            swap_router=swap_router,
            swap_calldata=swap_calldata,
            receive_a_token=False,
        )

        # Simulate
        try:
            result = await self._rpc_call(
                "eth_call",
                [
                    {
                        "from": self.account.address,
                        "to": self.executor_address,
                        "data": tx_calldata,
                    },
                    "latest",
                ],
            )

            if "error" in result:
                error_msg = result["error"].get("message", "unknown")
                error_data = result["error"].get("data", "")
                logger.warning("Simulation FAILED: %s | data=%s", error_msg, error_data)
                return False, error_msg

            logger.info("Simulation PASSED ✓")
            return True, ""

        except Exception as e:
            logger.error("Simulation exception: %s", e)
            return False, str(e)

    # ─── Transaction Broadcasting ───────────────────────────

    async def broadcast_liquidation(self, opp: LiquidationOpportunity) -> Optional[str]:
        """
        Sign and broadcast the liquidation transaction.
        Returns tx_hash hex string or None on failure.
        """
        logger.info("Broadcasting liquidation for %s...", opp.borrower)

        # Build swap calldata
        swap_calldata = "0x"
        swap_router = "0x0000000000000000000000000000000000000000"

        if opp.collateral_asset.lower() != opp.debt_asset.lower():
            # Estimate collateral amount for swap (same logic as simulation)
            collateral_price = await self.get_asset_price(opp.collateral_asset)
            debt_price = await self.get_asset_price(opp.debt_asset)
            debt_decimals = 18
            collateral_decimals = 18
            from scanner.aave_v3 import KNOWN_ASSETS
            for addr, sym, dec in KNOWN_ASSETS:
                if addr.lower() == opp.debt_asset.lower():
                    debt_decimals = dec
                if addr.lower() == opp.collateral_asset.lower():
                    collateral_decimals = dec

            est_collateral = self.estimate_collateral_amount(
                opp.debt_to_cover, debt_decimals, collateral_decimals,
                debt_price, collateral_price, opp.liquidation_bonus_bps
            )
            amount_in = int(est_collateral * 0.95)
            if amount_in == 0:
                amount_in = 1

            # Slippage protection: 2% max deviation from oracle-implied rate
            expected_out = (
                amount_in * collateral_price * (10 ** debt_decimals)
            ) // (debt_price * (10 ** collateral_decimals))
            amount_out_minimum = max(int(expected_out * 0.98), 1)

            # Find best swap route across all fee tiers + DEXs
            best_router, best_fee, best_quoted_out = await self._best_swap_route(
                opp.collateral_asset, opp.debt_asset, amount_in
            )
            # If quoted output is lower than minimum, tighten minimum to 95% of quote
            if best_quoted_out > 0:
                amount_out_minimum = max(amount_out_minimum, int(best_quoted_out * 0.95))

            deadline = int(time.time()) + 60
            swap_calldata = encode_uni_v3_exact_input_single(
                token_in=opp.collateral_asset,
                token_out=opp.debt_asset,
                fee=best_fee,
                recipient=self.executor_address,
                deadline=deadline,
                amount_in=amount_in,
                amount_out_minimum=amount_out_minimum,
            )
            swap_router = best_router

        # Build tx
        tx_calldata = FlashLoanCalldataBuilder.encode_execute_liquidation(
            collateral_asset=opp.collateral_asset,
            debt_asset=opp.debt_asset,
            borrower=opp.borrower,
            debt_to_cover=opp.debt_to_cover,
            swap_router=swap_router,
            swap_calldata=swap_calldata,
            receive_a_token=False,
        )

        # Get nonce and gas
        nonce = self.w3.eth.get_transaction_count(self.account.address)
        # Estimate gas (with buffer)
        try:
            gas_estimate = self.w3.eth.estimate_gas({
                "from": self.account.address,
                "to": self.executor_address,
                "data": tx_calldata,
            })
            gas_limit = int(gas_estimate * 1.5)
        except Exception as e:
            logger.warning("Gas estimation failed: %s, using default", e)
            gas_limit = LIQUIDATION_GAS_LIMIT

        # Build EIP-1559 tx
        block = self.w3.eth.get_block("latest")
        base_fee = block["baseFeePerGas"]

        # Dynamic gas: escalate when multiple liquidations seen (high competition)
        if self.competition_level == 0:
            priority_eth = "0.05"   # baseline: slow/cheap
        elif self.competition_level == 1:
            priority_eth = "0.5"    # moderate competition
        elif self.competition_level == 2:
            priority_eth = "2.0"    # high competition
        else:
            priority_eth = "5.0"    # extreme: must-win

        priority_fee = self.w3.to_wei(priority_eth, "gwei")
        max_fee = base_fee * 2 + priority_fee * 2
        logger.debug("Gas: competition_level=%d priority=%s gwei max=%s gwei",
                    self.competition_level, priority_eth,
                    self.w3.from_wei(max_fee, "gwei"))

        tx: TxParams = {
            "from": self.account.address,
            "to": self.executor_address,
            "data": tx_calldata,
            "nonce": nonce,
            "gas": gas_limit,
            "maxFeePerGas": max_fee,
            "maxPriorityFeePerGas": priority_fee,
            "chainId": ARBITRUM_CHAIN_ID,
            "type": 2,
        }

        # Sign
        private_key = os.getenv("BOT_PRIVATE_KEY")
        signed = self.w3.eth.account.sign_transaction(tx, private_key)

        # ─── Stale-position re-check ────────────────────────────
        try:
            fresh_data = await self.fetch_user_account_data(opp.borrower)
            if fresh_data and fresh_data.health_factor_float >= 1.0:
                logger.warning("Position no longer liquidatable (HF >= 1.0). Skipping broadcast.")
                self.consecutive_reverts += 1
                return None
        except Exception as e:
            logger.warning("Stale-position pre-check failed: %s", e)

        # ─── DRY RUN GATE ─────────────────────────────────────────
        if self.dry_run:
            tx_hex = "0xDRYRUN_" + signed.raw_transaction.hex()[:48]
            logger.info("[DRY RUN] Would have broadcasted: %s", tx_hex)
            logger.info("[DRY RUN] tx params: gas=%s, maxFee=%s, priority=%s",
                        gas_limit, max_fee, priority_fee)
            self.consecutive_reverts = 0  # dry runs don't count against circuit breaker
            return tx_hex

        # ─── Circuit breaker ──────────────────────────────────────
        if self.consecutive_reverts >= self.max_consecutive_reverts:
            logger.critical(
                "CIRCUIT BREAKER TRIPPED: %d consecutive reverts. PAUSING EXECUTION.",
                self.consecutive_reverts
            )
            try:
                # Try to call emergencyPause on the contract via owner tx
                self.executor.functions.emergencyPause().transact({"from": self.account.address})
                logger.info("Contract paused on-chain.")
            except Exception as pause_err:
                logger.error("Failed to auto-pause contract: %s", pause_err)
            await self.send_alert(
                "🚨 *CIRCUIT BREAKER*\n"
                "%d consecutive reverts. Execution halted. Check bot."
                % self.consecutive_reverts
            )
            return None

        # ─── Broadcast ────────────────────────────────────────────
        try:
            tx_hash = self.mevblocker_w3.eth.send_raw_transaction(signed.raw_transaction)
            tx_hex = tx_hash.hex()
            logger.info("Transaction broadcasted via MEV Blocker: %s", tx_hex)

            # Wait for receipt to confirm outcome (check via Alchemy, faster) (non-blocking timeout)
            try:
                receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash, timeout=15)
                if receipt.get("status") == 1:
                    logger.info("✅ TX CONFIRMED in block %s | gas=%s",
                               receipt["blockNumber"], receipt["gasUsed"])
                else:
                    logger.error("❌ TX REVERTED in block %s", receipt["blockNumber"])
                    self.consecutive_reverts += 1
                    return None
            except Exception as receipt_err:
                logger.warning("Receipt wait timed out (tx may still be pending): %s", receipt_err)

            self.consecutive_reverts = 0  # reset on success
            return tx_hex
        except Exception as e:
            logger.error("Transaction broadcast FAILED: %s", e)
            self.consecutive_reverts += 1
            logger.warning("Consecutive reverts: %d/%d", self.consecutive_reverts, self.max_consecutive_reverts)
            return None

    # ─── Telegram Alerts ────────────────────────────────────

    async def send_alert(self, message: str) -> None:
        """Send Telegram alert if configured."""
        if not self.telegram_token or not self.telegram_chat_id:
            logger.info("[TELEGRAM] %s", message)
            return

        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {
            "chat_id": self.telegram_chat_id,
            "text": message,
            "parse_mode": "Markdown",
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, json=payload) as resp:
                    if resp.status != 200:
                        logger.warning("Telegram send failed: %s", await resp.text())
            except Exception as e:
                logger.warning("Telegram send exception: %s", e)

    # ─── Main Execution Loop ────────────────────────────────

    async def run(self, duration_sec: Optional[float] = None) -> None:
        """Main execution loop."""
        logger.info("=" * 80)
        logger.info(" AAVE v3 LIQUIDATION LIVE EXECUTOR")
        logger.info(" Account: %s", self.account.address)
        logger.info(" Executor: %s", self.executor_address)
        logger.info(" Min Profit: $%.2f", self.min_profit_usd)
        logger.info("=" * 80)

        # Bootstrap
        await self.bootstrap_borrowers()

        # Initialize oracle watcher — discover oracles + map borrowers
        await self._init_oracle_watcher()

        # Start sequencer feed listener (background task)
        if self._sequencer_enabled:
            self._sequencer_task = asyncio.create_task(self._listen_sequencer())
            logger.info("Sequencer feed listener started")

        start_time = time.time()
        iteration = 0
        last_metrics_log = 0.0  # track when we last emitted metrics

        while True:
            iteration += 1
            logger.info("--- Poll #%d ---", iteration)

            # Emit RPC metrics every 60 seconds
            now = time.time()
            if now - last_metrics_log >= 60:
                await self.log_rpc_metrics()
                last_metrics_log = now

            # Get current block — from sequencer cache if available, else RPC
            if self._sequencer_enabled and self._latest_sequencer_block > 0:
                latest_block = self._latest_sequencer_block
            else:
                latest_block = await self.get_latest_block()

            # Dedup: skip if we already scanned this block (sequencer multi-msg)
            if latest_block <= self._last_scanned_block:
                continue
            self._last_scanned_block = latest_block

            # Scan known borrowers
            user_list = list(self.monitored_users.keys())
            opportunities: List[LiquidationOpportunity] = []

            # Single Multicall3 batch: ALL borrowers in ONE RPC call
            health_data = await self.batch_fetch_health_factors(user_list)

            # Live liquidatable count from this poll (not stale history)
            liquidatable_count = 0

            for addr, data in health_data.items():
                if data is None:
                    continue

                # Update history
                self.monitored_users[addr].setdefault("history", []).append({
                    "block": latest_block,
                    "hf": data.health_factor_float,
                    "collateral": data.total_collateral_base,
                    "debt": data.total_debt_base,
                })

                # Check if liquidatable (count for reporting)
                if data.health_factor_float < EXECUTE_HF_THRESHOLD:
                    liquidatable_count += 1
                    logger.debug(
                        "Liquidatable HF=%.4f for %s | debt=$%d | collat=$%d",
                        data.health_factor_float, addr,
                        data.total_debt_base, data.total_collateral_base,
                    )
                    opp = await self.assess_liquidation_opportunity(addr, data)
                    if opp:
                        opportunities.append(opp)

            # Consistency check: warn if stale history disagrees with live data
            history_liquidatable = len([
                u for u in self.monitored_users.values()
                if u.get("history") and u["history"][-1].get("hf", 99) < 1.0
            ])
            if history_liquidatable != liquidatable_count:
                logger.warning(
                    "History/live mismatch: history=%d liquidatable vs live=%d "
                    "(history may be stale from RPC failure or restart)",
                    history_liquidatable, liquidatable_count,
                )
            logger.info(
                "Block=%d | Tracked=%d | Liquidatable=%d | Opportunities=%d",
                latest_block, len(self.monitored_users), liquidatable_count, len(opportunities)
            )

            # Update competition level for gas escalation (integer, 0-4)
            if liquidatable_count > 0:
                self.competition_level = min(self.competition_level + liquidatable_count, 4)
            elif self.competition_level > 0:
                self.competition_level -= 1

            # Execute opportunities
            for opp in opportunities:
                # Rate limit alerts (don't spam same borrower)
                now = time.time()
                if opp.borrower in self.last_alerted and (now - self.last_alerted[opp.borrower]) < 300:
                    logger.info("Skipping %s (alerted recently)", opp.borrower)
                    continue

                self.last_alerted[opp.borrower] = now

                # Step 1: Alert
                alert_msg = (
                    f"🚨 *LIQUIDATABLE POSITION DETECTED*\n"
                    f"Borrower: `{opp.borrower}`\n"
                    f"Health Factor: `{opp.health_factor:.4f}`\n"
                    f"Debt: `${opp.debt_usd:,.2f}`\n"
                    f"Collateral: `${opp.collateral_usd:,.2f}`\n"
                    f"Est. Net Profit: `${opp.estimated_profit_wei / 1e6:,.2f}`"
                )
                await self.send_alert(alert_msg)

                # Step 2: Check oracle staleness (skip sequencer — Aave handles it internally)
                oracle_status = await self.oracle_guard.check_oracle_staleness(
                    opp.collateral_asset
                )
                if oracle_status.is_stale:
                    await self.send_alert(
                        f"🛑 *Liquidation BLOCKED* for `{opp.borrower}`\n"
                        f"Oracle STALE: {oracle_status.symbol} "
                        f"({oracle_status.staleness_seconds:.0f}s, "
                        f"heartbeat: {oracle_status.heartbeat}s)\n"
                        f"HF: `{opp.health_factor:.4f}`"
                    )
                    logger.warning("Oracle stale — blocked %s: %s", opp.borrower, oracle_status.symbol)
                    continue

                # Step 3: Simulate
                sim_ok, sim_reason = await self.simulate_liquidation(opp)
                if not sim_ok:
                    await self.send_alert(f"⚠️ Simulation failed for `{opp.borrower}`: {sim_reason}")
                    continue

                # Step 4: Execute
                tx_hash = await self.broadcast_liquidation(opp)
                if tx_hash:
                    await self.send_alert(
                        f"✅ *LIQUIDATION EXECUTED*\n"
                        f"Borrower: `{opp.borrower}`\n"
                        f"Tx: `{tx_hash}`\n"
                        f"[View on Arbiscan](https://arbiscan.io/tx/{tx_hash})"
                    )
                else:
                    await self.send_alert(f"❌ *BROADCAST FAILED* for `{opp.borrower}`")

            # Check duration
            if duration_sec and (time.time() - start_time) >= duration_sec:
                logger.info("Duration limit reached. Exiting.")
                break

            # Update borrower list periodically (every ~6 min)
            if iteration % 500 == 0:
                logger.info("Refreshing borrower list...")
                await self.bootstrap_borrowers(lookback_blocks=50_000)
                # Track new borrowers
                self._rebuild_oracle_borrower_map()

            # Oracle price change detection + ETH cascade alerts (every 5 polls — ~2.5s)
            if iteration % 5 == 0:
                await self._check_oracle_price_changes()
                await self._check_eth_price_alerts()

            await self._wait_for_block()

    # ─── Oracle Watcher Integration ──────────────────────────

    async def _init_oracle_watcher(self):
        """Discover oracle feeds and map borrowers to affected assets."""
        from scanner.aave_v3 import KNOWN_ASSETS

        assets = []
        for addr, sym, dec in KNOWN_ASSETS:
            assets.append((addr, sym))

        self.oracle_watcher.discover_oracles(assets)
        self._rebuild_oracle_borrower_map()
        logger.info(
            "Oracle watcher initialized: %d oracles tracked",
            len(self.oracle_watcher.watched_oracles),
        )

    def _rebuild_oracle_borrower_map(self):
        """Rebuild asset→borrowers mapping from current monitored_users."""
        from scanner.aave_v3 import KNOWN_ASSETS

        # Build address→symbol lookup
        addr_to_symbol = {addr.lower(): sym for addr, sym, _ in KNOWN_ASSETS}

        borrower_to_assets: Dict[str, Set[str]] = {}
        for borrower, data in self.monitored_users.items():
            history = data.get("history", [])
            if history:
                last = history[-1]
                collateral = last.get("collateral", 0)
                debt = last.get("debt", 0)
                if collateral > 0 or debt > 0:
                    # Map the borrower's assets — use the collateral/debt assets
                    # from the monitored data. For simplicity, use all known
                    # assets that this borrower might be exposed to.
                    for addr, sym in addr_to_symbol.items():
                        borrower_to_assets.setdefault(borrower, set()).add(sym)

        self.oracle_watcher.map_borrowers(borrower_to_assets)

    async def _check_oracle_price_changes(self):
        """Check for significant oracle price changes and trigger priority scans."""
        for oracle_addr, symbol in list(self.oracle_watcher.oracle_to_asset.items()):
            try:
                oracle = self.w3.eth.contract(
                    address=self.w3.to_checksum_address(oracle_addr),
                    abi=[
                        {
                            "inputs": [],
                            "name": "latestRoundData",
                            "outputs": [
                                {"name": "roundId", "type": "uint80"},
                                {"name": "answer", "type": "int256"},
                                {"name": "startedAt", "type": "uint256"},
                                {"name": "updatedAt", "type": "uint256"},
                                {"name": "answeredInRound", "type": "uint80"},
                            ],
                            "stateMutability": "view",
                            "type": "function",
                        },
                    ],
                )
                round_data = oracle.functions.latestRoundData().call()
                new_price = round_data[1] / 1e8
                old_price = self.oracle_watcher.last_prices.get(oracle_addr, new_price)

                if old_price > 0:
                    deviation = abs(new_price - old_price) / old_price
                    if deviation > 0.001:  # 0.1% change — significant for Chainlink
                        affected = self.oracle_watcher.get_affected_borrowers(symbol)
                        if affected:
                            logger.warning(
                                "🔮 ORACLE CHANGE: %s $%.2f→$%.2f (%.2f%%) | %d borrowers affected",
                                symbol, old_price, new_price, deviation * 100, len(affected),
                            )
                            # Priority: immediately scan affected borrowers
                            # (they're already in the normal poll, but this
                            #  logs the event for monitoring purposes)

                self.oracle_watcher.last_prices[oracle_addr] = new_price
            except Exception:
                pass  # Skip failed oracle reads silently

    async def _check_eth_price_alerts(self):
        """Check ETH price against cascade liquidation thresholds and alert."""
        eth_oracle = self.oracle_watcher.asset_to_oracle.get("WETH")
        if not eth_oracle:
            return

        try:
            oracle = self.w3.eth.contract(
                address=self.w3.to_checksum_address(eth_oracle),
                abi=[{
                    "inputs": [], "name": "latestRoundData",
                    "outputs": [
                        {"name": "roundId", "type": "uint80"},
                        {"name": "answer", "type": "int256"},
                        {"name": "startedAt", "type": "uint256"},
                        {"name": "updatedAt", "type": "uint256"},
                        {"name": "answeredInRound", "type": "uint80"},
                    ],
                    "stateMutability": "view", "type": "function",
                }],
            )
            round_data = oracle.functions.latestRoundData().call()
            eth_price = round_data[1] / 1e8

            thresholds = [
                (1850, "⚠️ *ETH at $1,850 — CASCADE WARNING*\nLeveraged WETH borrowers approaching liquidation. "
                         "Be ready to flip DRY_RUN=0."),
                (1750, "🚨 *ETH at $1,750 — LIQUIDATION CASCADE*\n"
                         "Mass WETH liquidation zone. Multiple positions breaching HF<1. "
                         "GO LIVE NOW."),
            ]

            for threshold, message in thresholds:
                alert_key = f"eth_{threshold}"
                if eth_price <= threshold and not self._eth_alert_fired.get(alert_key):
                    self._eth_alert_fired[alert_key] = True
                    logger.warning("ETH PRICE ALERT: $%.2f ≤ $%d", eth_price, threshold)
                    if self.telegram_token:
                        try:
                            await self.send_alert(
                                f"{message}\n\n"
                                f"ETH: `${eth_price:,.2f}` | Threshold: `${threshold}`\n"
                                f"Tracked: {len(self.monitored_users)} borrowers"
                            )
                        except Exception:
                            pass
                elif eth_price > threshold:
                    # Reset alert if price recovers above threshold
                    self._eth_alert_fired[alert_key] = False

        except Exception:
            pass


# ─── Entry Point ────────────────────────────────────────────

def main():
    load_dotenv()

    private_key = os.getenv("BOT_PRIVATE_KEY")
    if not private_key:
        print("ERROR: BOT_PRIVATE_KEY not set", file=sys.stderr)
        sys.exit(1)

    executor_address = os.getenv("FLASH_EXECUTOR_V3")
    if not executor_address:
        print("ERROR: FLASH_EXECUTOR_V3 not set. Deploy the contract first.", file=sys.stderr)
        print("  python3 scripts/deploy_v3.py", file=sys.stderr)
        sys.exit(1)

    rpc_url = os.getenv("ALCHEMY_HTTP_URL") or os.getenv("ARBITRUM_HTTP_URL")
    if not rpc_url:
        print("ERROR: RPC URL not set", file=sys.stderr)
        sys.exit(1)

    min_profit = float(os.getenv("MIN_PROFIT_USD", "50.0"))

    executor = AaveLiquidationExecutor(
        rpc_url=rpc_url,
        private_key=private_key,
        executor_address=executor_address,
        min_profit_usd=min_profit,
    )

    try:
        asyncio.run(executor.run())
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Shutting down.")


if __name__ == "__main__":
    main()
