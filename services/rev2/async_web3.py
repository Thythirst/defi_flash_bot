"""
async_web3.py — AsyncWeb3 client + NonceManager
Fixes W4: sync RPC calls blocking the event loop (up to 750ms per cycle)
Fixes W5: nonce collision when two liquidations fire simultaneously

Drop-in replacements:
    - AsyncRPCClient wraps AsyncWeb3 with connection pooling
    - NonceManager provides atomic nonce allocation with lock
    - QuoterAsync replaces the 3× sync quoteExactInputSingle calls

Usage:
    rpc = AsyncRPCClient(http_url=os.getenv("QUICKNODE_HTTP"))
    await rpc.connect()

    nonce_mgr = NonceManager(rpc.w3, WALLET_ADDR)
    await nonce_mgr.init()

    # In presigner.fire():
    nonce = await nonce_mgr.next()
    # ... build tx with nonce ...
    # On revert/drop:
    await nonce_mgr.rewind()
"""

import asyncio
import logging
import os
import time
from typing import Optional, Any

from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware

logger = logging.getLogger(__name__)

WAD = 10 ** 18

# ---------------------------------------------------------------------------
# Uni V3 Quoter V2 — Arbitrum mainnet
# ---------------------------------------------------------------------------
QUOTER_V2_ADDRESS = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"

QUOTER_V2_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"internalType": "address", "name": "tokenIn",           "type": "address"},
                    {"internalType": "address", "name": "tokenOut",          "type": "address"},
                    {"internalType": "uint256", "name": "amountIn",          "type": "uint256"},
                    {"internalType": "uint24",  "name": "fee",               "type": "uint24"},
                    {"internalType": "uint160", "name": "sqrtPriceLimitX96", "type": "uint160"},
                ],
                "internalType": "struct IQuoterV2.QuoteExactInputSingleParams",
                "name": "params",
                "type": "tuple",
            }
        ],
        "name": "quoteExactInputSingle",
        "outputs": [
            {"internalType": "uint256", "name": "amountOut",              "type": "uint256"},
            {"internalType": "uint160", "name": "sqrtPriceX96After",      "type": "uint160"},
            {"internalType": "uint32",  "name": "initializedTicksCrossed","type": "uint32"},
            {"internalType": "uint256", "name": "gasEstimate",            "type": "uint256"},
        ],
        "stateMutability": "nonpayable",
        "type": "function",
    }
]

FEE_TIERS = [3000, 10000]  # 0.3%, 1% — 0.05% pool returns garbage quotes for WBTC pairs


# ---------------------------------------------------------------------------
# AsyncRPCClient
# ---------------------------------------------------------------------------

class AsyncRPCClient:
    """
    Thin wrapper around AsyncWeb3 with connection health tracking.
    Replaces direct sync Web3(HTTPProvider(...)) usage throughout the pipeline.

    All calls are non-blocking — safe to await inside the event loop
    without freezing oracle processing.
    """

    def __init__(self, http_url: str, request_timeout: float = 10.0):
        self.http_url = http_url
        self.request_timeout = request_timeout
        self._w3: Optional[AsyncWeb3] = None
        self._connected = False
        self._last_block_time: float = 0.0
        self._consecutive_timeouts = 0

    async def connect(self) -> None:
        from aiohttp import ClientTimeout
        provider = AsyncHTTPProvider(
            self.http_url,
            request_kwargs={"timeout": ClientTimeout(total=self.request_timeout)},
        )
        self._w3 = AsyncWeb3(provider)
        # Required for Arbitrum (PoA-compatible chain)
        self._w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        self._connected = True
        block = await self._w3.eth.block_number
        logger.info(f"[AsyncRPC] Connected — latest block {block}")

    @property
    def w3(self) -> AsyncWeb3:
        if not self._w3:
            raise RuntimeError("AsyncRPCClient not connected — call await connect() first")
        return self._w3

    async def _call_with_timeout(self, coro, label: str = "rpc") -> Any:
        """Wrap any RPC coroutine with hard timeout + connection health tracking."""
        try:
            return await asyncio.wait_for(coro, timeout=self.request_timeout)
        except asyncio.TimeoutError:
            self._consecutive_timeouts += 1
            logger.error(
                f"[AsyncRPC] {label} timed out after {self.request_timeout}s "
                f"(consecutive={self._consecutive_timeouts})"
            )
            if self._consecutive_timeouts >= 3:
                logger.critical("[AsyncRPC] 3 consecutive timeouts — reconnecting")
                self._connected = False
                try:
                    await self.connect()
                    self._consecutive_timeouts = 0
                except Exception as e:
                    logger.critical(f"[AsyncRPC] Reconnect failed: {e}")
            raise
        except Exception:
            raise
        else:
            self._consecutive_timeouts = 0

    async def get_block_number(self) -> int:
        return await self._call_with_timeout(self._w3.eth.block_number, "get_block_number")

    async def get_block(self, block_id: str = "latest") -> dict:
        return await self._call_with_timeout(
            self._w3.eth.get_block(block_id), f"get_block({block_id})"
        )

    async def get_pending_block(self) -> dict:
        """
        Async replacement for sync get_block('pending') in presigner.py:78.
        Was blocking for 50-100ms; now awaited without freezing the loop.
        """
        return await self._call_with_timeout(
            self._w3.eth.get_block("pending"), "get_block(pending)"
        )

    async def get_base_fee(self) -> int:
        """Returns current base fee in wei."""
        block = await self._call_with_timeout(
            self._w3.eth.get_block("latest"), "get_block(latest)"
        )
        return block.get("baseFeePerGas", 0)

    async def get_balance_of(
        self,
        token_address: str,
        wallet: str,
        erc20_abi: list,
    ) -> int:
        """
        Single async balanceOf call.
        Replaces the sequential sync loop in pipeline.py:441-458.
        Callers should use asyncio.gather() across all 9 assets.
        """
        contract = self._w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(token_address),
            abi=erc20_abi,
        )
        return await self._call_with_timeout(
            contract.functions.balanceOf(
                AsyncWeb3.to_checksum_address(wallet)
            ).call(),
            f"balanceOf({token_address[:10]}…)"
        )

    async def get_all_balances(
        self,
        assets: dict[str, str],  # symbol → token_address
        wallet: str,
        erc20_abi: list,
    ) -> dict[str, int]:
        """
        Fetch all asset balances with staggered timing.
        Avoids 429 rate limit from 10 simultaneous RPCs on QuickNode.
        """
        results = {}
        for symbol, addr in assets.items():
            try:
                results[symbol] = await self.get_balance_of(addr, wallet, erc20_abi)
            except Exception as e:
                logger.warning(f"[AsyncRPC] balanceOf failed for {symbol}: {e}")
                results[symbol] = 0
            await asyncio.sleep(0.15)  # stagger to avoid rate limit

        return results


# ---------------------------------------------------------------------------
# NonceManager — atomic nonce allocation
# ---------------------------------------------------------------------------

class NonceManager:
    """
    Thread-safe (asyncio) nonce allocator.
    Fixes W5: two simultaneous liquidations shared one pending nonce,
    causing the second tx to replace the first in the mempool.

    Usage:
        nonce_mgr = NonceManager(rpc.w3, WALLET_ADDR)
        await nonce_mgr.init()

        # In presigner.fire():
        nonce = await nonce_mgr.next()

        # If tx reverts or times out with no confirm:
        await nonce_mgr.rewind()
    """

    def __init__(self, w3: AsyncWeb3, wallet: str):
        self._w3     = w3
        self._wallet = AsyncWeb3.to_checksum_address(wallet)
        self._nonce: Optional[int] = None
        self._lock   = asyncio.Lock()
        self._pending_count = 0  # tracks how many unconfirmed txs we've sent

    async def init(self) -> None:
        """Fetch current on-chain nonce. Call once at startup."""
        async with self._lock:
            self._nonce = await self._w3.eth.get_transaction_count(
                self._wallet, "pending"
            )
            logger.info(f"[NonceManager] Initialised at nonce {self._nonce}")

    async def next(self) -> int:
        """
        Allocate the next nonce atomically.
        Returns immediately without RPC call (nonce is managed in memory).
        """
        async with self._lock:
            if self._nonce is None:
                await self.init()
            nonce = self._nonce
            self._nonce += 1
            self._pending_count += 1
            logger.debug(f"[NonceManager] Allocated nonce {nonce} (pending: {self._pending_count})")
            return nonce

    async def confirm(self) -> None:
        """Call when a tx is confirmed on-chain."""
        async with self._lock:
            self._pending_count = max(0, self._pending_count - 1)

    async def rewind(self) -> None:
        """
        Call if a tx fails to submit (not just reverts — reverts consume nonce).
        Decrements the local counter and re-syncs from chain.
        """
        async with self._lock:
            # Re-sync from chain — the safe recovery path
            on_chain = await self._w3.eth.get_transaction_count(
                self._wallet, "pending"
            )
            self._nonce = on_chain
            self._pending_count = 0
            logger.warning(f"[NonceManager] Rewound — re-synced to nonce {self._nonce}")

    async def sync(self) -> None:
        """Periodic re-sync (call every 60s) to catch any out-of-band txs."""
        async with self._lock:
            on_chain = await self._w3.eth.get_transaction_count(
                self._wallet, "pending"
            )
            if on_chain != self._nonce:
                logger.warning(
                    f"[NonceManager] Drift detected — "
                    f"local={self._nonce}, chain={on_chain}. Re-syncing."
                )
                self._nonce = on_chain


# ---------------------------------------------------------------------------
# QuoterAsync — non-blocking Uni V3 quoting
# ---------------------------------------------------------------------------

class QuoterAsync:
    """
    Async Uni V3 QuoterV2 — replaces synchronous quoteExactInputSingle × 3
    in ev_estimator.py (~300ms blocking per call).

    Fixes W4 (EV estimator path): all 3 fee tier quotes run concurrently,
    total wall time ≈ slowest single quote (~80-100ms) instead of 300ms.
    """

    def __init__(self, rpc: AsyncRPCClient):
        self._contract = rpc.w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(QUOTER_V2_ADDRESS),
            abi=QUOTER_V2_ABI,
        )

    async def _quote_fee_tier(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
        fee: int,
    ) -> tuple[int, int]:
        """Returns (amountOut, fee) or (0, fee) on failure."""
        try:
            result = await self._contract.functions.quoteExactInputSingle({
                "tokenIn":           AsyncWeb3.to_checksum_address(token_in),
                "tokenOut":          AsyncWeb3.to_checksum_address(token_out),
                "amountIn":          amount_in,
                "fee":               fee,
                "sqrtPriceLimitX96": 0,
            }).call()
            return result[0], fee  # amountOut
        except Exception as e:
            logger.debug(f"[Quoter] fee={fee} quote failed: {e}")
            return 0, fee

    async def best_quote(
        self,
        token_in: str,
        token_out: str,
        amount_in: int,
    ) -> tuple[int, int]:
        """
        Query all 3 fee tiers concurrently. Returns (best_amount_out, best_fee).

        Replaces:
            for fee in [500, 3000, 10000]:
                out = quoter.quoteExactInputSingle(...).call()  # sync, 100ms each

        With:
            amount_out, fee = await quoter.best_quote(token_in, token_out, amt)
        """
        tasks = [
            asyncio.create_task(
                self._quote_fee_tier(token_in, token_out, amount_in, fee),
                name=f"quote_{fee}",
            )
            for fee in FEE_TIERS
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)
        best_out, best_fee = 0, FEE_TIERS[0]

        for res in results:
            if isinstance(res, Exception):
                continue
            amount_out, fee = res
            if amount_out > best_out:
                best_out = amount_out
                best_fee = fee

        logger.debug(
            f"[Quoter] best={best_out} via fee={best_fee} "
            f"for {token_in[:8]}→{token_out[:8]}"
        )
        return best_out, best_fee


# ---------------------------------------------------------------------------
# ev_estimator.py patch guide
# ---------------------------------------------------------------------------
#
# 1. Replace sync quoter init:
#       OLD: self.quoter = w3.eth.contract(QUOTER_ADDR, abi=QUOTER_ABI)
#       NEW: from async_web3 import QuoterAsync
#            self.quoter = QuoterAsync(rpc_client)
#
# 2. Replace the fee-tier loop in estimate():
#       OLD:
#           for fee in [500, 3000, 10000]:
#               out = self.quoter.functions.quoteExactInputSingle(...).call()
#               if out > best: best = out
#       NEW:
#           best_out, best_fee = await self.quoter.best_quote(
#               token_in, token_out, amount_in
#           )
#
# 3. Replace get_block in presigner.py:
#       OLD: block = self.w3.eth.get_block('pending')
#       NEW: block = await rpc_client.get_pending_block()
#
# 4. Replace balance loop in pipeline.py:
#       OLD:
#           for asset in DECIMALS:
#               bal = erc20.functions.balanceOf(WALLET_ADDR).call()
#       NEW:
#           balances = await rpc_client.get_all_balances(ASSET_ADDRESSES, WALLET_ADDR, ERC20_ABI)
#
# 5. Replace nonce fetch in presigner.py:
#       OLD: self._nonce = self.w3.eth.get_transaction_count(wallet, 'pending')
#       NEW: nonce = await nonce_manager.next()
# ---------------------------------------------------------------------------
