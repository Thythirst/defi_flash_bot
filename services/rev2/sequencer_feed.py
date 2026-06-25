"""
sequencer_feed.py — Arbitrum sequencer feed watcher.

Subscribes to wss://arb1.arbitrum.io/feed and invokes on_pending_swap()
for every pending transaction that targets a monitored UniV3 pool address
directly, or calls exactInputSingle on the SwapRouter(s) for a tracked pair.

Feed message format (Nitro L2MessageType_batch = 3):
  byte[0]   = 0x03  (batch type)
  repeated:
    uint64 BE = sub-message length (8 bytes, big-endian)
    byte[0]   = 0x04 (signedTx)
    rest      = RLP-encoded EIP-1559 / legacy signed transaction

Selectors watched:
  0x128acb08  IUniswapV3PoolActions.swap   (direct pool call)
  0x414bf389  SwapRouter.exactInputSingle  (v1 router)
  0x04e45aaf  SwapRouter02.exactInputSingle (v2 router)
"""

from __future__ import annotations

import asyncio
import base64
import collections
import json
import logging
import struct
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Optional

import rlp
import websockets
from eth_abi import decode as abi_decode
from eth_utils import keccak

logger = logging.getLogger(__name__)

FEED_URL = "wss://arb1.arbitrum.io/feed"

SEL_POOL_SWAP      = bytes.fromhex("128acb08")  # IUniswapV3Pool.swap
SEL_EXACT_IN_V1    = bytes.fromhex("414bf389")  # SwapRouter.exactInputSingle
SEL_EXACT_IN_V2    = bytes.fromhex("04e45aaf")  # SwapRouter02.exactInputSingle
SEL_EXACT_IN_MH_V1 = bytes.fromhex("c04b8d59")  # SwapRouter.exactInput   (multi-hop)
SEL_EXACT_IN_MH_V2 = bytes.fromhex("b858183f")  # SwapRouter02.exactInput (multi-hop)

# multicall wrappers — the Uniswap UI and most integrators wrap the real swap in
# one of these, so without unwrapping we miss ~75% of router volume.
SEL_MULTICALL          = bytes.fromhex("ac9650d8")  # multicall(bytes[])
SEL_MULTICALL_DEADLINE = bytes.fromhex("5ae401dc")  # multicall(uint256,bytes[])
SEL_MULTICALL_PREVBLK  = bytes.fromhex("1f0464d1")  # multicall(bytes32,bytes[])
MULTICALL_SELS = {SEL_MULTICALL, SEL_MULTICALL_DEADLINE, SEL_MULTICALL_PREVBLK}

SWAP_ROUTER    = "0xe592427a0aece92de3edee1f18e0157c05861564"
SWAP_ROUTER_02 = "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45"
ROUTER_ADDRS   = {SWAP_ROUTER, SWAP_ROUTER_02}

# Camelot V3 (Algebra) SwapRouter — exactInputSingle has NO fee field, and its
# exactInput path is token(20)|token(20)|… (no fee bytes — dynamic fee).
CAMELOT_ROUTER          = "0x1f721e2e82f6676fce4ea07a5958cf098d339e18"
SEL_CAMELOT_EXACT_IN_SI = bytes.fromhex("bc651188")  # Camelot exactInputSingle
# exactInput (multi-hop) shares Uniswap's selector but a different path layout:
# SEL_EXACT_IN_MH_V1 (0xc04b8d59) — handled per-router.

# 1inch AggregationRouter v5/v6 — the generic swap() carries a SwapDescription
# with srcToken/dstToken/amount, enough to identify the pair + size (we route
# the prediction to the pair's primary/deepest UniV3 pool).
ONEINCH_V5 = "0x1111111254eeb25477b68fb85ed929f73a960582"
ONEINCH_V6 = "0x111111125421ca6dc452d289314280a0f8842a65"
ONEINCH_ADDRS = {ONEINCH_V5, ONEINCH_V6}
SEL_1INCH_SWAP = bytes.fromhex("12aa3caf")  # swap(address,(SwapDescription),bytes,bytes)

# 0x ExchangeProxy (same address on every chain). transformERC20 is its dominant
# aggregated-swap entrypoint and exposes input/output token + amount directly;
# sellTokenForTokenToUniswapV3 carries an encoded UniV3 path → exact pool+fee.
ZEROX_EXCHANGE_PROXY   = "0xdef1c0ded9bec7f1a1670819833240f027b25eff"
ZEROX_ADDRS            = {ZEROX_EXCHANGE_PROXY}
SEL_0X_TRANSFORM_ERC20 = bytes.fromhex("415565b0")  # transformERC20(addr,addr,uint256,uint256,(uint32,bytes)[])
SEL_0X_SELL_UNIV3      = bytes.fromhex("6af479b2")  # sellTokenForTokenToUniswapV3(bytes,uint256,uint256,address)
# 0x represents native ETH legs with this sentinel in place of WETH.
ETH_SENTINEL = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
WETH_ARB     = "0x82af49447d8a07e3bd95bd0d56f35241523fbab1"

# Uniswap Universal Router — the current Uniswap UI routes here (NOT SwapRouter02),
# so it carries the bulk of large WETH/USDC flow the backrun path was missing
# (confirmed by PHASE0: 0x3593564c was the top undecoded entrypoint on tracked
# tokens). execute(bytes commands, bytes[] inputs[, uint256 deadline]); each
# command byte's low 6 bits select an op, and V3_SWAP_EXACT_IN's input carries
# (recipient, amountIn, amountOutMin, path, payerIsUser) — path → exact pool+fee.
UNIVERSAL_ROUTER_ADDRS = {
    "0xa51afafe0263b40edaef0df8781ea9aa03e381a3",  # observed via PHASE0
    "0x5e325eda8064b456f4781070c0738d849c824258",  # earlier UR deployment
}
SEL_UR_EXECUTE          = bytes.fromhex("3593564c")  # execute(bytes,bytes[],uint256)
SEL_UR_EXECUTE_NO_DL    = bytes.fromhex("24856bc3")  # execute(bytes,bytes[])
UR_CMD_MASK             = 0x3f
UR_V3_SWAP_EXACT_IN     = 0x00
UR_CONTRACT_BALANCE_BIT = 1 << 255   # amountIn sentinel "use contract balance" — unknown size
# Command-code labels (low 6 bits) for the diagnostic histogram.
UR_CMD_LABELS = {
    0x00: "V3_SWAP_EXACT_IN", 0x01: "V3_SWAP_EXACT_OUT",
    0x08: "V2_SWAP_EXACT_IN", 0x09: "V2_SWAP_EXACT_OUT",
    0x0a: "PERMIT2_PERMIT",   0x0b: "WRAP_ETH", 0x0c: "UNWRAP_WETH",
    0x04: "SWEEP", 0x05: "TRANSFER", 0x06: "PAY_PORTION",
    0x0d: "PERMIT2_TRANSFER_FROM", 0x21: "EXECUTE_SUB_PLAN",
}

# --- PHASE 0 INSTRUMENTATION (temporary; remove after starvation measurement) ---
# Labels for readability of the ranked undecoded-entrypoint report. Lower-case.
PHASE0_AGG_LABELS = {
    "0xdef1c0ded9bec7f1a1670819833240f027b25eff": "0x ExchangeProxy (handled)",
    "0xa669e7a0d4b3e4fa48af2de86bd4cd7126be4e13": "Odos RouterV2",
    "0xdef171fe48cf0115b1d80b88dc8eab59176fee57": "Paraswap Augustus v5",
    "0x6a000f20005980200259b80c5102003040001068": "Paraswap Augustus v6",
    "0x6131b5fae19ea4f9d964eac0408e4408b66337b5": "KyberSwap MetaAgg v2",
    "0x6352a56caadc4f1e25cd6c75970fa768a3304e64": "OpenOcean",
    "0xc873fecbd354f5a56e00e710b90ef4201db2448d": "Camelot V2 Router",
    "0x1111111254eeb25477b68fb85ed929f73a960582": "1inch v5 (handled)",
    "0x111111125421ca6dc452d289314280a0f8842a65": "1inch v6 (handled)",
}
PHASE0_REPORT_EVERY_S = 60.0
# --- END PHASE 0 INSTRUMENTATION ---


@dataclass
class PendingSwap:
    pool_addr:    str   # lower-case pool address
    token_in:     str   # lower-case
    token_out:    str   # lower-case
    fee:          int   # UniV3 fee tier
    amount_in:    int   # raw token units (0 if unavailable from calldata)
    zero_for_one: bool


# ---------------------------------------------------------------------------
# Low-level tx parsing
# ---------------------------------------------------------------------------

def _decode_tx(raw: bytes) -> Optional[tuple[str, bytes]]:
    """
    Decode a raw signed transaction (no type prefix) into (to_lower, calldata).
    Returns None on any error.
    """
    try:
        if raw[0] in (1, 2):            # EIP-2930 or EIP-1559
            fields = rlp.decode(raw[1:])
            # EIP-1559: chainId, nonce, maxPriorityFeePerGas, maxFeePerGas, gas, to, value, data, …
            to_b   = fields[5] if len(fields) > 5 else b''
            data_b = fields[7] if len(fields) > 7 else b''
        else:                           # legacy
            # nonce, gasPrice, gas, to, value, data, v, r, s
            fields = rlp.decode(raw)
            to_b   = fields[3] if len(fields) > 3 else b''
            data_b = fields[5] if len(fields) > 5 else b''
        if len(to_b) != 20:
            return None
        return "0x" + to_b.hex(), bytes(data_b)
    except Exception:
        return None


def _unwrap_multicall(sel: bytes, data: bytes) -> list[bytes]:
    """Return the inner call payloads of a multicall(...) variant; [] on error."""
    try:
        if sel == SEL_MULTICALL:
            (arr,) = abi_decode(["bytes[]"], data)
        elif sel == SEL_MULTICALL_DEADLINE:
            _deadline, arr = abi_decode(["uint256", "bytes[]"], data)
        elif sel == SEL_MULTICALL_PREVBLK:
            _blockhash, arr = abi_decode(["bytes32", "bytes[]"], data)
        else:
            return []
        return [bytes(x) for x in arr]
    except Exception:
        return []


def _iter_batch(payload: bytes):
    """
    Yield (to_lower, calldata, raw_tx) tuples from a L2MessageType_batch payload.
    raw_tx is the signed tx bytes (keccak → tx hash, for feed/WS correlation).
    Length prefix is uint64 big-endian (8 bytes).
    """
    pos = 1  # skip type byte 0x03
    while pos + 8 <= len(payload):
        sub_len = struct.unpack(">Q", payload[pos:pos + 8])[0]
        pos += 8
        if sub_len == 0 or pos + sub_len > len(payload):
            break
        sub = payload[pos:pos + sub_len]
        pos += sub_len
        if sub and sub[0] == 4:         # signedTx sub-message
            raw = sub[1:]
            parsed = _decode_tx(raw)
            if parsed:
                yield parsed[0], parsed[1], raw


# ---------------------------------------------------------------------------
# SequencerFeedWatcher
# ---------------------------------------------------------------------------

class SequencerFeedWatcher:
    """
    Real-time watcher for the Arbitrum sequencer broadcast feed.

    pool_by_addr        : { lower_addr → PoolInfo }
    pool_by_key         : { (token0_lower, token1_lower, fee) → PoolInfo }  (UniV3)
    camelot_by_pair     : { (token0_lower, token1_lower) → Camelot PoolInfo }
    univ3_primary_by_pair: { (token0_lower, token1_lower) → deepest UniV3 PoolInfo }
                          used to route 1inch swaps (exact pool unknown) to a best-guess pool
    on_pending_swap : async callback(PendingSwap)
    """

    def __init__(
        self,
        pool_by_addr: dict,
        pool_by_key:  dict,
        on_pending_swap: Callable[[PendingSwap], Awaitable[None]],
        camelot_by_pair: dict | None = None,
        univ3_primary_by_pair: dict | None = None,
    ) -> None:
        self._pool_by_addr = pool_by_addr
        self._pool_by_key  = pool_by_key
        self._camelot_by_pair = camelot_by_pair or {}
        self._univ3_primary_by_pair = univ3_primary_by_pair or {}
        self._callback     = on_pending_swap
        self._running      = False
        self._msgs_seen    = 0
        self._swaps_seen   = 0

        # --- PHASE 0 INSTRUMENTATION (temporary) ---
        # Set of tracked token addresses as raw 20-byte values, for cheap
        # substring detection in undecoded calldata (format-agnostic).
        self._tracked_token_bytes: set[bytes] = set()
        for p in pool_by_addr.values():
            for t in (getattr(p, "token0", None), getattr(p, "token1", None)):
                if isinstance(t, str) and len(t) == 42:
                    try:
                        self._tracked_token_bytes.add(bytes.fromhex(t[2:]))
                    except ValueError:
                        pass
        # Known venues we already decode — anything else is an "unknown entrypoint".
        self._phase0_known = (
            set(pool_by_addr) | set(ROUTER_ADDRS) | {CAMELOT_ROUTER}
            | set(ONEINCH_ADDRS) | set(ZEROX_ADDRS) | set(UNIVERSAL_ROUTER_ADDRS)
        )
        self._phase0_undecoded: collections.Counter = collections.Counter()  # (to, sel_hex) -> count, touching tracked tokens
        self._phase0_unknown_total = 0   # all unknown-entrypoint top-level calls
        self._phase0_touch_total   = 0   # of those, how many touched a tracked token
        self._phase0_last_report   = time.monotonic()
        # --- END PHASE 0 INSTRUMENTATION ---

        # --- UR match-rate diagnostic (temporary) ---
        # Shows where Universal Router V3_SWAP_EXACT_IN flow is lost: a swap on a
        # pair we don't track at all vs. a tracked pair but a fee tier missing
        # from pool_by_key vs. the contract-balance sentinel (size unknown).
        self._tracked_pairs = {(k[0], k[1]) for k in pool_by_key}  # sorted (t0,t1)
        self._ur_stats: collections.Counter = collections.Counter()
        self._ur_cmds:  collections.Counter = collections.Counter()  # masked cmd -> count
        # --- END UR diagnostic ---

        # --- feed/WS correlation: txhash -> monotonic time first seen in feed,
        # for every pending tx whose calldata references a tracked token. Lets the
        # WS path measure (a) whether large confirmed swaps were visible as pending
        # and (b) the lead time = our real same-block latency budget. ---
        self.seen_hashes: "collections.OrderedDict[bytes, float]" = collections.OrderedDict()
        self._seen_cap = 40000

    def stop(self) -> None:
        self._running = False

    async def run(self) -> None:
        self._running = True
        backoff = 1.0
        while self._running:
            try:
                logger.info(f"[SeqFeed] Connecting → {FEED_URL}")
                async with websockets.connect(
                    FEED_URL,
                    ping_interval=20,
                    ping_timeout=15,
                    open_timeout=10,
                    max_size=2 ** 23,       # 8 MB — large catch-up batches
                ) as ws:
                    backoff = 1.0
                    logger.info("[SeqFeed] Connected")
                    while self._running:
                        raw = await asyncio.wait_for(ws.recv(), timeout=30)
                        asyncio.create_task(self._handle(raw))
            except asyncio.CancelledError:
                break
            except Exception as e:
                if self._running:
                    logger.warning(f"[SeqFeed] {e} — retry in {backoff:.0f}s")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 30.0)

    # ── parsing ─────────────────────────────────────────────────────────────

    async def _handle(self, raw_msg: str) -> None:
        try:
            data = json.loads(raw_msg)
        except Exception:
            return
        self._msgs_seen += 1
        for m in data.get("messages", []):
            inner   = m.get("message", {}).get("message", {})
            b64     = inner.get("l2Msg", "")
            if not b64:
                continue
            payload = base64.b64decode(b64)
            if not payload:
                continue
            if payload[0] == 4:             # single signedTx
                raw = payload[1:]
                parsed = _decode_tx(raw)
                if parsed:
                    to_addr, calldata = parsed
                    # Record tx hash (feed receipt time) for any tracked-token tx so
                    # the WS path can correlate confirmed swaps + measure lead time.
                    if any(tb in calldata for tb in self._tracked_token_bytes):
                        self._record_seen(keccak(raw))
                    await self._check(to_addr, calldata)
            elif payload[0] == 3:           # batch
                for to_addr, calldata, raw in _iter_batch(payload):
                    if any(tb in calldata for tb in self._tracked_token_bytes):
                        self._record_seen(keccak(raw))
                    await self._check(to_addr, calldata)

    def _record_seen(self, h: bytes) -> None:
        self.seen_hashes[h] = time.monotonic()
        if len(self.seen_hashes) > self._seen_cap:
            self.seen_hashes.popitem(last=False)

    def feed_seen_at(self, txhash_hex: str) -> Optional[float]:
        """Monotonic time this tx hash was first seen in the feed, or None."""
        s = txhash_hex[2:] if txhash_hex.startswith("0x") else txhash_hex
        try:
            return self.seen_hashes.get(bytes.fromhex(s))
        except ValueError:
            return None

    # --- PHASE 0 INSTRUMENTATION (temporary) ---
    def _phase0_maybe_report(self) -> None:
        now = time.monotonic()
        if now - self._phase0_last_report < PHASE0_REPORT_EVERY_S:
            return
        self._phase0_last_report = now
        top = self._phase0_undecoded.most_common(20)
        logger.info(
            f"[SeqFeed PHASE0] unknown-entrypoint calls={self._phase0_unknown_total} "
            f"touching-tracked-token={self._phase0_touch_total} "
            f"distinct(to,sel)={len(self._phase0_undecoded)}"
        )
        if self._ur_stats:
            s = self._ur_stats
            logger.info(
                f"[SeqFeed UR-DIAG] execute={s['execute']} v3_in={s['v3_in']} "
                f"matched={s['matched']} | lost: pair_untracked={s['pair_untracked']} "
                f"fee_untracked={s['fee_untracked']} sentinel/zero={s['sentinel_or_zero']} "
                f"short_path={s['short_path']} decode_fail={s['decode_fail']}"
            )
            cmd_hist = "  ".join(
                f"{UR_CMD_LABELS.get(c, hex(c))}={n}"
                for c, n in self._ur_cmds.most_common(8)
            )
            logger.info(f"[SeqFeed UR-DIAG] commands: {cmd_hist or '(none)'}")
        if not top:
            logger.info("[SeqFeed PHASE0]   (no tracked-token traffic to unknown entrypoints yet)")
            return
        logger.info("[SeqFeed PHASE0]   count  selector   entrypoint")
        for (to_addr, sel_hex), cnt in top:
            label = PHASE0_AGG_LABELS.get(to_addr, "")
            logger.info(f"[SeqFeed PHASE0]   {cnt:5d}  0x{sel_hex}  {to_addr} {label}")
    # --- END PHASE 0 INSTRUMENTATION ---

    async def _check(self, to_lower: str, calldata: bytes, depth: int = 0) -> None:
        if len(calldata) < 4:
            return
        sel = calldata[:4]

        # --- PHASE 0 INSTRUMENTATION (temporary, observe-only) ---
        # Tally top-level calls to entrypoints we do NOT already decode, ranking
        # those whose calldata references a tracked token (a candidate swap that
        # is currently invisible to the backrun path). Pure measurement — does
        # not alter routing below.
        if depth == 0 and to_lower not in self._phase0_known:
            self._phase0_unknown_total += 1
            if any(tb in calldata for tb in self._tracked_token_bytes):
                self._phase0_touch_total += 1
                self._phase0_undecoded[(to_lower, sel.hex())] += 1
        self._phase0_maybe_report()
        # --- END PHASE 0 INSTRUMENTATION ---

        if to_lower in self._pool_by_addr:
            if sel == SEL_POOL_SWAP:
                await self._on_pool_swap(to_lower, calldata[4:])

        elif to_lower in ROUTER_ADDRS:
            if sel in MULTICALL_SELS:
                # Unwrap and recurse — inner calls are self-calls to this router.
                if depth < 3:
                    for inner in _unwrap_multicall(sel, calldata[4:]):
                        await self._check(to_lower, inner, depth + 1)
            elif sel == SEL_EXACT_IN_V1:
                await self._on_router_swap(calldata[4:], v2=False)
            elif sel == SEL_EXACT_IN_V2:
                await self._on_router_swap(calldata[4:], v2=True)
            elif sel in (SEL_EXACT_IN_MH_V1, SEL_EXACT_IN_MH_V2):
                await self._on_exact_input(calldata[4:], v2=(sel == SEL_EXACT_IN_MH_V2))

        elif to_lower == CAMELOT_ROUTER:
            if sel in MULTICALL_SELS:
                if depth < 3:
                    for inner in _unwrap_multicall(sel, calldata[4:]):
                        await self._check(to_lower, inner, depth + 1)
            elif sel == SEL_CAMELOT_EXACT_IN_SI:
                await self._on_camelot_single(calldata[4:])
            elif sel == SEL_EXACT_IN_MH_V1:        # Algebra exactInput (no-fee path)
                await self._on_camelot_exact_input(calldata[4:])

        elif to_lower in ONEINCH_ADDRS:
            if sel == SEL_1INCH_SWAP:
                await self._on_1inch_swap(calldata[4:])

        elif to_lower in ZEROX_ADDRS:
            if sel == SEL_0X_TRANSFORM_ERC20:
                await self._on_0x_transform(calldata[4:])
            elif sel == SEL_0X_SELL_UNIV3:
                await self._on_0x_sell_univ3(calldata[4:])

        elif to_lower in UNIVERSAL_ROUTER_ADDRS:
            if sel in (SEL_UR_EXECUTE, SEL_UR_EXECUTE_NO_DL):
                await self._on_universal_router(
                    calldata[4:], has_deadline=(sel == SEL_UR_EXECUTE))

    async def _on_pool_swap(self, pool_addr: str, data: bytes) -> None:
        """Decode IUniswapV3Pool.swap(recipient, zeroForOne, amountSpecified, …)."""
        try:
            _recipient, zero_for_one, amount_specified, _sqrtLimit, _hookData = abi_decode(
                ["address", "bool", "int256", "uint160", "bytes"], data
            )
        except Exception:
            return
        pool = self._pool_by_addr.get(pool_addr)
        if pool is None:
            return
        ti = pool.token0.lower() if zero_for_one else pool.token1.lower()
        to = pool.token1.lower() if zero_for_one else pool.token0.lower()
        self._swaps_seen += 1
        await self._callback(PendingSwap(
            pool_addr    = pool_addr,
            token_in     = ti,
            token_out    = to,
            fee          = pool.fee,
            amount_in    = abs(int(amount_specified)),
            zero_for_one = bool(zero_for_one),
        ))

    async def _on_router_swap(self, data: bytes, v2: bool) -> None:
        """Decode SwapRouter[02].exactInputSingle(params struct)."""
        try:
            if v2:
                # SwapRouter02: tokenIn, tokenOut, fee, recipient, amountIn, amountOutMin, sqrtPriceLimit
                ti, to, fee, _rec, amount_in, _min, _sqrtLimit = abi_decode(
                    ["address","address","uint24","address","uint256","uint256","uint160"], data
                )
            else:
                # SwapRouter: tokenIn, tokenOut, fee, recipient, deadline, amountIn, amountOutMin, sqrtPriceLimit
                ti, to, fee, _rec, _deadline, amount_in, _min, _sqrtLimit = abi_decode(
                    ["address","address","uint24","address","uint256","uint256","uint256","uint160"], data
                )
        except Exception:
            return
        t0, t1 = sorted([ti.lower(), to.lower()])
        pool = self._pool_by_key.get((t0, t1, int(fee)))
        if pool is None:
            return
        self._swaps_seen += 1
        await self._callback(PendingSwap(
            pool_addr    = pool.address.lower(),
            token_in     = ti.lower(),
            token_out    = to.lower(),
            fee          = int(fee),
            amount_in    = int(amount_in),
            zero_for_one = ti.lower() == pool.token0.lower(),
        ))

    async def _on_exact_input(self, data: bytes, v2: bool) -> None:
        """
        Decode SwapRouter[02].exactInput(ExactInputParams). The struct holds a
        dynamic `bytes path`, so it must be decoded as a tuple (with the outer
        offset), unlike the all-static exactInputSingle. We match the FIRST hop
        (it carries the full amountIn and moves the first pool's price most).

        path bytes: tokenIn(20) | fee(3) | tokenOut(20) | fee(3) | …
        v1 struct: (bytes path, address recipient, uint256 deadline, uint256 amountIn, uint256 amountOutMinimum)
        v2 struct: (bytes path, address recipient, uint256 amountIn, uint256 amountOutMinimum)
        """
        try:
            if v2:
                (params,) = abi_decode(["(bytes,address,uint256,uint256)"], data)
                path, _rec, amount_in, _min = params
            else:
                (params,) = abi_decode(["(bytes,address,uint256,uint256,uint256)"], data)
                path, _rec, _deadline, amount_in, _min = params
        except Exception:
            return
        if len(path) < 43:
            return
        ti  = "0x" + path[0:20].hex()
        fee = int.from_bytes(path[20:23], "big")
        to  = "0x" + path[23:43].hex()
        t0, t1 = sorted([ti.lower(), to.lower()])
        pool = self._pool_by_key.get((t0, t1, fee))
        if pool is None:
            return
        self._swaps_seen += 1
        await self._callback(PendingSwap(
            pool_addr    = pool.address.lower(),
            token_in     = ti.lower(),
            token_out    = to.lower(),
            fee          = fee,
            amount_in    = int(amount_in),
            zero_for_one = ti.lower() == pool.token0.lower(),
        ))

    async def _emit_for_pool(self, pool, ti: str, to: str, amount_in: int) -> None:
        """Emit a PendingSwap on a known PoolInfo (fee=0 for Camelot)."""
        if pool is None or amount_in <= 0:
            return
        self._swaps_seen += 1
        await self._callback(PendingSwap(
            pool_addr    = pool.address.lower(),
            token_in     = ti.lower(),
            token_out    = to.lower(),
            fee          = pool.fee,
            amount_in    = int(amount_in),
            zero_for_one = ti.lower() == pool.token0.lower(),
        ))

    async def _on_camelot_single(self, data: bytes) -> None:
        """
        Camelot (Algebra) exactInputSingle — NO fee field; all-static struct:
        (tokenIn, tokenOut, recipient, deadline, amountIn, amountOutMinimum, limitSqrtPrice)
        """
        try:
            ti, to, _rec, _dl, amount_in, _min, _lim = abi_decode(
                ["address", "address", "address", "uint256", "uint256", "uint256", "uint160"], data
            )
        except Exception:
            return
        t0, t1 = sorted([ti.lower(), to.lower()])
        await self._emit_for_pool(self._camelot_by_pair.get((t0, t1)), ti, to, int(amount_in))

    async def _on_camelot_exact_input(self, data: bytes) -> None:
        """
        Camelot (Algebra) exactInput(ExactInputParams) — path is token(20)|token(20)|…
        with NO fee bytes (dynamic fee). Match first hop to the Camelot pool.
        struct: (bytes path, address recipient, uint256 deadline, uint256 amountIn, uint256 amountOutMinimum)
        """
        try:
            (params,) = abi_decode(["(bytes,address,uint256,uint256,uint256)"], data)
            path, _rec, _dl, amount_in, _min = params
        except Exception:
            return
        if len(path) < 40:
            return
        ti = "0x" + path[0:20].hex()
        to = "0x" + path[20:40].hex()
        t0, t1 = sorted([ti.lower(), to.lower()])
        await self._emit_for_pool(self._camelot_by_pair.get((t0, t1)), ti, to, int(amount_in))

    async def _on_1inch_swap(self, data: bytes) -> None:
        """
        1inch v5/v6 swap(address executor, SwapDescription desc, bytes permit, bytes data).
        desc = (srcToken, dstToken, srcReceiver, dstReceiver, amount, minReturn, flags).
        The exact pool 1inch uses isn't in the desc, so we route the prediction to
        the pair's primary (deepest) UniV3 pool — a best-guess that's usually right.
        """
        try:
            _executor, desc, _permit, _inner = abi_decode(
                ["address",
                 "(address,address,address,address,uint256,uint256,uint256)",
                 "bytes", "bytes"], data
            )
        except Exception:
            return
        src, dst = desc[0].lower(), desc[1].lower()
        amount = int(desc[4])
        t0, t1 = sorted([src, dst])
        pool = self._univ3_primary_by_pair.get((t0, t1))
        if pool is None:
            return
        await self._emit_for_pool(pool, src, dst, amount)

    @staticmethod
    def _norm_eth(addr: str) -> str:
        """Map 0x's native-ETH sentinel to WETH so pair lookups resolve."""
        return WETH_ARB if addr == ETH_SENTINEL else addr

    async def _on_0x_transform(self, data: bytes) -> None:
        """
        0x transformERC20(inputToken, outputToken, inputTokenAmount,
        minOutputTokenAmount, (uint32,bytes)[] transformations).

        The exact pool 0x routes through lives inside the opaque transformations
        blob, so — like 1inch — we route the prediction to the pair's primary
        (deepest) UniV3 pool. src/dst/amount come straight from the head args.
        """
        try:
            ti, to, amount_in, _min, _transforms = abi_decode(
                ["address", "address", "uint256", "uint256", "(uint32,bytes)[]"], data
            )
        except Exception:
            return
        src = self._norm_eth(ti.lower())
        dst = self._norm_eth(to.lower())
        t0, t1 = sorted([src, dst])
        pool = self._univ3_primary_by_pair.get((t0, t1))
        if pool is None:
            return
        await self._emit_for_pool(pool, src, dst, int(amount_in))

    async def _on_0x_sell_univ3(self, data: bytes) -> None:
        """
        0x sellTokenForTokenToUniswapV3(bytes encodedPath, uint256 sellAmount,
        uint256 minBuyAmount, address recipient).

        encodedPath: tokenIn(20) | fee(3) | tokenOut(20) | …  — the first hop
        carries the full sellAmount and identifies the EXACT UniV3 pool + fee.
        """
        try:
            path, sell_amount, _min, _rec = abi_decode(
                ["bytes", "uint256", "uint256", "address"], data
            )
        except Exception:
            return
        if len(path) < 43:
            return
        ti  = self._norm_eth("0x" + path[0:20].hex())
        fee = int.from_bytes(path[20:23], "big")
        to  = self._norm_eth("0x" + path[23:43].hex())
        t0, t1 = sorted([ti, to])
        pool = self._pool_by_key.get((t0, t1, fee))
        if pool is None:
            return
        await self._emit_for_pool(pool, ti, to, int(sell_amount))

    async def _on_universal_router(self, data: bytes, has_deadline: bool) -> None:
        """
        Uniswap Universal Router execute(bytes commands, bytes[] inputs[, deadline]).

        `commands` is one byte per operation; the low 6 bits select the op. We
        decode each V3_SWAP_EXACT_IN input — (recipient, amountIn, amountOutMin,
        bytes path, bool payerIsUser) — and match its FIRST hop (tokenIn|fee|
        tokenOut) to the exact UniV3 pool. amountIn may be a "use contract
        balance" sentinel (bit 255 set) whose real size is unknown — skip those.
        """
        try:
            if has_deadline:
                commands, inputs, _deadline = abi_decode(["bytes", "bytes[]", "uint256"], data)
            else:
                commands, inputs = abi_decode(["bytes", "bytes[]"], data)
        except Exception:
            return
        # Only tally the diagnostic for UR calls that reference a tracked token —
        # otherwise NFT/Seaport traffic (which never resolves to our pools) swamps
        # the histogram and hides the real swap-command distribution.
        diag = any(tb in data for tb in self._tracked_token_bytes)
        if diag:
            self._ur_stats["execute"] += 1
        for i, cmd_byte in enumerate(commands):
            cmd = cmd_byte & UR_CMD_MASK
            if diag:
                self._ur_cmds[cmd] += 1
            if cmd != UR_V3_SWAP_EXACT_IN:
                continue
            if diag:
                self._ur_stats["v3_in"] += 1
            if i >= len(inputs):
                break
            try:
                _rec, amount_in, _min, path, _payer = abi_decode(
                    ["address", "uint256", "uint256", "bytes", "bool"], inputs[i]
                )
            except Exception:
                self._ur_stats["decode_fail"] += 1
                continue
            if amount_in <= 0 or amount_in >= UR_CONTRACT_BALANCE_BIT:
                self._ur_stats["sentinel_or_zero"] += 1
                continue  # zero or contract-balance sentinel — size unknown
            if len(path) < 43:
                self._ur_stats["short_path"] += 1
                continue
            ti  = self._norm_eth("0x" + path[0:20].hex())
            fee = int.from_bytes(path[20:23], "big")
            to  = self._norm_eth("0x" + path[23:43].hex())
            t0, t1 = sorted([ti, to])
            pool = self._pool_by_key.get((t0, t1, fee))
            if pool is None:
                # Diagnose the miss: tracked pair w/ missing fee tier, vs untracked pair.
                if (t0, t1) in self._tracked_pairs:
                    self._ur_stats["fee_untracked"] += 1
                else:
                    self._ur_stats["pair_untracked"] += 1
                continue
            self._ur_stats["matched"] += 1
            await self._emit_for_pool(pool, ti, to, int(amount_in))
