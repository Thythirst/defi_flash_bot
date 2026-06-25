"""
blast_submit.py — Parallel 4-endpoint tx submission with MEV Blocker
Fixes W2: blast_submit was imported but never called. send_raw_transaction()
was used instead — a single sync RPC with no failover.

Drop-in replacement. presigner.fire() calls await blast_submit(raw_tx)
instead of self.w3.eth.send_raw_transaction(raw_tx).
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Endpoint configuration
# ---------------------------------------------------------------------------

@dataclass
class Endpoint:
    name: str
    url: str
    is_mev_blocker: bool = False
    timeout_ms: int = 2000


@dataclass
class SubmitResult:
    """Rich submission outcome so the caller can manage the nonce correctly.

    status:
      "accepted"  — an endpoint returned a tx hash; tx is in a mempool.
      "ambiguous" — no hash, but at least one endpoint timed out or reported
                    already-known/nonce-too-low/replacement. The tx may be
                    in-flight (private/slow mempool). The caller MUST NOT reuse
                    this nonce, or it will collide with the in-flight tx.
      "failed"    — every endpoint hard-rejected the tx (e.g. insufficient
                    funds, intrinsic gas, malformed). Safe to reclaim the nonce.
    """
    tx_hash: Optional[str]
    status: str  # "accepted" | "ambiguous" | "failed"

    def __bool__(self) -> bool:
        return self.tx_hash is not None


# Keep strong refs to fire-and-forget redundancy submissions so the event loop
# does not garbage-collect them mid-flight after blast_submit() returns early.
_BG_TASKS: set = set()


# Populated by configure_endpoints() — call once at startup
_ENDPOINTS: list[Endpoint] = []
_SESSION: Optional[aiohttp.ClientSession] = None


_SEQUENCER_URL = "https://arb1-sequencer.arbitrum.io/rpc"


def configure_endpoints(
    primary_rpc: str,
    secondary_rpc: str,
    mev_blocker_url: str = "https://arb1.arbitrum.io/rpc",
    flashbots_url: str   = "https://arb1.arbitrum.io/rpc",
    sequencer_rpc: str   = _SEQUENCER_URL,
) -> None:
    """
    Register the submission endpoints.
    Call once at pipeline startup before any blast_submit() calls.

    On Arbitrum, MEV Blocker and Flashbots are Ethereum-only. We submit to:
      - primary: paid RPC relay (fastest, most reliable)
      - secondary: secondary RPC for redundancy
      - sequencer: arb1-sequencer.arbitrum.io — direct to Arbitrum sequencer,
                   bypasses relay hops for minimum-latency inclusion
    """
    global _ENDPOINTS
    _ENDPOINTS = [
        Endpoint(name="primary",    url=primary_rpc,    timeout_ms=2500),
        Endpoint(name="secondary",  url=secondary_rpc,  timeout_ms=2500),
        Endpoint(name="sequencer",  url=sequencer_rpc,  timeout_ms=2500),
    ]
    logger.info(
        f"[BlastSubmit] Configured {len(_ENDPOINTS)} endpoints: "
        f"primary({primary_rpc[:40]}) + secondary + sequencer(direct)"
    )


async def _get_session() -> aiohttp.ClientSession:
    global _SESSION
    if _SESSION is None or _SESSION.closed:
        connector = aiohttp.TCPConnector(
            limit=20,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        _SESSION = aiohttp.ClientSession(
            connector=connector,
            headers={"Content-Type": "application/json"},
        )
    return _SESSION


async def close_session() -> None:
    """Call at pipeline shutdown to cleanly close the aiohttp session."""
    global _SESSION
    if _SESSION and not _SESSION.closed:
        await _SESSION.close()
        _SESSION = None


# ---------------------------------------------------------------------------
# Core submission logic
# ---------------------------------------------------------------------------

async def _submit_to_endpoint(
    session: aiohttp.ClientSession,
    endpoint: Endpoint,
    raw_tx_hex: str,
) -> tuple[str, Optional[str], Optional[str]]:
    """
    Send eth_sendRawTransaction to one endpoint.
    Returns (endpoint_name, tx_hash_or_None, error_or_None).
    """
    payload = {
        "jsonrpc": "2.0",
        "id":      1,
        "method":  "eth_sendRawTransaction",
        "params":  [raw_tx_hex],
    }
    timeout = aiohttp.ClientTimeout(total=endpoint.timeout_ms / 1000)

    t0 = time.perf_counter()
    try:
        async with session.post(endpoint.url, json=payload, timeout=timeout) as resp:
            ms = (time.perf_counter() - t0) * 1000
            body = await resp.json(content_type=None)

            if "error" in body:
                err = body["error"].get("message", str(body["error"]))
                # Known-good errors — tx landed via another endpoint
                if any(k in err.lower() for k in ("already known", "nonce too low", "replacement")):
                    logger.debug(f"[BlastSubmit] {endpoint.name} — already known ({ms:.0f}ms)")
                    return endpoint.name, None, "already_known"
                logger.warning(f"[BlastSubmit] {endpoint.name} error in {ms:.0f}ms: {err}")
                return endpoint.name, None, err

            tx_hash = body.get("result")
            logger.info(f"[BlastSubmit] {endpoint.name} accepted {tx_hash} in {ms:.0f}ms")
            return endpoint.name, tx_hash, None

    except asyncio.TimeoutError:
        ms = (time.perf_counter() - t0) * 1000
        logger.warning(f"[BlastSubmit] {endpoint.name} timeout after {ms:.0f}ms")
        return endpoint.name, None, "timeout"

    except Exception as e:
        ms = (time.perf_counter() - t0) * 1000
        logger.warning(f"[BlastSubmit] {endpoint.name} failed in {ms:.0f}ms: {e}")
        return endpoint.name, None, str(e)


# Endpoint error strings that mean "the tx may already be in a mempool" —
# i.e. an ambiguous outcome where reusing the nonce would collide.
_AMBIGUOUS_ERRORS = ("already_known", "timeout")
_AMBIGUOUS_SUBSTRINGS = ("already known", "nonce too low", "replacement",
                         "known transaction", "already imported")


def _is_ambiguous(error: Optional[str]) -> bool:
    if not error:
        return False
    e = error.lower()
    if error in _AMBIGUOUS_ERRORS:
        return True
    return any(s in e for s in _AMBIGUOUS_SUBSTRINGS)


async def blast_submit_ex(raw_tx: bytes) -> SubmitResult:
    """
    Submit a signed raw transaction to all configured endpoints in parallel and
    return a SubmitResult describing the outcome (accepted / ambiguous / failed).

    Returns as soon as the FIRST endpoint accepts (fastest-wins) — remaining
    endpoints keep submitting in the background for redundancy/propagation.

    The accepted/ambiguous/failed distinction lets the caller manage the nonce
    correctly: only "failed" (every endpoint hard-rejected) is safe to rewind;
    "ambiguous" means the tx may be in-flight and the nonce must be held.
    """
    if not _ENDPOINTS:
        raise RuntimeError(
            "blast_submit: no endpoints configured. "
            "Call configure_endpoints() at pipeline startup."
        )

    raw_hex = raw_tx.hex() if isinstance(raw_tx, (bytes, bytearray)) else raw_tx
    if not raw_hex.startswith("0x"):
        raw_hex = "0x" + raw_hex

    session = await _get_session()

    tasks = [
        asyncio.create_task(
            _submit_to_endpoint(session, ep, raw_hex),
            name=f"blast_{ep.name}",
        )
        for ep in _ENDPOINTS
    ]

    errors: list[str] = []
    saw_ambiguous = False
    pending = set(tasks)

    while pending:
        done, pending = await asyncio.wait(
            pending,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            name, tx_hash, error = task.result()
            if tx_hash:
                # Fastest-wins: return immediately. Let the still-pending
                # endpoints finish in the background for redundancy (keep a
                # strong ref so they aren't GC'd mid-flight).
                logger.info(f"[BlastSubmit] First accept from {name}: {tx_hash}")
                for p in pending:
                    _BG_TASKS.add(p)
                    p.add_done_callback(_BG_TASKS.discard)
                return SubmitResult(tx_hash=tx_hash, status="accepted")
            if _is_ambiguous(error):
                saw_ambiguous = True
            elif error:
                errors.append(f"{name}:{error}")

    # No endpoint returned a hash.
    if saw_ambiguous:
        logger.warning(
            "[BlastSubmit] No hash but ambiguous (timeout/already-known) — "
            "tx may be in-flight; holding nonce. errors=%s", errors
        )
        return SubmitResult(tx_hash=None, status="ambiguous")

    logger.error(f"[BlastSubmit] All endpoints hard-failed: {errors}")
    return SubmitResult(tx_hash=None, status="failed")


async def blast_submit(raw_tx: bytes) -> Optional[str]:
    """
    Backward-compatible wrapper: returns the tx hash on success, else None.
    Callers that need the accepted/ambiguous/failed distinction (for nonce
    management) should call blast_submit_ex() directly.
    """
    return (await blast_submit_ex(raw_tx)).tx_hash


# ---------------------------------------------------------------------------
# presigner.py patch — change fire() to call blast_submit
# ---------------------------------------------------------------------------
#
# BEFORE (presigner.py ~line 64):
#     tx_hash = self.w3.eth.send_raw_transaction(presigned.raw_tx)
#
# AFTER:
#     from blast_submit import blast_submit
#     tx_hash = await blast_submit(presigned.raw_tx)
#     if tx_hash is None:
#         logger.error(f"[Presigner] blast_submit returned None for {borrower}")
#         return None
#
# Also update pipeline.py startup to call:
#     configure_endpoints(
#         primary_rpc   = os.getenv("QUICKNODE_HTTP"),
#         secondary_rpc = os.getenv("CHAINSTACK_HTTP"),
#     )
# ---------------------------------------------------------------------------
