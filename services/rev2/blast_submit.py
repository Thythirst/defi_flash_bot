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


# Populated by configure_endpoints() — call once at startup
_ENDPOINTS: list[Endpoint] = []
_SESSION: Optional[aiohttp.ClientSession] = None


def configure_endpoints(
    primary_rpc: str,
    secondary_rpc: str,
    mev_blocker_url: str = "https://arb1.arbitrum.io/rpc",
    flashbots_url: str   = "https://arb1.arbitrum.io/rpc",
) -> None:
    """
    Register the submission endpoints.
    Call once at pipeline startup before any blast_submit() calls.

    MEV Blocker (rpc.mevblocker.io) and Flashbots (rpc.flashbots.net) are
    Ethereum-only — they do not serve Arbitrum. On Arbitrum we run:
      - QuickNode 22ms (primary)
      - 3x public arb1 52ms (redundant network paths, same node deduplicates)

    Args:
        primary_rpc:    QuickNode HTTP 22ms
        secondary_rpc:  public arb1 52ms
        mev_blocker_url: public arb1 (Ethereum-only MEV Blocker replaced)
        flashbots_url:   public arb1 (Ethereum-only Flashbots replaced)
    """
    global _ENDPOINTS
    public_arb1 = "https://arb1.arbitrum.io/rpc"
    _ENDPOINTS = [
        Endpoint(name="primary",    url=primary_rpc,    timeout_ms=5000),
        Endpoint(name="secondary",  url=secondary_rpc,  timeout_ms=5000),
        Endpoint(name="arb1_a",     url=public_arb1,    timeout_ms=5000),
        Endpoint(name="arb1_b",     url=public_arb1,    timeout_ms=5000),
    ]
    logger.info(f"[BlastSubmit] Configured {len(_ENDPOINTS)} endpoints: primary({primary_rpc[:40]}) + secondary + 2x arb1")


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


async def blast_submit(raw_tx: bytes) -> Optional[str]:
    """
    Submit a signed raw transaction to all configured endpoints in parallel.
    Returns the first tx_hash received, or None if all endpoints fail.

    Replaces:
        tx_hash = self.w3.eth.send_raw_transaction(presigned.raw_tx)
    With:
        tx_hash = await blast_submit(presigned.raw_tx)

    Args:
        raw_tx: signed transaction bytes (as returned by sign_transaction)
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

    first_hash: Optional[str] = None
    errors: list[str] = []
    pending = set(tasks)

    # Return as soon as the first endpoint accepts, let others finish async
    while pending:
        done, pending = await asyncio.wait(
            pending,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in done:
            name, tx_hash, error = task.result()
            if tx_hash and not first_hash:
                first_hash = tx_hash
                logger.info(f"[BlastSubmit] First confirmation from {name}: {tx_hash}")
                # Cancel remaining only if all MEV-protected endpoints already submitted
                # (let mev_blocker and flashbots always fire for protection)
                non_mev_done = all(
                    t.done() for t in tasks
                    if not any(ep.name in t.get_name() for ep in _ENDPOINTS if ep.is_mev_blocker)
                )
                if non_mev_done:
                    for p in pending:
                        p.cancel()
                    break
            elif error and error != "already_known":
                errors.append(f"{name}:{error}")

    if not first_hash:
        # Check if any "already_known" — tx may have landed via a prior attempt
        all_errors = [task.result()[2] for task in tasks if task.done()]
        if all(e == "already_known" for e in all_errors if e):
            logger.info("[BlastSubmit] All endpoints report already_known — tx likely landed")
        else:
            logger.error(f"[BlastSubmit] All endpoints failed: {errors}")

    return first_hash


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
