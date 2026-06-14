#!/usr/bin/env python3
"""
scripts/sequencer_diagnostic.py — Standalone Arbitrum Sequencer Feed Diagnostic

Connects to the Arbitrum sequencer feed and provides real-time visibility into
feed health, message patterns, and transaction detection heuristics.

Use this to:
  - Validate sequencer feed connectivity and stability
  - Inspect raw message structure for parser improvement
  - Measure message throughput and latency
  - Detect liquidation-related transactions in real time
  - Capture raw payloads for offline analysis

Usage:
    python3 scripts/sequencer_diagnostic.py
    python3 scripts/sequencer_diagnostic.py --save-raw /tmp/sequencer_raw/
    python3 scripts/sequencer_diagnostic.py --duration 300 --verbose

Environment:
    No API keys required. The sequencer feed is public.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import signal
import struct
import sys
import time
from collections import Counter, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

import websockets

# ─── Logging Setup ──────────────────────────────────────────

def setup_logging(log_file: Optional[Path] = None, json_mode: bool = False, quiet: bool = False) -> logging.Logger:
    """Configure logging for daemon or interactive mode."""
    logger = logging.getLogger("sequencer_diagnostic")
    logger.setLevel(logging.INFO)
    logger.handlers = []  # Clear existing

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(message)s"
    ) if not json_mode else logging.Formatter(
        json.dumps({"ts": "%(asctime)s", "lvl": "%(levelname)s", "msg": "%(message)s"})
    )

    if not quiet:
        console = logging.StreamHandler(sys.stdout)
        console.setFormatter(formatter)
        logger.addHandler(console)

    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        # Use RotatingFileHandler for log rotation
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10_000_000, backupCount=5
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

# ─── Constants ──────────────────────────────────────────────

SEQUENCER_FEED_URL = "wss://arb1.arbitrum.io/feed"
SEQUENCER_FEED_HTTP = "https://arb1.arbitrum.io/feed"

# Default paths for daemon mode
DEFAULT_LOG_DIR = Path("/var/log/sequencer-diagnostic")
DEFAULT_STATUS_FILE = Path("/tmp/sequencer_diagnostic_status.json")
DEFAULT_PID_FILE = Path("/tmp/sequencer_diagnostic.pid")

# Known method signatures (4-byte selectors)
SIGS = {
    "aave_liquidation": "eabe7ea2",
    "aave_liquidation_v2": "6d6fd70d",
    "aave_flash_loan": "ab9c4b5d",
    "aave_flash_loan_simple": "3d7d3f5a",
    "uni_v3_exact_input_single": "04e45aaf",
    "uni_v3_exact_output_single": "5023b4df",
    "uni_v2_swap": "472b43f3",
    "balancer_flash_loan": "5c9a6f47",
    "balancer_flash_loan_simple": "6d6fd70d",
    "weth_deposit": "d0e30db0",
    "weth_withdraw": "2e1a7d4d",
    "erc20_transfer": "a9059cbb",
    "erc20_approve": "095ea7b3",
}

# Contracts we care about
AAVE_POOL = "794a61358D6845594F94dc1DB02A252b5b4814aD"
AAVE_POOL_DATA_PROVIDER = "69FA688f1Dc47d4B5d8029D5a35FB7a548310654"
BALANCER_VAULT = "BA12222222228d8Ba445958a75a0704d566BF2C8"
UNI_V3_ROUTER = "E592427A0AEce92De3Edee1F18E0157C05861564"
WETH = "82aF49447D8a07e3bd95BD0d56f35241523fBab1"

MONITORED_CONTRACTS = {
    AAVE_POOL.lower(),
    AAVE_POOL_DATA_PROVIDER.lower(),
    BALANCER_VAULT.lower(),
    UNI_V3_ROUTER.lower(),
    WETH.lower(),
}

# ─── Data Classes ───────────────────────────────────────────

@dataclass
class FeedMessage:
    """A raw message from the sequencer feed."""
    timestamp: float
    payload_len: int
    payload_hex: str
    payload_bytes: bytes
    sequence_hash: str


@dataclass
class DetectionResult:
    """What we found in a message."""
    sequence_hash: str
    timestamp: float
    detected_contracts: List[str] = field(default_factory=list)
    detected_signatures: List[str] = field(default_factory=list)
    is_liquidation_related: bool = False
    is_flash_loan: bool = False
    is_swap: bool = False
    raw_preview: str = ""


@dataclass
class DiagnosticsState:
    """Running state for the diagnostic session."""
    messages_total: int = 0
    messages_per_second: float = 0.0
    bytes_total: int = 0
    detections_total: int = 0
    liquidation_signals: int = 0
    flash_loan_signals: int = 0
    swap_signals: int = 0
    reconnects: int = 0
    last_message_time: float = 0.0
    connection_established: float = 0.0
    errors: List[str] = field(default_factory=list)

    # Rolling windows
    message_times: deque = field(default_factory=lambda: deque(maxlen=1000))
    detection_times: deque = field(default_factory=lambda: deque(maxlen=1000))
    signature_counts: Counter = field(default_factory=Counter)
    contract_counts: Counter = field(default_factory=Counter)


# ─── Message Parser ─────────────────────────────────────────

class SequencerMessageParser:
    """Parses raw sequencer feed messages and extracts heuristics."""

    def __init__(self, save_dir: Optional[Path] = None):
        self.save_dir = save_dir
        self.seen_hashes: Set[str] = set()
        self.max_seen = 50_000

    def parse(self, raw: bytes) -> Optional[FeedMessage]:
        """Parse a raw WebSocket message into a FeedMessage."""
        if not raw:
            return None

        payload = raw
        payload_len = len(raw)

        # Try length-prefixed format
        if len(raw) >= 8:
            try:
                declared_len = struct.unpack(">Q", raw[:8])[0]
                if 0 < declared_len <= len(raw) - 8:
                    payload = raw[8:8 + declared_len]
                    payload_len = declared_len
            except struct.error:
                pass

        payload_hex = payload.hex()
        seq_hash = hashlib.sha256(payload).hexdigest()[:16]

        # Deduplicate
        if seq_hash in self.seen_hashes:
            return None
        self.seen_hashes.add(seq_hash)
        if len(self.seen_hashes) > self.max_seen:
            self.seen_hashes = set(list(self.seen_hashes)[self.max_seen // 2:])

        return FeedMessage(
            timestamp=time.time(),
            payload_len=payload_len,
            payload_hex=payload_hex,
            payload_bytes=payload,
            sequence_hash=seq_hash,
        )

    def detect(self, msg: FeedMessage) -> Optional[DetectionResult]:
        """Run heuristic detection on a parsed message."""
        hex_lower = msg.payload_hex.lower()
        result = DetectionResult(
            sequence_hash=msg.sequence_hash,
            timestamp=msg.timestamp,
            raw_preview=msg.payload_hex[:128],
        )

        # Detect monitored contracts
        for contract in MONITORED_CONTRACTS:
            if contract.lower() in hex_lower:
                result.detected_contracts.append("0x" + contract)

        # Detect method signatures
        for name, sig in SIGS.items():
            if sig.lower() in hex_lower:
                result.detected_signatures.append(name)

        # Categorize
        result.is_liquidation_related = any(
            s in result.detected_signatures
            for s in ["aave_liquidation", "aave_liquidation_v2"]
        ) or AAVE_POOL.lower() in hex_lower

        result.is_flash_loan = any(
            s in result.detected_signatures
            for s in ["aave_flash_loan", "aave_flash_loan_simple", "balancer_flash_loan"]
        )

        result.is_swap = any(
            s in result.detected_signatures
            for s in ["uni_v3_exact_input_single", "uni_v3_exact_output_single", "uni_v2_swap"]
        )

        if result.detected_contracts or result.detected_signatures:
            return result
        return None

    def save_raw(self, msg: FeedMessage) -> None:
        """Save raw payload to disk for offline analysis."""
        if not self.save_dir:
            return
        self.save_dir.mkdir(parents=True, exist_ok=True)
        path = self.save_dir / f"{msg.sequence_hash}.bin"
        path.write_bytes(msg.payload_bytes)


# ─── Diagnostic Display ─────────────────────────────────────

class DiagnosticDisplay:
    """Handles terminal output for the diagnostic session."""

    def __init__(self, verbose: bool = False, quiet: bool = False, json_mode: bool = False):
        self.verbose = verbose
        self.quiet = quiet
        self.json_mode = json_mode
        self.start_time = time.time()
        self.last_print = 0.0
        self.print_interval = 5.0

    def should_print(self) -> bool:
        now = time.time()
        if now - self.last_print >= self.print_interval:
            self.last_print = now
            return True
        return False

    def _log(self, msg: str) -> None:
        if not self.quiet:
            if self.json_mode:
                print(json.dumps({"type": "status", "msg": msg, "ts": time.time()}))
            else:
                print(msg)

    def print_status(self, state: DiagnosticsState) -> None:
        """Print a status summary line."""
        elapsed = time.time() - self.start_time
        mps = state.messages_total / elapsed if elapsed > 0 else 0
        mbps = (state.bytes_total / 1024 / 1024) / elapsed if elapsed > 0 else 0

        if self.json_mode:
            print(json.dumps({
                "type": "metrics",
                "elapsed": round(elapsed, 1),
                "messages": state.messages_total,
                "msg_per_sec": round(mps, 1),
                "mb_per_sec": round(mbps, 2),
                "detections": state.detections_total,
                "liquidations": state.liquidation_signals,
                "flash_loans": state.flash_loan_signals,
                "reconnects": state.reconnects,
            }))
        else:
            status = (
                f"⏱ {elapsed:6.1f}s | "
                f"📨 {state.messages_total:6,} msgs | "
                f"⚡ {mps:5.1f} msg/s | "
                f"📦 {mbps:5.2f} MB/s | "
                f"🔍 {state.detections_total:4,} detected | "
                f"🚨 {state.liquidation_signals:3,} liq | "
                f"⚡ {state.flash_loan_signals:3,} flash | "
                f"🔄 {state.reconnects:2,} recon"
            )
            self._log(status)

    def print_detection(self, det: DetectionResult) -> None:
        """Print details of a detected message."""
        if self.quiet:
            return

        ts = datetime.fromtimestamp(det.timestamp).strftime("%H:%M:%S.%f")[:-3]
        cats = []
        if det.is_liquidation_related:
            cats.append("LIQUIDATION")
        if det.is_flash_loan:
            cats.append("FLASH_LOAN")
        if det.is_swap:
            cats.append("SWAP")

        cat_str = " | ".join(cats) if cats else "OTHER"

        if self.json_mode:
            print(json.dumps({
                "type": "detection",
                "ts": det.timestamp,
                "category": cat_str,
                "sequence_hash": det.sequence_hash,
                "contracts": det.detected_contracts,
                "signatures": det.detected_signatures,
                "preview": det.raw_preview if self.verbose else None,
            }))
        else:
            print(f"\n[{ts}] 🔔 {cat_str} seq={det.sequence_hash}")
            if det.detected_contracts:
                print(f"    Contracts: {', '.join(det.detected_contracts)}")
            if det.detected_signatures:
                print(f"    Signatures: {', '.join(det.detected_signatures)}")
            if self.verbose:
                print(f"    Preview: {det.raw_preview}")

    def print_summary(self, state: DiagnosticsState) -> None:
        """Print final summary."""
        if self.quiet:
            return

        elapsed = time.time() - self.start_time

        if self.json_mode:
            summary = {
                "type": "summary",
                "duration": round(elapsed, 1),
                "messages_total": state.messages_total,
                "bytes_total": state.bytes_total,
                "avg_throughput": round(state.messages_total / elapsed, 1) if elapsed > 0 else 0,
                "detections": state.detections_total,
                "liquidation_signals": state.liquidation_signals,
                "flash_loan_signals": state.flash_loan_signals,
                "swap_signals": state.swap_signals,
                "reconnects": state.reconnects,
                "errors": len(state.errors),
                "top_signatures": dict(state.signature_counts.most_common(10)),
                "top_contracts": dict(state.contract_counts.most_common(10)),
            }
            print(json.dumps(summary))
            return

        print("\n" + "=" * 80)
        print(" SEQUENCER FEED DIAGNOSTIC SUMMARY")
        print("=" * 80)
        print(f"Duration:           {elapsed:.1f}s")
        print(f"Messages received:  {state.messages_total:,}")
        print(f"Bytes received:     {state.bytes_total:,} ({state.bytes_total/1024/1024:.2f} MB)")
        print(f"Avg throughput:     {state.messages_total/elapsed:.1f} msg/s")
        print(f"Detections:         {state.detections_total:,}")
        print(f"Liquidation sigs:   {state.liquidation_signals:,}")
        print(f"Flash loan sigs:    {state.flash_loan_signals:,}")
        print(f"Swap sigs:          {state.swap_signals:,}")
        print(f"Reconnects:         {state.reconnects:,}")
        print(f"Errors:             {len(state.errors)}")

        if state.signature_counts:
            print("\nTop signatures:")
            for sig, count in state.signature_counts.most_common(10):
                print(f"  {sig}: {count}")

        if state.contract_counts:
            print("\nTop contracts:")
            for contract, count in state.contract_counts.most_common(10):
                print(f"  {contract}: {count}")

        if state.errors:
            print("\nErrors:")
            for err in state.errors[-10:]:
                print(f"  {err}")
        print("=" * 80)


# ─── Main Diagnostic Loop ───────────────────────────────────

async def run_diagnostic(
    duration: Optional[float] = None,
    save_dir: Optional[Path] = None,
    verbose: bool = False,
    quiet: bool = False,
    json_mode: bool = False,
    log_file: Optional[Path] = None,
    status_file: Optional[Path] = None,
    pid_file: Optional[Path] = None,
) -> None:
    """Run the sequencer feed diagnostic."""

    # Setup logging
    logger = setup_logging(log_file=log_file, json_mode=json_mode, quiet=quiet)

    parser = SequencerMessageParser(save_dir=save_dir)
    display = DiagnosticDisplay(verbose=verbose, quiet=quiet, json_mode=json_mode)
    state = DiagnosticsState()

    # Write PID file for daemon management
    if pid_file:
        pid_file.write_text(str(os.getpid()))

    # Signal handling for clean shutdown
    shutdown_event = asyncio.Event()

    def handle_signal(signum, frame):
        logger.info("Received signal %d, shutting down...", signum)
        shutdown_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    def write_status() -> None:
        """Write current state to status file for external monitoring."""
        if not status_file:
            return
        elapsed = time.time() - display.start_time
        status = {
            "timestamp": time.time(),
            "datetime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "running": True,
            "pid": os.getpid(),
            "elapsed_seconds": round(elapsed, 1),
            "messages_total": state.messages_total,
            "bytes_total": state.bytes_total,
            "messages_per_second": round(state.messages_total / elapsed, 2) if elapsed > 0 else 0,
            "detections_total": state.detections_total,
            "liquidation_signals": state.liquidation_signals,
            "flash_loan_signals": state.flash_loan_signals,
            "swap_signals": state.swap_signals,
            "reconnects": state.reconnects,
            "errors_count": len(state.errors),
            "last_error": state.errors[-1] if state.errors else None,
            "connected": True,
            "last_message_age_ms": None,
        }
        if state.last_message_time > 0:
            status["last_message_age_ms"] = round((time.time() - state.last_message_time) * 1000, 0)
        try:
            status_file.write_text(json.dumps(status, indent=2))
        except Exception as e:
            logger.debug("Failed to write status file: %s", e)

    if not quiet:
        print("=" * 80)
        print(" ARBITRUM SEQUENCER FEED DIAGNOSTIC")
        print("=" * 80)
        print(f"Feed URL:   {SEQUENCER_FEED_URL}")
        print(f"Duration:   {'infinite' if duration is None else f'{duration}s'}")
        print(f"Save raw:   {'yes -> ' + str(save_dir) if save_dir else 'no'}")
        print(f"Log file:   {log_file or 'none'}")
        print(f"Status file: {status_file or 'none'}")
        print(f"PID file:   {pid_file or 'none'}")
        print(f"Verbose:    {'yes' if verbose else 'no'}")
        print(f"JSON mode:  {'yes' if json_mode else 'no'}")
        print(f"Quiet:      {'yes' if quiet else 'no'}")
        print("=" * 80)
        print("Connecting...\n")

    logger.info("Starting sequencer feed diagnostic")

    reconnect_delay = 2.0
    start_time = time.time()
    status_counter = 0

    while not shutdown_event.is_set():
        # Check duration
        if duration and (time.time() - start_time) >= duration:
            break

        try:
            async with websockets.connect(
                SEQUENCER_FEED_URL,
                ping_interval=20,
                ping_timeout=10,
                compression=None,
                max_size=10_000_000,  # Sequencer feed sends large frames
            ) as ws:
                state.connection_established = time.time()
                reconnect_delay = 2.0
                logger.info("Connected to sequencer feed")

                async for raw in ws:
                    # Check shutdown
                    if shutdown_event.is_set():
                        break

                    # Duration check inside loop
                    if duration and (time.time() - start_time) >= duration:
                        break

                    # Handle both str and bytes from websockets
                    if isinstance(raw, str):
                        raw_bytes = raw.encode("utf-8", errors="replace")
                    else:
                        raw_bytes = raw

                    state.messages_total += 1
                    state.bytes_total += len(raw_bytes)
                    state.last_message_time = time.time()
                    state.message_times.append(time.time())

                    # Parse
                    msg = parser.parse(raw_bytes)
                    if not msg:
                        continue  # Duplicate or empty

                    # Save raw if requested
                    if save_dir:
                        parser.save_raw(msg)

                    # Detect patterns
                    det = parser.detect(msg)
                    if det:
                        state.detections_total += 1
                        state.detection_times.append(time.time())

                        for sig in det.detected_signatures:
                            state.signature_counts[sig] += 1
                        for contract in det.detected_contracts:
                            state.contract_counts[contract] += 1

                        if det.is_liquidation_related:
                            state.liquidation_signals += 1
                            logger.info(
                                "LIQUIDATION_SIGNAL seq=%s contracts=%s sigs=%s",
                                det.sequence_hash,
                                det.detected_contracts,
                                det.detected_signatures,
                            )
                        if det.is_flash_loan:
                            state.flash_loan_signals += 1
                        if det.is_swap:
                            state.swap_signals += 1

                        display.print_detection(det)

                    # Periodic status
                    if display.should_print():
                        display.print_status(state)

                    # Write status file every ~30 messages
                    status_counter += 1
                    if status_counter % 30 == 0:
                        write_status()

        except asyncio.CancelledError:
            break
        except Exception as e:
            state.reconnects += 1
            err_msg = f"Reconnect #{state.reconnects}: {type(e).__name__}: {e}"
            state.errors.append(err_msg)
            logger.error(err_msg)

            if duration and (time.time() - start_time) >= duration:
                break

            logger.info("Retrying in %.1fs...", reconnect_delay)
            await asyncio.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60.0)

    # Final status write
    if status_file:
        final_status = {
            "timestamp": time.time(),
            "datetime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "running": False,
            "pid": os.getpid(),
            "summary": {
                "messages_total": state.messages_total,
                "detections_total": state.detections_total,
                "liquidation_signals": state.liquidation_signals,
                "reconnects": state.reconnects,
            },
        }
        status_file.write_text(json.dumps(final_status, indent=2))

    # Clean up PID file
    if pid_file and pid_file.exists():
        pid_file.unlink()

    display.print_summary(state)
    logger.info("Diagnostic session ended")


# ─── Entry Point ────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Arbitrum Sequencer Feed Diagnostic",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Interactive:
    %(prog)s                          # Run indefinitely
    %(prog)s --duration 300           # Run for 5 minutes
    %(prog)s --save-raw ./raw_msgs/   # Save all raw payloads
    %(prog)s --verbose                # Show hex previews for detections

  Daemon / Cron mode:
    %(prog)s --daemon --log-file /var/log/sequencer/feed.log
    %(prog)s --daemon --json --quiet --status-file /tmp/sequencer_status.json
    %(prog)s --daemon --pid-file /tmp/sequencer.pid --log-file /var/log/sequencer.log

  Check status of running daemon:
    cat /tmp/sequencer_diagnostic_status.json
        """,
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Run duration in seconds (default: infinite)",
    )
    parser.add_argument(
        "--save-raw",
        type=str,
        default=None,
        metavar="DIR",
        help="Directory to save raw message payloads for offline analysis",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show hex previews for detected messages",
    )
    parser.add_argument(
        "--daemon", "-d",
        action="store_true",
        help="Daemon mode: run indefinitely, log to file, write status JSON",
    )
    parser.add_argument(
        "--log-file",
        type=str,
        default=None,
        metavar="PATH",
        help="Log file path (enables file logging with rotation)",
    )
    parser.add_argument(
        "--status-file",
        type=str,
        default=None,
        metavar="PATH",
        help="Write periodic status to this JSON file for external monitoring",
    )
    parser.add_argument(
        "--pid-file",
        type=str,
        default=None,
        metavar="PATH",
        help="Write PID to this file for process management",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output JSON instead of human-readable text",
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress stdout output (use with --log-file for daemon mode)",
    )
    args = parser.parse_args()

    # Daemon mode defaults
    if args.daemon:
        if not args.log_file:
            args.log_file = str(DEFAULT_LOG_DIR / "sequencer_diagnostic.log")
        if not args.status_file:
            args.status_file = str(DEFAULT_STATUS_FILE)
        if not args.pid_file:
            args.pid_file = str(DEFAULT_PID_FILE)
        if not args.quiet:
            args.quiet = True  # Daemon mode implies quiet unless overridden

    save_dir = Path(args.save_raw) if args.save_raw else None
    log_file = Path(args.log_file) if args.log_file else None
    status_file = Path(args.status_file) if args.status_file else None
    pid_file = Path(args.pid_file) if args.pid_file else None

    try:
        asyncio.run(run_diagnostic(
            duration=args.duration,
            save_dir=save_dir,
            verbose=args.verbose,
            quiet=args.quiet,
            json_mode=args.json,
            log_file=log_file,
            status_file=status_file,
            pid_file=pid_file,
        ))
    except KeyboardInterrupt:
        if not args.quiet:
            print("\nInterrupted by user")
        sys.exit(0)


if __name__ == "__main__":
    main()
