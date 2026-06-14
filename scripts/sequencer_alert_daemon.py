#!/usr/bin/env python3
"""
scripts/sequencer_alert_daemon.py — Telegram alerting for sequencer diagnostic.

Watches /tmp/sequencer_diagnostic_status.json and emits alerts on:
  • liquidation_signals > 0  → immediate alert
  • errors_count spikes      → alert if errors increased since last check
  • reconnects spike         → alert if reconnects increased since last check
  • silent feed              → alert if last_message_age_ms > 30_000

Usage (standalone):
    python3 scripts/sequencer_alert_daemon.py --once

Usage (daemon loop):
    python3 scripts/sequencer_alert_daemon.py --interval 15

Exit codes:
    0  — check completed, no alerts
    1  — alert emitted (useful for cron wrappers)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

STATUS_PATH = "/tmp/sequencer_diagnostic_status.json"
STATE_PATH = "/tmp/sequencer_alert_state.json"


@dataclass
class AlertState:
    last_errors: int = 0
    last_reconnects: int = 0
    last_liquidation_signals: int = 0
    last_check_ts: float = 0.0
    alerted_errors: bool = False
    alerted_reconnects: bool = False
    alerted_silent: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "AlertState":
        return cls(**d)


def load_state() -> AlertState:
    try:
        with open(STATE_PATH, "r") as f:
            return AlertState.from_dict(json.load(f))
    except Exception:
        return AlertState()


def save_state(state: AlertState) -> None:
    with open(STATE_PATH, "w") as f:
        json.dump(state.to_dict(), f, indent=2)


def load_status(path: str = STATUS_PATH) -> Optional[Dict[str, Any]]:
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception as e:
        print(f"ALERT: Cannot read status file: {e}", file=sys.stderr)
        return None


def format_alert(status: Dict[str, Any], reason: str, details: str) -> str:
    ts = status.get("datetime", datetime.now(timezone.utc).isoformat())
    return (
        f"🚨 *Sequencer Alert: {reason}*\n"
        f"```\n"
        f"Time: {ts}\n"
        f"Reason: {details}\n"
        f"Messages: {status.get('messages_total', 0):,}\n"
        f"Detections: {status.get('detections_total', 0)}\n"
        f"Liquidations: {status.get('liquidation_signals', 0)}\n"
        f"Reconnects: {status.get('reconnects', 0)}\n"
        f"Errors: {status.get('errors_count', 0)}\n"
        f"Connected: {status.get('connected', False)}\n"
        f"```"
    )


def check_and_alert(status: Dict[str, Any], state: AlertState, quiet: bool = False) -> bool:
    alerts_emitted = False

    # 1. Liquidation signal
    liq = status.get("liquidation_signals", 0)
    if liq > state.last_liquidation_signals:
        msg = format_alert(
            status,
            "LIQUIDATION DETECTED",
            f"Signals jumped from {state.last_liquidation_signals} → {liq}",
        )
        if not quiet:
            print(msg)
        state.last_liquidation_signals = liq
        alerts_emitted = True

    # 2. Error spike (only alert once per spike, reset when errors stop growing)
    errs = status.get("errors_count", 0)
    if errs > state.last_errors:
        if not state.alerted_errors:
            msg = format_alert(
                status,
                "ERROR SPIKE",
                f"Errors increased from {state.last_errors} → {errs}",
            )
            if not quiet:
                print(msg)
            state.alerted_errors = True
            alerts_emitted = True
        state.last_errors = errs
    elif errs == state.last_errors and state.alerted_errors:
        # Reset alert latch when errors stabilize
        state.alerted_errors = False

    # 3. Reconnect spike
    recon = status.get("reconnects", 0)
    if recon > state.last_reconnects:
        if not state.alerted_reconnects:
            msg = format_alert(
                status,
                "RECONNECT SPIKE",
                f"Reconnects increased from {state.last_reconnects} → {recon}",
            )
            if not quiet:
                print(msg)
            state.alerted_reconnects = True
            alerts_emitted = True
        state.last_reconnects = recon
    elif recon == state.last_reconnects and state.alerted_reconnects:
        state.alerted_reconnects = False

    # 4. Silent feed (no message for 30s)
    age_ms = status.get("last_message_age_ms", 0)
    connected = status.get("connected", False)
    if (age_ms > 30_000 or not connected) and not state.alerted_silent:
        msg = format_alert(
            status,
            "FEED SILENT",
            f"Last message {age_ms/1000:.1f}s ago, connected={connected}",
        )
        if not quiet:
            print(msg)
        state.alerted_silent = True
        alerts_emitted = True
    elif age_ms < 10_000 and connected and state.alerted_silent:
        state.alerted_silent = False

    state.last_check_ts = time.time()
    return alerts_emitted


def main() -> int:
    parser = argparse.ArgumentParser(description="Sequencer diagnostic alert daemon")
    parser.add_argument("--once", action="store_true", help="Run one check and exit")
    parser.add_argument("--interval", type=int, default=15, help="Seconds between checks")
    parser.add_argument("--quiet", action="store_true", help="Suppress non-alert output")
    parser.add_argument("--status-path", default=STATUS_PATH, help="Override status JSON path")
    args = parser.parse_args()

    status_path = args.status_path

    if args.once:
        status = load_status(status_path)
        if status is None:
            return 1
        state = load_state()
        alerted = check_and_alert(status, state, quiet=args.quiet)
        save_state(state)
        return 1 if alerted else 0

    # Daemon loop
    print(f"[{datetime.now(timezone.utc).isoformat()}] Alert daemon started (interval={args.interval}s)")
    while True:
        status = load_status(status_path)
        if status is not None:
            state = load_state()
            alerted = check_and_alert(status, state, quiet=args.quiet)
            save_state(state)
            if alerted and not args.quiet:
                print("--- alert emitted ---")
        time.sleep(args.interval)


if __name__ == "__main__":
    sys.exit(main())
