"""
scanner/liquidation_monitor.py — Live Aave v3 health-factor monitor.

Tracks borrowers, polls health factors, and alerts on positions near liquidation.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

import aiohttp
from eth_abi import decode, encode
from eth_utils import keccak, to_hex

from scanner.aave_v3 import (
    POOL,
    BORROW_TOPIC,
    SUPPLY_TOPIC,
    UserAccountData,
    format_hf_status,
)

logger = logging.getLogger("liquidation_monitor")

# Health-factor polling cadence (seconds)
DEFAULT_POLL_INTERVAL: float = 12.0  # ~1 block on Arbitrum

# HF threshold to trigger an alert
ALERT_HF_THRESHOLD: float = 1.05

# Track user history to see HF trend
MAX_HISTORY_POINTS: int = 20


@dataclass
class MonitoredUser:
    address: str
    first_seen_block: int
    history: List[tuple] = field(default_factory=list)  # [(block, hf, collateral, debt), ...]

    def add_reading(self, block: int, data: UserAccountData) -> None:
        self.history.append((
            block,
            data.health_factor_float,
            data.total_collateral_base,
            data.total_debt_base,
        ))
        if len(self.history) > MAX_HISTORY_POINTS:
            self.history.pop(0)

    @property
    def latest_hf(self) -> Optional[float]:
        if not self.history:
            return None
        return self.history[-1][1]

    @property
    def hf_trend(self) -> float:
        """Return HF delta over last two readings."""
        if len(self.history) < 2:
            return 0.0
        return self.history[-1][1] - self.history[-2][1]

    @property
    def is_liquidatable(self) -> bool:
        hf = self.latest_hf
        return hf is not None and hf < 1.0


class LiquidationMonitor:
    """
    Monitors Aave v3 borrowers' health factors via RPC polling.

    Usage (async):
        monitor = LiquidationMonitor(rpc_url)
        await monitor.bootstrap_borrowers(from_block=latest-100000)
        await monitor.poll_loop()
    """

    def __init__(
        self,
        rpc_url: str,
        pool_address: str = POOL,
        alert_threshold: float = ALERT_HF_THRESHOLD,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
    ):
        self.rpc_url = rpc_url
        self.pool = pool_address
        self.alert_threshold = alert_threshold
        self.poll_interval = poll_interval
        self.users: Dict[str, MonitoredUser] = {}
        self.known_alerted: Set[str] = set()
        self.block_number: int = 0

    async def _rpc_call(self, method: str, params: list) -> dict:
        """Single RPC call."""
        async with aiohttp.ClientSession() as session:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            }
            async with session.post(
                self.rpc_url,
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as resp:
                return await resp.json()

    async def get_latest_block(self) -> int:
        """Fetch current block number."""
        result = await self._rpc_call("eth_blockNumber", [])
        self.block_number = int(result["result"], 16)
        return self.block_number

    async def fetch_borrow_events(self, from_block: int, to_block: int) -> Set[str]:
        """Return set of unique borrower addresses from logs."""
        borrowers: Set[str] = set()
        # Aave can be slow; chunk into 2K blocks
        chunk_size = 2000
        for chunk_start in range(from_block, to_block + 1, chunk_size):
            chunk_end = min(chunk_start + chunk_size - 1, to_block)
            result = await self._rpc_call(
                "eth_getLogs",
                [
                    {
                        "address": self.pool,
                        "topics": [BORROW_TOPIC],
                        "fromBlock": to_hex(chunk_start),
                        "toBlock": to_hex(chunk_end),
                    }
                ],
            )
            logs = result.get("result", [])
            for log in logs:
                # topics[2] = user address
                user = "0x" + log["topics"][2][-40:]
                borrowers.add(user)
        return borrowers

    async def fetch_user_account_data(self, user: str) -> Optional[UserAccountData]:
        """Call getUserAccountData(address) on the Aave Pool."""
        selector = keccak(text="getUserAccountData(address)")[:4]
        calldata = "0x" + selector.hex() + user[2:].rjust(64, "0")
        result = await self._rpc_call(
            "eth_call",
            [{"to": self.pool, "data": calldata}, "latest"],
        )
        raw = result.get("result", "0x")
        if len(raw) < 2:
            return None
        try:
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
        except Exception:
            return None

    async def bootstrap_borrowers(self, lookback_blocks: int = 100_000) -> int:
        """Populate user set from recent Borrow events."""
        latest = await self.get_latest_block()
        from_block = max(latest - lookback_blocks, 0)
        borrowers = await self.fetch_borrow_events(from_block, latest)
        for user in borrowers:
            if user not in self.users:
                self.users[user] = MonitoredUser(
                    address=user,
                    first_seen_block=latest,
                )
        logger.info(
            "Bootstrapped %d borrowers from blocks %d-%d",
            len(borrowers),
            from_block,
            latest,
        )
        return len(borrowers)

    async def scan_all(self) -> List[MonitoredUser]:
        """Poll health factors for all tracked users. Returns those near liquidation."""
        latest = await self.get_latest_block()
        alerted: List[MonitoredUser] = []

        # Batch into small groups to avoid hammering RPC
        batch_size = 10
        user_list = list(self.users.values())
        for i in range(0, len(user_list), batch_size):
            batch = user_list[i : i + batch_size]
            tasks = [self.fetch_user_account_data(u.address) for u in batch]
            results = await asyncio.gather(*tasks)
            for user, data in zip(batch, results):
                if data is None:
                    continue
                user.add_reading(latest, data)
                hf = data.health_factor_float
                if hf < self.alert_threshold:
                    alerted.append(user)
                    if user.is_liquidatable and user.address not in self.known_alerted:
                        self.known_alerted.add(user.address)
                        logger.warning(
                            "🚨 LIQUIDATABLE: %s | HF=%.4f | Collat=%d | Debt=%d | Trend=%+.4f",
                            user.address,
                            hf,
                            data.total_collateral_base,
                            data.total_debt_base,
                            user.hf_trend,
                        )

        return alerted

    async def poll_loop(self, duration_sec: Optional[float] = None) -> None:
        """Continuous polling loop."""
        start = asyncio.get_event_loop().time()
        iteration = 0
        while True:
            iteration += 1
            alerted = await self.scan_all()
            near_count = len([u for u in self.users.values() if u.latest_hf is not None and u.latest_hf < self.alert_threshold])
            logger.info(
                "Poll #%d | Block=%d | Tracked=%d | Near liquidation=%d | Liquidatable=%d",
                iteration,
                self.block_number,
                len(self.users),
                near_count,
                len(self.known_alerted),
            )
            if duration_sec and (asyncio.get_event_loop().time() - start) >= duration_sec:
                break
            await asyncio.sleep(self.poll_interval)

    def print_status(self, top_n: int = 20) -> None:
        """Print current health-factor leaderboard."""
        active = [
            u for u in self.users.values()
            if u.latest_hf is not None and u.history[-1][3] > 0  # has debt
        ]
        active.sort(key=lambda u: u.latest_hf or float("inf"))
        print(f"\n{'='*80}")
        print(f" AAVE v3 BORROWER HEALTH FACTORS (sorted by lowest HF)")
        print(f"{'='*80}")
        print(f" {'Address':42} {'HF':>10} {'Collateral':>14} {'Debt':>14} {'Status'}")
        print(f"{'-'*80}")
        for u in active[:top_n]:
            _, hf, collat, debt = u.history[-1]
            status = format_hf_status(hf)
            print(f" {u.address:42} {hf:>10.4f} {collat:>14} {debt:>14} {status}")
        print(f"{'='*80}\n")


async def main():
    import os, sys

    rpc_url = os.getenv("ALCHEMY_HTTP_URL")
    if not rpc_url:
        print("Set ALCHEMY_HTTP_URL env var", file=sys.stderr)
        raise SystemExit(1)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    )

    monitor = LiquidationMonitor(rpc_url=rpc_url)
    count = await monitor.bootstrap_borrowers(lookback_blocks=200_000)
    if count == 0:
        logger.warning("No borrowers found in last 200K blocks")
        return

    # One scan + print
    await monitor.scan_all()
    monitor.print_status(top_n=30)

    # Optional: start continuous loop
    # await monitor.poll_loop(duration_sec=3600)


if __name__ == "__main__":
    asyncio.run(main())
