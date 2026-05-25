"""
scanner/priority_queue.py — Sorted watchlist for liquidation candidates.

Maintains borrowers in a heap ordered by health factor (lowest first).
Only the N most-at-risk borrowers are checked each block.
"""

import heapq
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Iterator


@dataclass(order=True)
class BorrowerNode:
    """Heap node: sorted by health_factor ascending (most at-risk first)."""
    health_factor: float
    last_checked: float = field(compare=False)
    address: str = field(compare=False)
    total_debt_base: int = field(compare=False, default=0)
    total_collateral_base: int = field(compare=False, default=0)
    check_count: int = field(compare=False, default=0)

    def __hash__(self):
        return hash(self.address)


class LiquidationPriorityQueue:
    """
    Priority queue of borrowers sorted by health factor.

    Design:
    - O(log n) insert/update
    - O(k) retrieval of top k at-risk borrowers
    - Automatic eviction of stale entries (>100 blocks old)
    """

    def __init__(self, max_size: int = 10_000, check_top_n: int = 20):
        self.max_size = max_size
        self.check_top_n = check_top_n
        self._heap: List[BorrowerNode] = []
        self._index: Dict[str, BorrowerNode] = {}
        self._block_seen: Dict[str, int] = {}
        self._updates = 0

    def __len__(self) -> int:
        return len(self._index)

    def update(
        self,
        address: str,
        health_factor: float,
        total_debt_base: int = 0,
        total_collateral_base: int = 0,
        block_number: int = 0,
    ) -> None:
        """Insert or update a borrower's health factor."""
        addr = address.lower()
        now = time.time()

        if addr in self._index:
            # Remove old node (mark as stale, lazy deletion)
            old_node = self._index[addr]
            old_node.health_factor = float('inf')  # Push to bottom of heap
            old_node.address = ""  # Mark as deleted

        node = BorrowerNode(
            health_factor=health_factor,
            last_checked=now,
            address=addr,
            total_debt_base=total_debt_base,
            total_collateral_base=total_collateral_base,
            check_count=0,
        )
        self._index[addr] = node
        heapq.heappush(self._heap, node)
        self._block_seen[addr] = block_number
        self._updates += 1

        # Periodic cleanup
        if self._updates % 1000 == 0:
            self._cleanup()

        # Enforce max size: evict healthiest
        while len(self._index) > self.max_size:
            self._evict_healthiest()

    def get_at_risk(self, threshold: float = 1.15, n: Optional[int] = None) -> List[BorrowerNode]:
        """
        Return top N borrowers with HF below threshold, sorted by HF ascending.

        This is the critical path: we only health-check these borrowers each block.
        """
        if n is None:
            n = self.check_top_n

        result = []
        temp_heap = []

        # Scan heap without modifying it
        for node in self._heap:
            if not node.address:  # Deleted marker
                continue
            if node.health_factor < threshold:
                heapq.heappush(temp_heap, node)

        # Extract top N
        while len(result) < n and temp_heap:
            node = heapq.heappop(temp_heap)
            if node.address and node.address in self._index:
                result.append(node)
                node.check_count += 1

        return result

    def get_liquidatable(self, threshold: float = 1.0) -> List[BorrowerNode]:
        """Return all borrowers with HF < threshold."""
        return [n for n in self._index.values() if n.health_factor < threshold and n.address]

    def remove(self, address: str) -> bool:
        """Remove a borrower from the queue."""
        addr = address.lower()
        if addr not in self._index:
            return False
        node = self._index[addr]
        node.health_factor = float('inf')
        node.address = ""
        del self._index[addr]
        del self._block_seen[addr]
        return True

    def _cleanup(self) -> None:
        """Rebuild heap to remove stale entries."""
        fresh_heap = []
        fresh_index = {}
        fresh_blocks = {}
        for node in self._heap:
            if node.address and node.address in self._index:
                fresh_heap.append(node)
                fresh_index[node.address] = node
                if node.address in self._block_seen:
                    fresh_blocks[node.address] = self._block_seen[node.address]
        heapq.heapify(fresh_heap)
        self._heap = fresh_heap
        self._index = fresh_index
        self._block_seen = fresh_blocks

    def _evict_healthiest(self) -> None:
        """Remove the borrower with highest health factor."""
        while self._heap:
            node = heapq.heappop(self._heap)
            if node.address and node.address in self._index:
                del self._index[node.address]
                if node.address in self._block_seen:
                    del self._block_seen[node.address]
                return

    def stats(self) -> Dict:
        """Return queue statistics."""
        liquidatable = len(self.get_liquidatable())
        critical = len([n for n in self._index.values() if n.health_factor < 1.05])
        return {
            "total_tracked": len(self._index),
            "liquidatable": liquidatable,
            "critical_hf_lt_1_05": critical,
            "check_top_n": self.check_top_n,
            "max_size": self.max_size,
        }
