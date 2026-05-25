"""
scanner/flashbots_relay.py — Flashbots bundle submission for Ethereum Mainnet.

MANDATORY for mainnet: public mempool = frontrunning = guaranteed failure.
Flashbots bundles land in target block or cost $0 (revert protection).
"""

import json
import logging
import time
import asyncio
from typing import List, Optional, Dict, Any

import aiohttp
from eth_account import Account
from eth_account.datastructures import SignedTransaction
from eth_utils import keccak, to_hex

logger = logging.getLogger("flashbots_relay")


class FlashbotsRelay:
    """
    Submit transaction bundles to Flashbots relay.

    Usage:
        relay = FlashbotsRelay(
            relay_url="https://relay.flashbots.net",
            auth_private_key="0x...",  # Flashbots auth key (separate from bot wallet)
        )
        bundle = [signed_tx1, signed_tx2]
        result = await relay.send_bundle(bundle, target_block=current_block + 1)
    """

    def __init__(
        self,
        relay_url: str,
        auth_private_key: str,
        timeout: float = 10.0,
    ):
        self.relay_url = relay_url.rstrip("/")
        self.auth_account = Account.from_key(auth_private_key)
        self.timeout = timeout

    def _sign_request(self, payload: Dict[str, Any]) -> str:
        """
        Sign the RPC payload with the Flashbots auth key.
        Flashbots requires X-Flashbots-Signature header.
        """
        message = json.dumps(payload)
        signature = self.auth_account.sign_message(
            text=message
        )
        return f"{self.auth_account.address}:{signature.signature.hex()}"

    async def send_bundle(
        self,
        signed_transactions: List[SignedTransaction],
        target_block: int,
        min_timestamp: Optional[int] = None,
        max_timestamp: Optional[int] = None,
        reverting_tx_hashes: Optional[List[str]] = None,
        replacement_uuid: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Submit a bundle to Flashbots.

        Args:
            signed_transactions: List of signed transaction objects
            target_block: Block number to target for inclusion
            min_timestamp: Minimum Unix timestamp for inclusion
            max_timestamp: Maximum Unix timestamp for inclusion
            reverting_tx_hashes: List of tx hashes allowed to revert
            replacement_uuid: UUID for bundle replacement/cancellation

        Returns:
            Flashbots response dict with bundleHash
        """
        txs = ["0x" + tx.raw_transaction.hex() for tx in signed_transactions]

        params = [{
            "txs": txs,
            "blockNumber": hex(target_block),
        }]

        if min_timestamp is not None:
            params[0]["minTimestamp"] = min_timestamp
        if max_timestamp is not None:
            params[0]["maxTimestamp"] = max_timestamp
        if reverting_tx_hashes:
            params[0]["revertingTxHashes"] = reverting_tx_hashes
        if replacement_uuid:
            params[0]["replacementUuid"] = replacement_uuid

        payload = {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000),
            "method": "eth_sendBundle",
            "params": params,
        }

        signature = self._sign_request(payload)
        headers = {
            "Content-Type": "application/json",
            "X-Flashbots-Signature": signature,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.relay_url,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise RuntimeError(f"Flashbots relay error {resp.status}: {text}")

                result = await resp.json()
                if "error" in result:
                    raise RuntimeError(f"Flashbots RPC error: {result['error']}")

                logger.info(
                    "Bundle submitted: target_block=%d, txs=%d, bundleHash=%s",
                    target_block, len(txs), result.get("result", {}).get("bundleHash", "?")
                )
                return result

    async def simulate_bundle(
        self,
        signed_transactions: List[SignedTransaction],
        target_block: int,
        state_block: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Simulate a bundle against a specific block state.

        Returns simulation results including gas used and profit.
        """
        txs = ["0x" + tx.raw_transaction.hex() for tx in signed_transactions]

        params = [{
            "txs": txs,
            "blockNumber": hex(target_block),
        }]
        if state_block is not None:
            params[0]["stateBlockNumber"] = hex(state_block)

        payload = {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000),
            "method": "eth_simulateV1",  # or eth_callBundle for older relay
            "params": params,
        }

        signature = self._sign_request(payload)
        headers = {
            "Content-Type": "application/json",
            "X-Flashbots-Signature": signature,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.relay_url,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                result = await resp.json()
                return result

    async def get_bundle_stats(
        self,
        bundle_hash: str,
        block_number: int,
    ) -> Dict[str, Any]:
        """Get inclusion stats for a submitted bundle."""
        payload = {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000),
            "method": "flashbots_getBundleStats",
            "params": [{
                "bundleHash": bundle_hash,
                "blockNumber": hex(block_number),
            }],
        }

        signature = self._sign_request(payload)
        headers = {
            "Content-Type": "application/json",
            "X-Flashbots-Signature": signature,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.relay_url,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                return await resp.json()

    async def get_user_stats(self) -> Dict[str, Any]:
        """Get historical stats for this auth key."""
        payload = {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000),
            "method": "flashbots_getUserStats",
            "params": [],
        }

        signature = self._sign_request(payload)
        headers = {
            "Content-Type": "application/json",
            "X-Flashbots-Signature": signature,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.relay_url,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                return await resp.json()


class MultiRelayManager:
    """
    Submit to multiple builders/relays simultaneously for highest inclusion rate.

    Builders: Flashbots, BeaverBuild, Titan, rsync, etc.
    """

    BUILDER_ENDPOINTS = {
        "flashbots": "https://relay.flashbots.net",
        "beaverbuild": "https://rpc.beaverbuild.org",
        "titan": "https://rpc.titanbuilder.net",
        "rsync": "https://rsync-builder.xyz",
        "ethbuilder": "https://eth-builder.com",
    }

    def __init__(self, auth_private_key: str):
        self.auth_private_key = auth_private_key
        self.relays: Dict[str, FlashbotsRelay] = {}

    def add_relay(self, name: str, endpoint: str) -> None:
        """Add a builder endpoint."""
        self.relays[name] = FlashbotsRelay(endpoint, self.auth_private_key)

    def add_default_relays(self) -> None:
        """Add all known builder endpoints."""
        for name, endpoint in self.BUILDER_ENDPOINTS.items():
            self.add_relay(name, endpoint)

    async def broadcast_to_all(
        self,
        signed_transactions: List[SignedTransaction],
        target_block: int,
    ) -> Dict[str, Any]:
        """
        Submit bundle to ALL configured relays simultaneously.
        Return first successful result.
        """
        if not self.relays:
            raise RuntimeError("No relays configured")

        tasks = {
            name: asyncio.create_task(
                relay.send_bundle(signed_transactions, target_block)
            )
            for name, relay in self.relays.items()
        }

        done, pending = await asyncio.wait(
            tasks.values(),
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Cancel pending
        for task in pending:
            task.cancel()

        # Return first result
        for task in done:
            try:
                result = task.result()
                return result
            except Exception as e:
                logger.warning("Relay failed: %s", e)

        raise RuntimeError("All relays failed")
