"""
scanner/multicall_batch.py — Batch RPC calls via Multicall3 for efficiency.

Replaces N sequential eth_call round-trips with 1 batched call.
Critical for sub-second health factor checks on large watchlists.
"""

from typing import List, Tuple, Optional
from eth_abi import encode, decode
from eth_utils import keccak
from web3 import Web3


# Multicall3 aggregate3 selector
MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"

# ABI fragment for aggregate3
def encode_aggregate3(calls: List[Tuple[str, str, bool]]) -> str:
    """
    Encode Multicall3.aggregate3(calls) calldata.

    Args:
        calls: List of (target, callData, allowFailure) tuples.
               callData should be hex string starting with 0x.

    Returns:
        Hex string calldata.
    """
    selector = keccak(text="aggregate3((address target,bool allowFailure,bytes callData)[]")[:4]

    # Build the tuple array encoding
    encoded = encode(
        ["(address,bool,bytes)[]"],
        [[(target, allow_failure, bytes.fromhex(cd[2:])) for target, cd, allow_failure in calls]],
    )
    return "0x" + selector.hex() + encoded.hex()


def decode_aggregate3_result(raw: bytes) -> List[Tuple[bool, bytes]]:
    """
    Decode Multicall3.aggregate3 return data.

    Returns list of (success, returnData) tuples.
    """
    # The return type is: (bool success, bytes returnData)[]
    # eth_abi can't directly decode dynamic arrays of dynamic bytes easily,
    # so we decode manually.

    if len(raw) < 64:
        return []

    # Skip first 32 bytes (offset to array)
    # Second 32 bytes: array length
    array_len = int.from_bytes(raw[32:64], "big")
    results = []

    offset = 64
    for _ in range(array_len):
        # Each element: success (32 bytes), offset to returnData (32 bytes), returnData
        success = bool(int.from_bytes(raw[offset:offset + 32], "big"))
        data_offset = int.from_bytes(raw[offset + 32:offset + 64], "big")
        # data_offset is relative to start of this element's tuple
        # Actually in aggregate3, each element is (bool, bytes) where bytes is dynamic
        # The encoding is: success, offset_to_bytes, length, data...
        # Let me use eth_abi for each element individually

        # Re-encode just this element and decode
        elem_start = offset
        # Find the actual bytes data
        # success = raw[offset:offset+32]
        # rel_offset = raw[offset+32:offset+64] -> points to start of bytes within tuple
        rel_offset = int.from_bytes(raw[offset + 32:offset + 64], "big")
        bytes_start = offset + 64 + rel_offset - 32  # adjust for tuple packing
        if bytes_start < len(raw):
            data_len = int.from_bytes(raw[bytes_start:bytes_start + 32], "big")
            data = raw[bytes_start + 32:bytes_start + 32 + data_len]
            results.append((success, data))
        else:
            results.append((success, b""))
        offset += 64 + 32 + ((len(results[-1][1]) + 31) // 32) * 32 if results else 64

    return results


class MulticallBatcher:
    """Batch eth_call requests via Multicall3."""

    def __init__(self, w3: Web3, multicall_address: str = MULTICALL3_ADDRESS):
        self.w3 = w3
        self.multicall_address = self.w3.to_checksum_address(multicall_address)

    def batch_health_factors(
        self,
        users: List[str],
        aave_pool: str,
    ) -> List[Optional[Tuple[int, int, int, int, int, int]]]:
        """
        Batch fetch getUserAccountData for multiple users.

        Returns list of (totalCollateral, totalDebt, availableBorrows, ltv, liqThreshold, hf)
        or None if call failed for that user.
        """
        selector = keccak(text="getUserAccountData(address)")[:4]
        calls = []
        for user in users:
            calldata = "0x" + selector.hex() + user[2:].rjust(64, "0")
            calls.append((aave_pool, calldata, True))  # allowFailure = True

        aggregate_calldata = encode_aggregate3(calls)

        try:
            raw = self.w3.eth.call({"to": self.multicall_address, "data": aggregate_calldata})
        except Exception:
            # Fallback: return all None, caller will retry individually
            return [None] * len(users)

        # Decode results
        results = []
        # Manual decode since aggregate3 returns (bool, bytes)[]
        # Use a simpler approach: decode the array length, then iterate
        if len(raw) < 64:
            return [None] * len(users)

        array_len = int.from_bytes(raw[32:64], "big")
        if array_len != len(users):
            return [None] * len(users)

        pos = 64
        for _ in range(array_len):
            # Each tuple element: bool success, bytes returnData
            # In ABI encoding: success (32), offset to bytes (32), bytes length (32), bytes data
            success = bool(int.from_bytes(raw[pos:pos + 32], "big"))
            rel_offset = int.from_bytes(raw[pos + 32:pos + 64], "big")
            # The relative offset points from the start of the tuple element
            bytes_pos = pos + 32 + rel_offset
            if bytes_pos + 32 > len(raw):
                results.append(None)
                pos += 64
                continue

            data_len = int.from_bytes(raw[bytes_pos:bytes_pos + 32], "big")
            data = raw[bytes_pos + 32:bytes_pos + 32 + data_len]

            if not success or len(data) < 192:
                results.append(None)
            else:
                try:
                    decoded = decode(
                        ["uint256", "uint256", "uint256", "uint256", "uint256", "uint256"],
                        data,
                    )
                    results.append(tuple(int(x) for x in decoded))
                except Exception:
                    results.append(None)

            # Advance position: 64 (header) + 32 (length) + padded data
            pos += 64 + 32 + ((data_len + 31) // 32) * 32

        return results

    def batch_reserve_data(
        self,
        user: str,
        assets: List[Tuple[str, str, int]],
        pool_data_provider: str,
    ) -> List[Optional[Tuple[int, int, int, int, int, int, int, int, bool]]]:
        """
        Batch fetch getUserReserveData for a single user across multiple assets.

        Returns list of reserve data tuples or None per asset.
        """
        selector = keccak(text="getUserReserveData(address,address)")[:4]
        calls = []
        for asset, _symbol, _decimals in assets:
            calldata = "0x" + selector.hex() + encode(
                ["address", "address"],
                [asset, user],
            ).hex()
            calls.append((pool_data_provider, calldata, True))

        aggregate_calldata = encode_aggregate3(calls)

        try:
            raw = self.w3.eth.call({"to": self.multicall_address, "data": aggregate_calldata})
        except Exception:
            return [None] * len(assets)

        results = []
        if len(raw) < 64:
            return [None] * len(assets)

        array_len = int.from_bytes(raw[32:64], "big")
        if array_len != len(assets):
            return [None] * len(assets)

        pos = 64
        for _ in range(array_len):
            success = bool(int.from_bytes(raw[pos:pos + 32], "big"))
            rel_offset = int.from_bytes(raw[pos + 32:pos + 64], "big")
            bytes_pos = pos + 32 + rel_offset
            if bytes_pos + 32 > len(raw):
                results.append(None)
                pos += 64
                continue

            data_len = int.from_bytes(raw[bytes_pos:bytes_pos + 32], "big")
            data = raw[bytes_pos + 32:bytes_pos + 32 + data_len]

            if not success or len(data) < 256:
                results.append(None)
            else:
                try:
                    decoded = decode(
                        ["uint256", "uint256", "uint256", "uint256", "uint256", "uint256", "uint256", "uint40", "bool"],
                        data,
                    )
                    results.append(tuple(decoded))
                except Exception:
                    results.append(None)

            pos += 64 + 32 + ((data_len + 31) // 32) * 32

        return results
