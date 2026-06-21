"""
rpc_provider.py — Centralised RPC provider selection with health-check + rotation.

Drop-in replacement for scattered os.getenv("QUICKNODE_HTTP_URL") calls.
Call once at startup; returns the best available AsyncRPCClient.

Usage:
    from rpc_provider import get_rpc_provider, RPCProviderConfig

    config = RPCProviderConfig()               # reads from .env
    rpc = await get_rpc_provider(config, purpose="exec")
    rpc_read = await get_rpc_provider(config, purpose="read")
"""
import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Optional, List, Tuple

logger = logging.getLogger(__name__)


@dataclass
class Provider:
    """A single RPC endpoint with metadata."""
    name: str
    url: str
    timeout: float = 3.0  # health check timeout


@dataclass
class RPCProviderConfig:
    """Ordered provider priority for each purpose. Reads from .env."""

    # Priority order (first = preferred)
    exec_providers: List[Provider] = field(default_factory=list)
    read_providers: List[Provider] = field(default_factory=list)
    light_providers: List[Provider] = field(default_factory=list)
    submit_providers: List[Provider] = field(default_factory=list)

    # Global timeout for health checks
    health_check_timeout: float = 3.0

    @classmethod
    def from_env(cls) -> "RPCProviderConfig":
        """Build config from environment variables.

        Rotation (all tiers): 1RPC → PublicNode → BlastAPI → PublicArb1
        Chainstack, Alchemy, DRPC removed — all monthly quotas exhausted or 500 on eth_call.
        """
        rpc_1rpc       = os.getenv("RPC_1RPC",        "https://1rpc.io/arb")
        rpc_publicnode = os.getenv("RPC_PUBLICNODE",   "https://arbitrum-one.publicnode.com")
        rpc_blastapi   = os.getenv("RPC_BLASTAPI",     "https://arbitrum-one.public.blastapi.io")
        rpc_public_arb = os.getenv("RPC_PUBLIC_ARB1",  "https://arb1.arbitrum.io/rpc")

        providers = [
            Provider("1RPC",        rpc_1rpc),
            Provider("PublicNode",  rpc_publicnode),
            Provider("BlastAPI",    rpc_blastapi),
            Provider("PublicArb1",  rpc_public_arb, timeout=5.0),
        ]

        return cls(
            exec_providers=list(providers),
            read_providers=list(providers),
            light_providers=list(providers),
            submit_providers=list(providers),
            health_check_timeout=3.0,
        )


async def get_rpc_provider(
    config: RPCProviderConfig,
    purpose: str = "read",
    request_timeout: float = 10.0,
):
    """
    Return the first healthy AsyncRPCClient from the priority list.

    Tries each provider in order with a quick eth_blockNumber health check.
    Falls through to the next if the check times out or fails.

    Args:
        config: Provider priority configuration
        purpose: "exec", "read", or "submit" — selects the priority list
        request_timeout: Timeout for subsequent RPC calls on the client

    Returns:
        Connected AsyncRPCClient

    Raises:
        RuntimeError: If no provider responds
    """
    # Lazy import to avoid circular dependency at module level
    from async_web3 import AsyncRPCClient

    providers = getattr(config, f"{purpose}_providers", config.read_providers)
    if not providers:
        providers = config.read_providers

    last_error = None

    for i, provider in enumerate(providers):
        marker = "★" if i == 0 else "↓"
        try:
            client = AsyncRPCClient(
                http_url=provider.url,
                request_timeout=request_timeout,
            )
            await client.connect()

            # Quick health check — get latest block
            block = await asyncio.wait_for(
                client.get_block_number(),
                timeout=config.health_check_timeout,
            )
            logger.info(
                f"[RPCProvider] {marker} {provider.name} healthy — block {block}"
            )
            return client

        except asyncio.TimeoutError:
            logger.warning(
                f"[RPCProvider] {provider.name} timed out "
                f"({config.health_check_timeout}s) — trying next"
            )
            last_error = TimeoutError(f"{provider.name}: health check timeout")
        except Exception as e:
            logger.warning(
                f"[RPCProvider] {provider.name} failed: {e} — trying next"
            )
            last_error = e

    raise RuntimeError(
        f"No healthy RPC provider found for purpose='{purpose}'. "
        f"Last error: {last_error}"
    )


async def get_rpc_providers(
    config: RPCProviderConfig,
    purposes: List[str],
    request_timeout: float = 10.0,
):
    """
    Get multiple RPC clients for different purposes in parallel.
    Returns a tuple in the same order as `purposes`.
    """
    tasks = [
        get_rpc_provider(config, purpose=p, request_timeout=request_timeout)
        for p in purposes
    ]
    results = await asyncio.gather(*tasks)
    return tuple(results)
