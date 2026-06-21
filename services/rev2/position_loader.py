"""
position_loader.py — Real on-chain Aave V3 position data via Multicall3
Replaces the fabricated CSV collateral_usd * 1e6 bootstrap in pipeline.py

Usage:
    loader = PositionLoader(w3, pool_contract, multicall3_addr)
    await loader.bootstrap(watchlist)           # run once at startup
    await loader.refresh_hot(hf_threshold=1.2) # run every N blocks
    pos = loader.get(address)                   # called by HF engine

Requires:
    pip install web3 asyncio
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

from web3 import AsyncWeb3
from web3 import Web3  # for keccak, codec, sync ops
from web3.contract import AsyncContract

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Multicall3 — deployed at same address on all major EVM chains
# ---------------------------------------------------------------------------
MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"
AAVE_POOL_DATA_PROVIDER = "0x243Aa95cAC2a25651eda86e80bEe66114413c43b"  # from AddressesProvider.getPoolDataProvider()

MULTICALL3_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"internalType": "address", "name": "target", "type": "address"},
                    {"internalType": "bool",    "name": "allowFailure", "type": "bool"},
                    {"internalType": "bytes",   "name": "callData", "type": "bytes"},
                ],
                "internalType": "struct Multicall3.Call3[]",
                "name": "calls",
                "type": "tuple[]",
            }
        ],
        "name": "aggregate3",
        "outputs": [
            {
                "components": [
                    {"internalType": "bool",  "name": "success", "type": "bool"},
                    {"internalType": "bytes", "name": "returnData", "type": "bytes"},
                ],
                "internalType": "struct Multicall3.Result[]",
                "name": "returnData",
                "type": "tuple[]",
            }
        ],
        "stateMutability": "payable",
        "type": "function",
    }
]

# Aave V3 Pool — getUserAccountData return signature
# (totalCollateralBase, totalDebtBase, availableBorrowsBase,
#  currentLiquidationThreshold, ltv, healthFactor)
POOL_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "user", "type": "address"}],
        "name": "getUserAccountData",
        "outputs": [
            {"internalType": "uint256", "name": "totalCollateralBase",          "type": "uint256"},
            {"internalType": "uint256", "name": "totalDebtBase",                "type": "uint256"},
            {"internalType": "uint256", "name": "availableBorrowsBase",         "type": "uint256"},
            {"internalType": "uint256", "name": "currentLiquidationThreshold", "type": "uint256"},
            {"internalType": "uint256", "name": "ltv",                          "type": "uint256"},
            {"internalType": "uint256", "name": "healthFactor",                 "type": "uint256"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "getReservesList",
        "outputs": [{"internalType": "address[]", "name": "", "type": "address[]"}],
        "stateMutability": "view",
        "type": "function",
    },
]

# getUserReserveData — per (asset, user)
# Returns currentATokenBalance, currentStableDebt, currentVariableDebt,
#         principalStableDebt, scaledVariableDebt, stableBorrowRate,
#         liquidityRate, stableRateLastUpdated, usageAsCollateralEnabled
USER_RESERVE_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "asset", "type": "address"},
            {"internalType": "address", "name": "user",  "type": "address"},
        ],
        "name": "getUserReserveData",
        "outputs": [
            {"internalType": "uint256", "name": "currentATokenBalance",     "type": "uint256"},
            {"internalType": "uint256", "name": "currentStableDebt",        "type": "uint256"},
            {"internalType": "uint256", "name": "currentVariableDebt",      "type": "uint256"},
            {"internalType": "uint256", "name": "principalStableDebt",      "type": "uint256"},
            {"internalType": "uint256", "name": "scaledVariableDebt",       "type": "uint256"},
            {"internalType": "uint256", "name": "stableBorrowRate",         "type": "uint256"},
            {"internalType": "uint256", "name": "liquidityRate",            "type": "uint256"},
            {"internalType": "uint16",  "name": "stableRateLastUpdated",    "type": "uint16"},
            {"internalType": "bool",    "name": "usageAsCollateralEnabled", "type": "bool"},
        ],
        "stateMutability": "view",
        "type": "function",
    }
]

# getReserveData — per asset (for liquidation bonus + threshold)
RESERVE_DATA_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "asset", "type": "address"}],
        "name": "getReserveData",
        "outputs": [
            {
                "components": [
                    {"internalType": "uint256", "name": "configuration",        "type": "uint256"},
                    {"internalType": "uint128", "name": "liquidityIndex",       "type": "uint128"},
                    {"internalType": "uint128", "name": "currentLiquidityRate", "type": "uint128"},
                    {"internalType": "uint128", "name": "variableBorrowIndex",  "type": "uint128"},
                    {"internalType": "uint128", "name": "currentVariableBorrowRate", "type": "uint128"},
                    {"internalType": "uint128", "name": "currentStableBorrowRate",   "type": "uint128"},
                    {"internalType": "uint40",  "name": "lastUpdateTimestamp",       "type": "uint40"},
                    {"internalType": "uint16",  "name": "id",                        "type": "uint16"},
                    {"internalType": "address", "name": "aTokenAddress",             "type": "address"},
                    {"internalType": "address", "name": "stableDebtTokenAddress",    "type": "address"},
                    {"internalType": "address", "name": "variableDebtTokenAddress",  "type": "address"},
                    {"internalType": "address", "name": "interestRateStrategyAddress","type": "address"},
                    {"internalType": "uint128", "name": "accruedToTreasury",         "type": "uint128"},
                    {"internalType": "uint128", "name": "unbacked",                  "type": "uint128"},
                    {"internalType": "uint128", "name": "isolationModeTotalDebt",    "type": "uint128"},
                ],
                "internalType": "struct DataTypes.ReserveData",
                "name": "",
                "type": "tuple",
            }
        ],
        "stateMutability": "view",
        "type": "function",
    }
]

# ReserveConfigurationMap bitmask positions (Aave V3)
# Bits 0-15:   LTV
# Bits 16-31:  Liquidation threshold
# Bits 32-47:  Liquidation bonus
# Bit  56:     Active flag
LTV_MASK        = 0xFFFF
THRESHOLD_SHIFT = 16
THRESHOLD_MASK  = 0xFFFF
BONUS_SHIFT     = 32
BONUS_MASK      = 0xFFFF

WAD = 10**18  # Aave HF is WAD-scaled

# Pre-computed function selectors (no RPC needed)
SELECTOR_GET_USER_ACCOUNT_DATA = Web3.keccak(text="getUserAccountData(address)")[:4]
SELECTOR_GET_USER_RESERVE_DATA = Web3.keccak(text="getUserReserveData(address,address)")[:4]
SELECTOR_GET_RESERVE_DATA      = Web3.keccak(text="getReserveData(address)")[:4]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ReserveConfig:
    """Immutable per-asset config — fetched once, cached until restart."""
    asset: str
    ltv: int                   # basis points, e.g. 7500 = 75%
    liquidation_threshold: int # basis points
    liquidation_bonus: int     # basis points, e.g. 10500 = 5% bonus
    last_fetched: float = field(default_factory=time.time)


@dataclass
class UserReservePosition:
    """Per-asset balance for a single user."""
    asset: str
    a_token_balance: int       # collateral (raw units)
    stable_debt: int
    variable_debt: int
    total_debt: int            # stable + variable
    usage_as_collateral: bool


@dataclass
class AccountData:
    """
    Full on-chain position for one borrower.
    Mirrors getUserAccountData + per-asset breakdown.
    """
    address: str
    total_collateral_base: int   # USD, 8 decimals (Aave oracle units)
    total_debt_base: int         # USD, 8 decimals
    liquidation_threshold: int   # weighted average, basis points
    health_factor: int           # WAD (1e18 = HF of 1.0)
    reserves: list[UserReservePosition] = field(default_factory=list)
    last_updated: float = field(default_factory=time.time)

    @property
    def hf_float(self) -> float:
        return self.health_factor / WAD

    @property
    def is_liquidatable(self) -> bool:
        return self.health_factor < WAD and self.total_debt_base > 0


# ---------------------------------------------------------------------------
# PositionLoader
# ---------------------------------------------------------------------------

class PositionLoader:
    """
    Loads and caches real Aave V3 position data via Multicall3.

    Bootstrap:   batch getUserAccountData for all watchlist addresses
    Refresh hot: re-fetch only positions with HF < threshold every N blocks
    get():       O(1) lookup for HF engine
    """

    # Maximum addresses per multicall batch — stay well under gas limit
    BATCH_SIZE = 200

    def __init__(
        self,
        w3: AsyncWeb3,
        pool_address: str,
        multicall3_address: str = MULTICALL3_ADDRESS,
    ):
        self.w3 = w3
        self.pool_address = AsyncWeb3.to_checksum_address(pool_address)
        self.multicall3_address = AsyncWeb3.to_checksum_address(multicall3_address)

        self._pool = w3.eth.contract(address=self.pool_address, abi=POOL_ABI + USER_RESERVE_ABI + RESERVE_DATA_ABI)
        self._mc   = w3.eth.contract(address=self.multicall3_address, abi=MULTICALL3_ABI)

        # address (checksum) → AccountData
        self._positions: dict[str, AccountData] = {}

        # asset (checksum) → ReserveConfig
        self._reserve_configs: dict[str, ReserveConfig] = {}

        # known reserve list — fetched once
        self._reserves: list[str] = []

        self._bootstrapped = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def bootstrap(self, watchlist: list[str]) -> int:
        """
        Fetch getUserAccountData for every address in watchlist via Multicall3.
        Returns the number of positions successfully loaded.

        Call once at startup to replace the CSV fabrication loop.
        """
        logger.info(f"[PositionLoader] Bootstrap starting — {len(watchlist)} addresses")

        # Normalise addresses
        addresses = [AsyncWeb3.to_checksum_address(a) for a in watchlist]

        # Fetch reserve list once (needed for hot-refresh asset breakdown)
        await self._fetch_reserves_list()

        # Fetch reserve configs once (liquidation bonus/threshold per asset)
        await self._fetch_all_reserve_configs()

        # Batch getUserAccountData for all addresses
        loaded = await self._batch_account_data(addresses)

        self._bootstrapped = True
        logger.info(
            f"[PositionLoader] Bootstrap complete — "
            f"{loaded} loaded, {len(watchlist) - loaded} failed/zero-debt"
        )
        return loaded

    async def refresh_hot(self, hf_threshold: float = 1.2) -> int:
        """
        Re-fetch account data for all positions with HF < hf_threshold.
        Call every N blocks (suggested: every 5-10 blocks) to keep
        near-liquidatable positions fresh without hammering the RPC.

        Returns count of positions refreshed.
        """
        if not self._bootstrapped:
            raise RuntimeError("Call bootstrap() before refresh_hot()")

        hot = [
            addr for addr, pos in self._positions.items()
            if pos.hf_float < hf_threshold
        ]

        if not hot:
            return 0

        logger.debug(f"[PositionLoader] Refreshing {len(hot)} hot positions (HF < {hf_threshold})")
        refreshed = await self._batch_account_data(hot)

        # For truly hot positions (HF < 1.05), fetch per-asset breakdown too
        critical = [
            addr for addr in hot
            if self._positions[addr].hf_float < 1.05
        ]
        if critical:
            await self._batch_reserve_data(critical)

        return refreshed

    def get(self, address: str) -> Optional[AccountData]:
        """
        O(1) lookup for HF engine. Returns None if address not in watchlist.
        Replace: collateral_addrs[a] = int(float(row.get("collateral_usd", 0)) * 1e6)
        With:    pos = loader.get(a); if pos and pos.is_liquidatable: ...
        """
        key = AsyncWeb3.to_checksum_address(address)
        return self._positions.get(key)

    def get_reserve_config(self, asset: str) -> Optional[ReserveConfig]:
        key = AsyncWeb3.to_checksum_address(asset)
        return self._reserve_configs.get(key)

    @property
    def position_count(self) -> int:
        return len(self._positions)

    @property
    def liquidatable(self) -> list[AccountData]:
        """All currently-tracked positions with HF < 1.0."""
        return [p for p in self._positions.values() if p.is_liquidatable]

    # ------------------------------------------------------------------
    # Internal — batch loaders
    # ------------------------------------------------------------------

    async def _batch_account_data(self, addresses: list[str]) -> int:
        """
        Multicall3 aggregate3 over getUserAccountData for a list of addresses.
        Processes in BATCH_SIZE chunks to avoid gas limit.
        Returns count of successfully decoded positions.
        """
        # Build calldata for getUserAccountData(address)
        loaded = 0

        chunks = [
            addresses[i:i + self.BATCH_SIZE]
            for i in range(0, len(addresses), self.BATCH_SIZE)
        ]

        for chunk_idx, chunk in enumerate(chunks):
            # Build calldata directly — zero RPC (selector + encoded address)
            calls = [
                {
                    "target":       self.pool_address,
                    "allowFailure": True,
                    "callData":     "0x" + SELECTOR_GET_USER_ACCOUNT_DATA.hex()
                                    + self.w3.codec.encode(["address"], [addr]).hex(),
                }
                for addr in chunk
            ]

            try:
                results = await self._mc.functions.aggregate3(calls).call()
            except Exception as e:
                err_str = str(e)
                logger.error(f"[PositionLoader] Multicall chunk {chunk_idx} failed: {e}")
                if '429' in err_str:
                    await asyncio.sleep(3.0)
                continue
            await asyncio.sleep(0.3)  # rate-limit guard — ~3 req/s during bootstrap

            for addr, (success, raw) in zip(chunk, results):
                if not success or not raw:
                    continue
                try:
                    decoded = self.w3.codec.decode(["uint256","uint256","uint256","uint256","uint256","uint256"], raw)
                    (
                        total_collateral_base,
                        total_debt_base,
                        _available_borrows,
                        liquidation_threshold,
                        _ltv,
                        health_factor,
                    ) = decoded

                    # Skip dust / zero-debt positions
                    if total_debt_base == 0:
                        continue

                    self._positions[addr] = AccountData(
                        address=addr,
                        total_collateral_base=total_collateral_base,
                        total_debt_base=total_debt_base,
                        liquidation_threshold=liquidation_threshold,
                        health_factor=health_factor,
                    )
                    loaded += 1

                except Exception as e:
                    logger.warning(f"[PositionLoader] Decode failed for {addr}: {e}")

            # Progress every 10% or every chunk for small batches
            pct = (chunk_idx + 1) / len(chunks) * 100
            if len(chunks) <= 10 or (chunk_idx + 1) % max(1, len(chunks) // 10) == 0:
                logger.info(
                    f"[PositionLoader] Chunk {chunk_idx + 1}/{len(chunks)} "
                    f"({pct:.0f}%) — {loaded} loaded so far"
                )

            # Yield control between chunks — don't starve the event loop
            await asyncio.sleep(0)

        return loaded

    async def _batch_reserve_data(self, addresses: list[str]) -> None:
        """
        For a set of near-liquidatable addresses, fetch per-asset
        getUserReserveData to populate the reserves breakdown.
        Only runs for critical positions (HF < 1.05).
        """
        if not self._reserves:
            logger.warning("[PositionLoader] No reserve list — skipping per-asset fetch")
            return

        for addr in addresses:
            addr = AsyncWeb3.to_checksum_address(addr)  # normalize — _positions keys are checksummed
            # Build calldata directly — zero RPC (selector + encoded params)
            calls = [
                {
                    "target":       AAVE_POOL_DATA_PROVIDER,  # getUserReserveData is on PoolDataProvider, not Pool
                    "allowFailure": True,
                    "callData":     "0x" + SELECTOR_GET_USER_RESERVE_DATA.hex()
                                    + self.w3.codec.encode(
                                        ["address", "address"], [asset, addr]
                                    ).hex(),
                }
                for asset in self._reserves
            ]

            try:
                results = await self._mc.functions.aggregate3(calls).call()
            except Exception as e:
                logger.error(f"[PositionLoader] Reserve data multicall failed for {addr}: {e}")
                continue

            user_reserves = []
            for asset, (success, raw) in zip(self._reserves, results):
                if not success or not raw:
                    continue
                try:
                    decoded = self.w3.codec.decode(["uint256","uint256","uint256","uint256","uint256","uint256","uint256","uint16","bool"], raw)
                    (
                        a_token_balance,
                        stable_debt,
                        variable_debt,
                        _principal_stable,
                        _scaled_variable,
                        _stable_borrow_rate,
                        _liquidity_rate,
                        _stable_rate_last_updated,
                        usage_as_collateral,
                    ) = decoded

                    # Skip assets with no position
                    if a_token_balance == 0 and stable_debt == 0 and variable_debt == 0:
                        continue

                    user_reserves.append(UserReservePosition(
                        asset=asset,
                        a_token_balance=a_token_balance,
                        stable_debt=stable_debt,
                        variable_debt=variable_debt,
                        total_debt=stable_debt + variable_debt,
                        usage_as_collateral=usage_as_collateral,
                    ))
                except Exception as e:
                    logger.warning(f"[PositionLoader] getUserReserveData decode failed {asset}/{addr}: {e}")

            if addr in self._positions:
                self._positions[addr].reserves = user_reserves
                self._positions[addr].last_updated = time.time()

            await asyncio.sleep(0.5)   # 0.5s gap between addresses ≈ 2 req/s sustained

    async def _fetch_reserves_list(self) -> None:
        """Fetch the Aave V3 reserve asset list once at bootstrap."""
        try:
            self._reserves = await self._pool.functions.getReservesList().call()
            logger.info(f"[PositionLoader] {len(self._reserves)} reserves found")
        except Exception as e:
            logger.error(f"[PositionLoader] getReservesList failed: {e}")

    async def _fetch_all_reserve_configs(self) -> None:
        """
        Multicall3 getReserveData for every asset to extract
        liquidation threshold and bonus from the configuration bitmask.
        """
        if not self._reserves:
            return

        # Build calldata directly — zero RPC (selector + encoded address)
        calls = [
            {
                "target":       self.pool_address,
                "allowFailure": True,
                "callData":     "0x" + SELECTOR_GET_RESERVE_DATA.hex()
                                + self.w3.codec.encode(["address"], [asset]).hex(),
            }
            for asset in self._reserves
        ]

        try:
            results = await self._mc.functions.aggregate3(calls).call()
        except Exception as e:
            logger.error(f"[PositionLoader] getReserveData multicall failed: {e}")
            return

        for asset, (success, raw) in zip(self._reserves, results):
            if not success or not raw:
                continue
            try:
                decoded = self.w3.codec.decode(["uint256","uint128","uint128","uint128","uint128","uint128","uint40","uint16","address","address","address","address","uint128","uint128","uint128"], raw)
                config_map = decoded[0]  # configuration uint256 (flat tuple from codec.decode)

                ltv       = config_map & LTV_MASK
                threshold = (config_map >> THRESHOLD_SHIFT) & THRESHOLD_MASK
                bonus     = (config_map >> BONUS_SHIFT) & BONUS_MASK

                self._reserve_configs[asset] = ReserveConfig(
                    asset=asset,
                    ltv=ltv,
                    liquidation_threshold=threshold,
                    liquidation_bonus=bonus,
                )
            except Exception as e:
                logger.warning(f"[PositionLoader] ReserveConfig decode failed for {asset}: {e}")

        logger.info(f"[PositionLoader] {len(self._reserve_configs)} reserve configs cached")


# ---------------------------------------------------------------------------
# pipeline.py integration guide
# ---------------------------------------------------------------------------
#
# 1. Import at top of pipeline.py:
#       from position_loader import PositionLoader
#
# 2. In your pipeline __init__ or setup():
#       self.loader = PositionLoader(self.w3, AAVE_POOL_ADDRESS)
#
# 3. Replace the CSV bootstrap loop with:
#       watchlist = list(redis_client.smembers("watchlist"))
#       await self.loader.bootstrap(watchlist)
#
# 4. Add to your block handler (every 5 blocks):
#       await self.loader.refresh_hot(hf_threshold=1.2)
#
# 5. In HF engine callback, replace:
#       collateral = collateral_addrs[address]   # <- fabricated
#   With:
#       pos = self.loader.get(address)
#       if pos is None or not pos.is_liquidatable:
#           return
#       # pos.total_collateral_base, pos.total_debt_base, pos.hf_float
#       # pos.reserves  <- per-asset breakdown (populated if HF < 1.05)
#
# 6. In EVEstimator._best_asset(), use reserve configs:
#       cfg = self.loader.get_reserve_config(asset)
#       bonus = cfg.liquidation_bonus if cfg else 10500
#       threshold = cfg.liquidation_threshold if cfg else 8000
#
# ---------------------------------------------------------------------------
