"""
compound_v3.py — Compound V3 (Comet) liquidation pipeline module
Integrates with pipeline_v3.py as a second protocol alongside Aave V3.

Architecture differences from Aave V3:
    - No global HF — uses isLiquidatable() boolean per account
    - One base asset per market (USDC market, ETH market separate)
    - absorb() is free and earns liquidator points — always call it
    - buyCollateral() requires base token, reverts if reserves >= target
    - Event: Absorb(address absorber, address borrower, address collateralAsset, uint256 basePaidOut, uint256 usdValue)
    - No LiquidationCall event — different log parsing needed

Arbitrum markets:
    USDC: 0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf  ($24M TVL)
    USDT: 0xd98Be00b5D27fc98112BdE293e487f8D4cA57d07  ($21M TVL)
    ETH:  0x6f7D514bbD4aFf3BcD1140B7344b32f063dEe486  ($2M TVL)

Usage:
    compound = CompoundV3Module(
        rpc           = rpc_client,
        rpc_read      = rpc_read_client,
        redis         = redis_client,
        shared_state  = shared_state,
        skip_tel      = skip_telemetry,
        executor_addr = COMPOUND_EXECUTOR_ADDR,
        private_key   = PRIVATE_KEY,
        wallet        = WALLET_ADDR,
        markets       = COMPOUND_MARKETS,
    )
    await compound.start()
    # compound.on_new_block(block_number) in block handler
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional

from web3 import AsyncWeb3, Web3

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Arbitrum market addresses
# ---------------------------------------------------------------------------

COMPOUND_MARKETS = {
    "USDC": {
        "comet":      "0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf",
        "base_token": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",  # native USDC
        "base_decimals": 6,
        "redis_key":  "watchlist:compound:usdc",
        "executor":   os.getenv("COMPOUND_EXECUTOR_ADDR", ""),
    },
    "USDT": {
        "comet":      "0xd98Be00b5D27fc98112BdE293e487f8D4cA57d07",
        "base_token": "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9",  # USDT
        "base_decimals": 6,
        "redis_key":  "watchlist:compound:usdt",
        "executor":   os.getenv("COMPOUND_USDT_EXECUTOR_ADDR", ""),
    },
    "ETH": {
        "comet":      "0x6f7D514bbD4aFf3BcD1140B7344b32f063dEe486",
        "base_token": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",  # WETH
        "base_decimals": 18,
        "redis_key":  "watchlist:compound:eth",
        "executor":   "",   # not deployed — $1K TVL, skip
    },
}

MULTICALL3_ADDR = "0xcA11bde05977b3631167028862bE2a173976CA11"

# ---------------------------------------------------------------------------
# ABIs
# ---------------------------------------------------------------------------

COMET_ABI = [
    {
        "inputs": [{"internalType": "address", "name": "account", "type": "address"}],
        "name": "isLiquidatable",
        "outputs": [{"internalType": "bool", "name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "address", "name": "account", "type": "address"}],
        "name": "borrowBalanceOf",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "getReserves",
        "outputs": [{"internalType": "int256", "name": "", "type": "int256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "targetReserves",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address", "name": "asset",      "type": "address"},
            {"internalType": "uint256", "name": "baseAmount", "type": "uint256"},
        ],
        "name": "quoteCollateral",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "address", "name": "asset", "type": "address"}],
        "name": "getCollateralReserves",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "numAssets",
        "outputs": [{"internalType": "uint8", "name": "", "type": "uint8"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "uint8", "name": "i", "type": "uint8"}],
        "name": "getAssetInfo",
        "outputs": [
            {
                "components": [
                    {"internalType": "uint8",   "name": "offset",                  "type": "uint8"},
                    {"internalType": "address", "name": "asset",                   "type": "address"},
                    {"internalType": "address", "name": "priceFeed",               "type": "address"},
                    {"internalType": "uint64",  "name": "scale",                   "type": "uint64"},
                    {"internalType": "uint64",  "name": "borrowCollateralFactor",  "type": "uint64"},
                    {"internalType": "uint64",  "name": "liquidateCollateralFactor","type": "uint64"},
                    {"internalType": "uint64",  "name": "liquidationFactor",       "type": "uint64"},
                    {"internalType": "uint128", "name": "supplyCap",               "type": "uint128"},
                ],
                "internalType": "struct AssetInfo",
                "name":         "",
                "type":         "tuple",
            }
        ],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address", "name": "account", "type": "address"},
            {"internalType": "address", "name": "asset",   "type": "address"},
        ],
        "name": "userCollateral",
        "outputs": [
            {"internalType": "uint128", "name": "balance",  "type": "uint128"},
            {"internalType": "uint128", "name": "reserved", "type": "uint128"},
        ],
        "stateMutability": "view",
        "type": "function",
    },
]

EXECUTOR_ABI = [
    {
        "inputs": [
            {"internalType": "address",   "name": "borrower",         "type": "address"},
            {"internalType": "address",   "name": "collateralAsset",  "type": "address"},
            {"internalType": "uint256",   "name": "baseAmount",       "type": "uint256"},
            {"internalType": "uint256",   "name": "minCollateralOut", "type": "uint256"},
            {"internalType": "uint24",    "name": "swapFee",          "type": "uint24"},
        ],
        "name": "executeLiquidation",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [{"internalType": "address[]", "name": "accounts", "type": "address[]"}],
        "name": "absorbAccounts",
        "outputs": [],
        "stateMutability": "nonpayable",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "canBuyCollateral",
        "outputs": [{"internalType": "bool", "name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "address", "name": "asset",      "type": "address"},
            {"internalType": "uint256", "name": "baseAmount", "type": "uint256"},
        ],
        "name": "quoteCollateral",
        "outputs": [{"internalType": "uint256", "name": "collateralAmount", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [],
        "name": "paused",
        "outputs": [{"internalType": "bool", "name": "", "type": "bool"}],
        "stateMutability": "view",
        "type": "function",
    },
]

MULTICALL3_ABI = [
    {
        "inputs": [
            {
                "components": [
                    {"internalType": "address", "name": "target",       "type": "address"},
                    {"internalType": "bool",    "name": "allowFailure", "type": "bool"},
                    {"internalType": "bytes",   "name": "callData",     "type": "bytes"},
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
                    {"internalType": "bool",  "name": "success",    "type": "bool"},
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

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class CompoundPosition:
    address:        str
    market:         str           # "USDC" | "USDT" | "ETH"
    comet:          str           # Comet proxy address
    borrow_balance: int           # raw base token units
    is_liquidatable:bool
    best_collateral:Optional[str] = None
    last_updated:   float = field(default_factory=time.time)


@dataclass
class LiquidationParams:
    borrower:          str
    collateral_asset:  str
    base_amount:       int    # base token to spend on buyCollateral
    min_collateral_out:int    # slippage guard
    swap_fee:          int    # Uni V3 fee tier for collateral→base swap
    estimated_profit:  float  # USD


# ---------------------------------------------------------------------------
# CompoundPositionLoader — isLiquidatable via Multicall3
# ---------------------------------------------------------------------------

class CompoundPositionLoader:
    """
    Checks isLiquidatable() for all watchlist addresses via Multicall3.
    Much simpler than Aave's getUserAccountData — just a boolean per address.
    """

    BATCH_SIZE = 300

    def __init__(self, w3: AsyncWeb3, market_config: dict):
        self._w3     = w3
        self._config = market_config
        self._comet  = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(market_config["comet"]),
            abi=COMET_ABI,
        )
        self._mc = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(MULTICALL3_ADDR),
            abi=MULTICALL3_ABI,
        )
        self._positions: dict[str, CompoundPosition] = {}

    async def bootstrap(self, watchlist: list[str]) -> int:
        """Check borrow balance for all addresses, keep active borrowers."""
        logger.info(
            f"[CompoundLoader:{self._config['market']}] "
            f"Bootstrap {len(watchlist)} addresses"
        )
        loaded = 0
        chunks = [
            watchlist[i:i + self.BATCH_SIZE]
            for i in range(0, len(watchlist), self.BATCH_SIZE)
        ]

        for chunk in chunks:
            calls = [
                {
                    "target":       AsyncWeb3.to_checksum_address(self._config["comet"]),
                    "allowFailure": True,
                    "callData":     self._comet.encode_abi(
                        abi_element_identifier="borrowBalanceOf",
                        args=[AsyncWeb3.to_checksum_address(addr)],
                    ),
                }
                for addr in chunk
            ]
            try:
                results = await self._mc.functions.aggregate3(calls).call()
            except Exception as e:
                logger.warning(f"[CompoundLoader] Multicall failed: {e}")
                await asyncio.sleep(2)
                continue

            for addr, (success, raw) in zip(chunk, results):
                if not success or len(raw) < 32:
                    continue
                borrow_balance = int(raw.hex()[:64], 16)
                if borrow_balance == 0:
                    continue
                self._positions[addr.lower()] = CompoundPosition(
                    address        = addr.lower(),
                    market         = self._config["market"],
                    comet          = self._config["comet"],
                    borrow_balance = borrow_balance,
                    is_liquidatable= False,
                )
                loaded += 1
            await asyncio.sleep(0)

        logger.info(f"[CompoundLoader:{self._config['market']}] {loaded} active borrowers")
        return loaded

    async def refresh_liquidatable(self) -> list[CompoundPosition]:
        """
        Check isLiquidatable() for all tracked positions.
        Returns list of currently liquidatable positions.
        Called every N blocks.
        """
        if not self._positions:
            return []

        addrs  = list(self._positions.keys())
        chunks = [addrs[i:i + self.BATCH_SIZE] for i in range(0, len(addrs), self.BATCH_SIZE)]
        liquidatable = []

        for chunk in chunks:
            calls = [
                {
                    "target":       AsyncWeb3.to_checksum_address(self._config["comet"]),
                    "allowFailure": True,
                    "callData":     self._comet.encode_abi(
                        abi_element_identifier="isLiquidatable",
                        args=[AsyncWeb3.to_checksum_address(addr)],
                    ),
                }
                for addr in chunk
            ]
            try:
                results = await self._mc.functions.aggregate3(calls).call()
            except Exception as e:
                logger.warning(f"[CompoundLoader] isLiquidatable multicall failed: {e}")
                continue

            for addr, (success, raw) in zip(chunk, results):
                if not success or len(raw) < 32:
                    continue
                is_liq = int(raw.hex()[:64], 16) != 0
                pos    = self._positions.get(addr.lower())
                if pos:
                    pos.is_liquidatable = is_liq
                    pos.last_updated    = time.time()
                    if is_liq:
                        liquidatable.append(pos)

            await asyncio.sleep(0)

        return liquidatable

    @property
    def position_count(self) -> int:
        return len(self._positions)


# ---------------------------------------------------------------------------
# CompoundEVEstimator — profit estimation for Compound liquidations
# ---------------------------------------------------------------------------

class CompoundEVEstimator:
    """
    Estimates profit for a Compound V3 liquidation.

    Flow:
        1. quoteCollateral(asset, baseAmount) → how much collateral we'd receive
        2. Quote Uni V3 swap: collateral → base token → how much base we get back
        3. profit = base_received - base_spent - gas_cost - flash_fee

    The discount on buyCollateral is:
        DiscountFactor = StoreFrontPriceFactor × (1e18 - LiquidationFactor)
    Typically 3-8% discount on collateral purchase price.
    """

    GAS_LIMIT     = 600_000   # absorb + buyCollateral + swap
    FLASH_FEE_BPS = 5         # Uni V3 0.05% pool flash fee (500 bps = 0.05%)

    def __init__(self, w3: AsyncWeb3, market_config: dict, quoter_contract):
        self._w3     = w3
        self._config = market_config
        self._comet  = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(market_config["comet"]),
            abi=COMET_ABI,
        )
        self._quoter = quoter_contract  # QuoterV2 from async_web3.py

    async def estimate(
        self,
        position:    CompoundPosition,
        base_amount: int,              # how much base to spend
        eth_price:   float = 3500.0,
        gas_price:   int   = 100_000_000,
    ) -> Optional[LiquidationParams]:
        """
        Returns LiquidationParams if profitable, None if not.
        """
        market = self._config

        # Find best collateral asset
        best = await self._best_collateral(position, base_amount)
        if best is None:
            return None

        collateral_asset, quoted_collateral, swap_fee, swap_out = best

        # Flash fee on base_amount
        flash_fee   = int(base_amount * self.FLASH_FEE_BPS / 10_000)
        total_repay = base_amount + flash_fee

        # Profit in base token units
        if swap_out <= total_repay:
            return None

        profit_base = swap_out - total_repay

        # Gas cost in base token equivalent
        gas_cost_eth = (self.GAS_LIMIT * gas_price) / 1e18
        gas_cost_usd = gas_cost_eth * eth_price

        # Convert profit to USD
        base_dec    = market["base_decimals"]
        profit_usd  = (profit_base / 10 ** base_dec)
        if market["market"] == "ETH":
            profit_usd *= eth_price

        profit_usd -= gas_cost_usd

        if profit_usd <= 0:
            return None

        # Slippage guard: minCollateralOut = quoted × 99.5%
        min_collateral_out = int(quoted_collateral * 0.995)

        logger.debug(
            f"[CompoundEV:{market['market']}] "
            f"borrower={position.address[:10]}… "
            f"collateral={collateral_asset[:10]}… "
            f"base_spent={base_amount} "
            f"swap_out={swap_out} "
            f"profit_usd=${profit_usd:.2f}"
        )

        return LiquidationParams(
            borrower           = position.address,
            collateral_asset   = collateral_asset,
            base_amount        = base_amount,
            min_collateral_out = min_collateral_out,
            swap_fee           = swap_fee,
            estimated_profit   = profit_usd,
        )

    async def _best_collateral(
        self,
        position:    CompoundPosition,
        base_amount: int,
    ) -> Optional[tuple[str, int, int, int]]:
        """
        Find the best collateral asset to buy.
        Returns (asset_addr, quoted_collateral, swap_fee, swap_out_base) or None.
        """
        comet_addr = self._config["comet"]
        base_token = self._config["base_token"]

        # Get number of collateral assets
        try:
            num = await self._comet.functions.numAssets().call()
        except Exception:
            return None

        best_profit = 0
        best_result = None

        for i in range(num):
            try:
                asset_info = await self._comet.functions.getAssetInfo(i).call()
                asset_addr = asset_info[1]  # asset address

                # Check user has this collateral
                bal, _ = await self._comet.functions.userCollateral(
                    AsyncWeb3.to_checksum_address(position.address),
                    AsyncWeb3.to_checksum_address(asset_addr),
                ).call()
                if bal == 0:
                    continue

                # Quote: how much collateral for base_amount
                quoted = await self._comet.functions.quoteCollateral(
                    AsyncWeb3.to_checksum_address(asset_addr),
                    base_amount,
                ).call()
                if quoted == 0:
                    continue

                # Quote swap: collateral → base token
                swap_out, swap_fee = await self._quoter.best_quote(
                    token_in  = asset_addr,
                    token_out = base_token,
                    amount_in = quoted,
                )
                if swap_out == 0:
                    continue

                profit = swap_out - base_amount
                if profit > best_profit:
                    best_profit = profit
                    best_result = (asset_addr, quoted, swap_fee, swap_out)

            except Exception as e:
                logger.debug(f"[CompoundEV] Asset {i} error: {e}")
                continue

        return best_result


# ---------------------------------------------------------------------------
# CompoundWatchlistBuilder — event-based borrower discovery
# ---------------------------------------------------------------------------

# Comet events for watchlist growth
# Withdraw(address indexed src, address indexed to, uint256 amount)
#   topics[1] = src  <- borrower address (confirmed on-chain at block 470M)
#   topics[2] = to   <- recipient (ignore)
# topic0 confirmed: keccak256("Withdraw(address,address,uint256)")
#
# Note: This Comet uses "Withdraw" not "WithdrawBase" — older deployment version.
#
# Absorb(address indexed absorber, address indexed borrower, address collateralAsset, uint256 basePaidOut, uint256 usdValue)
#   topics[2] = account <- liquidated borrower (remove from watchlist)
TOPIC_WITHDRAW_BASE = "0x9b1bfa7fa9ee420a16e124f794c35ac9f90472acc99140eb2f6447c714cad8eb"
TOPIC_ABSORB        = "0xe6092fee7f4518f259b26e0abecc0492cb6a1a46309419ec69ba0c97efa90b85"  # Absorb(address,address,address,uint256,uint256) — verified


class CompoundWatchlistBuilder:
    """
    Builds and maintains Compound V3 borrower watchlist.
    Seeds from WithdrawBase events (borrowers withdraw base = they borrowed it).
    Removes on Absorb (liquidated — no longer a borrower).
    """

    def __init__(
        self,
        w3: AsyncWeb3,
        redis,
        market_config: dict,
        blocks_per_chunk: int = 2000,
    ):
        self._w3      = w3
        self._redis   = redis
        self._config  = market_config
        self._chunk   = blocks_per_chunk
        self._key     = market_config["redis_key"]

    async def backfill(self, start_block: int, end_block: Optional[int] = None) -> int:
        """Scrape WithdrawBase events to find all historical borrowers."""
        if end_block is None:
            end_block = await self._w3.eth.block_number

        addresses: set[str] = set()
        block = start_block

        while block < end_block:
            to_block = min(block + self._chunk - 1, end_block)
            try:
                logs = await self._w3.eth.get_logs({
                    "address":   self._config["comet"],
                    "fromBlock": block,
                    "toBlock":   to_block,
                    "topics":    [TOPIC_WITHDRAW_BASE],
                })
                for log in logs:
                    topics = log.get("topics", [])
                    # Withdraw(address indexed src, address indexed to, uint256)
                    # topics[1] = src = the withdrawer/borrower address
                    if len(topics) >= 2:
                        t1   = topics[1] if isinstance(topics[1], str) else topics[1].hex()
                        addr = "0x" + t1[-40:]
                        addresses.add(addr.lower())

                block = to_block + 1
                await asyncio.sleep(0.05)

            except Exception as e:
                logger.warning(f"[CompoundWatchlist] getLogs failed: {e}")
                await asyncio.sleep(5)
                continue

        if addresses:
            pipe = self._redis.pipeline()
            for addr in addresses:
                pipe.zadd(self._key, {addr: 1.0})
            await pipe.execute()

        logger.info(
            f"[CompoundWatchlist:{self._config['market']}] "
            f"Backfill: {len(addresses)} addresses added"
        )
        return len(addresses)


# ---------------------------------------------------------------------------
# CompoundV3Module — top-level coordinator for pipeline_v3.py
# ---------------------------------------------------------------------------

class CompoundV3Module:
    """
    Drop-in module for pipeline_v3.py.
    Runs alongside the Aave V3 pipeline, same process.

    Usage in pipeline_v3.py:
        # In setup():
        self.compound = CompoundV3Module(...)
        await self.compound.start()

        # In block handler:
        await self.compound.on_new_block(block_number)

        # In shutdown():
        await self.compound.stop()
    """

    def __init__(
        self,
        rpc,                    # AsyncRPCClient (execution)
        rpc_read,               # AsyncRPCClient (reads)
        redis,                  # aioredis client
        shared_state,           # SharedState from hot_path_fix.py
        nonce_mgr,              # NonceManager from async_web3.py
        skip_tel,               # SkipTelemetry from skip_telemetry.py
        quoter,                 # QuoterAsync from async_web3.py
        executor_addr: str,     # CompoundV3Executor deployed address
        private_key:   str,
        wallet:        str,
        markets:       dict = None,
        min_profit_usd:float = 2.0,
        check_interval:int  = 10,   # blocks between isLiquidatable checks
    ):
        self._rpc          = rpc
        self._rpc_read     = rpc_read
        self._redis        = redis
        self._state        = shared_state
        self._nonce        = nonce_mgr
        self._skip         = skip_tel
        self._quoter       = quoter
        self._executor_addr= AsyncWeb3.to_checksum_address(executor_addr) if executor_addr else executor_addr
        self._pk           = private_key
        self._wallet       = AsyncWeb3.to_checksum_address(wallet)
        self._markets_cfg  = markets or COMPOUND_MARKETS
        self._min_profit   = min_profit_usd
        self._check_interval = check_interval

        # Per-market components
        self._loaders:    dict[str, CompoundPositionLoader]  = {}
        self._estimators: dict[str, CompoundEVEstimator]     = {}
        self._executors:  dict[str, object]                  = {}  # web3 contracts

        # Execution state
        self._in_flight: set[str] = set()  # borrower:market keys
        self._running = False
        self._sync_w3 = Web3()

    async def start(self) -> None:
        """Initialise all markets and load watchlists."""
        self._running = True

        for market_name, config in self._markets_cfg.items():
            cfg = {**config, "market": market_name}

            loader = CompoundPositionLoader(self._rpc_read.w3, cfg)
            estimator = CompoundEVEstimator(self._rpc_read.w3, cfg, self._quoter)

            # Per-market executor address — config overrides global default.
            # An explicitly empty string means "intentionally no executor" (e.g. WETH $1K TVL).
            # Only fall back to the global default if the key is absent entirely.
            if "executor" in cfg:
                market_executor = cfg["executor"]
            else:
                market_executor = self._executor_addr

            if not market_executor:
                logger.warning(
                    f"[CompoundV3] {market_name}: No executor configured — "
                    f"monitoring only (cannot execute)"
                )
                self._executors[market_name] = None
                continue

            executor_contract = self._rpc.w3.eth.contract(
                address=AsyncWeb3.to_checksum_address(str(market_executor)),
                abi=EXECUTOR_ABI,
            )

            self._loaders[market_name]    = loader
            self._estimators[market_name] = estimator
            self._executors[market_name]  = executor_contract

            # Load watchlist from Redis
            members = await self._redis.zrange(cfg["redis_key"], 0, -1)
            if members:
                watchlist = [
                    m.decode() if isinstance(m, bytes) else m
                    for m in members
                ]
                await loader.bootstrap(watchlist)
                logger.info(
                    f"[CompoundV3] {market_name}: "
                    f"{loader.position_count} borrowers loaded"
                )
            else:
                logger.warning(
                    f"[CompoundV3] {market_name}: empty watchlist — "
                    f"run CompoundWatchlistBuilder.backfill() first"
                )

        logger.info(f"[CompoundV3] Started — {len(self._markets_cfg)} markets")

    async def stop(self) -> None:
        self._running = False

    async def on_new_block(self, block_number: int) -> None:
        """
        Call from pipeline_v3.py block handler.
        Checks all markets for liquidatable positions every check_interval blocks.
        """
        if not self._running:
            return
        if block_number % self._check_interval != 0:
            return

        for market_name, loader in self._loaders.items():
            try:
                liquidatable = await loader.refresh_liquidatable()
                for pos in liquidatable:
                    flight_key = f"{pos.address}:{market_name}"
                    if flight_key in self._in_flight:
                        continue
                    asyncio.create_task(
                        self._execute(pos, market_name),
                        name=f"compound_liq_{market_name}_{pos.address[:8]}",
                    )
            except Exception as e:
                logger.error(f"[CompoundV3] {market_name} refresh error: {e}")

    async def _execute(self, pos: CompoundPosition, market_name: str) -> None:
        """Full liquidation attempt for one Compound position."""
        flight_key = f"{pos.address}:{market_name}"
        self._in_flight.add(flight_key)

        try:
            executor = self._executors.get(market_name)
            if executor is None:
                logger.debug(
                    f"[CompoundV3:{market_name}] "
                    f"No executor — skipping {pos.address[:10]}…"
                )
                return

            config    = {**self._markets_cfg[market_name], "market": market_name}
            estimator = self._estimators[market_name]

            # Check canBuyCollateral — reverts if reserves >= target
            can_buy = await executor.functions.canBuyCollateral().call()
            if not can_buy:
                logger.info(
                    f"[CompoundV3:{market_name}] "
                    f"Reserves >= target — absorb only for {pos.address[:10]}…"
                )
                # Still worth calling absorb — earns liquidator points
                await self._absorb_only(pos, executor, market_name)
                return

            # Estimate profit
            base_amount = pos.borrow_balance // 2   # 50% of debt
            params = await estimator.estimate(
                position    = pos,
                base_amount = base_amount,
                gas_price   = self._state.base_fee_wei or 100_000_000,
            )

            if params is None or params.estimated_profit < self._min_profit:
                logger.debug(
                    f"[CompoundV3:{market_name}] "
                    f"Below profit floor — {pos.address[:10]}…"
                )
                return

            # Build and submit tx
            await self._submit(params, executor, market_name)

        except Exception as e:
            logger.error(f"[CompoundV3] Execute error for {pos.address[:10]}: {e}")
        finally:
            self._in_flight.discard(flight_key)

    async def _absorb_only(self, pos: CompoundPosition, executor, market_name: str) -> None:
        """Call absorbAccounts() when buyCollateral is not available."""
        try:
            nonce     = await self._nonce.next()
            gas_price = int((self._state.base_fee_wei or 100_000_000) * 1.1)

            tx_dict = await executor.functions.absorbAccounts(
                [AsyncWeb3.to_checksum_address(pos.address)]
            ).build_transaction({
                "from":     self._wallet,
                "nonce":    nonce,
                "gas":      200_000,
                "gasPrice": gas_price,
                "chainId":  42161,
            })

            signed  = self._sync_w3.eth.account.sign_transaction(tx_dict, self._pk)
            from blast_submit import blast_submit
            tx_hash = await blast_submit(signed.raw_transaction)

            if tx_hash:
                logger.info(
                    f"[CompoundV3:{market_name}] Absorb-only submitted — "
                    f"hash={tx_hash[:12]}… borrower={pos.address[:10]}…"
                )
            else:
                await self._nonce.rewind()

        except Exception as e:
            logger.error(f"[CompoundV3] absorb_only failed: {e}")
            await self._nonce.rewind()

    async def _submit(
        self,
        params:       LiquidationParams,
        executor,
        market_name:  str,
    ) -> None:
        """Build and submit executeLiquidation() tx."""
        try:
            nonce     = await self._nonce.next()
            gas_price = int((self._state.base_fee_wei or 100_000_000) * 1.1)

            tx_dict = await executor.functions.executeLiquidation(
                AsyncWeb3.to_checksum_address(params.borrower),
                AsyncWeb3.to_checksum_address(params.collateral_asset),
                params.base_amount,
                params.min_collateral_out,
                params.swap_fee,
            ).build_transaction({
                "from":     self._wallet,
                "nonce":    nonce,
                "gas":      600_000,
                "gasPrice": gas_price,
                "chainId":  42161,
            })

            signed  = self._sync_w3.eth.account.sign_transaction(tx_dict, self._pk)
            from blast_submit import blast_submit
            tx_hash = await blast_submit(signed.raw_transaction)

            if tx_hash:
                logger.info(
                    f"[CompoundV3:{market_name}] Submitted — "
                    f"hash={tx_hash[:12]}… "
                    f"borrower={params.borrower[:10]}… "
                    f"collateral={params.collateral_asset[:10]}… "
                    f"est_profit=${params.estimated_profit:.2f}"
                )
            else:
                await self._nonce.rewind()
                logger.warning(
                    f"[CompoundV3:{market_name}] blast_submit returned None"
                )

        except Exception as e:
            logger.error(f"[CompoundV3] submit failed: {e}")
            await self._nonce.rewind()

    def status(self) -> str:
        lines = [f"[CompoundV3] {len(self._markets_cfg)} markets"]
        for name, loader in self._loaders.items():
            lines.append(f"  {name}: {loader.position_count} borrowers")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# pipeline_v3.py integration
# ---------------------------------------------------------------------------
#
# 1. Deploy CompoundV3Executor.sol (see forge command below)
#
# 2. Build Compound watchlists (run once):
#       from compound_v3 import CompoundWatchlistBuilder, COMPOUND_MARKETS
#       for market_name, cfg in COMPOUND_MARKETS.items():
#           builder = CompoundWatchlistBuilder(
#               w3     = rpc_read.w3,
#               redis  = redis_client,
#               market_config = {**cfg, "market": market_name},
#           )
#           # Compound V3 Arbitrum deployed ~block 72M
#           await builder.backfill(start_block=72_000_000)
#
# 3. Add to pipeline_v3.py imports:
#       from compound_v3 import CompoundV3Module, COMPOUND_MARKETS
#
# 4. In setup(), after existing components:
#       self.compound = CompoundV3Module(
#           rpc           = self.rpc,
#           rpc_read      = self.rpc_read,
#           redis         = redis_client,
#           shared_state  = self.shared_state,
#           nonce_mgr     = self.nonce_mgr,
#           skip_tel      = self.skip_tel,
#           quoter        = self.quoter,
#           executor_addr = os.getenv("COMPOUND_EXECUTOR_ADDR"),
#           private_key   = PRIVATE_KEY,
#           wallet        = WALLET_ADDR,
#           markets       = COMPOUND_MARKETS,
#       )
#       await self.compound.start()
#       logger.info(self.compound.status())
#
# 5. In block handler:
#       await self.compound.on_new_block(block_number)
#
# 6. In shutdown():
#       await self.compound.stop()
#
# 7. In stats loop:
#       logger.info(self.compound.status())
#
# ── Deploy CompoundV3Executor.sol ─────────────────────────────────────────
#
# You need three parameters:
#   _comet:      Comet proxy address (use USDC market for first deploy)
#   _uniRouter:  0xE592427A0AEce92De3Edee1F18E0157C05861564 (Uni V3 SwapRouter)
#   _flashPool:  Uni V3 USDC/WETH 0.05% pool on Arbitrum
#                = 0xC6962004f452bE9203591991D15f6b388e09E8D0
#
# forge create contracts/CompoundV3Executor.sol:CompoundV3Executor \
#   --constructor-args \
#     0x9c4ec768c28520B50860ea7a15bd7213a9fF58bf \
#     0xE592427A0AEce92De3Edee1F18E0157C05861564 \
#     0xC6962004f452bE9203591991D15f6b388e09E8D0 \
#   --private-key $PK \
#   --rpc-url $ALCHEMY_HTTP \
#   --verify
#
# Then approve Uni V3 router:
# cast send $COMPOUND_EXECUTOR_ADDR \
#   "approveRouter(address)" 0xE592427A0AEce92De3Edee1F18E0157C05861564 \
#   --private-key $PK --rpc-url $ALCHEMY_HTTP
#
# ---------------------------------------------------------------------------
