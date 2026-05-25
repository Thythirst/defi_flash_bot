"""
scanner/chains.py — Chain-agnostic configuration for Aave V3 liquidation bot.
Supports Arbitrum and Ethereum Mainnet. Add more chains as needed.
"""

from dataclasses import dataclass
from typing import List, Tuple, Optional


@dataclass(frozen=True)
class ChainConfig:
    name: str
    chain_id: int
    rpc_env_var: str           # Environment variable name for RPC URL
    ws_env_var: str            # Environment variable name for WebSocket URL

    # Aave V3 contracts
    aave_pool: str
    aave_pool_data_provider: str
    aave_ui_pool_data_provider: str
    aave_oracle: str

    # Flash loan source
    balancer_vault: str

    # DEX routers
    uni_v3_swaprouter: str
    uni_v3_quoter: str

    # Multicall3 (universal)
    multicall3: str = "0xcA11bde05977b3631167028862bE2a173976CA11"

    # Flashbots relay (mainnet only)
    flashbots_relay: Optional[str] = None
    flashbots_auth_signer: Optional[str] = None  # env var name

    # Gas settings
    eip1559: bool = True
    default_gas_limit: int = 500_000
    min_priority_fee_gwei: float = 0.01

    # Position filters
    min_debt_usd: float = 5000.0
    close_factor_bps: int = 5000  # 50%

    # Assets: list of (address, symbol, decimals)
    known_assets: Tuple[Tuple[str, str, int], ...] = ()

    @property
    def is_mainnet(self) -> bool:
        return self.chain_id == 1

    @property
    def is_arbitrum(self) -> bool:
        return self.chain_id == 42161

    @property
    def uses_flashbots(self) -> bool:
        return self.flashbots_relay is not None


# ───────────────────────────────────────────────────────────────────────────────
# Arbitrum One
# ───────────────────────────────────────────────────────────────────────────────

ARBITRUM_ASSETS: List[Tuple[str, str, int]] = [
    ("0x82aF49447D8a07e3bd95BD0d56f35241523fBab1", "WETH", 18),
    ("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "USDC", 6),
    ("0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9", "USDT", 6),
    ("0x912CE59144191C1204E64559FE8253a0e49E6548", "ARB", 18),
    ("0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f", "WBTC", 8),
    ("0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1", "DAI", 18),
    ("0xf97f4df75117a78c1A5a0DBb814Af92458539FB4", "LINK", 18),
    ("0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8", "USDC.e", 6),
]

ARBITRUM = ChainConfig(
    name="arbitrum",
    chain_id=42161,
    rpc_env_var="ARBITRUM_HTTP_URL",
    ws_env_var="ARBITRUM_WS_URL",
    aave_pool="0x794a61358D6845594F94dc1DB02A252b5b4814aD",
    aave_pool_data_provider="0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
    aave_ui_pool_data_provider="0x5c5228dC8E3a47AeCf1b2eB5C152d024C705AcE6",
    aave_oracle="0x81387c40C24a43cE66c44473D5317217351A9781",
    balancer_vault="0xBA12222222228d8Ba445958a75a0704d566BF2C8",
    uni_v3_swaprouter="0xE592427A0AEce92De3Edee1F18E0157C05861564",
    uni_v3_quoter="0xb27308f9F90D607463bb33eA1BeBb41C27CE5AB6",
    eip1559=True,
    default_gas_limit=500_000,
    min_priority_fee_gwei=0.01,
    min_debt_usd=500.0,          # Lower on Arbitrum due to cheap gas
    close_factor_bps=5000,
    known_assets=tuple(ARBITRUM_ASSETS),
)

# ───────────────────────────────────────────────────────────────────────────────
# Ethereum Mainnet
# ───────────────────────────────────────────────────────────────────────────────

MAINNET_ASSETS: List[Tuple[str, str, int]] = [
    ("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", "WETH", 18),
    ("0xA0b86a33E6441E6C7D3D4B4f6c7D3D4B4f6c7D3", "USDC", 6),  # placeholder — verify
    ("0xdAC17F958D2ee523a2206206994597C13D831ec7", "USDT", 6),
    ("0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599", "WBTC", 8),
    ("0x6B175474E89094C44Da98b954EedeAC495271d0F", "DAI", 18),
    ("0x514910771AF9Ca656af840dff83E8264EcF986CA", "LINK", 18),
    ("0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0", "wstETH", 18),
    ("0xae78736Cd615f374D3085123A210448E74Fc6393", "rETH", 18),
    ("0xBe9895146f7AF43049ca1c1AE358B0541Ea49704", "cbETH", 18),
    ("0x5f98805A4E8be255a32880FDeC7F6728C6568bA0", "LUSD", 18),
    ("0xD533a949740bb3306d119CC777fa900bA034cd52", "CRV", 18),
    ("0xba100000625a3754423978a60c9317c58a424e3D", "BAL", 18),
]

MAINNET = ChainConfig(
    name="mainnet",
    chain_id=1,
    rpc_env_var="MAINNET_HTTP_URL",
    ws_env_var="MAINNET_WS_URL",
    aave_pool="0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
    aave_pool_data_provider="0x7C82513b69C1B42C23760C0CC7515F6bA9E9cF4A",
    aave_ui_pool_data_provider="0x91c0eA31b49B69Ea18607702c5d9aC360bf3dE7d",
    aave_oracle="0x54586bE62E3c3580375aE3723C145253060Ca0C2",
    balancer_vault="0xBA12222222228d8Ba445958a75a0704d566BF2C8",
    uni_v3_swaprouter="0xE592427A0AEce92De3Edee1F18E0157C05861564",
    uni_v3_quoter="0xb27308f9F90D607463bb33eA1BeBb41C27CE5AB6",
    flashbots_relay="https://relay.flashbots.net",
    flashbots_auth_signer="FLASHBOTS_AUTH_KEY",
    eip1559=True,
    default_gas_limit=500_000,
    min_priority_fee_gwei=0.1,
    min_debt_usd=5000.0,         # Higher due to mainnet gas costs
    close_factor_bps=5000,
    known_assets=tuple(MAINNET_ASSETS),
)

# ───────────────────────────────────────────────────────────────────────────────
# Registry
# ───────────────────────────────────────────────────────────────────────────────

CHAINS = {
    1: MAINNET,
    42161: ARBITRUM,
    "mainnet": MAINNET,
    "arbitrum": ARBITRUM,
}


def get_chain_config(chain_id_or_name) -> ChainConfig:
    """Resolve chain config by ID (int) or name (str)."""
    if isinstance(chain_id_or_name, int):
        if chain_id_or_name not in CHAINS:
            raise ValueError(f"Unsupported chain ID: {chain_id_or_name}")
        return CHAINS[chain_id_or_name]
    if isinstance(chain_id_or_name, str):
        key = chain_id_or_name.lower()
        if key not in CHAINS:
            raise ValueError(f"Unsupported chain: {chain_id_or_name}")
        return CHAINS[key]
    raise ValueError(f"Invalid chain identifier: {chain_id_or_name}")
