"""
scanner/liquidation_executor.py — Encode calldata for FlashExecutorV3 liquidation.

Usage:
    from scanner.liquidation_executor import build_liquidation_bundle
    calldata = build_liquidation_bundle(
        borrower="0x48309ff3f59c9e7662d4efea1e4007d54e19bb69",
        collateral_asset="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        debt_asset="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        debt_to_cover=1676732016000000,
        swap_router="0xE592427A0AEce92De3Edee1F18E0157C05861564",
        swap_calldata="0x...",
    )
"""

from eth_abi import encode
from eth_utils import keccak


def encode_liquidation_call(
    collateral_asset: str,
    debt_asset: str,
    user: str,
    debt_to_cover: int,
    receive_a_token: bool = False,
) -> str:
    """Encode `Pool.liquidationCall(...)` calldata for Aave v3."""
    selector = keccak(text="liquidationCall(address,address,address,uint256,bool)")[:4]
    return "0x" + selector.hex() + encode(
        ["address", "address", "address", "uint256", "bool"],
        [collateral_asset, debt_asset, user, debt_to_cover, receive_a_token],
    ).hex()


def encode_uni_v3_exact_input_single(
    token_in: str,
    token_out: str,
    fee: int,
    recipient: str,
    deadline: int,
    amount_in: int,
    amount_out_minimum: int,
    sqrt_price_limit_x96: int = 0,
) -> str:
    """Encode `SwapRouter.exactInputSingle(...)` calldata.

    Matches the actual Uni V3 SwapRouter signature:
    exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))
    """
    selector = keccak(
        text="exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))"
    )[:4]
    return "0x" + selector.hex() + encode(
        ["(address,address,uint24,address,uint256,uint256,uint256,uint160)"],
        [(token_in, token_out, fee, recipient, deadline, amount_in, amount_out_minimum, sqrt_price_limit_x96)],
    ).hex()


def encode_uni_v3_exact_input_single_from_balance(
    token_in: str,
    token_out: str,
    fee: int,
    recipient: str,
    deadline: int,
    amount_out_minimum: int,
    sqrt_price_limit_x96: int = 0,
) -> str:
    """Encode swap with amountIn = 0 placeholder.

    WARNING: The live executor must replace amountIn with the actual collateral
    balance post-liquidation before broadcasting. Uni V3 rejects amountIn=0.
    This helper is for simulation only; use the full encoder for live txs.
    """
    return encode_uni_v3_exact_input_single(
        token_in=token_in,
        token_out=token_out,
        fee=fee,
        recipient=recipient,
        deadline=deadline,
        amount_in=0,
        amount_out_minimum=amount_out_minimum,
        sqrt_price_limit_x96=sqrt_price_limit_x96,
    )


def build_liquidation_bundle(
    borrower: str,
    collateral_asset: str,
    debt_asset: str,
    debt_to_cover: int,
    swap_router: str = "0x0000000000000000000000000000000000000000",
    swap_calldata: str = "0x",
    receive_a_token: bool = False,
) -> str:
    """
    Build the full executeLiquidation calldata for FlashExecutorV3.

    Returns hex string ready for eth_call or sendTransaction.
    """
    selector = keccak(
        text="executeLiquidation(address,address,address,uint256,bool,address,bytes)"
    )[:4]

    encoded = encode(
        ["address", "address", "address", "uint256", "bool", "address", "bytes"],
        [
            collateral_asset,
            debt_asset,
            borrower,
            debt_to_cover,
            receive_a_token,
            swap_router,
            bytes.fromhex(swap_calldata[2:]) if swap_calldata != "0x" else b"",
        ],
    )

    return "0x" + selector.hex() + encoded.hex()


if __name__ == "__main__":
    # Example: WETH collateral, USDC debt
    bundle = build_liquidation_bundle(
        borrower="0x48309ff3f59c9e7662d4efea1e4007d54e19bb69",
        collateral_asset="0x82af49447d8a07e3bd95bd0d56f35241523fbab1",
        debt_asset="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
        debt_to_cover=1676732016000000,
    )
    print(f"Calldata: {bundle[:80]}...")
