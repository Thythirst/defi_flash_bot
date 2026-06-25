"""Unit tests for batch_liquidation_builder (#5)."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "services", "rev2"))

from eth_abi import decode
import batch_liquidation_builder as B


USDC = "0x" + "aa" * 20
WETH = "0x" + "bb" * 20
_ITEMS_T = ["address", "(address,address,uint256,bool,address,bytes)[]"]


def _pos(borrower, debt_asset, amt, profit, router="0x" + "00" * 20, cd=b""):
    return B.LiqPosition(
        borrower=borrower, collateral_asset="0x" + "11" * 20,
        debt_asset=debt_asset, debt_to_cover=amt, swap_router=router,
        swap_calldata=cd, est_profit_usd=profit,
    )


def test_selector_matches_contract():
    # keccak("executeLiquidationBatch(address,(address,address,uint256,bool,address,bytes)[])")[:4]
    assert "0x" + B.SELECTOR.hex() == "0x57a01849"


def test_grouping_and_profit_ordering():
    positions = [
        _pos("0x" + "01" * 20, USDC, 1000, 50.0, "0x" + "22" * 20, b"\xde\xad\xbe\xef"),
        _pos("0x" + "02" * 20, USDC, 2000, 90.0, "0x" + "22" * 20, b"\xca\xfe"),
        _pos("0x" + "03" * 20, WETH, 5 * 10**17, 30.0),
    ]
    batches = B.build_batches(positions)
    assert len(batches) == 2                       # two debt-asset groups
    assert batches[0]["debt_asset"] == USDC.lower()  # most profitable group first
    assert batches[0]["total_debt"] == 3000
    assert batches[0]["items"][0].est_profit_usd == 90.0  # profit desc within group


def test_calldata_roundtrip():
    positions = [
        _pos("0x" + "02" * 20, USDC, 2000, 90.0, "0x" + "22" * 20, b"\xca\xfe"),
        _pos("0x" + "01" * 20, USDC, 1000, 50.0, "0x" + "22" * 20, b"\xde\xad\xbe\xef"),
    ]
    cd = B.build_batches(positions)[0]["calldata"]
    assert cd[:4] == B.SELECTOR
    debt_asset, items = decode(_ITEMS_T, cd[4:])
    assert debt_asset.lower() == USDC.lower()
    assert len(items) == 2
    assert items[0][2] == 2000 and items[0][5] == b"\xca\xfe"  # highest profit first
    assert items[1][2] == 1000


def test_chunking_respects_max():
    many = [_pos(f"0x{i:040x}", USDC, 100 + i, float(i)) for i in range(15)]
    batches = B.build_batches(many)
    assert sorted(len(b["items"]) for b in batches) == [3, 12]


def test_empty_batch_rejected():
    raised = False
    try:
        B.encode_batch_calldata(USDC, [])
    except ValueError:
        raised = True
    assert raised, "empty batch must raise ValueError"
