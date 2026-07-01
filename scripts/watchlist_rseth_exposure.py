"""
watchlist_rseth_exposure.py — Check the live watchlist for rsETH collateral
exposure, and detect the eMode gap where Aave's post-exploit freeze
doesn't actually apply.

Background: rsETH (Kelp DAO) suffered a $292M cross-chain bridge exploit
in April 2026 (LayerZero verifier infrastructure compromised). Aave froze
the rsETH reserve in response — LTV=0%, liquidation threshold slashed to
0.10%. But existing positions enrolled in eMode category 5
("rsETH/wstETH/WETH ETH Correlated", threshold 95%) are grandfathered:
Aave revoked rsETH's reserve-level eMode assignment (blocking *new*
entrants) but eMode fully overrides the frozen per-asset threshold for
users already in the category. getUserAccountData() for those users
still returns a health factor computed off ~95%, not 0.10% — the freeze
protects against new risk, not the risk already on the books.

As of 2026-07-01: 3 whale positions on this bot's Arbitrum watchlist
hold ~99.8% of Aave Arbitrum's entire rsETH supply ($34.9M collateral
combined), all with HF between 1.02 and 1.04, all riding the eMode
override. See project memory for the full writeup.

Why this matters for liquidation risk specifically (not just accounting):
Arbitrum rsETH DEX liquidity has evacuated post-exploit (see
lst_lrt_cross_dex_depth.py) — pools exhaust at ~1-5 units. If any of
these positions cross HF<1.0, seizing tens of thousands of rsETH via
liquidation and trying to sell it hits a wall the pools can't remotely
absorb. Worth periodic re-running given how tight these HFs already are.

Usage:
    python scripts/watchlist_rseth_exposure.py
"""

from __future__ import annotations

import asyncio

import redis
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
RSETH = "0x4186BFC76E2E237523CBC30FD220FE055156b41F"
RSETH_ATOKEN = "0x6b030Ff3FB9956B1B69f475B77aE0d3Cf2CC5aFa"
POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
BALANCE_OF_SELECTOR = "70a08231"
WHALE_THRESHOLD_RSETH = 1.0  # report holders above this, others are dust

MULTICALL3_ABI = [{
    "inputs": [{"components": [
        {"name": "target", "type": "address"},
        {"name": "allowFailure", "type": "bool"},
        {"name": "callData", "type": "bytes"}],
        "name": "calls", "type": "tuple[]"}],
    "name": "aggregate3",
    "outputs": [{"components": [
        {"name": "success", "type": "bool"},
        {"name": "returnData", "type": "bytes"}],
        "name": "returnData", "type": "tuple[]"}],
    "stateMutability": "payable", "type": "function",
}]

POOL_ABI = [
    {"inputs": [{"name": "user", "type": "address"}], "name": "getUserEMode",
     "outputs": [{"name": "", "type": "uint256"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "id", "type": "uint8"}], "name": "getEModeCategoryData",
     "outputs": [{"components": [
         {"name": "ltv", "type": "uint16"},
         {"name": "liquidationThreshold", "type": "uint16"},
         {"name": "liquidationBonus", "type": "uint16"},
         {"name": "priceSource", "type": "address"},
         {"name": "label", "type": "string"}],
         "name": "", "type": "tuple"}], "stateMutability": "view", "type": "function"},
    {"inputs": [{"name": "user", "type": "address"}], "name": "getUserAccountData",
     "outputs": [
         {"name": "totalCollateralBase", "type": "uint256"},
         {"name": "totalDebtBase", "type": "uint256"},
         {"name": "availableBorrowsBase", "type": "uint256"},
         {"name": "currentLiquidationThreshold", "type": "uint256"},
         {"name": "ltv", "type": "uint256"},
         {"name": "healthFactor", "type": "uint256"}],
     "stateMutability": "view", "type": "function"},
]


async def find_rseth_holders(w3):
    r = redis.Redis(host="localhost", port=6379, decode_responses=True)
    addrs = r.zrange("arb:watchlist:active", 0, -1)
    print(f"Loaded {len(addrs)} watchlist addresses")

    mc = w3.eth.contract(address=AsyncWeb3.to_checksum_address(MULTICALL3), abi=MULTICALL3_ABI)
    BATCH = 800
    holders = []
    for i in range(0, len(addrs), BATCH):
        chunk = addrs[i:i + BATCH]
        calls = []
        for a in chunk:
            addr_clean = a[2:] if a.startswith("0x") else a
            calldata = bytes.fromhex(BALANCE_OF_SELECTOR + addr_clean.rjust(64, "0"))
            calls.append((AsyncWeb3.to_checksum_address(RSETH_ATOKEN), True, calldata))
        try:
            results = await mc.functions.aggregate3(calls).call()
        except Exception as e:
            print(f"  batch {i}-{i+len(chunk)}: FAILED {str(e)[:150]}")
            continue
        for addr, (success, data) in zip(chunk, results):
            if success and len(data) >= 32:
                bal = int.from_bytes(data[:32], "big")
                if bal > 0:
                    holders.append((addr, bal / 1e18))
    return holders


async def report_whale(w3, pool, addr, bal):
    emode = await pool.functions.getUserEMode(AsyncWeb3.to_checksum_address(addr)).call()
    d = await pool.functions.getUserAccountData(AsyncWeb3.to_checksum_address(addr)).call()
    total_col, total_debt, _avail, cur_liq_thresh, _cur_ltv, hf = d
    print(f"  {addr}: {bal:,.4f} rsETH  collateral=${total_col/1e8:,.2f}  debt=${total_debt/1e8:,.2f}  "
          f"HF={hf/1e18:.4f}  eMode={emode}  currentLiqThreshold={cur_liq_thresh/100:.2f}%")
    if emode != 0:
        cat = await pool.functions.getEModeCategoryData(emode).call()
        _ltv, liq_th, _bonus, _price_source, label = cat
        if abs(cur_liq_thresh / 100 - liq_th / 100) < 1.0:
            print(f"    -> eMode {emode} ('{label}', threshold {liq_th/100:.2f}%) is overriding "
                  f"rsETH's frozen reserve-level threshold. Not protected by the freeze.")


async def main():
    w3 = AsyncWeb3(AsyncHTTPProvider("https://arb1.arbitrum.io/rpc"))
    holders = await find_rseth_holders(w3)
    print(f"\nrsETH aToken holders in watchlist: {len(holders)}")

    whales = [(a, b) for a, b in holders if b >= WHALE_THRESHOLD_RSETH]
    dust = [(a, b) for a, b in holders if b < WHALE_THRESHOLD_RSETH]
    print(f"  {len(whales)} above {WHALE_THRESHOLD_RSETH} rsETH (checking eMode/HF), {len(dust)} dust\n")

    pool = w3.eth.contract(address=AsyncWeb3.to_checksum_address(POOL), abi=POOL_ABI)
    for addr, bal in sorted(whales, key=lambda x: -x[1]):
        await report_whale(w3, pool, addr, bal)


if __name__ == "__main__":
    asyncio.run(main())
