// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

// Real on-chain gas for executeLiquidationBatch, measured against REAL Aave V3
// positions on an Arbitrum fork. Robust harness: instead of fabricating a
// position via vm.store (the old harness did this and broke — HF read as
// max-uint), we use REAL borrowers (debt/collateral/userConfig already on chain)
// and only (a) crash collateral prices via the oracle to force HF<1, and
// (b) deal() WETH to the executor so flash repayment succeeds regardless of the
// swap (receiveAToken=true skips the DEX leg). The liquidationCall itself runs
// against real Aave => real gas.
//
// Run:
//   forge test --match-path tests/BatchLiquidationForkGas.t.sol \
//     --fork-url <arbitrum-rpc> -vv

import "forge-std/Test.sol";
import "../contracts/FlashExecutorV3.sol";

interface IAaveOracle { function getAssetPrice(address) external view returns (uint256); }

contract BatchLiquidationForkGas is Test {
    address constant WETH           = 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1;
    address constant USDC_N         = 0xaf88d065e77c8cC2239327C5EDb3A432268e5831;
    address constant WBTC           = 0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f;
    address constant ARB            = 0x912CE59144191C1204E64559FE8253a0e49E6548;
    address constant AAVE_POOL      = 0x794a61358D6845594F94dc1DB02A252b5b4814aD;
    address constant BALANCER_VAULT = 0xBA12222222228d8Ba445958a75a0704d566BF2C8;
    address constant AAVE_ORACLE    = 0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7;

    // Real WETH-debt borrowers (collateral != WETH) found on the live watchlist.
    address[5] BORROWERS = [
        0x8b81420441aC3933C58D1190C8499c2F89Eb1263,
        0x1424d354526b50a00e1Ff2f7Fc395c5D7126fB6a,
        0x2c48A579668F76dbf59c20B9273dA9083bE65f52,
        0x191dc09deF85aBf9Ab4F9f76d802708229bb2cCc,
        0x60E993F043d02DE1090f6BCB0c7BC6322D4A8eFF
    ];
    // Collateral asset to seize per borrower (one they actually hold).
    address[5] COLLS = [WBTC, USDC_N, USDC_N, WBTC, WBTC];

    FlashExecutorV3 exec;

    function setUp() public {
        exec = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
        // Crash non-WETH collateral prices to 60% so HF dips below 1 while seize
        // amounts stay feasible (not a near-zero price that would over-seize).
        _crash(USDC_N); _crash(WBTC); _crash(ARB);
    }

    function _crash(address asset) internal {
        uint256 p = IAaveOracle(AAVE_ORACLE).getAssetPrice(asset);
        vm.mockCall(AAVE_ORACLE,
            abi.encodeWithSelector(IAaveOracle.getAssetPrice.selector, asset),
            abi.encode(p * 60 / 100));
    }

    function _debt(address user) internal view returns (uint256 vDebt) {
        // AaveProtocolDataProvider.getUserReserveData(WETH, user) -> variableDebt is word 2
        (, bytes memory d) = address(0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654).staticcall(
            abi.encodeWithSelector(0x28dd2d01, WETH, user));
        if (d.length >= 96) { assembly { vDebt := mload(add(d, 96)) } }
    }

    function _items(uint256 n) internal view returns (BatchItem[] memory items, uint256 total) {
        items = new BatchItem[](n);
        for (uint256 i = 0; i < n; i++) {
            uint256 cover = _debt(BORROWERS[i]) * 40 / 100; // under close factor
            total += cover;
            items[i] = BatchItem({
                collateralAsset: COLLS[i],
                borrower: BORROWERS[i],
                debtToCover: cover,
                receiveAToken: true,        // receive aTokens => no DEX swap needed
                swapRouter: address(0),
                swapCalldata: ""
            });
        }
    }

    function _run(uint256 n) internal returns (uint256 gasUsed) {
        (BatchItem[] memory items, uint256 total) = _items(n);
        // Backstop flash repayment (we keep aTokens, not WETH) so the profit gate passes.
        deal(WETH, address(exec), total * 2);
        uint256 g0 = gasleft();
        exec.executeLiquidationBatch(WETH, items);
        gasUsed = g0 - gasleft();
        emit log_named_uint(string.concat("REAL batch gas N=", vm.toString(n)), gasUsed);
        emit log_named_uint("  real gas/item", gasUsed / n);
    }

    function test_realfork_gas_N1() public { _run(1); }
    function test_realfork_gas_N3() public { _run(3); }
    function test_realfork_gas_N5() public {
        uint256 g = _run(5);
        // Sanity: a 5-position real batch must fit well under Arbitrum per-tx gas.
        assertLt(g, 15_000_000, "5-batch must fit per-tx gas budget");
    }
}
