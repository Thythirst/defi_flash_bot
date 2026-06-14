// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;
import {Test, console} from "forge-std/Test.sol";

interface IAaveOracle {
    function getAssetPrice(address asset) external view returns (uint256);
}
interface IAavePool {
    function getUserAccountData(address user) external view returns (
        uint256 totalCollateralBase, uint256 totalDebtBase,
        uint256 availableBorrowsBase, uint256 currentLiquidationThreshold,
        uint256 ltv, uint256 healthFactor
    );
}

contract DebugTest is Test {
    address constant AAVE_POOL   = 0x794a61358D6845594F94dc1DB02A252b5b4814aD;
    address constant AAVE_ORACLE = 0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7;
    address constant WETH = 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1;
    address constant C_A = 0x572372831A9d6B2E3ee8fa284505599e6125Fea9;

    function test_debug_mock() public {
        vm.createSelectFork("https://arb1.arbitrum.io/rpc");

        // Get original prices and HF
        uint256 wethP = IAaveOracle(AAVE_ORACLE).getAssetPrice(WETH);
        emit log_named_uint("weth_before", wethP);

        (uint256 coll, uint256 debt, , , , uint256 hf) = IAavePool(AAVE_POOL).getUserAccountData(C_A);
        emit log_named_uint("hf_before", hf);

        // Mock WETH to 50% of price
        uint256 newP = wethP / 2;
        vm.mockCall(
            AAVE_ORACLE,
            abi.encodeWithSelector(IAaveOracle.getAssetPrice.selector, WETH),
            abi.encode(newP)
        );

        // Verify mock works
        uint256 wethAfter = IAaveOracle(AAVE_ORACLE).getAssetPrice(WETH);
        emit log_named_uint("weth_after", wethAfter);

        // Check HF
        (coll, debt, , , , hf) = IAavePool(AAVE_POOL).getUserAccountData(C_A);
        emit log_named_uint("hf_after", hf);

        // Should be well underwater at 50% WETH
        assertLt(hf, 1e18, "HF should be < 1.0 at 50% WETH price");
    }
}
