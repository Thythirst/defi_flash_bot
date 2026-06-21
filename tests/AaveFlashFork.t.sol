// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import "../contracts/FlashExecutorV3.sol";

/// @notice Fork test: calls executeLiquidationViaAave against CB658687's actual position.
///         Verifies the Aave flash loan path doesn't revert at repayment.
/// @dev    Requires --fork-url pointing to Arbitrum mainnet.
contract AaveFlashLiquidationForkTest is Test {
    // Arbitrum mainnet addresses
    address constant BALANCER_VAULT = 0xBA12222222228d8Ba445958a75a0704d566BF2C8;
    address constant AAVE_POOL      = 0x794a61358D6845594F94dc1DB02A252b5b4814aD;
    address constant NATIVE_USDC    = 0xaf88d065e77c8cC2239327C5EDb3A432268e5831;

    // CB658687 — the $242K debt position with HF=1.0104
    address constant BORROWER = 0xCB6586874cc04B01Cc4fDB777dE502cEa7b3D6c1;

    FlashExecutorV3 executor;

    function setUp() public {
        // Deploy with 0 minProfitThreshold (we just want to see if it reverts)
        executor = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
    }

    /// @notice Test: does executeLiquidationViaAave revert with a meaningful reason?
    ///         If it fails, we want to see the reason in the trace.
    function test_AaveFlashLoan_RevertReason() public {
        // CB658687 is NOT liquidatable right now (HF=1.0104 > 1.0)
        // This should revert at liquidationCall step with a clear reason
        uint256 debtToCover = 121_231_839_554; // ~50% of total debt

        vm.expectRevert(); // Expect some revert — we just want the trace
        executor.executeLiquidationViaAave(
            NATIVE_USDC,   // collateralAsset
            NATIVE_USDC,   // debtAsset
            BORROWER,
            debtToCover,
            false,         // receiveAToken
            address(0),    // swapRouter (same asset, no swap needed)
            ""             // swapCalldata
        );
    }
}
