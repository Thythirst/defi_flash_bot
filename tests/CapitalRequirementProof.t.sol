// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import "../contracts/FlashExecutorV3.sol";

contract CapitalRequirementProof is Test {
    address constant WETH           = 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1;
    address constant USDT           = 0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9;
    address constant AAVE_POOL      = 0x794a61358D6845594F94dc1DB02A252b5b4814aD;
    address constant BALANCER_VAULT = 0xBA12222222228d8Ba445958a75a0704d566BF2C8;
    address constant UNI_V3_ROUTER  = 0xE592427A0AEce92De3Edee1F18E0157C05861564;

    // C10 borrower: liquidatable at block 469920464
    address constant BORROWER = 0x5197434559b0B382fEBB96CCc80488A8D14383b7;

    function test_A_RevertWithZeroBalance() public {
        emit log_string("=== TEST A: executeLiquidationDirect with 0 USDT ===");

        (,,, ,, uint256 hf) = IAavePoolFull(AAVE_POOL).getUserAccountData(BORROWER);
        emit log_named_uint("Borrower HF", hf);
        require(hf < 1e18, "Not liquidatable");

        FlashExecutorV3 exec = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);

        uint256 bal = IERC20(USDT).balanceOf(address(exec));
        emit log_named_uint("Executor USDT", bal);
        assertEq(bal, 0, "Must be 0");

        // Revert: Must hit InsufficientBalance (0xf4d678b8) when swapRouter=address(0)
        // Production _encode_execute_liquidation() passes address(0) as swapRouter
        vm.expectRevert(bytes4(0xf4d678b8));
        exec.executeLiquidationDirect(
            WETH, USDT, BORROWER, 1000000,
            false, address(0), ""  // address(0) matches production encoding
        );
        emit log_string("[PASS] Reverted: InsufficientBalance()");
    }

    function test_B_SuccessWithFundedBalance() public {
        emit log_string("=== TEST B: executeLiquidationDirect with funded USDT ===");

        (,,, ,, uint256 hf) = IAavePoolFull(AAVE_POOL).getUserAccountData(BORROWER);
        emit log_named_uint("Borrower HF", hf);
        require(hf < 1e18, "Not liquidatable");

        FlashExecutorV3 exec = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
        exec.approveRouter(UNI_V3_ROUTER);

        uint256 debtToCover = 1962730; // from historical C10 log

        // FUND via deal()
        deal(USDT, address(exec), debtToCover * 2);
        uint256 bal = IERC20(USDT).balanceOf(address(exec));
        emit log_named_uint("Executor USDT funded", bal);
        assertGe(bal, debtToCover, "Must have USDT");

        uint256 collBefore = IERC20(WETH).balanceOf(address(exec));
        uint256 profit = exec.executeLiquidationDirect(
            WETH, USDT, BORROWER, debtToCover,
            false, address(0), ""  // address(0) matches production encoding
        );
        uint256 collAfter = IERC20(WETH).balanceOf(address(exec));

        emit log_string("[PASS] LIQUIDATION SUCCEEDED");
        emit log_named_uint("Profit (USDT wei)", profit);
        emit log_named_uint("WETH received", collAfter - collBefore);
        assertGe(profit, 0, "Profit >= 0");
    }
}

interface IAavePoolFull {
    function getUserAccountData(address user) external view returns (
        uint256 totalCollateralBase, uint256 totalDebtBase,
        uint256 availableBorrowsBase, uint256 currentLiquidationThreshold,
        uint256 ltv, uint256 healthFactor
    );
}
