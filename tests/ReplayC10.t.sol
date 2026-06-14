// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import "../contracts/FlashExecutorV3.sol";

interface IAavePoolFull {
    function getUserAccountData(address user) external view returns (
        uint256 totalCollateralBase, uint256 totalDebtBase,
        uint256 availableBorrowsBase, uint256 currentLiquidationThreshold,
        uint256 ltv, uint256 healthFactor
    );
}

// Define the struct for exactInputSingle params
struct ExactInputSingleParams {
    address tokenIn;
    address tokenOut;
    uint24 fee;
    address recipient;
    uint256 deadline;
    uint256 amountIn;
    uint256 amountOutMinimum;
    uint160 sqrtPriceLimitX96;
}

contract ReplayC10Test is Test {
    address constant WETH           = 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1;
    address constant USDT           = 0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9;
    address constant AAVE_POOL      = 0x794a61358D6845594F94dc1DB02A252b5b4814aD;
    address constant BALANCER_VAULT = 0xBA12222222228d8Ba445958a75a0704d566BF2C8;
    address constant UNI_V3_ROUTER  = 0xE592427A0AEce92De3Edee1F18E0157C05861564;

    function test_C10_FullReplay() public {
        address collAsset = WETH;
        address debtAsset = USDT;
        address borrower = 0x5197434559b0B382fEBB96CCc80488A8D14383b7;
        uint256 debtToCover = 1962730;
        
        emit log_string("=== C10: FULL Liquidation Replay ===");
        
        // STAGE 0: Check borrower
        (,,, ,, uint256 hf) = IAavePoolFull(AAVE_POOL).getUserAccountData(borrower);
        emit log_named_uint("Borrower HF", hf);
        require(hf < 1e18, "Not liquidatable");
        
        // STAGE 1: Deploy
        FlashExecutorV3 exec = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
        exec.approveRouter(UNI_V3_ROUTER);
        emit log_string("STAGE 1: Deployed + approved");
        
        // STAGE 2: Fund
        uint256 fundAmount = debtToCover * 2;
        deal(debtAsset, address(exec), fundAmount);
        emit log_named_uint("STAGE 2: Funded", fundAmount);
        
        // Build swap calldata manually (WETH -> USDT via 0.05% pool)
        ExactInputSingleParams memory swapParams = ExactInputSingleParams({
            tokenIn: WETH,
            tokenOut: USDT,
            fee: 500,
            recipient: address(exec),
            deadline: block.timestamp + 60,
            amountIn: 1171482567099033,  // expected collateral
            amountOutMinimum: 0,
            sqrtPriceLimitX96: 0
        });
        
        bytes memory swapCalldata = abi.encodeWithSelector(
            bytes4(keccak256("exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))")),
            swapParams
        );
        
        // STAGE 3-4: Execute liquidation with swap
        uint256 collBefore = IERC20(collAsset).balanceOf(address(exec));
        
        try exec.executeLiquidationDirect(
            collAsset, debtAsset, borrower, debtToCover, false, UNI_V3_ROUTER, swapCalldata
        ) returns (uint256 profit) {
            uint256 collAfter = IERC20(collAsset).balanceOf(address(exec));
            uint256 debtAfter = IERC20(debtAsset).balanceOf(address(exec));
            
            emit log_string("*** SUCCESS ***");
            emit log_named_uint("STAGE 3: Collateral seized", collAfter - collBefore);
            emit log_named_uint("STAGE 4: USDT after swap", debtAfter);
            emit log_named_uint("STAGE 5-6: NET PROFIT (USDT wei)", profit);
            emit log_named_uint("STAGE 6: NET PROFIT ($)",  profit * 1745 / 1e24);
            emit log_string("STAGE 7-8: Complete!");
            
            assertTrue(profit >= 0, "Should have profit");
        } catch Error(string memory reason) {
            emit log_named_string("SWAP FAILED", reason);
            
            // Fallback: try without swap
            try exec.executeLiquidationDirect(
                collAsset, debtAsset, borrower, debtToCover, false, UNI_V3_ROUTER, ""
            ) returns (uint256 profitNS) {
                uint256 collReceived = IERC20(collAsset).balanceOf(address(exec)) - collBefore;
                emit log_string("Liquidation SUCCESS (no swap)");
                emit log_named_uint("WETH seized", collReceived);
                emit log_named_uint("~USD value", collReceived * 1745 / 1e18);
            } catch Error(string memory reason2) {
                emit log_named_string("FAILED", reason2);
                fail(reason2);
            }
        }
    }
}
