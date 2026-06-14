// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import "../contracts/FlashExecutorV3.sol";

interface IAavePoolFull {
    function getUserAccountData(address) external view returns (
        uint256 totalCollateralBase, uint256 totalDebtBase,
        uint256 availableBorrowsBase, uint256 currentLiquidationThreshold,
        uint256 ltv, uint256 healthFactor
    );
}

interface IAaveOracle {
    function getAssetPrice(address asset) external view returns (uint256);
}

contract CrossAssetReplayTest is Test {
    address constant WETH           = 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1;
    address constant USDC_NATIVE    = 0xaf88d065e77c8cC2239327C5EDb3A432268e5831;
    address constant USDC_BRIDGE    = 0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8;
    address constant USDT           = 0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9;
    address constant DAI            = 0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1;
    address constant AAVE_POOL      = 0x794a61358D6845594F94dc1DB02A252b5b4814aD;
    address constant BALANCER_VAULT = 0xBA12222222228d8Ba445958a75a0704d566BF2C8;
    address constant UNI_V3_ROUTER  = 0xE592427A0AEce92De3Edee1F18E0157C05861564;
    address constant AAVE_ORACLE    = 0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7;
    string  constant RPC_URL        = "https://necessary-flashy-fog.arbitrum-mainnet.quiknode.pro/9359711fe3d68c27e68e33106299b588e43c96db/";

    // ─── WETH -> USDC.native ──────────────────────────────────

    function test_CrossAsset_WETH_to_USDC() public {
        uint256 BLOCK = 350031196;
        address borrower = 0xf753299a01609ffeBA4BD9b2D272085a618A33d8;
        uint256 profit = _runCrossAsset(
            BLOCK, borrower, WETH, USDC_NATIVE, 500, 1376904682, 477543821343834044, true
        );
        emit log_named_uint("PROFIT WETH->USDC (USDC wei)", profit);
        assertGt(profit, 0, "Profit must be > 0");
    }

    // ─── WETH -> USDT ────────────────────────────────────────

    function test_CrossAsset_WETH_to_USDT() public {
        uint256 BLOCK = 350031795;
        address borrower = 0x79169c573aEb0Ea29534FEA8d64417b408284CD0;
        uint256 profit = _runCrossAsset(
            BLOCK, borrower, WETH, USDT, 500, 6280633, 2162207890197948, true
        );
        emit log_named_uint("PROFIT WETH->USDT (USDT wei)", profit);
        assertGt(profit, 0, "Profit must be > 0");
    }

    // ─── USDC -> DAI ─────────────────────────────────────────

    function test_CrossAsset_USDC_to_DAI() public {
        uint256 BLOCK = 240473822;
        address borrower = 0xe10FC9f5B33469A9C1e86dbC0326B4cDA77f7FC2;
        // Known: debtToCover=1,655,740,486,121,650,851, liqCollat=1,730,098
        uint256 profit = _runCrossAsset(
            BLOCK, borrower, USDC_BRIDGE, DAI, 100, 
            1655740486121650851, 1730098, false
        );
        emit log_named_uint("PROFIT USDC->DAI (DAI wei)", profit);
        assertGt(profit, 0, "Profit must be > 0");
    }

    // ─── Core ──────────────────────────────────────────────────

    function _runCrossAsset(
        uint256 forkBlock, address borrower,
        address collAsset, address debtAsset, uint24 fee,
        uint256 debtToCover, uint256 expectedCollateral,
        bool crashCollateralOracle
    ) internal returns (uint256 profit) {
        vm.createSelectFork(RPC_URL, forkBlock);

        // Crash collateral oracle if needed (HF barely above 1.0 at fork block)
        if (crashCollateralOracle) {
            uint256 collPrice = IAaveOracle(AAVE_ORACLE).getAssetPrice(collAsset);
            vm.mockCall(
                AAVE_ORACLE,
                abi.encodeWithSelector(IAaveOracle.getAssetPrice.selector, collAsset),
                abi.encode(collPrice * 80 / 100) // crash by 20%
            );
            // Verify HF dropped
            (,,, ,, uint256 hfAfter) = IAavePoolFull(AAVE_POOL).getUserAccountData(borrower);
            emit log_named_uint("HF after oracle crash", hfAfter);
            require(hfAfter < 1e18, "Oracle crash did not drop HF below 1.0");
        }

        // ── Build swap calldata ──────────────────────────────
        FlashExecutorV3 exec = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
        exec.approveRouter(UNI_V3_ROUTER);

        uint256 balBefore = IERC20(debtAsset).balanceOf(address(exec));
        require(balBefore == 0, "Executor must start with 0 debt token balance");

        // For oracle-crashed tests, bump amountIn to account for more collateral seized
        uint256 amountIn = crashCollateralOracle ? expectedCollateral * 150 / 100 : expectedCollateral;
        uint256 amountOutMin = crashCollateralOracle ? debtToCover * 90 / 100 : debtToCover * 95 / 100;
        emit log_named_uint("swap amountIn", amountIn);
        emit log_named_uint("swap amountOutMin", amountOutMin);

        bytes memory swapCalldata = abi.encodeWithSelector(
            bytes4(keccak256("exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))")),
            collAsset, debtAsset, fee, address(exec),
            block.timestamp + 3600, amountIn, amountOutMin, uint160(0)
        );

        // ── Execute ──────────────────────────────────────────
        exec.executeLiquidation(
            collAsset, debtAsset, borrower, debtToCover,
            false, UNI_V3_ROUTER, swapCalldata
        );

        uint256 balAfter = IERC20(debtAsset).balanceOf(address(exec));
        uint256 collAfter = IERC20(collAsset).balanceOf(address(exec));
        profit = balAfter;

        emit log_named_uint("debt bal after", balAfter);
        emit log_named_uint("coll bal after", collAfter);
        emit log_string("CROSS-ASSET FLASH LOAN LIQUIDATION PASSED");
        emit log_named_uint("Pre-funding required", 0);
        emit log_named_uint("Profit retained", profit);

        require(profit > 0, "Profit must be > 0");
    }
}
