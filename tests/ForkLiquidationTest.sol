// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {Test, console} from "forge-std/Test.sol";
import {FlashExecutorV3} from "../contracts/FlashExecutorV3.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

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

contract ForkLiquidationTest is Test {
    FlashExecutorV3 public executor;

    address constant AAVE_POOL   = 0x794a61358D6845594F94dc1DB02A252b5b4814aD;
    address constant AAVE_ORACLE = 0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7;
    address constant BALANCER_VAULT = 0xBA12222222228d8Ba445958a75a0704d566BF2C8;

    address constant WETH = 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1;
    address constant WBTC = 0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f;
    address constant USDC = 0xaf88d065e77c8cC2239327C5EDb3A432268e5831;
    address constant USDT = 0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9;

    // Variable debt token addresses — from fork traces (authoritative)
    address constant vWETH = 0x0c84331e39d6658Cd6e6b9ba04736cC4c4734351;
    address constant vWBTC = 0x92b42c66840C7AD907b4BF74879FF3eF7c529473;
    address constant vUSDC = 0xFCcF00A7bc5b60DFe47664c79E86e11fd869Efc8;
    address constant vUSDT = 0xf611aEb5013fD2c0511c9CD55c7dc5C1140741A6;  // corrected from trace

    // aToken addresses
    address constant aWETH = 0xe50fA9b3c56FfB159cB0FCA61F5c9D750e8128c8;
    address constant aUSDC = 0x724dc807b04555b71ed48a6896b6F41593b8C637;
    address constant aUSDT = 0x6ab707Aca953eDAeFBc4fD23bA73294241490620;

    address constant C_A = 0x572372831A9d6B2E3ee8fa284505599e6125Fea9;
    address constant C_B = 0xF9D4FD46E2d1435e7BaC9BCee6fA9536e76e5101;
    address constant C_C = 0x6f46C54D556FC8e040AC9226196605EeBDf334A1;
    address constant C_D = 0x270d1C8C0f13fF925f710dFf38BF806BDbb4e6B2;
    address constant C_E = 0x2406B3e14C2A2A7D394e24C5Dc0170F9Bc9f0166;

    function setUp() public {
        string memory rpc = vm.envOr("ARBITRUM_RPC_URL", string("https://arb1.arbitrum.io/rpc"));
        vm.createSelectFork(rpc);
        executor = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
    }

    function getData(address u) internal view returns (uint256 hf, uint256 coll, uint256 debt, uint256 lt) {
        (coll, debt, , lt, , hf) = IAavePool(AAVE_POOL).getUserAccountData(u);
    }

    function debtOf(address vToken, address user) internal view returns (uint256) {
        return IERC20(vToken).balanceOf(user);
    }

    function mockPrice(address asset, uint256 newPrice) internal {
        vm.mockCall(AAVE_ORACLE,
            abi.encodeWithSelector(IAaveOracle.getAssetPrice.selector, asset),
            abi.encode(newPrice));
    }

    // ---------------------------------------------------------------
    // CANDIDATE A: WETH debt. Drop WBTC by 18%.
    // debtInNative = WETH variableDebt balance
    // ---------------------------------------------------------------
    function test_candidate_A() public {
        (uint256 hf,,,) = getData(C_A);
        assertLt(hf * 82 / 100, 1e18); // sanity: 18% WBTC drop should push underwater

        uint256 wbtcP = IAaveOracle(AAVE_ORACLE).getAssetPrice(WBTC);
        mockPrice(WBTC, wbtcP * 82 / 100);
        (hf,,,) = getData(C_A);
        assertLt(hf, 1e18);

        uint256 wethDebt = debtOf(vWETH, C_A);
        uint256 cover = wethDebt / 2;
        deal(WETH, address(executor), cover);

        uint256 g0 = gasleft();
        executor.executeLiquidationDirect(WETH, WETH, C_A, cover, false, address(0), "");
        uint256 gasUsed = g0 - gasleft();
        uint256 wethEnd = IERC20(WETH).balanceOf(address(executor));
        uint256 profit = wethEnd > cover ? wethEnd - cover : 0;
        emit log_named_uint("A_gas", gasUsed);
        emit log_named_uint("A_profitWei", profit);
        assertGt(profit, 0);
    }

    // ---------------------------------------------------------------
    // CANDIDATE D: USDC coll, USDT debt (from trace: vUSDT scalBal > 0)
    // Inflate USDT 15% to push underwater
    // ---------------------------------------------------------------
    function test_candidate_D() public {
        uint256 usdtDebt = debtOf(vUSDT, C_D);
        if (usdtDebt == 0) {
            // Try WETH
            usdtDebt = debtOf(vWETH, C_D);
            if (usdtDebt == 0) { emit log("D:no_debt"); return; }
        }
        emit log_named_uint("D_debtNative", usdtDebt);

        (uint256 hf,,,) = getData(C_D);
        emit log_named_uint("D_hf_pre", hf);

        // Identify which debt asset is non-zero, mock IT
        if (debtOf(vUSDT, C_D) > 0) {
            uint256 p = IAaveOracle(AAVE_ORACLE).getAssetPrice(USDT);
            mockPrice(USDT, p * 115 / 100);
        } else if (debtOf(vWETH, C_D) > 0) {
            uint256 p = IAaveOracle(AAVE_ORACLE).getAssetPrice(WETH);
            mockPrice(WETH, p * 115 / 100);
        }
        (hf,,,) = getData(C_D);
        emit log_named_uint("D_hf_post", hf);
        assertLt(hf, 1e18);

        uint256 cover = usdtDebt / 2;
        address debtAsset = debtOf(vUSDT, C_D) > 0 ? USDT : WETH;
        deal(debtAsset, address(executor), cover);

        uint256 g0 = gasleft();
        executor.executeLiquidationDirect(USDC, debtAsset, C_D, cover, false, address(0), "");
        uint256 gasUsed = g0 - gasleft();
        uint256 collEnd = IERC20(USDC).balanceOf(address(executor));
        emit log_named_uint("D_gas", gasUsed);
        emit log_named_uint("D_usdcSeized", collEnd);
        assertGt(collEnd, 0);
    }

    // ---------------------------------------------------------------
    // CANDIDATE C: USDC coll, WETH debt
    // ---------------------------------------------------------------
    function test_candidate_C() public {
        uint256 wethDebt = debtOf(vWETH, C_C);
        emit log_named_uint("C_wethDebt", wethDebt);
        if (wethDebt == 0) { emit log("C:no WETH debt"); return; }

        (uint256 hf,,,) = getData(C_C);
        uint256 wethP = IAaveOracle(AAVE_ORACLE).getAssetPrice(WETH);
        mockPrice(WETH, wethP * 130 / 100); // 30% WETH increase
        (hf,,,) = getData(C_C);
        emit log_named_uint("C_hf_post", hf);
        assertLt(hf, 1e18);

        uint256 cover = wethDebt / 2;
        deal(WETH, address(executor), cover);
        uint256 g0 = gasleft();
        executor.executeLiquidationDirect(USDC, WETH, C_C, cover, false, address(0), "");
        uint256 gasUsed = g0 - gasleft();
        uint256 usdcEnd = IERC20(USDC).balanceOf(address(executor));
        emit log_named_uint("C_gas", gasUsed);
        emit log_named_uint("C_usdc", usdcEnd);
        assertGt(usdcEnd, 0);
    }

    // ---------------------------------------------------------------
    // CANDIDATE B: USDC + WBTC coll, unknown debt
    // ---------------------------------------------------------------
    function test_candidate_B() public {
        // Scan debt
        uint256 wethD = debtOf(vWETH, C_B);
        uint256 usdtD = debtOf(vUSDT, C_B);
        uint256 usdcD = debtOf(vUSDC, C_B);
        emit log_named_uint("B_debtWETH", wethD);
        emit log_named_uint("B_debtUSDT", usdtD);
        emit log_named_uint("B_debtUSDC", usdcD);

        address debtAsset;
        uint256 debtNative;
        if (wethD > 0) { debtAsset = WETH; debtNative = wethD; }
        else if (usdtD > 0) { debtAsset = USDT; debtNative = usdtD; }
        else if (usdcD > 0) { debtAsset = USDC; debtNative = usdcD; }
        else { emit log("B:no_debt_found"); return; }

        (uint256 hf,,,) = getData(C_B);
        if (debtAsset == WETH) {
            uint256 p = IAaveOracle(AAVE_ORACLE).getAssetPrice(WETH);
            mockPrice(WETH, p * 130 / 100);
        }
        (hf,,,) = getData(C_B);
        emit log_named_uint("B_hf_post", hf);
        if (hf >= 1e18) { emit log("B:still_above_1"); return; }

        uint256 cover = debtNative / 2;
        deal(debtAsset, address(executor), cover);
        executor.executeLiquidationDirect(USDC, debtAsset, C_B, cover, false, address(0), "");
        emit log("B_PASS");
    }

    // ---------------------------------------------------------------
    // CANDIDATE E: USDT coll, unknown debt
    // ---------------------------------------------------------------
    function test_candidate_E() public {
        uint256 wethD = debtOf(vWETH, C_E);
        uint256 usdtD = debtOf(vUSDT, C_E);
        emit log_named_uint("E_debtWETH", wethD);
        emit log_named_uint("E_debtUSDT", usdtD);

        if (usdtD == 0 && wethD == 0) { emit log("E:no_debt"); return; }
        address dAsset = usdtD > 0 ? USDT : WETH;
        uint256 dNat = usdtD > 0 ? usdtD : wethD;

        (uint256 hf,,,) = getData(C_E);
        // Push: if USDT debt, drop USDT (coll shrinks); if WETH debt, inflate WETH
        if (dAsset == USDT) {
            uint256 p = IAaveOracle(AAVE_ORACLE).getAssetPrice(USDT);
            mockPrice(USDT, p * 95 / 100);
        } else {
            uint256 p = IAaveOracle(AAVE_ORACLE).getAssetPrice(WETH);
            mockPrice(WETH, p * 120 / 100);
        }
        (hf,,,) = getData(C_E);
        emit log_named_uint("E_hf_post", hf);
        assertLt(hf, 1e18);

        uint256 cover = dNat / 2;
        deal(dAsset, address(executor), cover);
        executor.executeLiquidationDirect(USDT, dAsset, C_E, cover, false, address(0), "");
        emit log("E_PASS");
    }
}
