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

contract Phase3Retest is Test {
    FlashExecutorV3 public executor;
    address constant AAVE_POOL   = 0x794a61358D6845594F94dc1DB02A252b5b4814aD;
    address constant AAVE_ORACLE = 0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7;
    address constant BALANCER_VAULT = 0xBA12222222228d8Ba445958a75a0704d566BF2C8;

    address constant WETH = 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1;
    address constant WBTC = 0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f;
    address constant USDC_N = 0xaf88d065e77c8cC2239327C5EDb3A432268e5831; // native
    address constant USDC_E = 0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8; // bridged
    address constant USDT   = 0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9;

    // CANONICAL vDebt addresses from Pool.getReserveData()
    address constant vWETH   = 0x0c84331e39d6658Cd6e6b9ba04736cC4c4734351;
    address constant vWBTC   = 0x92b42c66840C7AD907b4BF74879FF3eF7c529473;
    address constant vUSDC_N = 0xf611aEb5013fD2c0511c9CD55c7dc5C1140741A6; // native USDC
    address constant vUSDC_E = 0xFCCf3cAbbe80101232d343252614b6A3eE81C989; // bridged USDC.e
    address constant vUSDT   = 0xfb00AC187a8Eb5AFAE4eACE434F493Eb62672df7;

    address constant C_A = 0x572372831A9d6B2E3ee8fa284505599e6125Fea9;
    address constant C_B = 0xF9D4FD46E2d1435e7BaC9BCee6fA9536e76e5101;
    address constant C_C = 0x6f46C54D556FC8e040AC9226196605EeBDf334A1;
    address constant C_D = 0x270d1C8C0f13fF925f710dFf38BF806BDbb4e6B2;
    address constant C_E = 0x2406B3e14C2A2A7D394e24C5Dc0170F9Bc9f0166;

    function setUp() public {
        vm.createSelectFork(vm.envOr("ARBITRUM_RPC_URL", string("https://arb1.arbitrum.io/rpc")));
        executor = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
    }

    function getData(address u) internal view returns (uint256 hf, uint256 coll, uint256 debt, uint256 lt) {
        (coll, debt, , lt, , hf) = IAavePool(AAVE_POOL).getUserAccountData(u);
    }

    function mockPrice(address asset, uint256 newPrice) internal {
        vm.mockCall(AAVE_ORACLE,
            abi.encodeWithSelector(IAaveOracle.getAssetPrice.selector, asset),
            abi.encode(newPrice));
    }

    // ---------------------------------------------------------------
    // PHASE 3: Full re-test with canonical addresses
    // ---------------------------------------------------------------

    function test_scan_all_debt() public {
        // Scan all 5 candidates across all 5 vDebt tokens
        address[5] memory users = [C_A, C_B, C_C, C_D, C_E];
        string[5] memory labels = ["A", "B", "C", "D", "E"];
        address[5] memory vTokens = [vWETH, vWBTC, vUSDC_N, vUSDC_E, vUSDT];
        string[5] memory vNames = ["WETH", "WBTC", "USDC_n", "USDC_e", "USDT"];

        for (uint i = 0; i < 5; i++) {
            (uint256 hf,, uint256 debt,) = getData(users[i]);
            emit log_named_uint(string(abi.encodePacked(labels[i], "_HF")), hf);
            emit log_named_uint(string(abi.encodePacked(labels[i], "_debtUSD")), debt);
            for (uint j = 0; j < 5; j++) {
                uint256 d = IERC20(vTokens[j]).balanceOf(users[i]);
                if (d > 0) {
                    emit log_named_uint(string(abi.encodePacked(labels[i], "_", vNames[j])), d);
                }
            }
        }
    }

    // --- Candidate D: USDC native coll + USDC native debt = SAME ASSET ---
    function test_candidate_D_sameAsset() public {
        emit log("D: Checking same-asset status...");
        uint256 d_weth  = IERC20(vWETH).balanceOf(C_D);
        uint256 d_wbtc  = IERC20(vWBTC).balanceOf(C_D);
        uint256 d_usdcN = IERC20(vUSDC_N).balanceOf(C_D);
        uint256 d_usdcE = IERC20(vUSDC_E).balanceOf(C_D);
        uint256 d_usdt  = IERC20(vUSDT).balanceOf(C_D);
        emit log_named_uint("D_vWETH", d_weth);
        emit log_named_uint("D_vWBTC", d_wbtc);
        emit log_named_uint("D_vUSDC_n", d_usdcN);
        emit log_named_uint("D_vUSDC_e", d_usdcE);
        emit log_named_uint("D_vUSDT", d_usdt);
        
        if (d_usdcN > 0) {
            emit log("D: Native USDC debt found - same-asset with USDC collateral");
        }
    }

    // --- Phase 4: Flash Loan path for Candidate A ---
    function test_candidate_A_flashLoan() public {
        (uint256 hf,,,) = getData(C_A);
        uint256 wbtcP = IAaveOracle(AAVE_ORACLE).getAssetPrice(WBTC);
        mockPrice(WBTC, wbtcP * 82 / 100);
        (hf,,,) = getData(C_A);
        assertLt(hf, 1e18);

        uint256 wethDebt = IERC20(vWETH).balanceOf(C_A);
        uint256 cover = wethDebt / 2;

        // Flash loan path - Balancer Vault will call back receiveFlashLoan
        uint256 g0 = gasleft();
        try executor.executeLiquidation(WETH, WETH, C_A, cover, false, address(0), "") {
            uint256 gasUsed = g0 - gasleft();
            uint256 wethEnd = IERC20(WETH).balanceOf(address(executor));
            emit log_named_uint("FL_gas", gasUsed);
            emit log_named_uint("FL_profit", wethEnd);
            emit log("FLASH_LOAN_PASS");
        } catch Error(string memory reason) {
            emit log_named_string("FL_revert", reason);
        } catch (bytes memory) {
            emit log("FL_lowlevel_revert");
        }
    }
}
