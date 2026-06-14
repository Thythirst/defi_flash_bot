// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import "../contracts/FlashExecutorV3.sol";

interface IAaveOracle {
    function getAssetPrice(address asset) external view returns (uint256);
}

contract FlashExecutorV3ForkTest is Test {
    address constant WETH           = 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1;
    address constant USDC           = 0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8;
    address constant aWETH          = 0xe50fA9b3c56FfB159cB0FCA61F5c9D750e8128c8;
    address constant vdUSDC         = 0xFCCf3cAbbe80101232d343252614b6A3eE81C989;
    address constant AAVE_POOL      = 0x794a61358D6845594F94dc1DB02A252b5b4814aD;
    address constant BALANCER_VAULT = 0xBA12222222228d8Ba445958a75a0704d566BF2C8;
    address constant AAVE_ORACLE    = 0xb56c2F0B653B2e0b10C9b928C8580Ac5Df02C7C7;
    address constant UNI_V3_ROUTER  = 0xE592427A0AEce92De3Edee1F18E0157C05861564;

    function test_FullLiquidation_EndToEnd() public {
        FlashExecutorV3 exec = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
        exec.approveRouter(UNI_V3_ROUTER);

        // We are the borrower
        address borrower = address(this);

        // ─── Set up collateral: mint aWETH directly ─────────
        // aWETH is proxy at 0xe50f..., implementation at 0xadcb...
        // We need to set our aWETH scaled balance in the aToken storage
        // aToken scaledBalances slot: keccak256(abi.encode(user, slot_index))
        // slot_index for scaledBalances mapping is typically position 3 in the contract
        bytes32 scaledBalanceSlot = keccak256(abi.encode(borrower, uint256(3)));
        // Set scaled balance to ~1 WETH worth
        // At current index ~1.064, raw aWETH = scaled * index
        // We want ~1 WETH aToken => scaled = 1e18 / 1.064 ≈ 0.94e18
        vm.store(aWETH, scaledBalanceSlot, bytes32(uint256(0.94e18)));

        // Also update totalSupply to include our balance
        bytes32 totalSupplySlot = keccak256(abi.encode(uint256(0))); // slot 0? Actually aToken slot 0
        // Read current totalSupply first
        bytes32 currentTS = vm.load(aWETH, bytes32(uint256(0)));
        uint256 newTS = uint256(currentTS) + 0.94e18;
        vm.store(aWETH, bytes32(uint256(0)), bytes32(newTS));

        // ─── Set up debt: set vdUSDC scaled balance ─────────
        bytes32 debtBalanceSlot = keccak256(abi.encode(borrower, uint256(3)));
        // ~1000 USDC worth of debt
        vm.store(vdUSDC, debtBalanceSlot, bytes32(uint256(1000e6)));

        // Update vdUSDC totalSupply
        bytes32 debtTSSlot = bytes32(uint256(0));
        bytes32 currentDebtTS = vm.load(vdUSDC, debtTSSlot);
        uint256 newDebtTS = uint256(currentDebtTS) + 1000e6;
        vm.store(vdUSDC, debtTSSlot, bytes32(newDebtTS));

        // ─── Force HF < 1.0 by crashing WETH price ─────────
        uint256 wethPrice = IAaveOracle(AAVE_ORACLE).getAssetPrice(WETH);
        vm.mockCall(
            AAVE_ORACLE,
            abi.encodeWithSelector(IAaveOracle.getAssetPrice.selector, WETH),
            abi.encode(wethPrice / 100) // crash to 1%
        );

        // Verify HF
        (,,, ,, uint256 hf) = _getAccountData(borrower);
        emit log_named_uint("HF", hf);
        require(hf > 0 && hf < 1e18, "HF not liquidatable");

        // ─── Execute liquidation ────────────────────────────
        uint256 debtToCover = 500e6; // 500 USDC
        uint256 wp = IAaveOracle(AAVE_ORACLE).getAssetPrice(WETH);
        // Debt * bonus / price = collateral WETH
        uint256 collWeth = (debtToCover * 110 / 100) * 1e20 / (wp > 0 ? wp : 1);
        // Cap at something reasonable
        if (collWeth > 1000 ether) collWeth = 1 ether;

        bytes memory swapCalldata = abi.encodeWithSelector(
            bytes4(keccak256("exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))")),
            WETH, USDC, uint24(500), address(exec),
            block.timestamp + 60, collWeth, uint256(1), uint160(0)
        );

        try exec.executeLiquidation(
            WETH, USDC, borrower, debtToCover, false, UNI_V3_ROUTER, swapCalldata
        ) {
            emit log("*** FORK LIQUIDATION SUCCEEDED ***");
        } catch Error(string memory reason) {
            emit log_named_string("FAILED", reason);
            revert(string(abi.encodePacked("Failed: ", reason)));
        } catch (bytes memory lowLevel) {
            emit log_named_bytes("FAILED low-level", lowLevel);
            revert("Failed: low-level error");
        }
    }

    function _getAccountData(address u) internal view returns (uint256,uint256,uint256,uint256,uint256,uint256) {
        (bool ok, bytes memory d) = AAVE_POOL.staticcall(
            abi.encodeWithSignature("getUserAccountData(address)", u)
        );
        require(ok, "getUserAccountData failed");
        return abi.decode(d, (uint256,uint256,uint256,uint256,uint256,uint256));
    }
}
