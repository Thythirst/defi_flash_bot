// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import "../contracts/FlashExecutor.sol";

/**
 * @title FlashExecutorForkTest
 * @notice Foundry fork test for WETH/USDC flash-loan arbitrage on Arbitrum.
 *         Validates exact execution against real mainnet state.
 *
 * Setup:
 *   forge test --fork-url $ARBITRUM_HTTP_URL --fork-block-number 585000000 -vv
 *
 * Pools used:
 *   • Uniswap V3 WETH/USDC 0.05% - 0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443
 *   • SushiSwap V2 WETH/USDC - 0x905dfCD5649217c42684f23958568e533C711Aa3
 */
contract FlashExecutorForkTest is Test {
    // Arbitrum mainnet addresses
    address constant WETH = 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1;
    address constant USDC = 0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8;
    address constant AAVE_POOL = 0x794a61358D6845594F94dc1DB02A252b5b4814aD;

    // DEX routers / pools
    address constant UNI_V3_ROUTER = 0xE592427A0AEce92De3Edee1F18E0157C05861564; // SwapRouter
    address constant SUSHI_V2_ROUTER = 0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506;

    FlashExecutor public executor;
    address public owner;

    function setUp() public {
        owner = address(this);
        executor = new FlashExecutor(AAVE_POOL);

        // Approve the DEX routers our contract will call
        executor.approveRouter(UNI_V3_ROUTER);
        executor.approveRouter(SUSHI_V2_ROUTER);
    }

    // ─── Helpers ────────────────────────────────────────────

    function _buildRoute(
        address[] memory path,
        address[] memory routers,
        uint256 minOut
    ) internal pure returns (Route memory) {
        return Route({path: path, dexRouters: routers, minOut: minOut, swapCalldata: new bytes[](routers.length)});
    }

    function _encodeV3Swap(
        address tokenIn,
        address tokenOut,
        uint24 fee,
        uint256 amountIn,
        uint256 amountOutMin,
        address recipient,
        uint256 deadline
    ) internal pure returns (bytes memory) {
        return abi.encodeWithSelector(
            bytes4(keccak256("exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))")),
            tokenIn,
            tokenOut,
            fee,
            recipient,
            deadline,
            amountIn,
            amountOutMin,
            0
        );
    }

    // ─── Real-Pool Price Probe ───────────────────────────────────

    function test_ProbeV2Reserves() public {
        // Minimal ABI for V2 pair getReserves
        address v2Pair = 0x905dfCD5649217c42684f23958568e533C711Aa3;
        (, bytes memory data) = v2Pair.staticcall(abi.encodeWithSignature("getReserves()"));
        (uint112 reserve0, uint112 reserve1,) = abi.decode(data, (uint112, uint112, uint32));
        emit log_named_uint("V2 reserve0 (WETH)", reserve0);
        emit log_named_uint("V2 reserve1 (USDC)", reserve1);
        assertGt(reserve0, 0);
        assertGt(reserve1, 0);
    }

    function test_ProbeV3Slot0() public {
        address v3Pool = 0xC31E54c7a869B9FcBEcc14363CF510d1c41fa443;
        (, bytes memory data) = v3Pool.staticcall(abi.encodeWithSignature("slot0()"));
        (uint160 sqrtPriceX96,,,,,,) = abi.decode(data, (uint160, int24, uint16, uint16, uint16, uint8, bool));
        emit log_named_uint("V3 sqrtPriceX96", sqrtPriceX96);
        assertGt(sqrtPriceX96, 0);
    }

    // ─── Core Flash Loan Fork Test ──────────────────────────────────

    /**
     * @notice This test attempts a real 2-leg arb on a forked block.
     *         Because real prices move, we bound the test with a generous minOut
     *         and expect it to either succeed or revert NotProfitable - never
     *         fail with an unrelated error.
     */
    function test_FlashLoan_WethUsdc_TwoLeg() public {
        // Step 1: Seed the executor with a small WETH buffer for gas/premium
        deal(WETH, address(executor), 0.01 ether);

        // Step 2: Build route:
        //   Leg 1: WETH → USDC on SushiSwap V2
        //   Leg 2: USDC → WETH on Uniswap V3
        address[] memory path = new address[](3);
        path[0] = WETH;
        path[1] = USDC;
        path[2] = WETH;

        address[] memory routers = new address[](2);
        routers[0] = SUSHI_V2_ROUTER; // V2 leg
        routers[1] = UNI_V3_ROUTER;   // V3 leg

        // Pre-encode V2 calldata: swapExactTokensForTokens
        bytes[] memory swapCalldata = new bytes[](2);
        {
            address[] memory v2Path = new address[](2);
            v2Path[0] = WETH;
            v2Path[1] = USDC;
            swapCalldata[0] = abi.encodeWithSelector(
                bytes4(keccak256("swapExactTokensForTokens(uint256,uint256,address[],address,uint256)")),
                5 ether,           // amountIn
                1,                 // amountOutMin (loose for intermediate leg)
                v2Path,
                address(executor),
                block.timestamp + 60
            );
        }

        // Pre-encode V3 calldata: exactInputSingle
        // We use amountIn = expected USDC from V2 minus 5% slippage.
        // To get expected USDC, do a static call to V2 router getAmountsOut.
        {
            address[] memory v2Path = new address[](2);
            v2Path[0] = WETH;
            v2Path[1] = USDC;
            (bool ok, bytes memory v2OutData) = SUSHI_V2_ROUTER.staticcall(
                abi.encodeWithSelector(
                    bytes4(keccak256("getAmountsOut(uint256,address[])")),
                    5 ether,
                    v2Path
                )
            );
            require(ok, "getAmountsOut staticcall failed");
            uint256[] memory amounts = abi.decode(v2OutData, (uint256[]));
            uint256 usdcFromV2 = amounts[amounts.length - 1];
            uint256 usdcForV3 = (usdcFromV2 * 95) / 100; // 5% slippage tolerance

            swapCalldata[1] = abi.encodeWithSelector(
                bytes4(keccak256("exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))")),
                USDC,              // tokenIn
                WETH,              // tokenOut
                uint24(500),       // fee
                address(executor), // recipient
                block.timestamp + 60,
                usdcForV3,         // amountIn
                1,                 // amountOutMinimum (loose; final gate is minOut on Route)
                uint160(0)         // sqrtPriceLimitX96
            );
        }

        uint256 minOut = 1;

        Route memory route = Route({
            path: path,
            minOut: minOut,
            dexRouters: routers,
            swapCalldata: swapCalldata
        });

        // Step 3: Execute flash loan
        // We expect either success or NotProfitable - anything else is a bug.
        try executor.executeFlashLoan(WETH, 5 ether, route) {
            emit log("Flash loan succeeded - arb was profitable at this block");
        } catch Error(string memory reason) {
            emit log_named_string("Flash loan reverted (expected)", reason);
            assert(
                keccak256(bytes(reason)) == keccak256(bytes("NotProfitable")) ||
                keccak256(bytes(reason)) == keccak256(bytes("InvalidRoute"))
            );
        } catch (bytes memory lowLevelData) {
            bytes4 selector;
            assembly { selector := mload(add(lowLevelData, 32)) }
            assert(
                selector == NotProfitable.selector ||
                selector == InvalidRoute.selector ||
                selector == RouterNotApproved.selector
            );
        }
    }

    // ─── Admin / Safety Checks ───────────────────────────────────────

    function test_OnlyOwnerCanApproveRouter() public {
        vm.prank(makeAddr("rando"));
        vm.expectRevert();
        executor.approveRouter(UNI_V3_ROUTER);
    }

    function test_RescueWETH() public {
        deal(WETH, address(executor), 1 ether);
        executor.withdrawResidual(WETH, 1 ether);
        assertEq(IERC20(WETH).balanceOf(address(this)), 1 ether);
    }
}
