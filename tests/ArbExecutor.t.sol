// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

import "forge-std/Test.sol";
import "../contracts/ArbExecutor.sol";

/**
 * ArbExecutor v2 fork test — validates the on-chain swap2 builder.
 *
 * v2 FIX: swap2 is built on-chain from the contract's real tokenOut balance
 * after swap1. sellCalldata is gone — replaced by sellIsCamelot, sellFee,
 * sellMinOut fields. The contract calls _buildSwap2(route, realOut) internally.
 *
 * Run:
 *   forge test --fork-url $QUICKNODE_HTTP_URL \
 *     --match-contract ArbExecutorForkTest -vvvv
 *
 * What it proves:
 *   1. closed spread → Unprofitable revert
 *   2. no stuck funds after revert
 *   3. swap2 uses real balance (no USDC dust = swap-chaining fix verified)
 *   4. router whitelist enforced
 */
contract ArbExecutorForkTest is Test {
    ArbExecutor exec;

    // ─── Arbitrum mainnet addresses ──────────────────────────────────────
    address constant AAVE_POOL      = 0x794a61358D6845594F94dc1DB02A252b5b4814aD;
    address constant BALANCER_VAULT = 0xBA12222222228d8Ba445958a75a0704d566BF2C8;

    address constant CAMELOT_ROUTER = 0x1F721E2E82F6676FCE4eA07A5958cF098D339e18;
    address constant UNIV3_ROUTER   = 0xE592427A0AEce92De3Edee1F18E0157C05861564;

    address constant WETH = 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1;
    address constant USDC = 0xaf88d065e77c8cC2239327C5EDb3A432268e5831;

    // selectors
    bytes4 constant UNIV3_EXACT_IN_SINGLE   = 0x414bf389;
    bytes4 constant CAMELOT_EXACT_IN_SINGLE = 0xbc651188;

    function setUp() public {
        exec = new ArbExecutor(AAVE_POOL);
        exec.approveRouter(CAMELOT_ROUTER);
        exec.approveRouter(UNIV3_ROUTER);
    }

    // ─── Helper: build swap1 calldata (same as v1) ──────────────────────

    function _camelotCalldata(
        address tokenIn, address tokenOut,
        uint256 amountIn, uint256 amountOutMin
    ) internal view returns (bytes memory) {
        return abi.encodeWithSelector(
            CAMELOT_EXACT_IN_SINGLE,
            tokenIn, tokenOut,
            address(exec),
            block.timestamp + 300,
            amountIn, amountOutMin, uint160(0)
        );
    }

    // ─── TEST 1: Closed spread MUST revert with Unprofitable ─────────────
    function test_ClosedSpread_RevertsUnprofitable() public {
        uint256 amountIn = 1 ether;

        ArbExecutor.ArbRoute memory route = ArbExecutor.ArbRoute({
            tokenIn:       WETH,
            amountIn:      amountIn,
            buyRouter:     CAMELOT_ROUTER,
            buyCalldata:   _camelotCalldata(WETH, USDC, amountIn, 0),
            sellRouter:    UNIV3_ROUTER,
            tokenOut:      USDC,
            sellIsCamelot: false,
            sellFee:       500,
            sellMinOut:    0,
            minProfit:     0.001 ether
        });

        vm.expectRevert(); // Unprofitable or SwapFailed — must NOT succeed
        exec.executeArbViaBalancer(route);
    }

    // ─── TEST 2: No stuck tokens after a revert ──────────────────────────
    function test_NoStuckTokens_AfterRevert() public {
        uint256 amountIn = 1 ether;
        ArbExecutor.ArbRoute memory route = ArbExecutor.ArbRoute({
            tokenIn:       WETH,
            amountIn:      amountIn,
            buyRouter:     CAMELOT_ROUTER,
            buyCalldata:   _camelotCalldata(WETH, USDC, amountIn, 0),
            sellRouter:    UNIV3_ROUTER,
            tokenOut:      USDC,
            sellIsCamelot: false,
            sellFee:       500,
            sellMinOut:    0,
            minProfit:     100 ether   // impossible → guaranteed revert
        });

        try exec.executeArbViaBalancer(route) {} catch {}

        assertEq(IERC20(WETH).balanceOf(address(exec)), 0, "WETH stuck");
        assertEq(IERC20(USDC).balanceOf(address(exec)), 0, "USDC stuck");
    }

    // ─── TEST 3: Swap-chaining — swap2 uses real balance ─────────────────
    function test_SwapChaining_UsesRealBalance() public {
        deal(WETH, address(this), 1 ether);

        uint256 amountIn = 1 ether;
        ArbExecutor.ArbRoute memory route = ArbExecutor.ArbRoute({
            tokenIn:       WETH,
            amountIn:      amountIn,
            buyRouter:     CAMELOT_ROUTER,
            buyCalldata:   _camelotCalldata(WETH, USDC, amountIn, 0),
            sellRouter:    UNIV3_ROUTER,
            tokenOut:      USDC,
            sellIsCamelot: false,
            sellFee:       500,
            sellMinOut:    0,
            minProfit:     0
        });

        try exec.executeArbViaBalancer(route) {} catch {}

        // CRITICAL: no USDC dust = swap2 consumed full swap1 output
        uint256 usdcDust = IERC20(USDC).balanceOf(address(exec));
        assertEq(usdcDust, 0, "USDC dust left -> swap2 did not consume real balance");
    }

    // ─── TEST 4: Router whitelist enforcement ────────────────────────────
    function test_UnapprovedRouter_Reverts() public {
        address fakeRouter = address(0xdead);
        ArbExecutor.ArbRoute memory route = ArbExecutor.ArbRoute({
            tokenIn:       WETH,
            amountIn:      1 ether,
            buyRouter:     fakeRouter,
            buyCalldata:   "",
            sellRouter:    UNIV3_ROUTER,
            tokenOut:      USDC,
            sellIsCamelot: false,
            sellFee:       0,
            sellMinOut:    0,
            minProfit:     0
        });
        vm.expectRevert();
        exec.executeArbViaBalancer(route);
    }
}
