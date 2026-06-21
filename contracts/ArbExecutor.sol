// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/**
 * ArbExecutor v2 — Dual-source flash-loan DEX arbitrage executor
 *
 * FIX vs v1: swap2 is built ON-CHAIN from the contract's real tokenOut
 * balance after swap1, instead of a pre-encoded amountIn that could never
 * match swap1's runtime output. This is the fix the fork test demanded.
 *
 * Flow:
 *   flashLoan(tokenIn, amountIn)
 *     -> swap1: tokenIn -> tokenOut on buyRouter  (PRE-ENCODED; amountIn known)
 *     -> read REAL tokenOut balance                (runtime)
 *     -> swap2: tokenOut -> tokenIn on sellRouter  (BUILT ON-CHAIN, real balance)
 *     -> require(finalBal >= owed + minProfit)      [PROFIT GUARD]
 *     -> repay (Balancer: transfer | Aave: approve+pull)
 *     -> sweep profit to owner
 *
 * Safety (fork-test validated in v1, unchanged):
 *   - dual flash source: Balancer (0%) + Aave (~9bps) fallback
 *   - profit guard BEFORE repayment: bad arb reverts, costs only gas
 *   - forceApprove (USDT-safe), approvals reset to 0 after use
 *   - onlyOwner entry, onlyVault/onlyPool callback gates
 *   - initiator == address(this) anti-hijack on Aave callback
 *   - approvedRouters whitelist enforced for BOTH legs
 */

interface IERC20 {
    function balanceOf(address) external view returns (uint256);
    function approve(address, uint256) external returns (bool);
    function transfer(address, uint256) external returns (bool);
}

interface IBalancerVault {
    function flashLoan(
        address recipient,
        address[] calldata tokens,
        uint256[] calldata amounts,
        bytes calldata userData
    ) external;
}

interface IFlashLoanRecipient {
    function receiveFlashLoan(
        address[] calldata tokens,
        uint256[] calldata amounts,
        uint256[] calldata feeAmounts,
        bytes calldata userData
    ) external;
}

interface IAavePool {
    function flashLoanSimple(
        address receiverAddress,
        address asset,
        uint256 amount,
        bytes calldata params,
        uint16 referralCode
    ) external;
}

interface IFlashLoanSimpleReceiver {
    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address initiator,
        bytes calldata params
    ) external returns (bool);
}

library SafeERC20 {
    function _call(address token, bytes memory data) private {
        (bool ok, bytes memory ret) = token.call(data);
        require(ok && (ret.length == 0 || abi.decode(ret, (bool))), "ERC20 op failed");
    }
    function safeTransfer(address token, address to, uint256 v) internal {
        _call(token, abi.encodeWithSelector(IERC20.transfer.selector, to, v));
    }
    function forceApprove(address token, address spender, uint256 v) internal {
        _call(token, abi.encodeWithSelector(IERC20.approve.selector, spender, 0));
        if (v > 0) {
            _call(token, abi.encodeWithSelector(IERC20.approve.selector, spender, v));
        }
    }
}

contract ArbExecutor is IFlashLoanRecipient, IFlashLoanSimpleReceiver {
    using SafeERC20 for address;

    address public immutable owner;
    address public constant BALANCER_VAULT = 0xBA12222222228d8Ba445958a75a0704d566BF2C8;
    IAavePool public immutable AAVE_POOL;

    bytes4 constant UNIV3_EXACT_IN_SINGLE   = 0x414bf389;
    bytes4 constant CAMELOT_EXACT_IN_SINGLE = 0xbc651188; // NO fee field

    mapping(address => bool) public approvedRouters;

    event ArbExecuted(address indexed tokenIn, uint256 amountIn, uint256 profit, bool viaAave);
    event RouterApproved(address indexed router);
    event RouterRevoked(address indexed router);

    error NotOwner();
    error NotVault();
    error NotPool();
    error BadInitiator();
    error InvalidParameters();
    error RouterNotApproved(address router);
    error SwapFailed(uint8 leg);
    error Unprofitable(uint256 got, uint256 owed, uint256 minProfit);

    struct ArbRoute {
        address tokenIn;
        uint256 amountIn;
        address buyRouter;
        bytes   buyCalldata;    // swap1 PRE-ENCODED (amountIn = flash amount)
        address sellRouter;
        address tokenOut;
        bool    sellIsCamelot;  // true=Camelot sig, false=UniV3
        uint24  sellFee;        // UniV3 only
        uint256 sellMinOut;     // swap2 slippage guard (tokenIn units)
        uint256 minProfit;
    }

    modifier onlyOwner() {
        if (msg.sender != owner) revert NotOwner();
        _;
    }

    constructor(address _aavePool) {
        owner = msg.sender;
        AAVE_POOL = IAavePool(_aavePool);
    }

    function approveRouter(address router) external onlyOwner {
        approvedRouters[router] = true;
        emit RouterApproved(router);
    }
    function revokeRouter(address router) external onlyOwner {
        approvedRouters[router] = false;
        emit RouterRevoked(router);
    }

    function executeArbViaBalancer(ArbRoute calldata route) external onlyOwner {
        if (route.amountIn == 0 || route.tokenIn == address(0)) revert InvalidParameters();
        address[] memory tokens = new address[](1);
        uint256[] memory amounts = new uint256[](1);
        tokens[0] = route.tokenIn;
        amounts[0] = route.amountIn;
        IBalancerVault(BALANCER_VAULT).flashLoan(address(this), tokens, amounts, abi.encode(route));
    }

    function executeArbViaAave(ArbRoute calldata route) external onlyOwner {
        if (route.amountIn == 0 || route.tokenIn == address(0)) revert InvalidParameters();
        AAVE_POOL.flashLoanSimple(address(this), route.tokenIn, route.amountIn, abi.encode(route), 0);
    }

    function receiveFlashLoan(
        address[] calldata,
        uint256[] calldata amounts,
        uint256[] calldata feeAmounts,
        bytes calldata userData
    ) external override {
        if (msg.sender != BALANCER_VAULT) revert NotVault();
        ArbRoute memory route = abi.decode(userData, (ArbRoute));
        uint256 owed = amounts[0] + feeAmounts[0];
        _doArb(route, owed, false);
        route.tokenIn.safeTransfer(BALANCER_VAULT, owed);
    }

    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address initiator,
        bytes calldata params
    ) external override returns (bool) {
        if (msg.sender != address(AAVE_POOL)) revert NotPool();
        if (initiator != address(this)) revert BadInitiator();
        ArbRoute memory route = abi.decode(params, (ArbRoute));
        if (asset != route.tokenIn || amount != route.amountIn) revert InvalidParameters();
        uint256 owed = amount + premium;
        _doArb(route, owed, true);
        route.tokenIn.forceApprove(address(AAVE_POOL), owed);
        return true;
    }

    function _doArb(ArbRoute memory route, uint256 owed, bool viaAave) internal {
        if (!approvedRouters[route.buyRouter])  revert RouterNotApproved(route.buyRouter);
        if (!approvedRouters[route.sellRouter]) revert RouterNotApproved(route.sellRouter);

        // Swap 1: tokenIn -> tokenOut (pre-encoded, amountIn known)
        route.tokenIn.forceApprove(route.buyRouter, route.amountIn);
        (bool ok1, ) = route.buyRouter.call(route.buyCalldata);
        if (!ok1) revert SwapFailed(1);
        route.tokenIn.forceApprove(route.buyRouter, 0);

        // Read REAL swap1 output (THE FIX)
        uint256 realOut = IERC20(route.tokenOut).balanceOf(address(this));
        if (realOut == 0) revert SwapFailed(1);

        // Swap 2: tokenOut -> tokenIn, built on-chain with real balance
        route.tokenOut.forceApprove(route.sellRouter, realOut);
        bytes memory sellData = _buildSwap2(route, realOut);
        (bool ok2, ) = route.sellRouter.call(sellData);
        if (!ok2) revert SwapFailed(2);
        route.tokenOut.forceApprove(route.sellRouter, 0);

        // Profit guard BEFORE repayment
        uint256 finalBal = IERC20(route.tokenIn).balanceOf(address(this));
        if (finalBal < owed + route.minProfit) {
            revert Unprofitable(finalBal, owed, route.minProfit);
        }

        uint256 profit = finalBal - owed;
        if (profit > 0) route.tokenIn.safeTransfer(owner, profit);
        emit ArbExecuted(route.tokenIn, route.amountIn, profit, viaAave);
    }

    function _buildSwap2(ArbRoute memory route, uint256 amountIn)
        internal view returns (bytes memory)
    {
        if (route.sellIsCamelot) {
            return abi.encodeWithSelector(
                CAMELOT_EXACT_IN_SINGLE,
                route.tokenOut, route.tokenIn,
                address(this), block.timestamp + 1,
                amountIn, route.sellMinOut, uint160(0)
            );
        } else {
            return abi.encodeWithSelector(
                UNIV3_EXACT_IN_SINGLE,
                route.tokenOut, route.tokenIn, route.sellFee,
                address(this), block.timestamp + 1,
                amountIn, route.sellMinOut, uint160(0)
            );
        }
    }

    function rescue(address token, uint256 amount) external onlyOwner {
        token.safeTransfer(owner, amount);
    }

    receive() external payable {}
}
