// SPDX-License-Identifier: MIT
// DexArbExecutor — DEX-DEX cyclic arbitrage via Balancer flash loans (0% fee)
//
// Execution flow:
//   1. Owner calls executeArbitrage(tokenA, tokenB, amountA, routerFwd, feeFwd,
//                                 routerRev, feeRev, amountOutMin)
//   2. Contract flash-loans amountA of tokenA from Balancer Vault
//   3. Balancer calls receiveFlashLoan → we:
//      a. Swap tokenA → tokenB on routerFwd (exactInputSingle)
//      b. Swap tokenB → tokenA on routerRev (exactInputSingle, entire balance)
//      c. Validate profit: tokenA balance > amountA + minProfitThreshold
//      d. Repay principal (Balancer fee = 0 on Arbitrum)
//
// Uses UniV3-compatible exactInputSingle interface — works with
// UniV3, SushiV3, PancakeSwapV3 SwapRouters.

pragma solidity ^0.8.24;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {ReentrancyGuard} from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";
import {Pausable} from "@openzeppelin/contracts/utils/Pausable.sol";

interface IBalancerVault {
    function flashLoan(
        address recipient,
        address[] memory tokens,
        uint256[] memory amounts,
        bytes memory userData
    ) external;
}

interface IFlashLoanRecipient {
    function receiveFlashLoan(
        address[] memory tokens,
        uint256[] memory amounts,
        uint256[] memory feeAmounts,
        bytes memory userData
    ) external;
}

// ─── Uni V3 SwapRouter ──────────────────────────────────────

interface ISwapRouter {
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

    function exactInputSingle(
        ExactInputSingleParams calldata params
    ) external payable returns (uint256 amountOut);
}

// ─── Structs ────────────────────────────────────────────────

struct ArbRoute {
    address tokenA;
    address tokenB;
    uint256 amountA;
    address routerFwd;
    uint24 feeFwd;
    address routerRev;
    uint24 feeRev;
    uint256 amountOutMin; // minimum tokenA to receive from reverse swap
}

// ─── Events ─────────────────────────────────────────────────

event ArbitrageExecuted(
    address indexed tokenA,
    address indexed tokenB,
    address routerFwd,
    address routerRev,
    uint256 amountA,
    uint256 profit,
    uint256 blockNumber
);
event ArbitrageFailed(
    address indexed tokenA,
    address indexed tokenB,
    string reason,
    uint256 blockNumber
);
event Rescue(address indexed token, uint256 amount);
event EmergencyStop(address indexed triggeredBy);
event RouterApproved(address indexed router);
event RouterRevoked(address indexed router);

// ─── Custom Errors ──────────────────────────────────────────

error UnauthorizedCallback(address caller);
error NotProfitable(uint256 balanceAfter, uint256 threshold);
error SwapFailed();
error FlashLoanFailed();
error InvalidParameters();
error RouterNotApproved(address router);
error FlashLoanInProgress();
error FlashLoanMismatch();

contract DexArbExecutor is Ownable, ReentrancyGuard, Pausable, IFlashLoanRecipient {
    using SafeERC20 for IERC20;

    IBalancerVault public immutable BALANCER_VAULT;

    uint256 public minProfitThreshold;
    mapping(address => bool) public approvedRouters;
    bool private _flashLocked;

    modifier onlyVault() {
        if (msg.sender != address(BALANCER_VAULT)) revert UnauthorizedCallback(msg.sender);
        _;
    }

    modifier noFlashLock() {
        if (_flashLocked) revert FlashLoanInProgress();
        _;
    }

    constructor(address _balancerVault, uint256 _minProfitThreshold) Ownable(msg.sender) {
        if (_balancerVault == address(0)) revert InvalidParameters();
        BALANCER_VAULT = IBalancerVault(_balancerVault);
        minProfitThreshold = _minProfitThreshold;
    }

    // ─── Admin ────────────────────────────────────────────────

    function setMinProfitThreshold(uint256 _minProfitThreshold) external onlyOwner {
        minProfitThreshold = _minProfitThreshold;
    }

    function approveRouter(address router) external onlyOwner {
        if (router == address(0)) revert InvalidParameters();
        approvedRouters[router] = true;
        emit RouterApproved(router);
    }

    function revokeRouter(address router) external onlyOwner {
        approvedRouters[router] = false;
        emit RouterRevoked(router);
    }

    function emergencyPause() external onlyOwner {
        _pause();
        emit EmergencyStop(msg.sender);
    }

    function emergencyUnpause() external onlyOwner {
        _unpause();
    }

    // ─── Core Entry Point ─────────────────────────────────────

    function executeArbitrage(
        address tokenA,
        address tokenB,
        uint256 amountA,
        address routerFwd,
        uint24 feeFwd,
        address routerRev,
        uint24 feeRev,
        uint256 amountOutMin
    ) external onlyOwner whenNotPaused {
        if (amountA == 0 || tokenA == address(0) || tokenB == address(0)) revert InvalidParameters();
        if (!approvedRouters[routerFwd]) revert RouterNotApproved(routerFwd);
        if (!approvedRouters[routerRev]) revert RouterNotApproved(routerRev);
        if (tokenA == tokenB) revert InvalidParameters();

        address[] memory tokens = new address[](1);
        tokens[0] = tokenA;
        uint256[] memory amounts = new uint256[](1);
        amounts[0] = amountA;

        ArbRoute memory route = ArbRoute({
            tokenA: tokenA,
            tokenB: tokenB,
            amountA: amountA,
            routerFwd: routerFwd,
            feeFwd: feeFwd,
            routerRev: routerRev,
            feeRev: feeRev,
            amountOutMin: amountOutMin
        });

        _flashLocked = true;

        try BALANCER_VAULT.flashLoan(address(this), tokens, amounts, abi.encode(route)) {
            // success
        } catch Error(string memory reason) {
            _flashLocked = false;
            emit ArbitrageFailed(tokenA, tokenB, reason, block.number);
            revert FlashLoanFailed();
        } catch (bytes memory) {
            _flashLocked = false;
            emit ArbitrageFailed(tokenA, tokenB, "lowLevelRevert", block.number);
            revert FlashLoanFailed();
        }

        _flashLocked = false;
    }

    // ─── Balancer Flash Loan Callback ─────────────────────────

    function receiveFlashLoan(
        address[] memory tokens,
        uint256[] memory amounts,
        uint256[] memory feeAmounts,
        bytes memory userData
    ) external onlyVault nonReentrant {
        ArbRoute memory route = abi.decode(userData, (ArbRoute));

        if (tokens.length != 1 || tokens[0] != route.tokenA) revert FlashLoanMismatch();
        if (amounts.length != 1 || amounts[0] != route.amountA) revert FlashLoanMismatch();

        // ─── Step 1: Swap tokenA → tokenB ─────────────────
        IERC20(route.tokenA).forceApprove(route.routerFwd, route.amountA);

        ISwapRouter fwdRouter = ISwapRouter(route.routerFwd);
        fwdRouter.exactInputSingle(ISwapRouter.ExactInputSingleParams({
            tokenIn: route.tokenA,
            tokenOut: route.tokenB,
            fee: route.feeFwd,
            recipient: address(this),
            deadline: block.timestamp + 300,
            amountIn: route.amountA,
            amountOutMinimum: 0,
            sqrtPriceLimitX96: 0
        }));

        IERC20(route.tokenA).forceApprove(route.routerFwd, 0);

        // ─── Step 2: Swap tokenB → tokenA ─────────────────
        uint256 tokenBBalance = IERC20(route.tokenB).balanceOf(address(this));
        require(tokenBBalance > 0, "no tokenB received from fwd swap");

        IERC20(route.tokenB).forceApprove(route.routerRev, tokenBBalance);

        ISwapRouter revRouter = ISwapRouter(route.routerRev);
        revRouter.exactInputSingle(ISwapRouter.ExactInputSingleParams({
            tokenIn: route.tokenB,
            tokenOut: route.tokenA,
            fee: route.feeRev,
            recipient: address(this),
            deadline: block.timestamp + 300,
            amountIn: tokenBBalance,
            amountOutMinimum: route.amountOutMin,
            sqrtPriceLimitX96: 0
        }));

        // ─── Step 3: Profit validation + repay ────────────
        uint256 totalOwed = amounts[0] + feeAmounts[0];
        uint256 tokenABalance = IERC20(route.tokenA).balanceOf(address(this));

        if (tokenABalance < totalOwed + minProfitThreshold) {
            revert NotProfitable(tokenABalance, totalOwed + minProfitThreshold);
        }

        uint256 profit = tokenABalance - totalOwed;

        IERC20(route.tokenA).safeTransfer(address(BALANCER_VAULT), totalOwed);

        emit ArbitrageExecuted(
            route.tokenA,
            route.tokenB,
            route.routerFwd,
            route.routerRev,
            route.amountA,
            profit,
            block.number
        );
    }

    // ─── Rescues ──────────────────────────────────────────────

    function withdrawResidual(address token, uint256 amount) external onlyOwner noFlashLock nonReentrant {
        if (token == address(0) || amount == 0) revert InvalidParameters();
        IERC20(token).safeTransfer(owner(), amount);
        emit Rescue(token, amount);
    }

    function withdrawETH(uint256 amount) external onlyOwner noFlashLock nonReentrant {
        (bool sent, ) = payable(owner()).call{value: amount}("");
        require(sent, "ETH transfer failed");
        emit Rescue(address(0), amount);
    }

    receive() external payable {}
}
