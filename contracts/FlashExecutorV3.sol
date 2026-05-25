// SPDX-License-Identifier: MIT
// FlashExecutorV3 — Production liquidation executor with Balancer flash loans (0% fee)
// Forks from FlashExecutor v2, adds Balancer IFlashLoanRecipient support.
//
// Execution flow:
//   1. Owner calls executeLiquidation(collateral, debt, borrower, debtToCover, ...)
//   2. Contract flash-loans debtAsset from Balancer Vault
//   3. Balancer calls receiveFlashLoan → we:
//      a. Call Aave Pool.liquidationCall()
//      b. Optionally swap collateral back to debt asset on Uni V3
//      c. Approve Balancer Vault for repayment
//      d. Check profit > minProfitThreshold
//      e. Repay principal + fee (fee = 0 for Balancer)
//
// Requires: @openzeppelin/contracts@5.x, Solidity ^0.8.24

pragma solidity ^0.8.24;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {ReentrancyGuard} from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";
import {Pausable} from "@openzeppelin/contracts/utils/Pausable.sol";

// ─── Balancer Vault ─────────────────────────────────────────
interface IBalancerVault {
    function flashLoan(
        address recipient,
        address[] memory tokens,
        uint256[] memory amounts,
        bytes memory userData
    ) external;
}

// ─── Balancer IFlashLoanRecipient ───────────────────────────
interface IFlashLoanRecipient {
    function receiveFlashLoan(
        address[] memory tokens,
        uint256[] memory amounts,
        uint256[] memory feeAmounts,
        bytes memory userData
    ) external;
}

// ─── Aave V3 Pool ───────────────────────────────────────────
interface IAavePool {
    function liquidationCall(
        address collateralAsset,
        address debtAsset,
        address user,
        uint256 debtToCover,
        bool receiveAToken
    ) external;
}

// ─── Structs ────────────────────────────────────────────────
struct LiquidationRoute {
    address aavePool;
    address collateralAsset;
    address debtAsset;
    address borrower;
    uint256 debtToCover;
    bool receiveAToken;
    address swapRouter;   // address(0) if no swap needed
    bytes swapCalldata;   // empty if no swap needed
}

// ─── Events ─────────────────────────────────────────────────
event LiquidationExecuted(
    address indexed borrower,
    address indexed collateralAsset,
    address indexed debtAsset,
    uint256 debtToCover,
    uint256 profit,
    uint256 blockNumber
);
event LiquidationFailed(
    address indexed borrower,
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
error LiquidationCallFailed();
error FlashLoanFailed();
error InvalidParameters();
error InsufficientBalance();
error RouterNotApproved(address router);
error TransferETHFailed();
error FlashLoanInProgress();
error FlashLoanMismatch(address expectedToken, uint256 expectedAmount);

contract FlashExecutorV3 is Ownable, ReentrancyGuard, Pausable, IFlashLoanRecipient {
    using SafeERC20 for IERC20;

    IBalancerVault public immutable BALANCER_VAULT;
    IAavePool public immutable AAVE_POOL;

    // Minimum profit threshold (in debt asset wei). Must exceed gas + opportunity cost.
    uint256 public minProfitThreshold;

    // Router whitelist — approved DEX routers for collateral→debt swaps
    mapping(address => bool) public approvedRouters;

    // Prevents rescue while a flash loan is in-flight (re-entrant safety)
    bool private _flashLocked;

    modifier onlyVault() {
        if (msg.sender != address(BALANCER_VAULT)) revert UnauthorizedCallback(msg.sender);
        _;
    }

    modifier noFlashLock() {
        if (_flashLocked) revert FlashLoanInProgress();
        _;
    }

    constructor(address _balancerVault, address _aavePool, uint256 _minProfitThreshold) Ownable(msg.sender) {
        if (_balancerVault == address(0) || _aavePool == address(0)) revert InvalidParameters();
        BALANCER_VAULT = IBalancerVault(_balancerVault);
        AAVE_POOL = IAavePool(_aavePool);
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
    /**
     * @notice Initiate a flash-loan liquidation on Aave v3 via Balancer.
     * @param collateralAsset  Asset used as collateral (what we receive)
     * @param debtAsset        Asset we repay (flash-loan + liquidate)
     * @param borrower         User to liquidate (HF < 1.0)
     * @param debtToCover      Amount of debt to repay (in debt asset wei)
     * @param receiveAToken    false = receive underlying collateral
     * @param swapRouter       Router for collateral→debt swap (address(0) if same asset)
     * @param swapCalldata     Pre-encoded swap calldata (empty if no swap needed)
     */
    function executeLiquidation(
        address collateralAsset,
        address debtAsset,
        address borrower,
        uint256 debtToCover,
        bool receiveAToken,
        address swapRouter,
        bytes calldata swapCalldata
    ) external onlyOwner whenNotPaused {
        if (debtToCover == 0) revert InvalidParameters();
        if (borrower == address(0)) revert InvalidParameters();

        // Validate swap router if provided
        if (swapRouter != address(0) && !approvedRouters[swapRouter]) {
            revert RouterNotApproved(swapRouter);
        }

        address[] memory tokens = new address[](1);
        tokens[0] = debtAsset;
        uint256[] memory amounts = new uint256[](1);
        amounts[0] = debtToCover;

        LiquidationRoute memory route = LiquidationRoute({
            aavePool: address(AAVE_POOL),
            collateralAsset: collateralAsset,
            debtAsset: debtAsset,
            borrower: borrower,
            debtToCover: debtToCover,
            receiveAToken: receiveAToken,
            swapRouter: swapRouter,
            swapCalldata: swapCalldata
        });

        _flashLocked = true;

        try BALANCER_VAULT.flashLoan(address(this), tokens, amounts, abi.encode(route)) {
            // Flash loan completed successfully
        } catch Error(string memory reason) {
            _flashLocked = false;
            emit LiquidationFailed(borrower, reason, block.number);
            revert FlashLoanFailed();
        } catch (bytes memory) {
            _flashLocked = false;
            emit LiquidationFailed(borrower, "lowLevelRevert", block.number);
            revert FlashLoanFailed();
        }

        _flashLocked = false;
    }

    // ─── Balancer Flash Loan Callback ─────────────────────────
    /**
     * @notice Called by Balancer Vault after transferring flash-loaned tokens.
     * @param tokens       Flash-loaned token addresses
     * @param amounts      Flash-loaned amounts
     * @param feeAmounts   Fees (0 for Balancer on Arbitrum)
     * @param userData     Encoded LiquidationRoute
     */
    function receiveFlashLoan(
        address[] memory tokens,
        uint256[] memory amounts,
        uint256[] memory feeAmounts,
        bytes memory userData
    ) external onlyVault nonReentrant {
        LiquidationRoute memory route = abi.decode(userData, (LiquidationRoute));

        // Validate flash loan matches what we requested
        if (tokens.length != 1 || tokens[0] != route.debtAsset) {
            revert FlashLoanMismatch(route.debtAsset, route.debtToCover);
        }
        if (amounts.length != 1 || amounts[0] != route.debtToCover) {
            revert FlashLoanMismatch(route.debtAsset, route.debtToCover);
        }

        // Pre-flight: verify we hold the principal
        uint256 debtAssetBalanceBefore = IERC20(route.debtAsset).balanceOf(address(this));
        if (debtAssetBalanceBefore < route.debtToCover) revert InsufficientBalance();

        // ─── Step 1: Aave liquidationCall ─────────────────────
        // Approve Aave Pool to pull debt asset
        IERC20(route.debtAsset).forceApprove(route.aavePool, route.debtToCover);

        // Execute liquidation
        (bool liqSuccess, bytes memory liqReturnData) = route.aavePool.call(
            abi.encodeWithSelector(
                IAavePool.liquidationCall.selector,
                route.collateralAsset,
                route.debtAsset,
                route.borrower,
                route.debtToCover,
                route.receiveAToken
            )
        );

        if (!liqSuccess) {
            // Bubble up revert reason
            if (liqReturnData.length > 0) {
                assembly { revert(add(liqReturnData, 32), mload(liqReturnData)) }
            } else {
                revert LiquidationCallFailed();
            }
        }

        // Revoke approval
        IERC20(route.debtAsset).forceApprove(route.aavePool, 0);

        // ─── Step 2: Optional collateral → debt swap ──────────
        if (route.swapRouter != address(0) && route.swapCalldata.length > 0) {
            if (!approvedRouters[route.swapRouter]) revert RouterNotApproved(route.swapRouter);

            uint256 collateralBalance = IERC20(route.collateralAsset).balanceOf(address(this));
            if (collateralBalance > 0) {
                IERC20(route.collateralAsset).forceApprove(route.swapRouter, collateralBalance);

                (bool swapSuccess, bytes memory swapReturnData) = route.swapRouter.call(route.swapCalldata);

                if (!swapSuccess) {
                    if (swapReturnData.length > 0) {
                        assembly { revert(add(swapReturnData, 32), mload(swapReturnData)) }
                    } else {
                        revert SwapFailed();
                    }
                }

                IERC20(route.collateralAsset).forceApprove(route.swapRouter, 0);
            }
        }

        // ─── Step 3: Repayment + Profit validation ────────────
        // Balancer fee is 0 on Arbitrum, but keep the math general
        uint256 totalOwed = amounts[0] + feeAmounts[0];

        uint256 debtAssetBalanceAfter = IERC20(route.debtAsset).balanceOf(address(this));

        if (debtAssetBalanceAfter < totalOwed + minProfitThreshold) {
            revert NotProfitable(debtAssetBalanceAfter, totalOwed + minProfitThreshold);
        }

        uint256 profit = debtAssetBalanceAfter - totalOwed;

        // Repay Balancer (transfer back; vault checks balances)
        IERC20(route.debtAsset).safeTransfer(address(BALANCER_VAULT), totalOwed);

        emit LiquidationExecuted(
            route.borrower,
            route.collateralAsset,
            route.debtAsset,
            route.debtToCover,
            profit,
            block.number
        );
    }

    // ─── Rescues ──────────────────────────────────────────────
    function withdrawResidual(
        address token,
        uint256 amount
    ) external onlyOwner noFlashLock nonReentrant {
        if (token == address(0)) revert InvalidParameters();
        if (amount == 0) revert InvalidParameters();
        IERC20(token).safeTransfer(owner(), amount);
        emit Rescue(token, amount);
    }

    function withdrawETH(uint256 amount) external onlyOwner noFlashLock nonReentrant {
        (bool sent, ) = payable(owner()).call{value: amount}("");
        if (!sent) revert TransferETHFailed();
        emit Rescue(address(0), amount);
    }

    receive() external payable {}
}
