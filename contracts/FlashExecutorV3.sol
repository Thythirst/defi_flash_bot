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

    /// @notice Aave V3 flash loan — borrows from pool reserves with 0.05% fee
    /// @param receiverAddress  Contract implementing IFlashLoanSimpleReceiver
    /// @param asset            Token to borrow (must be a pool reserve)
    /// @param amount           Amount to borrow
    /// @param params           Arbitrary data passed to executeOperation()
    /// @param referralCode     Referral code (0 = none)
    function flashLoanSimple(
        address receiverAddress,
        address asset,
        uint256 amount,
        bytes calldata params,
        uint16 referralCode
    ) external;
}

// ─── Aave V3 Flash Loan Receiver ────────────────────────────
interface IFlashLoanSimpleReceiver {
    /// @notice Called by Aave Pool after flash-loaning tokens
    /// @return true if successful, reverts otherwise
    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address initiator,
        bytes calldata params
    ) external returns (bool);
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

// One position in a batch. All items in a batch share a single debtAsset
// (passed separately) so they can be covered by one flash loan / one nonce.
struct BatchItem {
    address collateralAsset;
    address borrower;
    uint256 debtToCover;
    bool receiveAToken;
    address swapRouter;   // address(0) if no swap needed
    bytes swapCalldata;   // collateral->debt swap, empty if same asset
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
event MinProfitThresholdSet(address indexed asset, uint256 threshold);

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

contract FlashExecutorV3 is Ownable, ReentrancyGuard, Pausable, IFlashLoanRecipient, IFlashLoanSimpleReceiver {
    using SafeERC20 for IERC20;

    IBalancerVault public immutable BALANCER_VAULT;
    IAavePool public immutable AAVE_POOL;

    // Minimum profit threshold (in debt asset wei). Must exceed gas + opportunity cost.
    // Default for assets without a per-token override.
    uint256 public minProfitThreshold;

    // Per-token overrides — allows different thresholds per debt asset
    // e.g. WETH at 0.001 WETH, USDC at 2 USDC, etc.
    mapping(address => uint256) public minProfitThresholds;

    // Router whitelist — approved DEX routers for collateral→debt swaps
    mapping(address => bool) public approvedRouters;

    // Prevents rescue while a flash loan is in-flight (re-entrant safety)
    bool private _flashLocked;

    // Set while a batch flash loan is in flight so the shared Balancer callback
    // knows to decode batch userData instead of a single LiquidationRoute.
    bool private _batchInProgress;

    modifier onlyVault() {
        if (msg.sender != address(BALANCER_VAULT)) revert UnauthorizedCallback(msg.sender);
        _;
    }

    // Restricts the per-item liquidation step to internal self-calls, so the
    // batch loop can wrap each item in try/catch (try/catch needs an external call).
    modifier onlySelf() {
        if (msg.sender != address(this)) revert UnauthorizedCallback(msg.sender);
        _;
    }

    modifier onlyPool() {
        if (msg.sender != address(AAVE_POOL)) revert UnauthorizedCallback(msg.sender);
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

    /// @notice Set a per-token profit threshold override.
    /// @param asset The debt asset token address
    /// @param threshold Minimum profit in that asset's native units (e.g. 2 USDC = 2e6)
    function setMinProfitThreshold(address asset, uint256 threshold) external onlyOwner {
        if (asset == address(0)) revert InvalidParameters();
        minProfitThresholds[asset] = threshold;
        emit MinProfitThresholdSet(asset, threshold);
    }

    /// @dev Returns the effective threshold for an asset — per-token override if set,
    ///      otherwise falls back to the global default.
    function _minProfitForAsset(address asset) internal view returns (uint256) {
        uint256 override_ = minProfitThresholds[asset];
        return override_ > 0 ? override_ : minProfitThreshold;
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

    // ─── Batch Liquidation Entry (#4) ──────────────────────────
    /**
     * @notice Liquidate many positions sharing one debtAsset in a SINGLE tx /
     *         single nonce, funded by one Balancer flash loan of the summed debt.
     *         Per-item failures are isolated (try/catch) so one reverting position
     *         does not kill the rest. Profit is validated in aggregate.
     * @param debtAsset  The common debt asset for every item (one flash loan).
     * @param items      Positions to liquidate.
     */
    function executeLiquidationBatch(
        address debtAsset,
        BatchItem[] calldata items
    ) external onlyOwner whenNotPaused {
        if (debtAsset == address(0) || items.length == 0) revert InvalidParameters();

        uint256 totalDebt;
        for (uint256 i = 0; i < items.length; i++) {
            if (items[i].borrower == address(0) || items[i].debtToCover == 0) {
                revert InvalidParameters();
            }
            if (items[i].swapRouter != address(0) && !approvedRouters[items[i].swapRouter]) {
                revert RouterNotApproved(items[i].swapRouter);
            }
            totalDebt += items[i].debtToCover;
        }

        address[] memory tokens = new address[](1);
        tokens[0] = debtAsset;
        uint256[] memory amounts = new uint256[](1);
        amounts[0] = totalDebt;

        _flashLocked = true;
        _batchInProgress = true;

        try BALANCER_VAULT.flashLoan(address(this), tokens, amounts, abi.encode(debtAsset, items)) {
            // success
        } catch Error(string memory reason) {
            _batchInProgress = false;
            _flashLocked = false;
            emit LiquidationFailed(address(0), reason, block.number);
            revert FlashLoanFailed();
        } catch (bytes memory) {
            _batchInProgress = false;
            _flashLocked = false;
            emit LiquidationFailed(address(0), "batchLowLevelRevert", block.number);
            revert FlashLoanFailed();
        }

        _batchInProgress = false;
        _flashLocked = false;
    }

    // ─── Aave V3 Flash Loan Entry ──────────────────────────────
    /**
     * @notice Initiate a flash-loan liquidation using Aave V3's native flash loan.
     *         Borrows from Aave pool reserves (deep liquidity, same token as debt).
     *         Use this when Balancer vault has insufficient debtAsset liquidity.
     * @dev  Aave V3 charges 0.05% (5 bps) flash loan fee on most assets.
     *       Fee is deducted in the executeOperation callback.
     * @param collateralAsset  Asset used as collateral (what we receive)
     * @param debtAsset        Asset we repay (flash-loan + liquidate)
     * @param borrower         User to liquidate (HF < 1.0)
     * @param debtToCover      Amount of debt to repay (in debt asset wei)
     * @param receiveAToken    false = receive underlying collateral
     * @param swapRouter       Router for collateral→debt swap (address(0) if same asset)
     * @param swapCalldata     Pre-encoded swap calldata (empty if no swap needed)
     */
    function executeLiquidationViaAave(
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

        try AAVE_POOL.flashLoanSimple(address(this), debtAsset, debtToCover, abi.encode(route), 0) {
            // Flash loan completed successfully — executeOperation() handled everything
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

    // ─── Direct Liquidation (Pre-Funded, No Flash Loan) ──────
    /**
     * @notice Execute a liquidation using the contract's own balance instead of a
     *         Balancer flash loan. Saves ~150k gas by skipping Balancer overhead.
     * @dev  Owner must pre-fund the contract with sufficient debtAsset balance.
     *       All profit stays in the contract; owner withdraws via withdrawResidual().
     * @param collateralAsset  Asset used as collateral (what we receive)
     * @param debtAsset        Asset we repay (liquidate with contract's balance)
     * @param borrower         User to liquidate (HF < 1.0)
     * @param debtToCover      Amount of debt to repay (in debt asset wei)
     * @param receiveAToken    false = receive underlying collateral
     * @param swapRouter       Router for collateral→debt swap (address(0) if same asset)
     * @param swapCalldata     Pre-encoded swap calldata (empty if no swap needed)
     * @return profit          Profit in debtAsset wei after liquidation + swap
     */
    function executeLiquidationDirect(
        address collateralAsset,
        address debtAsset,
        address borrower,
        uint256 debtToCover,
        bool receiveAToken,
        address swapRouter,
        bytes calldata swapCalldata
    ) external onlyOwner whenNotPaused nonReentrant returns (uint256 profit) {
        if (debtToCover == 0) revert InvalidParameters();
        if (borrower == address(0)) revert InvalidParameters();

        // Validate swap router if provided
        if (swapRouter != address(0) && !approvedRouters[swapRouter]) {
            revert RouterNotApproved(swapRouter);
        }

        // Verify we hold enough debt asset
        uint256 debtBalanceBefore = IERC20(debtAsset).balanceOf(address(this));
        if (debtBalanceBefore < debtToCover) revert InsufficientBalance();

        // ─── Step 1: Aave liquidationCall ─────────────────────
        IERC20(debtAsset).forceApprove(address(AAVE_POOL), debtToCover);

        (bool liqSuccess, bytes memory liqReturnData) = address(AAVE_POOL).call(
            abi.encodeWithSelector(
                IAavePool.liquidationCall.selector,
                collateralAsset,
                debtAsset,
                borrower,
                debtToCover,
                receiveAToken
            )
        );

        if (!liqSuccess) {
            if (liqReturnData.length > 0) {
                assembly { revert(add(liqReturnData, 32), mload(liqReturnData)) }
            } else {
                revert LiquidationCallFailed();
            }
        }

        IERC20(debtAsset).forceApprove(address(AAVE_POOL), 0);

        // ─── Step 2: Optional collateral → debt swap ──────────
        if (swapRouter != address(0) && swapCalldata.length > 0) {
            uint256 collateralBalance = IERC20(collateralAsset).balanceOf(address(this));
            if (collateralBalance > 0) {
                IERC20(collateralAsset).forceApprove(swapRouter, collateralBalance);

                (bool swapSuccess, bytes memory swapReturnData) = swapRouter.call(swapCalldata);

                if (!swapSuccess) {
                    if (swapReturnData.length > 0) {
                        assembly { revert(add(swapReturnData, 32), mload(swapReturnData)) }
                    } else {
                        revert SwapFailed();
                    }
                }

                IERC20(collateralAsset).forceApprove(swapRouter, 0);
            }
        }

        // ─── Step 3: Profit validation ────────────────────────
        uint256 debtBalanceAfter = IERC20(debtAsset).balanceOf(address(this));

        // Profit = what we have now minus what we started with (pre-funded)
        // Started with: debtBalanceBefore, spent: debtToCover on liquidation
        // Net starting position: debtBalanceBefore - debtToCover
        profit = debtBalanceAfter + debtToCover;
        unchecked { profit -= debtBalanceBefore; }
        if (profit < _minProfitForAsset(debtAsset)) revert NotProfitable(profit, _minProfitForAsset(debtAsset));

        emit LiquidationExecuted(
            borrower,
            collateralAsset,
            debtAsset,
            debtToCover,
            profit,
            block.number
        );
    }

    // ─── Balancer Flash Loan Callback ─────────────────────────
    /**
     * @notice Called by Balancer Vault after transferring flash-loaned tokens.
     */
    function receiveFlashLoan(
        address[] memory tokens,
        uint256[] memory amounts,
        uint256[] memory feeAmounts,
        bytes memory userData
    ) external onlyVault nonReentrant {
        // Batch path (#4): userData is (address debtAsset, BatchItem[] items)
        if (_batchInProgress) {
            (address batchDebtAsset, BatchItem[] memory items) =
                abi.decode(userData, (address, BatchItem[]));
            if (tokens.length != 1 || tokens[0] != batchDebtAsset) {
                revert FlashLoanMismatch(batchDebtAsset, amounts.length > 0 ? amounts[0] : 0);
            }
            _doBatch(batchDebtAsset, items, amounts[0], feeAmounts[0]);
            return;
        }

        LiquidationRoute memory route = abi.decode(userData, (LiquidationRoute));

        // Validate flash loan matches what we requested
        if (tokens.length != 1 || tokens[0] != route.debtAsset) {
            revert FlashLoanMismatch(route.debtAsset, route.debtToCover);
        }
        if (amounts.length != 1 || amounts[0] != route.debtToCover) {
            revert FlashLoanMismatch(route.debtAsset, route.debtToCover);
        }

        _doLiquidation(route, amounts[0], feeAmounts[0], address(BALANCER_VAULT));
    }

    // ─── Aave V3 Flash Loan Callback ──────────────────────────
    /**
     * @notice Called by Aave Pool after flash-loaning tokens.
     * @dev  Must return true on success (Aave V3 interface requirement).
     */
    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address /* initiator */,
        bytes calldata params
    ) external onlyPool nonReentrant returns (bool) {
        LiquidationRoute memory route = abi.decode(params, (LiquidationRoute));

        // Validate flash loan matches what we requested
        if (asset != route.debtAsset) {
            revert FlashLoanMismatch(route.debtAsset, route.debtToCover);
        }
        if (amount != route.debtToCover) {
            revert FlashLoanMismatch(route.debtAsset, route.debtToCover);
        }

        _doLiquidation(route, amount, premium, address(AAVE_POOL));
        return true;
    }

    // ─── Shared Liquidation Logic ─────────────────────────────
    /**
     * @notice Execute liquidation + optional swap + repay flash loan.
     * @dev  Called by both Balancer and Aave flash loan callbacks.
     *       Balancer fee is 0 on Arbitrum; Aave premium is typically 5 bps.
     * @param route            Liquidation parameters
     * @param principal        Amount borrowed (debtToCover)
     * @param fee              Flash loan fee (0 for Balancer, premium for Aave)
     * @param repaymentTarget  Address to repay (BALANCER_VAULT or AAVE_POOL)
     */
    function _doLiquidation(
        LiquidationRoute memory route,
        uint256 principal,
        uint256 fee,
        address repaymentTarget
    ) internal {
        // Pre-flight: verify we hold the principal
        uint256 debtAssetBalanceBefore = IERC20(route.debtAsset).balanceOf(address(this));
        if (debtAssetBalanceBefore < principal) revert InsufficientBalance();

        // ─── Step 1: Aave liquidationCall ─────────────────────
        IERC20(route.debtAsset).forceApprove(route.aavePool, principal);

        (bool liqSuccess, bytes memory liqReturnData) = route.aavePool.call(
            abi.encodeWithSelector(
                IAavePool.liquidationCall.selector,
                route.collateralAsset,
                route.debtAsset,
                route.borrower,
                principal,
                route.receiveAToken
            )
        );

        if (!liqSuccess) {
            if (liqReturnData.length > 0) {
                assembly { revert(add(liqReturnData, 32), mload(liqReturnData)) }
            } else {
                revert LiquidationCallFailed();
            }
        }

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
        uint256 totalOwed = principal + fee;

        uint256 debtAssetBalanceAfter = IERC20(route.debtAsset).balanceOf(address(this));

        uint256 threshold = _minProfitForAsset(route.debtAsset);
        if (debtAssetBalanceAfter < totalOwed + threshold) {
            revert NotProfitable(debtAssetBalanceAfter, totalOwed + threshold);
        }

        uint256 profit = debtAssetBalanceAfter - totalOwed;

        IERC20(route.debtAsset).safeTransfer(repaymentTarget, totalOwed);

        emit LiquidationExecuted(
            route.borrower,
            route.collateralAsset,
            route.debtAsset,
            principal,
            profit,
            block.number
        );
    }

    // ─── Batch Liquidation Logic (#4) ─────────────────────────
    /**
     * @notice Run every item against one flash loan, isolating per-item reverts,
     *         then repay once and validate aggregate profit.
     * @param debtAsset  Common debt asset (the flash-loaned token).
     * @param items      Positions to liquidate.
     * @param principal  Flash-loaned amount (sum of debtToCover).
     * @param fee        Flash loan fee (0 for Balancer on Arbitrum).
     */
    function _doBatch(
        address debtAsset,
        BatchItem[] memory items,
        uint256 principal,
        uint256 fee
    ) internal {
        uint256 balanceBefore = IERC20(debtAsset).balanceOf(address(this));
        if (balanceBefore < principal) revert InsufficientBalance();

        uint256 succeeded;
        for (uint256 i = 0; i < items.length; i++) {
            // External self-call so a single bad position reverts only its own
            // state changes (caught here) instead of aborting the whole batch.
            try this.doOneLiquidationStep(debtAsset, items[i]) {
                succeeded++;
                emit LiquidationExecuted(
                    items[i].borrower,
                    items[i].collateralAsset,
                    debtAsset,
                    items[i].debtToCover,
                    0,                // per-item profit not isolated; see aggregate
                    block.number
                );
            } catch Error(string memory reason) {
                emit LiquidationFailed(items[i].borrower, reason, block.number);
            } catch (bytes memory) {
                emit LiquidationFailed(items[i].borrower, "itemReverted", block.number);
            }
        }

        // Repay once + aggregate profit gate.
        uint256 totalOwed = principal + fee;
        uint256 balanceAfter = IERC20(debtAsset).balanceOf(address(this));
        uint256 threshold = _minProfitForAsset(debtAsset);
        if (balanceAfter < totalOwed + threshold) {
            revert NotProfitable(balanceAfter, totalOwed + threshold);
        }

        IERC20(debtAsset).safeTransfer(address(BALANCER_VAULT), totalOwed);
    }

    /**
     * @notice One position's liquidationCall + optional collateral->debt swap.
     *         No repayment here — the batch repays once after the loop.
     * @dev    onlySelf so it is only reachable via the try/catch in _doBatch.
     */
    function doOneLiquidationStep(
        address debtAsset,
        BatchItem calldata item
    ) external onlySelf {
        // Step 1: Aave liquidationCall
        IERC20(debtAsset).forceApprove(address(AAVE_POOL), item.debtToCover);
        (bool liqSuccess, bytes memory liqReturnData) = address(AAVE_POOL).call(
            abi.encodeWithSelector(
                IAavePool.liquidationCall.selector,
                item.collateralAsset,
                debtAsset,
                item.borrower,
                item.debtToCover,
                item.receiveAToken
            )
        );
        if (!liqSuccess) {
            if (liqReturnData.length > 0) {
                assembly { revert(add(liqReturnData, 32), mload(liqReturnData)) }
            } else {
                revert LiquidationCallFailed();
            }
        }
        IERC20(debtAsset).forceApprove(address(AAVE_POOL), 0);

        // Step 2: optional collateral -> debt swap
        if (item.swapRouter != address(0) && item.swapCalldata.length > 0) {
            if (!approvedRouters[item.swapRouter]) revert RouterNotApproved(item.swapRouter);
            uint256 collateralBalance = IERC20(item.collateralAsset).balanceOf(address(this));
            if (collateralBalance > 0) {
                IERC20(item.collateralAsset).forceApprove(item.swapRouter, collateralBalance);
                (bool swapSuccess, bytes memory swapReturnData) = item.swapRouter.call(item.swapCalldata);
                if (!swapSuccess) {
                    if (swapReturnData.length > 0) {
                        assembly { revert(add(swapReturnData, 32), mload(swapReturnData)) }
                    } else {
                        revert SwapFailed();
                    }
                }
                IERC20(item.collateralAsset).forceApprove(item.swapRouter, 0);
            }
        }
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
