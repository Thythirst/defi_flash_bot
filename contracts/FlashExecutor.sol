// SPDX-License-Identifier: MIT
// FlashExecutor v2 — Production-grade flash-loan arbitrage executor
// Fixes: whenNotPaused, router whitelist, multi-leg swaps, initiator validation,
//        nonReentrant callback compatibility, safe rescue locks.
// Requires: @openzeppelin/contracts@5.x, Solidity ^0.8.24

pragma solidity ^0.8.24;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {SafeERC20} from "@openzeppelin/contracts/token/ERC20/utils/SafeERC20.sol";
import {ReentrancyGuard} from "@openzeppelin/contracts/utils/ReentrancyGuard.sol";
import {Ownable} from "@openzeppelin/contracts/access/Ownable.sol";
import {Pausable} from "@openzeppelin/contracts/utils/Pausable.sol";

// ─── Aave V3 Pool Interface ─────────────────────────────────
interface IPool {
    function flashLoanSimple(
        address receiver,
        address asset,
        uint256 amount,
        bytes calldata params,
        uint16 referralCode
    ) external;
}

// ─── Route Struct ───────────────────────────────────────────
struct Route {
    address[] path;        // e.g. [WETH, USDC, WETH]
    uint256 minOut;        // minimum output of final token (must be asset)
    address[] dexRouters;  // one router per leg: length == path.length - 1
    bytes[] swapCalldata;  // pre-encoded swap call for each leg
}

// ─── Events ─────────────────────────────────────────────────
event FlashLoanInitiated(
    address indexed asset,
    uint256 amount,
    bytes32 routeHash
);
event ArbitrageExecuted(
    address indexed asset,
    uint256 amount,
    uint256 premium,
    uint256 profit,
    uint256 blockNumber
);
event ArbitrageFailed(
    address indexed asset,
    uint256 amount,
    string reason,
    uint256 blockNumber
);
event Rescue(address indexed token, uint256 amount);
event EmergencyStop(address indexed triggeredBy);
event RouterApproved(address indexed router);
event RouterRevoked(address indexed router);

// ─── Custom Errors ──────────────────────────────────────────
error UnauthorizedCallback(address caller);
error WrongInitiator(address initiator);
error NotProfitable(uint256 balanceAfter, uint256 threshold);
error InvalidRoute();
error FlashLoanInProgress();
error ZeroAmount();
error TransferETHFailed();
error RouterNotApproved(address router);
error NoRouters();
error PathTooShort();
error MismatchedRouters(uint256 pathLen, uint256 routersLen);

contract FlashExecutor is Ownable, ReentrancyGuard, Pausable {
    using SafeERC20 for IERC20;

    IPool public immutable POOL;

    // Operator-configurable minimum profit threshold (wei)
    uint256 public minProfitThreshold;

    // Router whitelist — only approved routers may be called during a flash loan
    mapping(address => bool) public approvedRouters;

    // Prevents rescue withdrawals while a flash loan is in-flight.
    // NOTE: Storage change is atomic per tx; if the external call reverts,
    // _flashLocked is rolled back automatically. No try/catch needed.
    bool private _flashLocked;

    modifier onlyPool() {
        if (msg.sender != address(POOL)) revert UnauthorizedCallback(msg.sender);
        _;
    }

    modifier noFlashLock() {
        if (_flashLocked) revert FlashLoanInProgress();
        _;
    }

    constructor(address _pool) Ownable(msg.sender) {
        if (_pool == address(0)) revert InvalidRoute();
        POOL = IPool(_pool);
    }

    // ─── Admin ────────────────────────────────────────────────
    function setMinProfitThreshold(uint256 _minProfitThreshold) external onlyOwner {
        minProfitThreshold = _minProfitThreshold;
    }

    function approveRouter(address router) external onlyOwner {
        if (router == address(0)) revert InvalidRoute();
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

    // ─── Core Flash Loan Entry ────────────────────────────────
    // NOT marked nonReentrant so the Aave callback into executeOperation succeeds.
    function executeFlashLoan(
        address asset,
        uint256 amount,
        Route calldata route
    ) external onlyOwner whenNotPaused {
        if (amount == 0) revert ZeroAmount();
        if (route.path.length < 2) revert PathTooShort();
        if (route.dexRouters.length == 0) revert NoRouters();
        if (route.dexRouters.length != route.path.length - 1) {
            revert MismatchedRouters(route.path.length, route.dexRouters.length);
        }
        if (route.swapCalldata.length != route.dexRouters.length) {
            revert MismatchedRouters(route.swapCalldata.length, route.dexRouters.length);
        }

        // Validate router whitelist
        for (uint256 i = 0; i < route.dexRouters.length; ++i) {
            if (!approvedRouters[route.dexRouters[i]]) {
                revert RouterNotApproved(route.dexRouters[i]);
            }
        }

        bytes32 routeHash = keccak256(abi.encode(route));
        emit FlashLoanInitiated(asset, amount, routeHash);

        _flashLocked = true;
        POOL.flashLoanSimple(address(this), asset, amount, abi.encode(route), 0);
        _flashLocked = false;
    }

    // ─── Aave V3 Callback ─────────────────────────────────────
    function executeOperation(
        address asset,
        uint256 amount,
        uint256 premium,
        address initiator,
        bytes calldata params
    ) external onlyPool nonReentrant returns (bool) {
        // 1. Reject flash loans initiated by anyone else.
        if (initiator != address(this)) revert WrongInitiator(initiator);

        Route memory route = abi.decode(params, (Route));
        if (route.path.length < 2 || route.dexRouters.length == 0) revert InvalidRoute();
        if (route.dexRouters.length != route.path.length - 1) {
            revert MismatchedRouters(route.path.length, route.dexRouters.length);
        }
        if (route.swapCalldata.length != route.dexRouters.length) {
            revert MismatchedRouters(route.swapCalldata.length, route.dexRouters.length);
        }

        // 2. Re-validate routers (defense in depth)
        for (uint256 i = 0; i < route.dexRouters.length; ++i) {
            if (!approvedRouters[route.dexRouters[i]]) revert RouterNotApproved(route.dexRouters[i]);
        }

        // 3. Sanity check: we must hold the loaned principal
        uint256 balanceBefore = IERC20(asset).balanceOf(address(this));
        if (balanceBefore < amount) revert InvalidRoute();

        // 4. Execute multi-leg swap sequence
        uint256 currentAmount = amount;

        for (uint256 i = 0; i < route.dexRouters.length; ++i) {
            address router = route.dexRouters[i];
            address tokenIn = route.path[i];

            // After the first leg, our balance of tokenIn is whatever the previous swap yielded.
            if (i > 0) {
                currentAmount = IERC20(tokenIn).balanceOf(address(this));
            }

            // Exact approval per leg; no infinite allowances left behind.
            IERC20(tokenIn).forceApprove(router, currentAmount);

            // Use caller-provided calldata so V2, V3, Camelot, etc. all work
            (bool success, bytes memory returnData) = router.call(route.swapCalldata[i]);

            if (!success) {
                if (returnData.length > 0) {
                    assembly {
                        revert(add(returnData, 32), mload(returnData))
                    }
                } else {
                    revert InvalidRoute();
                }
            }

            // Revoke approval immediately after swap
            IERC20(tokenIn).forceApprove(router, 0);
        }

        // 5. Strict profit validation
        uint256 balanceAfter = IERC20(asset).balanceOf(address(this));
        uint256 totalOwed = amount + premium;

        if (balanceAfter < totalOwed + minProfitThreshold) {
            revert NotProfitable(balanceAfter, totalOwed + minProfitThreshold);
        }

        uint256 profit = balanceAfter - totalOwed;

        // 6. Repay flash loan + premium
        IERC20(asset).safeTransfer(address(POOL), totalOwed);

        emit ArbitrageExecuted(asset, amount, premium, profit, block.number);
        return true;
    }

    // ─── Helpers ──────────────────────────────────────────────
    function _slice(
        address[] memory arr,
        uint256 start,
        uint256 end
    ) internal pure returns (address[] memory) {
        if (end <= start || end > arr.length) revert InvalidRoute();
        address[] memory out = new address[](end - start);
        for (uint256 i = start; i < end; ++i) {
            out[i - start] = arr[i];
        }
        return out;
    }

    // ─── Rescues ──────────────────────────────────────────────
    function withdrawResidual(
        address token,
        uint256 amount
    ) external onlyOwner noFlashLock nonReentrant {
        if (token == address(0)) revert InvalidRoute();
        if (amount == 0) revert ZeroAmount();
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
