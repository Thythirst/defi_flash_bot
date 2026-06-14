// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import "../contracts/FlashExecutor.sol";

// Minimal mock ERC20 for testing
contract MockERC20 is IERC20 {
    string public name;
    string public symbol;
    uint8 public decimals;
    uint256 public totalSupply;
    mapping(address => uint256) public balanceOf;
    mapping(address => mapping(address => uint256)) public allowance;

    constructor(string memory _name, string memory _symbol, uint8 _decimals) {
        name = _name;
        symbol = _symbol;
        decimals = _decimals;
    }

    function mint(address to, uint256 amount) external {
        balanceOf[to] += amount;
        totalSupply += amount;
    }

    function transfer(address to, uint256 amount) external returns (bool) {
        require(balanceOf[msg.sender] >= amount, "insufficient");
        balanceOf[msg.sender] -= amount;
        balanceOf[to] += amount;
        return true;
    }

    function transferFrom(address from, address to, uint256 amount) external returns (bool) {
        require(allowance[from][msg.sender] >= amount, "allowance");
        require(balanceOf[from] >= amount, "insufficient");
        allowance[from][msg.sender] -= amount;
        balanceOf[from] -= amount;
        balanceOf[to] += amount;
        return true;
    }

    function approve(address spender, uint256 amount) external returns (bool) {
        allowance[msg.sender][spender] = amount;
        return true;
    }
}

// Minimal mock Aave V3 Pool
contract MockAavePool {
    uint16 public premiumBps = 5;

    function flashLoanSimple(
        address receiver,
        address asset,
        uint256 amount,
        bytes calldata params,
        uint16 /*referralCode*/
    ) external {
        MockERC20(asset).mint(receiver, amount);
        uint256 premium = (amount * premiumBps) / 10000;
        bool success = FlashExecutor(payable(receiver)).executeOperation(
            asset,
            amount,
            premium,
            receiver,
            params
        );
        require(success, "callback failed");
        uint256 owed = amount + premium;
        require(MockERC20(asset).balanceOf(address(this)) >= owed, "repayment failed");
    }
}

// Minimal mock DEX Router (returns same token for testing)
contract MockRouter {
    function swapExactTokensForTokens(
        uint256 amountIn,
        uint256,
        address[] calldata path,
        address to,
        uint256
    ) external returns (uint256[] memory amounts) {
        // Mock: just transfer the same amount of the output token
        MockERC20(path[path.length - 1]).mint(to, amountIn);
        amounts = new uint256[](path.length);
        amounts[0] = amountIn;
        amounts[amounts.length - 1] = amountIn;
    }

    function getAmountsOut(uint256 amountIn, address[] calldata) external pure returns (uint256[] memory amounts) {
        amounts = new uint256[](2);
        amounts[0] = amountIn;
        amounts[1] = amountIn;
    }
}

contract FlashExecutorTest is Test {
    FlashExecutor public executor;
    MockAavePool public pool;
    MockERC20 public weth;
    MockRouter public router;
    address public owner;
    address public attacker;

    function setUp() public {
        owner = address(this);
        attacker = makeAddr("attacker");

        weth = new MockERC20("WETH", "WETH", 18);
        pool = new MockAavePool();
        router = new MockRouter();

        executor = new FlashExecutor(address(pool));

        // Approve router
        executor.approveRouter(address(router));
    }

    function test_ExecuteFlashLoan_Success() public {
        Route memory route;
        route.path = new address[](2);
        route.path[0] = address(weth);
        route.path[1] = address(weth);
        route.dexRouters = new address[](1);
        route.dexRouters[0] = address(router);
        route.minOut = 0;
        route.swapCalldata = new bytes[](1);
        route.swapCalldata[0] = "";

        executor.executeFlashLoan(address(weth), 1e18, route);
        // If we reach here without revert, the multi-leg logic and repayment succeeded.
    }

    function test_RevertIf_RouterNotApproved() public {
        Route memory route;
        route.path = new address[](2);
        route.path[0] = address(weth);
        route.path[1] = address(weth);
        route.dexRouters = new address[](1);
        route.dexRouters[0] = address(0xDEAD); // unapproved
        route.minOut = 0;
        route.swapCalldata = new bytes[](1);
        route.swapCalldata[0] = "";

        vm.expectRevert(abi.encodeWithSelector(RouterNotApproved.selector, address(0xDEAD)));
        executor.executeFlashLoan(address(weth), 1e18, route);
    }

    function test_RevertIf_WrongInitiator() public {
        vm.prank(address(pool));
        Route memory route;
        vm.expectRevert(abi.encodeWithSelector(WrongInitiator.selector, attacker));
        executor.executeOperation(address(weth), 1e18, 0, attacker, abi.encode(route));
    }

    function test_Rescue_NoFlashLock() public {
        // Ensure we can withdraw when no flash loan is active
        weth.mint(address(executor), 1e18);
        executor.withdrawResidual(address(weth), 1e18);
        assertEq(weth.balanceOf(owner), 1e18);
    }

    function test_Pause_Unpause() public {
        executor.emergencyPause();
        assertTrue(executor.paused());

        Route memory route;
        route.path = new address[](2);
        route.path[0] = address(weth);
        route.path[1] = address(weth);
        route.dexRouters = new address[](1);
        route.dexRouters[0] = address(router);
        route.minOut = 0;
        route.swapCalldata = new bytes[](1);
        route.swapCalldata[0] = "";

        vm.expectRevert();
        executor.executeFlashLoan(address(weth), 1e18, route);

        executor.emergencyUnpause();
        assertFalse(executor.paused());

        // Should succeed now
        executor.executeFlashLoan(address(weth), 1e18, route);
    }

    function test_RevertIf_MismatchedRouters() public {
        Route memory route;
        route.path = new address[](3); // 3 tokens = 2 legs expected
        route.path[0] = address(weth);
        route.path[1] = address(weth);
        route.path[2] = address(weth);
        route.dexRouters = new address[](1); // only 1 router provided
        route.minOut = 0;
        route.swapCalldata = new bytes[](1);
        route.swapCalldata[0] = "";

        vm.expectRevert(abi.encodeWithSelector(MismatchedRouters.selector, 3, 1));
        executor.executeFlashLoan(address(weth), 1e18, route);
    }
}
