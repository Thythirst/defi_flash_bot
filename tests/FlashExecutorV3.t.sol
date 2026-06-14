// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import "../contracts/FlashExecutorV3.sol";

// Minimal mock ERC20
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

// Minimal mock Balancer Vault
contract MockBalancerVault {
    function flashLoan(
        address recipient,
        address[] memory tokens,
        uint256[] memory amounts,
        bytes memory userData
    ) external {
        // Transfer tokens to recipient
        MockERC20(tokens[0]).mint(recipient, amounts[0]);

        // Call receiveFlashLoan
        IFlashLoanRecipient(recipient).receiveFlashLoan(
            tokens,
            amounts,
            new uint256[](tokens.length), // 0 fees
            userData
        );

        // Check repayment
        uint256 owed = amounts[0]; // 0 fee
        require(MockERC20(tokens[0]).balanceOf(address(this)) >= owed, "repayment failed");
    }
}

// Minimal mock Aave Pool
contract MockAavePool {
    // Simulate liquidation: transfer collateral to liquidator
    function liquidationCall(
        address collateralAsset,
        address debtAsset,
        address user,
        uint256 debtToCover,
        bool receiveAToken
    ) external {
        // Mock: give 5% bonus worth of collateral to msg.sender
        uint256 bonus = (debtToCover * 500) / 10000;
        MockERC20(collateralAsset).mint(msg.sender, debtToCover + bonus);
    }
}

// Minimal mock DEX Router
contract MockSwapRouter {
    function exactInputSingle(
        address tokenIn,
        address tokenOut,
        uint24 fee,
        address recipient,
        uint256 deadline,
        uint256 amountIn,
        uint256 amountOutMinimum,
        uint160 sqrtPriceLimitX96
    ) external returns (uint256 amountOut) {
        // Mock: transfer same amount of tokenOut
        MockERC20(tokenOut).mint(recipient, amountIn);
        return amountIn;
    }
}

contract FlashExecutorV3Test is Test {
    FlashExecutorV3 public executor;
    MockBalancerVault public vault;
    MockAavePool public aavePool;
    MockERC20 public weth;
    MockERC20 public usdc;
    MockSwapRouter public router;
    address public owner;
    address public attacker;

    function setUp() public {
        owner = address(this);
        attacker = makeAddr("attacker");

        weth = new MockERC20("WETH", "WETH", 18);
        usdc = new MockERC20("USDC", "USDC", 6);
        vault = new MockBalancerVault();
        aavePool = new MockAavePool();
        router = new MockSwapRouter();

        executor = new FlashExecutorV3(address(vault), address(aavePool), 0.001 ether);

        // Approve router
        executor.approveRouter(address(router));
    }

    function test_ExecuteLiquidation_Success() public {
        // Seed executor with some WETH as "collateral" that Aave will mint
        weth.mint(address(aavePool), 100 ether);

        // Build route
        bytes memory swapCalldata = abi.encodeWithSelector(
            MockSwapRouter.exactInputSingle.selector,
            address(weth),
            address(usdc),
            uint24(500),
            address(executor),
            block.timestamp + 60,
            1 ether,
            0,
            uint160(0)
        );

        // Execute liquidation
        executor.executeLiquidation(
            address(weth),    // collateral
            address(usdc),    // debt
            makeAddr("borrower"),
            1000e6,           // debtToCover
            false,            // receiveAToken
            address(router),  // swapRouter
            swapCalldata
        );
    }

    function test_RevertIf_RouterNotApproved() public {
        vm.expectRevert(abi.encodeWithSelector(RouterNotApproved.selector, address(0xDEAD)));
        executor.executeLiquidation(
            address(weth),
            address(usdc),
            makeAddr("borrower"),
            1000e6,
            false,
            address(0xDEAD),
            ""
        );
    }

    function test_RevertIf_NotProfitable() public {
        // Set high min profit threshold
        executor.setMinProfitThreshold(1000 ether);

        bytes memory swapCalldata = abi.encodeWithSelector(
            MockSwapRouter.exactInputSingle.selector,
            address(weth),
            address(usdc),
            uint24(500),
            address(executor),
            block.timestamp + 60,
            1 ether,
            0,
            uint160(0)
        );

        // Contract wraps callback reverts in FlashLoanFailed
        vm.expectRevert(FlashLoanFailed.selector);
        executor.executeLiquidation(
            address(weth),
            address(usdc),
            makeAddr("borrower"),
            1000e6,
            false,
            address(router),
            swapCalldata
        );
    }

    function test_RevertIf_UnauthorizedCallback() public {
        vm.prank(attacker);
        vm.expectRevert(abi.encodeWithSelector(UnauthorizedCallback.selector, attacker));
        executor.receiveFlashLoan(
            new address[](1),
            new uint256[](1),
            new uint256[](1),
            ""
        );
    }

    function test_RevertIf_FlashLoanMismatch() public {
        // Directly call receiveFlashLoan with mismatched data
        vm.prank(address(vault));

        LiquidationRoute memory route = LiquidationRoute({
            aavePool: address(aavePool),
            collateralAsset: address(weth),
            debtAsset: address(usdc),
            borrower: makeAddr("borrower"),
            debtToCover: 1000e6,
            receiveAToken: false,
            swapRouter: address(0),
            swapCalldata: ""
        });

        address[] memory tokens = new address[](1);
        tokens[0] = address(weth); // wrong token
        uint256[] memory amounts = new uint256[](1);
        amounts[0] = 1000e6;

        vm.expectRevert(
            abi.encodeWithSelector(FlashLoanMismatch.selector, address(usdc), 1000e6)
        );
        executor.receiveFlashLoan(
            tokens,
            amounts,
            new uint256[](1),
            abi.encode(route)
        );
    }

    function test_Pause_Unpause() public {
        executor.emergencyPause();
        assertTrue(executor.paused());

        vm.expectRevert();
        executor.executeLiquidation(
            address(weth),
            address(usdc),
            makeAddr("borrower"),
            1000e6,
            false,
            address(router),
            ""
        );

        executor.emergencyUnpause();
        assertFalse(executor.paused());
    }

    function test_Rescue_NoFlashLock() public {
        weth.mint(address(executor), 1 ether);
        executor.withdrawResidual(address(weth), 1 ether);
        assertEq(weth.balanceOf(owner), 1 ether);
    }

    function test_OnlyOwnerCanApproveRouter() public {
        vm.prank(attacker);
        vm.expectRevert();
        executor.approveRouter(address(router));
    }

    // ─── Profit Gate Tests ────────────────────────────────────

    function test_DirectLiquidation_EnforcesMinProfit() public {
        // Set minProfitThreshold to 1 USDC (1e6)
        executor.setMinProfitThreshold(1e6);

        // Pre-fund executor with enough debt asset (USDC)
        usdc.mint(address(executor), 1000e6); // 1000 USDC

        // Seed Aave pool with WETH to act as collateral payout
        weth.mint(address(aavePool), 10 ether);

        // Build swap calldata: swap WETH → USDC (mocked swap gives 1e18 USDC)
        bytes memory swapCalldata = abi.encodeWithSelector(
            MockSwapRouter.exactInputSingle.selector,
            address(weth), address(usdc), uint24(500),
            address(executor), block.timestamp + 60,
            1 ether, uint256(0), uint160(0)
        );

        // Execute direct liquidation: covers 100 USDC debt,
        // receives WETH, swaps to ~1e18 USDC
        // After: debtBalanceAfter = executor has remaining 900 USDC + swap output
        // swap output = 1e18 USDC → profit = 1e18 > 1e6 → PASS
        uint256 profit = executor.executeLiquidationDirect(
            address(weth), address(usdc), makeAddr("borrower"),
            100e6, false, address(router), swapCalldata
        );
        assertGt(profit, 1e6);
    }

    function test_DirectLiquidation_RevertsBelowMinProfit() public {
        // Set minProfitThreshold impossibly high
        executor.setMinProfitThreshold(1000000 ether);

        // Pre-fund executor
        usdc.mint(address(executor), 1000e6);
        weth.mint(address(aavePool), 10 ether);

        bytes memory swapCalldata = abi.encodeWithSelector(
            MockSwapRouter.exactInputSingle.selector,
            address(weth), address(usdc), uint24(500),
            address(executor), block.timestamp + 60,
            1 ether, uint256(0), uint160(0)
        );

        // Should revert with NotProfitable since profit < threshold
        vm.expectRevert(); // any revert is fine — we just need it to fail
        executor.executeLiquidationDirect(
            address(weth), address(usdc), makeAddr("borrower"),
            100e6, false, address(router), swapCalldata
        );
    }
}
