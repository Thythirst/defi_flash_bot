// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import "../contracts/FlashExecutorV3.sol";

// ─── Minimal mocks ─────────────────────────────────────────────
contract MockERC20 {
    string public name; uint8 public decimals = 18;
    mapping(address => uint256) public balanceOf;
    mapping(address => mapping(address => uint256)) public allowance;
    constructor(string memory n) { name = n; }
    function mint(address to, uint256 a) external { balanceOf[to] += a; }
    function approve(address s, uint256 a) external returns (bool) { allowance[msg.sender][s] = a; return true; }
    function transfer(address to, uint256 a) external returns (bool) { _xfer(msg.sender, to, a); return true; }
    function transferFrom(address f, address to, uint256 a) external returns (bool) {
        uint256 al = allowance[f][msg.sender];
        if (al != type(uint256).max) { require(al >= a, "allow"); allowance[f][msg.sender] = al - a; }
        _xfer(f, to, a); return true;
    }
    function _xfer(address f, address to, uint256 a) internal { require(balanceOf[f] >= a, "bal"); balanceOf[f] -= a; balanceOf[to] += a; }
}

// Simulates Aave: caller repays `debtToCover` of debt token, receives collateral
// worth debtToCover * (1 + bonus). Reverts for any borrower in `badBorrower`.
contract MockAavePool {
    MockERC20 public debt; MockERC20 public coll; uint256 public bonusBps;
    mapping(address => bool) public badBorrower;
    constructor(MockERC20 _debt, MockERC20 _coll, uint256 _bonusBps) { debt = _debt; coll = _coll; bonusBps = _bonusBps; }
    function setBad(address b, bool v) external { badBorrower[b] = v; }
    function liquidationCall(address, address, address borrower, uint256 debtToCover, bool) external {
        require(!badBorrower[borrower], "AAVE: position not liquidatable");
        debt.transferFrom(msg.sender, address(this), debtToCover);
        uint256 seized = debtToCover * (10000 + bonusBps) / 10000;
        coll.mint(msg.sender, seized);
    }
}

// Simulates a DEX: pulls all collateral from caller, pays out debt 1:1 (collateral
// already includes the bonus, so caller nets the bonus in debt token).
contract MockRouter {
    MockERC20 public debt; MockERC20 public coll;
    constructor(MockERC20 _debt, MockERC20 _coll) { debt = _debt; coll = _coll; }
    function swapAll() external {
        uint256 c = coll.balanceOf(msg.sender);
        coll.transferFrom(msg.sender, address(this), c);
        debt.mint(msg.sender, c); // 1:1, debt and coll both 18-dec here
    }
}

// Balancer-style vault: lends `amounts`, calls receiveFlashLoan, requires repayment.
contract MockBalancerVault {
    function flashLoan(address recipient, address[] memory tokens, uint256[] memory amounts, bytes memory userData) external {
        MockERC20 t = MockERC20(tokens[0]);
        uint256 before = t.balanceOf(address(this));
        t.transfer(recipient, amounts[0]);
        uint256[] memory fees = new uint256[](1); fees[0] = 0;
        IFlashLoanRecipient(recipient).receiveFlashLoan(tokens, amounts, fees, userData);
        require(t.balanceOf(address(this)) >= before, "VAULT: not repaid");
    }
}

contract BatchLiquidationTest is Test {
    MockERC20 debt; MockERC20 coll;
    MockAavePool pool; MockRouter router; MockBalancerVault vault;
    FlashExecutorV3 exec;

    function setUp() public {
        debt = new MockERC20("USDC");
        coll = new MockERC20("WETH");
        pool = new MockAavePool(debt, coll, 800);   // 8% liquidation bonus
        router = new MockRouter(debt, coll);
        vault = new MockBalancerVault();
        exec = new FlashExecutorV3(address(vault), address(pool), 0);
        exec.approveRouter(address(router));
        // Fund the vault so it can lend the debt token.
        debt.mint(address(vault), 1_000_000e18);
    }

    function _item(address borrower, uint256 amt) internal view returns (BatchItem memory) {
        return BatchItem({
            collateralAsset: address(coll),
            borrower: borrower,
            debtToCover: amt,
            receiveAToken: false,
            swapRouter: address(router),
            swapCalldata: abi.encodeWithSelector(MockRouter.swapAll.selector)
        });
    }

    function test_batch_all_succeed_profit_repaid() public {
        BatchItem[] memory items = new BatchItem[](3);
        items[0] = _item(address(0xA1), 1000e18);
        items[1] = _item(address(0xA2), 2000e18);
        items[2] = _item(address(0xA3), 1500e18);
        uint256 vaultBefore = debt.balanceOf(address(vault));
        exec.executeLiquidationBatch(address(debt), items);
        // Vault fully repaid; executor keeps the 8% bonus on 4500 debt = 360.
        assertEq(debt.balanceOf(address(vault)), vaultBefore, "vault must be made whole");
        assertEq(debt.balanceOf(address(exec)), 4500e18 * 800 / 10000, "executor keeps aggregate bonus");
    }

    function test_batch_isolates_one_reverting_item() public {
        pool.setBad(address(0xB2), true); // middle item will revert in liquidationCall
        BatchItem[] memory items = new BatchItem[](3);
        items[0] = _item(address(0xB1), 1000e18);
        items[1] = _item(address(0xB2), 2000e18); // reverts, isolated
        items[2] = _item(address(0xB3), 1500e18);
        exec.executeLiquidationBatch(address(debt), items);
        // Only 0xB1 + 0xB3 (2500 debt) liquidated; bonus = 2500 * 8% = 200.
        assertEq(debt.balanceOf(address(exec)), 2500e18 * 800 / 10000, "survivors still profit");
    }

    function test_batch_reverts_when_aggregate_unprofitable() public {
        // Set a min-profit threshold higher than the bonus → whole batch must revert.
        exec.setMinProfitThreshold(address(debt), 1_000_000e18);
        BatchItem[] memory items = new BatchItem[](1);
        items[0] = _item(address(0xC1), 1000e18);
        vm.expectRevert();
        exec.executeLiquidationBatch(address(debt), items);
    }

    function test_doOneLiquidationStep_onlySelf() public {
        BatchItem memory it = _item(address(0xD1), 1000e18);
        vm.expectRevert(abi.encodeWithSelector(UnauthorizedCallback.selector, address(this)));
        exec.doOneLiquidationStep(address(debt), it);
    }

    function test_batch_rejects_unapproved_router() public {
        BatchItem[] memory items = new BatchItem[](1);
        items[0] = _item(address(0xE1), 1000e18);
        items[0].swapRouter = address(0xdead); // not approved
        vm.expectRevert(abi.encodeWithSelector(RouterNotApproved.selector, address(0xdead)));
        exec.executeLiquidationBatch(address(debt), items);
    }

    function test_batch_rejects_empty_and_zero() public {
        BatchItem[] memory empty = new BatchItem[](0);
        vm.expectRevert(InvalidParameters.selector);
        exec.executeLiquidationBatch(address(debt), empty);
    }
}
