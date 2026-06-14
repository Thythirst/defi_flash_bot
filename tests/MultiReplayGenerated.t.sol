// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import "forge-std/Test.sol";
import "../contracts/FlashExecutorV3.sol";

interface IAavePoolFull {
    function getUserAccountData(address user) external view returns (
        uint256 totalCollateralBase, uint256 totalDebtBase,
        uint256 availableBorrowsBase, uint256 currentLiquidationThreshold,
        uint256 ltv, uint256 healthFactor
    );
}

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

contract MultiReplayTest is Test {
    address constant AAVE_POOL      = 0x794a61358D6845594F94dc1DB02A252b5b4814aD;
    address constant BALANCER_VAULT = 0xBA12222222228d8Ba445958a75a0704d566BF2C8;
    address constant UNI_V3_ROUTER  = 0xE592427A0AEce92De3Edee1F18E0157C05861564;

    event Result(uint indexed idx, bool s5, uint256 s6_coll, bool s7, int256 s8_profit, string err);


    function test_C1_Replay() public {
        string memory rpc = vm.envString("FORK_RPC_URL");
        vm.createSelectFork(rpc, 468870381);

        FlashExecutorV3 executor = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
        executor.approveRouter(UNI_V3_ROUTER);

        address B = 0x6cde149662C41E7eA01918a9f42650b1CA1be7f2;
        address C = 0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f;
        address D = 0xaf88d065e77c8cC2239327C5EDb3A432268e5831;

        (,,,,, uint256 hf) = IAavePoolFull(AAVE_POOL).getUserAccountData(B);
        require(hf < 1e18, "Not liquidatable");

        deal(D, address(executor), 701989538 * 3);

        bytes memory swapCalldata = "";
        if (!false) {
            uint256 estColl = uint256(701989538 * 105) / 100;
            ExactInputSingleParams memory sp = ExactInputSingleParams({
                tokenIn: C, tokenOut: D, fee: 500, recipient: address(executor),
                deadline: block.timestamp + 600, amountIn: estColl,
                amountOutMinimum: 0, sqrtPriceLimitX96: 0
            });
            swapCalldata = abi.encodeWithSelector(
                bytes4(keccak256("exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))")), sp);
        }

        uint256 debtBefore = IERC20(D).balanceOf(address(executor));
        try executor.executeLiquidationDirect(
            C, D, B, 701989538, false,
            false ? address(0) : UNI_V3_ROUTER, swapCalldata
        ) returns (uint256 profit) {
            uint256 collAfter = IERC20(C).balanceOf(address(executor));
            uint256 debtAfter = IERC20(D).balanceOf(address(executor));
            int256 ps = int256(debtAfter) - int256(debtBefore - 701989538);
            emit Result(1, true, collAfter, collAfter == 0 || false, ps, "");
        } catch (bytes memory reason) {
            emit Result(1, false, 0, false, 0, string(reason));
        }
    }


    function test_C2_Replay() public {
        string memory rpc = vm.envString("FORK_RPC_URL");
        vm.createSelectFork(rpc, 468923015);

        FlashExecutorV3 executor = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
        executor.approveRouter(UNI_V3_ROUTER);

        address B = 0x12BB24D553161036461eF1Dc789C6f73765e1a57;
        address C = 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1;
        address D = 0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9;

        (,,,,, uint256 hf) = IAavePoolFull(AAVE_POOL).getUserAccountData(B);
        require(hf < 1e18, "Not liquidatable");

        deal(D, address(executor), 5990764391 * 3);

        bytes memory swapCalldata = "";
        if (!false) {
            uint256 estColl = uint256(5990764391 * 105) / 100;
            ExactInputSingleParams memory sp = ExactInputSingleParams({
                tokenIn: C, tokenOut: D, fee: 500, recipient: address(executor),
                deadline: block.timestamp + 600, amountIn: estColl,
                amountOutMinimum: 0, sqrtPriceLimitX96: 0
            });
            swapCalldata = abi.encodeWithSelector(
                bytes4(keccak256("exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))")), sp);
        }

        uint256 debtBefore = IERC20(D).balanceOf(address(executor));
        try executor.executeLiquidationDirect(
            C, D, B, 5990764391, false,
            false ? address(0) : UNI_V3_ROUTER, swapCalldata
        ) returns (uint256 profit) {
            uint256 collAfter = IERC20(C).balanceOf(address(executor));
            uint256 debtAfter = IERC20(D).balanceOf(address(executor));
            int256 ps = int256(debtAfter) - int256(debtBefore - 5990764391);
            emit Result(2, true, collAfter, collAfter == 0 || false, ps, "");
        } catch (bytes memory reason) {
            emit Result(2, false, 0, false, 0, string(reason));
        }
    }


    function test_C3_Replay() public {
        string memory rpc = vm.envString("FORK_RPC_URL");
        vm.createSelectFork(rpc, 468948148);

        FlashExecutorV3 executor = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
        executor.approveRouter(UNI_V3_ROUTER);

        address B = 0xFd8AA5b28Ee3AfeE3259a980F33B115cdF6433C5;
        address C = 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1;
        address D = 0xDA10009cBd5D07dd0CeCc66161FC93D7c9000da1;

        (,,,,, uint256 hf) = IAavePoolFull(AAVE_POOL).getUserAccountData(B);
        require(hf < 1e18, "Not liquidatable");

        deal(D, address(executor), 2887967591881449677545 * 3);

        bytes memory swapCalldata = "";
        if (!false) {
            uint256 estColl = uint256(2887967591881449677545 * 105) / 100;
            ExactInputSingleParams memory sp = ExactInputSingleParams({
                tokenIn: C, tokenOut: D, fee: 500, recipient: address(executor),
                deadline: block.timestamp + 600, amountIn: estColl,
                amountOutMinimum: 0, sqrtPriceLimitX96: 0
            });
            swapCalldata = abi.encodeWithSelector(
                bytes4(keccak256("exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))")), sp);
        }

        uint256 debtBefore = IERC20(D).balanceOf(address(executor));
        try executor.executeLiquidationDirect(
            C, D, B, 2887967591881449677545, false,
            false ? address(0) : UNI_V3_ROUTER, swapCalldata
        ) returns (uint256 profit) {
            uint256 collAfter = IERC20(C).balanceOf(address(executor));
            uint256 debtAfter = IERC20(D).balanceOf(address(executor));
            int256 ps = int256(debtAfter) - int256(debtBefore - 2887967591881449677545);
            emit Result(3, true, collAfter, collAfter == 0 || false, ps, "");
        } catch (bytes memory reason) {
            emit Result(3, false, 0, false, 0, string(reason));
        }
    }


    function test_C4_Replay() public {
        string memory rpc = vm.envString("FORK_RPC_URL");
        vm.createSelectFork(rpc, 468948148);

        FlashExecutorV3 executor = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
        executor.approveRouter(UNI_V3_ROUTER);

        address B = 0xFdefEf7cB2855C322B671537d23F86F6e0C33FD5;
        address C = 0x82aF49447D8a07e3bd95BD0d56f35241523fBab1;
        address D = 0xaf88d065e77c8cC2239327C5EDb3A432268e5831;

        (,,,,, uint256 hf) = IAavePoolFull(AAVE_POOL).getUserAccountData(B);
        require(hf < 1e18, "Not liquidatable");

        deal(D, address(executor), 2485406838 * 3);

        bytes memory swapCalldata = "";
        if (!false) {
            uint256 estColl = uint256(2485406838 * 105) / 100;
            ExactInputSingleParams memory sp = ExactInputSingleParams({
                tokenIn: C, tokenOut: D, fee: 500, recipient: address(executor),
                deadline: block.timestamp + 600, amountIn: estColl,
                amountOutMinimum: 0, sqrtPriceLimitX96: 0
            });
            swapCalldata = abi.encodeWithSelector(
                bytes4(keccak256("exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))")), sp);
        }

        uint256 debtBefore = IERC20(D).balanceOf(address(executor));
        try executor.executeLiquidationDirect(
            C, D, B, 2485406838, false,
            false ? address(0) : UNI_V3_ROUTER, swapCalldata
        ) returns (uint256 profit) {
            uint256 collAfter = IERC20(C).balanceOf(address(executor));
            uint256 debtAfter = IERC20(D).balanceOf(address(executor));
            int256 ps = int256(debtAfter) - int256(debtBefore - 2485406838);
            emit Result(4, true, collAfter, collAfter == 0 || false, ps, "");
        } catch (bytes memory reason) {
            emit Result(4, false, 0, false, 0, string(reason));
        }
    }


    function test_C5_Replay() public {
        string memory rpc = vm.envString("FORK_RPC_URL");
        vm.createSelectFork(rpc, 468948148);

        FlashExecutorV3 executor = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
        executor.approveRouter(UNI_V3_ROUTER);

        address B = 0x30187648AA95022FA23030d68351acB1ebd514f4;
        address C = 0x35751007a407ca6FEFfE80b3cB397736D2cf4dbe;
        address D = 0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9;

        (,,,,, uint256 hf) = IAavePoolFull(AAVE_POOL).getUserAccountData(B);
        require(hf < 1e18, "Not liquidatable");

        deal(D, address(executor), 1152141702 * 3);

        bytes memory swapCalldata = "";
        if (!false) {
            uint256 estColl = uint256(1152141702 * 105) / 100;
            ExactInputSingleParams memory sp = ExactInputSingleParams({
                tokenIn: C, tokenOut: D, fee: 500, recipient: address(executor),
                deadline: block.timestamp + 600, amountIn: estColl,
                amountOutMinimum: 0, sqrtPriceLimitX96: 0
            });
            swapCalldata = abi.encodeWithSelector(
                bytes4(keccak256("exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))")), sp);
        }

        uint256 debtBefore = IERC20(D).balanceOf(address(executor));
        try executor.executeLiquidationDirect(
            C, D, B, 1152141702, false,
            false ? address(0) : UNI_V3_ROUTER, swapCalldata
        ) returns (uint256 profit) {
            uint256 collAfter = IERC20(C).balanceOf(address(executor));
            uint256 debtAfter = IERC20(D).balanceOf(address(executor));
            int256 ps = int256(debtAfter) - int256(debtBefore - 1152141702);
            emit Result(5, true, collAfter, collAfter == 0 || false, ps, "");
        } catch (bytes memory reason) {
            emit Result(5, false, 0, false, 0, string(reason));
        }
    }


    function test_C6_Replay() public {
        string memory rpc = vm.envString("FORK_RPC_URL");
        vm.createSelectFork(rpc, 469024868);

        FlashExecutorV3 executor = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
        executor.approveRouter(UNI_V3_ROUTER);

        address B = 0x9c70dbDA71De7A71a56221C05e585953969F85a6;
        address C = 0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f;
        address D = 0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9;

        (,,,,, uint256 hf) = IAavePoolFull(AAVE_POOL).getUserAccountData(B);
        require(hf < 1e18, "Not liquidatable");

        deal(D, address(executor), 790956 * 3);

        bytes memory swapCalldata = "";
        if (!false) {
            uint256 estColl = uint256(790956 * 105) / 100;
            ExactInputSingleParams memory sp = ExactInputSingleParams({
                tokenIn: C, tokenOut: D, fee: 500, recipient: address(executor),
                deadline: block.timestamp + 600, amountIn: estColl,
                amountOutMinimum: 0, sqrtPriceLimitX96: 0
            });
            swapCalldata = abi.encodeWithSelector(
                bytes4(keccak256("exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))")), sp);
        }

        uint256 debtBefore = IERC20(D).balanceOf(address(executor));
        try executor.executeLiquidationDirect(
            C, D, B, 790956, false,
            false ? address(0) : UNI_V3_ROUTER, swapCalldata
        ) returns (uint256 profit) {
            uint256 collAfter = IERC20(C).balanceOf(address(executor));
            uint256 debtAfter = IERC20(D).balanceOf(address(executor));
            int256 ps = int256(debtAfter) - int256(debtBefore - 790956);
            emit Result(6, true, collAfter, collAfter == 0 || false, ps, "");
        } catch (bytes memory reason) {
            emit Result(6, false, 0, false, 0, string(reason));
        }
    }


    function test_C7_Replay() public {
        string memory rpc = vm.envString("FORK_RPC_URL");
        vm.createSelectFork(rpc, 469123580);

        FlashExecutorV3 executor = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
        executor.approveRouter(UNI_V3_ROUTER);

        address B = 0xe51e8f0D3D1dbf66ae865BC506B9eB2B265bC894;
        address C = 0xba5DdD1f9d7F570dc94a51479a000E3BCE967196;
        address D = 0x7dfF72693f6A4149b17e7C6314655f6A9F7c8B33;

        (,,,,, uint256 hf) = IAavePoolFull(AAVE_POOL).getUserAccountData(B);
        require(hf < 1e18, "Not liquidatable");

        deal(D, address(executor), 736831656812882001872 * 3);

        bytes memory swapCalldata = "";
        if (!false) {
            uint256 estColl = uint256(736831656812882001872 * 105) / 100;
            ExactInputSingleParams memory sp = ExactInputSingleParams({
                tokenIn: C, tokenOut: D, fee: 500, recipient: address(executor),
                deadline: block.timestamp + 600, amountIn: estColl,
                amountOutMinimum: 0, sqrtPriceLimitX96: 0
            });
            swapCalldata = abi.encodeWithSelector(
                bytes4(keccak256("exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))")), sp);
        }

        uint256 debtBefore = IERC20(D).balanceOf(address(executor));
        try executor.executeLiquidationDirect(
            C, D, B, 736831656812882001872, false,
            false ? address(0) : UNI_V3_ROUTER, swapCalldata
        ) returns (uint256 profit) {
            uint256 collAfter = IERC20(C).balanceOf(address(executor));
            uint256 debtAfter = IERC20(D).balanceOf(address(executor));
            int256 ps = int256(debtAfter) - int256(debtBefore - 736831656812882001872);
            emit Result(7, true, collAfter, collAfter == 0 || false, ps, "");
        } catch (bytes memory reason) {
            emit Result(7, false, 0, false, 0, string(reason));
        }
    }


    function test_C8_Replay() public {
        string memory rpc = vm.envString("FORK_RPC_URL");
        vm.createSelectFork(rpc, 469123581);

        FlashExecutorV3 executor = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
        executor.approveRouter(UNI_V3_ROUTER);

        address B = 0xe179976FD6d47274815FDe480Dc2786d3b6A5127;
        address C = 0xba5DdD1f9d7F570dc94a51479a000E3BCE967196;
        address D = 0xaf88d065e77c8cC2239327C5EDb3A432268e5831;

        (,,,,, uint256 hf) = IAavePoolFull(AAVE_POOL).getUserAccountData(B);
        require(hf < 1e18, "Not liquidatable");

        deal(D, address(executor), 71777970 * 3);

        bytes memory swapCalldata = "";
        if (!false) {
            uint256 estColl = uint256(71777970 * 105) / 100;
            ExactInputSingleParams memory sp = ExactInputSingleParams({
                tokenIn: C, tokenOut: D, fee: 500, recipient: address(executor),
                deadline: block.timestamp + 600, amountIn: estColl,
                amountOutMinimum: 0, sqrtPriceLimitX96: 0
            });
            swapCalldata = abi.encodeWithSelector(
                bytes4(keccak256("exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))")), sp);
        }

        uint256 debtBefore = IERC20(D).balanceOf(address(executor));
        try executor.executeLiquidationDirect(
            C, D, B, 71777970, false,
            false ? address(0) : UNI_V3_ROUTER, swapCalldata
        ) returns (uint256 profit) {
            uint256 collAfter = IERC20(C).balanceOf(address(executor));
            uint256 debtAfter = IERC20(D).balanceOf(address(executor));
            int256 ps = int256(debtAfter) - int256(debtBefore - 71777970);
            emit Result(8, true, collAfter, collAfter == 0 || false, ps, "");
        } catch (bytes memory reason) {
            emit Result(8, false, 0, false, 0, string(reason));
        }
    }


    function test_C9_Replay() public {
        string memory rpc = vm.envString("FORK_RPC_URL");
        vm.createSelectFork(rpc, 469193987);

        FlashExecutorV3 executor = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
        executor.approveRouter(UNI_V3_ROUTER);

        address B = 0x78E84b30a752661c030a5Ae5e4377D80c8EF0885;
        address C = 0xba5DdD1f9d7F570dc94a51479a000E3BCE967196;
        address D = 0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9;

        (,,,,, uint256 hf) = IAavePoolFull(AAVE_POOL).getUserAccountData(B);
        require(hf < 1e18, "Not liquidatable");

        deal(D, address(executor), 1247284232 * 3);

        bytes memory swapCalldata = "";
        if (!false) {
            uint256 estColl = uint256(1247284232 * 105) / 100;
            ExactInputSingleParams memory sp = ExactInputSingleParams({
                tokenIn: C, tokenOut: D, fee: 500, recipient: address(executor),
                deadline: block.timestamp + 600, amountIn: estColl,
                amountOutMinimum: 0, sqrtPriceLimitX96: 0
            });
            swapCalldata = abi.encodeWithSelector(
                bytes4(keccak256("exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))")), sp);
        }

        uint256 debtBefore = IERC20(D).balanceOf(address(executor));
        try executor.executeLiquidationDirect(
            C, D, B, 1247284232, false,
            false ? address(0) : UNI_V3_ROUTER, swapCalldata
        ) returns (uint256 profit) {
            uint256 collAfter = IERC20(C).balanceOf(address(executor));
            uint256 debtAfter = IERC20(D).balanceOf(address(executor));
            int256 ps = int256(debtAfter) - int256(debtBefore - 1247284232);
            emit Result(9, true, collAfter, collAfter == 0 || false, ps, "");
        } catch (bytes memory reason) {
            emit Result(9, false, 0, false, 0, string(reason));
        }
    }


    function test_C10_Replay() public {
        string memory rpc = vm.envString("FORK_RPC_URL");
        vm.createSelectFork(rpc, 469193987);

        FlashExecutorV3 executor = new FlashExecutorV3(BALANCER_VAULT, AAVE_POOL, 0);
        executor.approveRouter(UNI_V3_ROUTER);

        address B = 0x674cb93Ce397C47a315bCb216e9f0A85B01616BB;
        address C = 0xba5DdD1f9d7F570dc94a51479a000E3BCE967196;
        address D = 0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f;

        (,,,,, uint256 hf) = IAavePoolFull(AAVE_POOL).getUserAccountData(B);
        require(hf < 1e18, "Not liquidatable");

        deal(D, address(executor), 88601 * 3);

        bytes memory swapCalldata = "";
        if (!false) {
            uint256 estColl = uint256(88601 * 105) / 100;
            ExactInputSingleParams memory sp = ExactInputSingleParams({
                tokenIn: C, tokenOut: D, fee: 500, recipient: address(executor),
                deadline: block.timestamp + 600, amountIn: estColl,
                amountOutMinimum: 0, sqrtPriceLimitX96: 0
            });
            swapCalldata = abi.encodeWithSelector(
                bytes4(keccak256("exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))")), sp);
        }

        uint256 debtBefore = IERC20(D).balanceOf(address(executor));
        try executor.executeLiquidationDirect(
            C, D, B, 88601, false,
            false ? address(0) : UNI_V3_ROUTER, swapCalldata
        ) returns (uint256 profit) {
            uint256 collAfter = IERC20(C).balanceOf(address(executor));
            uint256 debtAfter = IERC20(D).balanceOf(address(executor));
            int256 ps = int256(debtAfter) - int256(debtBefore - 88601);
            emit Result(10, true, collAfter, collAfter == 0 || false, ps, "");
        } catch (bytes memory reason) {
            emit Result(10, false, 0, false, 0, string(reason));
        }
    }

}