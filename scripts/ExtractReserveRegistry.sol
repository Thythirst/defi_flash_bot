// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

import {Script, console} from "forge-std/Script.sol";

interface IAavePool {
    function getReservesList() external view returns (address[] memory);
    function getReserveData(address asset) external view returns (
        uint256 configuration, uint128 liquidityIndex, uint128 currentLiquidityRate,
        uint128 variableBorrowIndex, uint128 currentVariableBorrowRate,
        uint128 currentStableBorrowRate, uint40 lastUpdateTimestamp, uint16 id,
        address aTokenAddress, address stableDebtTokenAddress, address variableDebtTokenAddress,
        address interestRateStrategyAddress, uint128 accruedToTreasury,
        uint128 unbacked, uint128 isolationModeTotalDebt
    );
}

interface IERC20Min {
    function symbol() external view returns (string memory);
    function decimals() external view returns (uint8);
}

contract ExtractReserveRegistry is Script {
    IAavePool pool = IAavePool(0x794a61358D6845594F94dc1DB02A252b5b4814aD);

    function run() external {
        address[] memory reserves = pool.getReservesList();
        console.log("Reserves found:", reserves.length);

        string memory json = "[";
        string memory csv = "symbol,underlying,aToken,varDebtToken,stableDebtToken,decimals,id\n";

        for (uint256 i = 0; i < reserves.length; i++) {
            (string memory j, string memory c) = _reserveToRecord(reserves[i], i);
            if (i > 0) json = string.concat(json, ",");
            json = string.concat(json, j);
            csv = string.concat(csv, c);
        }
        json = string.concat(json, "\n]");

        vm.writeFile("reports/reserve_registry.json", json);
        vm.writeFile("reports/reserve_registry.csv", csv);
        console.log("Wrote 20 reserves to reports/");
    }

    function _reserveToRecord(address reserve, uint256 idx) internal view returns (string memory jsonFrag, string memory csvLine) {
        string memory sym = "?";
        uint8 dec = 18;
        try IERC20Min(reserve).symbol() returns (string memory s) { sym = s; } catch {}
        try IERC20Min(reserve).decimals() returns (uint8 d) { dec = d; } catch {}

        (,, ,,, ,, uint16 id, address aT, address sT, address vT,,,,) = pool.getReserveData(reserve);

        jsonFrag = string.concat(
            '\n  {"symbol":"', sym, '",',
            '"underlying":"', vm.toString(reserve), '",',
            '"aToken":"', vm.toString(aT), '",',
            '"varDebt":"', vm.toString(vT), '",',
            '"stblDebt":"', vm.toString(sT), '",',
            '"dec":', vm.toString(dec), ',"id":', vm.toString(id), '}'
        );

        csvLine = string.concat(
            sym, ",", vm.toString(reserve), ",", vm.toString(aT), ",",
            vm.toString(vT), ",", vm.toString(sT), ",",
            vm.toString(dec), ",", vm.toString(id), "\n"
        );

        console.log(vm.toString(idx));
        console.logString(sym);
        console.logString(vm.toString(aT));
        console.logString(vm.toString(vT));
    }
}
