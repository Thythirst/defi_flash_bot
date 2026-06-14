// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

/**
 * @title CompoundV3Executor
 * @notice Liquidation executor for Compound V3 (Comet) markets on Arbitrum.
 *
 * Flow:
 *   1. absorb(address[] accounts)          — free, anyone can call, earns liquidator points
 *   2. Check getReserves() < targetReserves — buyCollateral reverts if reserves >= target
 *   3. Flash loan USDC from Uni V3 pool    — fund the buyCollateral call
 *   4. buyCollateral(asset, minOut, base, self) — buy seized collateral at discount
 *   5. Swap collateral → USDC via Uni V3   — recover base asset
 *   6. Repay flash loan + fee              — keep profit
 *
 * Key difference from Aave V3 (FlashExecutorV3):
 *   - No single liquidationCall() — absorb + buyCollateral are separate steps
 *   - Flash loan is Uni V3 (not Balancer) — Compound's own reference uses Uni V3 flash
 *   - buyCollateral can fail if protocol reserves >= targetReserves
 *   - absorb() alone is always safe to call and earns liquidator points
 *
 * Deployed separately per market (USDC market, ETH market have different Comet addresses).
 */

interface IComet {
    function absorb(address absorber, address[] calldata accounts) external;
    function buyCollateral(
        address asset,
        uint minAmount,
        uint baseAmount,
        address recipient
    ) external;
    function quoteCollateral(address asset, uint baseAmount) external view returns (uint);
    function getCollateralReserves(address asset) external view returns (uint);
    function getReserves() external view returns (int);
    function targetReserves() external view returns (uint);
    function isLiquidatable(address account) external view returns (bool);
    function borrowBalanceOf(address account) external view returns (uint);
    function baseToken() external view returns (address);
    function baseScale() external view returns (uint);
    function numAssets() external view returns (uint8);
    function getAssetInfo(uint8 i) external view returns (AssetInfo memory);
    function userCollateral(address account, address asset)
        external view returns (uint128 balance, uint128 reserved);
}

struct AssetInfo {
    uint8   offset;
    address asset;
    address priceFeed;
    uint64  scale;
    uint64  borrowCollateralFactor;
    uint64  liquidateCollateralFactor;
    uint64  liquidationFactor;
    uint128 supplyCap;
}

interface IERC20 {
    function approve(address spender, uint amount) external returns (bool);
    function transfer(address to, uint amount) external returns (bool);
    function balanceOf(address account) external view returns (uint);
    function decimals() external view returns (uint8);
}

interface IUniswapV3Pool {
    function flash(
        address recipient,
        uint amount0,
        uint amount1,
        bytes calldata data
    ) external;
    function token0() external view returns (address);
    function token1() external view returns (address);
}

interface ISwapRouter {
    struct ExactInputSingleParams {
        address tokenIn;
        address tokenOut;
        uint24  fee;
        address recipient;
        uint256 deadline;
        uint256 amountIn;
        uint256 amountOutMinimum;
        uint160 sqrtPriceLimitX96;
    }
    function exactInputSingle(ExactInputSingleParams calldata params)
        external returns (uint amountOut);
}

contract CompoundV3Executor {

    // ── Immutables ───────────────────────────────────────────────────────────
    address public immutable owner;
    address public immutable COMET;          // Comet proxy (e.g. cUSDCv3 on Arbitrum)
    address public immutable BASE_TOKEN;     // USDC for USDC market, WETH for ETH market
    address public immutable UNI_ROUTER;     // Uni V3 SwapRouter
    address public immutable FLASH_POOL;     // Uni V3 pool used for flash loan
    bool    private immutable FLASH_IS_TOKEN0; // BASE_TOKEN position in flash pool

    // ── Constants ────────────────────────────────────────────────────────────
    uint public minProfitThreshold = 1_000_000; // $1 in USDC 6-decimal units

    // ── Errors ───────────────────────────────────────────────────────────────
    error NotOwner();
    error NotUniPool();
    error ReservesAboveTarget();
    error InsufficientProfit(uint profit, uint threshold);
    error BuyCollateralFailed(address asset);
    error Paused();

    // ── Events ───────────────────────────────────────────────────────────────
    event Liquidated(
        address indexed borrower,
        address indexed collateralAsset,
        uint    baseUsed,
        uint    collateralReceived,
        uint    profit
    );
    event AbsorbOnly(address indexed borrower, uint liquidatorPoints);

    // ── State ────────────────────────────────────────────────────────────────
    bool public paused;
    mapping(address => bool) public approvedRouters;

    // ── Flash loan callback data ─────────────────────────────────────────────
    struct FlashData {
        address borrower;
        address collateralAsset;
        uint    baseAmount;        // USDC to spend on buyCollateral
        uint    minCollateralOut;  // slippage guard
        uint    swapFee;           // Uni V3 fee tier for collateral→base swap
        uint    flashFee;          // Uni V3 flash loan fee amount
    }

    // ── Constructor ──────────────────────────────────────────────────────────
    constructor(
        address _comet,
        address _uniRouter,
        address _flashPool
    ) {
        owner       = msg.sender;
        COMET       = _comet;
        UNI_ROUTER  = _uniRouter;
        FLASH_POOL  = _flashPool;

        // Derive base token from Comet
        BASE_TOKEN  = IComet(_comet).baseToken();

        // Determine BASE_TOKEN position in flash pool
        address t0  = IUniswapV3Pool(_flashPool).token0();
        FLASH_IS_TOKEN0 = (t0 == BASE_TOKEN);
    }

    // ── Modifiers ────────────────────────────────────────────────────────────
    modifier onlyOwner() {
        if (msg.sender != owner) revert NotOwner();
        _;
    }

    modifier notPaused() {
        if (paused) revert Paused();
        _;
    }

    // ── Admin ────────────────────────────────────────────────────────────────
    function setMinProfitThreshold(uint threshold) external onlyOwner {
        minProfitThreshold = threshold;
    }

    function approveRouter(address router) external onlyOwner {
        approvedRouters[router] = true;
    }

    function setPaused(bool _paused) external onlyOwner {
        paused = _paused;
    }

    function withdrawToken(address token, uint amount) external onlyOwner {
        IERC20(token).transfer(owner, amount);
    }

    // ── Core: absorb only ────────────────────────────────────────────────────

    /**
     * @notice Absorb one or more underwater accounts.
     * Free to call — no flash loan needed. Earns liquidator points.
     * Call this regardless of whether buyCollateral is possible.
     *
     * @param accounts Array of underwater borrower addresses
     */
    function absorbAccounts(address[] calldata accounts) external notPaused {
        IComet(COMET).absorb(address(this), accounts);
        emit AbsorbOnly(accounts[0], 0);
    }

    // ── Core: absorb + buyCollateral + swap ──────────────────────────────────

    /**
     * @notice Full liquidation: absorb + flash loan + buyCollateral + swap to profit.
     *
     * @param borrower         Underwater borrower address
     * @param collateralAsset  Which collateral asset to buy after absorb
     * @param baseAmount       How much BASE_TOKEN to spend on buyCollateral
     * @param minCollateralOut Minimum collateral to receive (slippage guard)
     * @param swapFee          Uni V3 fee tier for collateral → base swap
     */
    function executeLiquidation(
        address borrower,
        address collateralAsset,
        uint    baseAmount,
        uint    minCollateralOut,
        uint24  swapFee
    ) external notPaused {
        // ── Pre-checks ────────────────────────────────────────────────────
        require(IComet(COMET).isLiquidatable(borrower), "not liquidatable");

        // Check reserves < target — buyCollateral reverts otherwise
        int  reserves       = IComet(COMET).getReserves();
        uint targetReserves = IComet(COMET).targetReserves();
        if (reserves >= 0 && uint(reserves) >= targetReserves) {
            revert ReservesAboveTarget();
        }

        // ── Step 1: Absorb ────────────────────────────────────────────────
        address[] memory accounts = new address[](1);
        accounts[0] = borrower;
        IComet(COMET).absorb(address(this), accounts);

        // ── Step 2: Flash loan BASE_TOKEN to fund buyCollateral ───────────
        // Compute flash fee: Uni V3 flash fee is pool.fee() * amount / 1e6
        // We encode the data and let uniswapV3FlashCallback handle the rest
        bytes memory data = abi.encode(FlashData({
            borrower:         borrower,
            collateralAsset:  collateralAsset,
            baseAmount:       baseAmount,
            minCollateralOut: minCollateralOut,
            swapFee:          swapFee,
            flashFee:         0  // filled in callback
        }));

        uint amount0 = FLASH_IS_TOKEN0 ? baseAmount : 0;
        uint amount1 = FLASH_IS_TOKEN0 ? 0 : baseAmount;

        IUniswapV3Pool(FLASH_POOL).flash(address(this), amount0, amount1, data);
    }

    // ── Uni V3 flash loan callback ───────────────────────────────────────────

    /**
     * @notice Called by Uni V3 pool after flash loan is disbursed.
     * Executes buyCollateral, swaps collateral → base, repays flash loan.
     */
    function uniswapV3FlashCallback(
        uint fee0,
        uint fee1,
        bytes calldata data
    ) external {
        if (msg.sender != FLASH_POOL) revert NotUniPool();

        FlashData memory fd = abi.decode(data, (FlashData));
        uint flashFee = FLASH_IS_TOKEN0 ? fee0 : fee1;

        // ── Step 3: buyCollateral ─────────────────────────────────────────
        uint baseBalBefore = IERC20(BASE_TOKEN).balanceOf(address(this));

        IERC20(BASE_TOKEN).approve(COMET, fd.baseAmount);
        IComet(COMET).buyCollateral(
            fd.collateralAsset,
            fd.minCollateralOut,
            fd.baseAmount,
            address(this)
        );

        uint collateralReceived = IERC20(fd.collateralAsset).balanceOf(address(this));
        require(collateralReceived >= fd.minCollateralOut, "slippage exceeded");

        // ── Step 4: Swap collateral → BASE_TOKEN via Uni V3 ──────────────
        IERC20(fd.collateralAsset).approve(UNI_ROUTER, collateralReceived);

        uint baseReceived = ISwapRouter(UNI_ROUTER).exactInputSingle(
            ISwapRouter.ExactInputSingleParams({
                tokenIn:           fd.collateralAsset,
                tokenOut:          BASE_TOKEN,
                fee:               uint24(fd.swapFee),
                recipient:         address(this),
                deadline:          block.timestamp + 180,
                amountIn:          collateralReceived,
                amountOutMinimum:  fd.baseAmount + flashFee, // must cover repayment
                sqrtPriceLimitX96: 0
            })
        );

        // ── Step 5: Repay flash loan ──────────────────────────────────────
        uint repayAmount = fd.baseAmount + flashFee;
        IERC20(BASE_TOKEN).transfer(FLASH_POOL, repayAmount);

        // ── Step 6: Profit check ──────────────────────────────────────────
        uint profit = baseReceived > repayAmount ? baseReceived - repayAmount : 0;
        if (profit < minProfitThreshold) {
            revert InsufficientProfit(profit, minProfitThreshold);
        }

        emit Liquidated(
            fd.borrower,
            fd.collateralAsset,
            fd.baseAmount,
            collateralReceived,
            profit
        );
    }

    // ── View helpers ─────────────────────────────────────────────────────────

    /**
     * @notice Check if buyCollateral is currently possible.
     * Returns false if protocol reserves >= targetReserves.
     */
    function canBuyCollateral() external view returns (bool) {
        int reserves = IComet(COMET).getReserves();
        uint target  = IComet(COMET).targetReserves();
        return reserves < 0 || uint(reserves) < target;
    }

    /**
     * @notice Quote how much collateral you'd receive for baseAmount.
     * Use to size baseAmount and compute minCollateralOut with slippage.
     */
    function quoteCollateral(address asset, uint baseAmount)
        external view returns (uint collateralAmount)
    {
        return IComet(COMET).quoteCollateral(asset, baseAmount);
    }

    /**
     * @notice How much of a collateral asset is available to buy.
     */
    function collateralReserves(address asset) external view returns (uint) {
        return IComet(COMET).getCollateralReserves(asset);
    }
}
