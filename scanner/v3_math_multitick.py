"""
scanner/v3_math_multitick.py — Multi-tick swap solver for Uniswap V3 / Algebra.

Extends v3_math.py with tick-traversal logic.  In production this would be backed
by live tickBitmap + ticks() RPC calls; for backtesting we use a cached
initialized-tick map fetched once on the current state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from scanner.v3_math import (
    Q96,
    MIN_TICK,
    MAX_TICK,
    TICK_SPACING,
    get_sqrt_price_at_tick,
    get_next_initialized_tick,
    _get_amount0_delta,
    _get_amount0_delta_floor,
    _get_amount1_delta,
    _get_amount1_delta_rounding_up,
    _validate_fee,
    SwapResult as SingleTickSwapResult,
)


@dataclass
class TickData:
    """Liquidity state at a single initialized tick."""
    tick: int
    liquidity_net: int       # signed liquidity change when crossing this tick
    liquidity_gross: int     # total position width at this tick


@dataclass
class MultiTickSwapResult:
    """Result of a multi-tick V3 swap."""
    amount_out: int
    new_tick: int
    new_sqrt_price_x96: int
    price_shift_bps: int
    ticks_crossed: int
    fee_paid: int

    # Diagnostics
    steps: List[Tuple[int, int, int]] = field(default_factory=list)  # [(tick_after, liquidity, amount_used), ...]


class TickLiquidityMap:
    """
    Lightweight wrapper around initialized-tick data.

    For *production*, fetch from on-chain `ticks()` + `tickBitmap()`.
    For *backtest*, provide an estimated map or supply an RPC helper.
    """

    def __init__(self, tick_data: Dict[int, TickData], tick_spacing: int):
        self._data = tick_data
        self._spacing = tick_spacing
        self._ticks = sorted(tick_data.keys())

    @classmethod
    def from_constant_liquidity(cls, current_tick: int, liquidity: int, tick_spacing: int,
                                 num_ticks_each_side: int = 50) -> "TickLiquidityMap":
        """
        Build a synthetic map assuming every tick has zero net delta except the outer ones.
        Liquidity is constant across all interior ticks (best-guess for backtesting
        when no bitmap is available).
        """
        data: Dict[int, TickData] = {}
        boundary_low = current_tick - num_ticks_each_side * tick_spacing
        boundary_high = current_tick + num_ticks_each_side * tick_spacing

        # Every interior tick has zero net; we only need the boundaries
        data[boundary_low] = TickData(tick=boundary_low, liquidity_net=0, liquidity_gross=0)
        data[boundary_high] = TickData(tick=boundary_high, liquidity_net=0, liquidity_gross=0)
        # Insert current tick with full liquidity
        data[current_tick] = TickData(tick=current_tick, liquidity_net=0, liquidity_gross=liquidity)
        return cls(data, tick_spacing)

    def next_initialized(self, tick: int, zero_for_one: bool) -> Optional[int]:
        """
        Return the next initialized tick boundary in the given direction,
        or None if we hit MIN_TICK / MAX_TICK.
        """
        spacing = self._spacing
        if zero_for_one:
            # Look lower
            target = (tick // spacing) * spacing
            if target == tick:
                target -= spacing
            # Walk down until we find an initialized tick or hit MIN_TICK
            while target >= MIN_TICK and target not in self._data:
                target -= spacing
            return target if target >= MIN_TICK else None
        else:
            # Look higher
            target = -((-tick) // spacing) * spacing
            if target == tick:
                target += spacing
            while target <= MAX_TICK and target not in self._data:
                target += spacing
            return target if target <= MAX_TICK else None

    def get_liquidity_net(self, tick: int) -> int:
        td = self._data.get(tick)
        return td.liquidity_net if td else 0


def compute_v3_multi_tick_swap(
    amount_in: int,
    current_tick: int,
    current_liquidity: int,
    fee_tier: int,
    *,
    zero_for_one: bool,
    tick_map: TickLiquidityMap,
) -> MultiTickSwapResult:
    """
    Iterative multi-tick swap matching Uniswap V3 SwapMath behavior.

    At each step we:
      1. Load the next initialized tick boundary.
      2. Compute the maximum input that would reach exactly that boundary.
      3. If net amount_remaining > max_for_boundary, consume the boundary,
         update liquidityNet, and continue.
      4. Otherwise, compute exact new sqrtP within this tick.

    Args:
        amount_in:      Gross input amount (before fee).
        current_tick:   Pool tick at swap start.
        current_liquidity: Active liquidity L at current_tick.
        fee_tier:       V3 fee tier (100/500/3000/10000).
        zero_for_one:   True → selling token0 (price↓), False → token1 (price↑).
        tick_map:       Initialized-tick liquidity map.

    Returns:
        MultiTickSwapResult with total amount_out, final tick, price shift, etc.
    """
    _validate_fee(fee_tier)
    tick_spacing = TICK_SPACING[fee_tier]

    # ── Step 0: deduct fee ──
    fee_amount = (amount_in * fee_tier) // 1_000_000
    amount_remaining = amount_in - fee_amount

    if amount_remaining <= 0:
        return MultiTickSwapResult(
            amount_out=0,
            new_tick=current_tick,
            new_sqrt_price_x96=get_sqrt_price_at_tick(current_tick),
            price_shift_bps=0,
            ticks_crossed=0,
            fee_paid=fee_amount,
        )

    sqrt_p = get_sqrt_price_at_tick(current_tick)
    tick = current_tick
    liquidity = current_liquidity
    total_out: int = 0
    ticks_crossed: int = 0
    steps: List[Tuple[int, int, int]] = []

    while amount_remaining > 0:
        boundary_tick = tick_map.next_initialized(tick, zero_for_one)
        if boundary_tick is None:
            # Hit global boundary — abort with partial fill
            break

        sqrt_boundary = get_sqrt_price_at_tick(boundary_tick)

        # Maximum net input that reaches this boundary exactly
        if zero_for_one:
            max_net = _get_amount0_delta_floor(sqrt_boundary, sqrt_p, liquidity)
        else:
            max_net = _get_amount1_delta(sqrt_boundary, sqrt_p, liquidity)

        if amount_remaining >= max_net and max_net > 0:
            # ── Saturate this tick ──
            if zero_for_one:
                # Price moved down to boundary; token1 output from this step
                step_out = _get_amount1_delta_rounding_up(sqrt_boundary, sqrt_p, liquidity)
                total_out += step_out
            else:
                # Price moved up to boundary; token0 output from this step
                step_out = _get_amount0_delta(sqrt_boundary, sqrt_p, liquidity)
                total_out += step_out

            amount_remaining -= max_net
            ticks_crossed += 1

            # Cross the tick: apply liquidityNet
            liquidity_net = tick_map.get_liquidity_net(boundary_tick)
            if zero_for_one:
                liquidity += liquidity_net  # crossing down adds net (or subtracts)
            else:
                liquidity -= liquidity_net  # crossing up reverses net

            if liquidity < 0:
                liquidity = 0

            steps.append((boundary_tick, liquidity, max_net))
            tick = boundary_tick
            sqrt_p = sqrt_boundary
        else:
            # ── Partial step within current tick ──
            if zero_for_one:
                # token0 → token1, price down
                numerator = liquidity * Q96 * sqrt_p
                denominator = amount_remaining * sqrt_p + liquidity * Q96
                sqrt_new = numerator // denominator
                step_out = _get_amount1_delta_rounding_up(sqrt_new, sqrt_p, liquidity)
            else:
                # token1 → token0, price up
                delta = (amount_remaining * Q96) // liquidity
                sqrt_new = sqrt_p + delta
                step_out = _get_amount0_delta(sqrt_new, sqrt_p, liquidity)

            total_out += step_out
            amount_remaining = 0
            # tick stays at current position; we didn't reach the boundary
            sqrt_p = sqrt_new
            steps.append((tick, liquidity, amount_remaining))

        if liquidity == 0:
            break  # dead zone

    # Price shift in bps
    start_sqrt = get_sqrt_price_at_tick(current_tick)
    if start_sqrt == 0:
        price_shift_bps = 0
    else:
        price_shift_bps = abs(sqrt_p - start_sqrt) * 10_000 // start_sqrt

    return MultiTickSwapResult(
        amount_out=total_out,
        new_tick=tick,
        new_sqrt_price_x96=sqrt_p,
        price_shift_bps=price_shift_bps,
        ticks_crossed=ticks_crossed,
        fee_paid=fee_amount,
        steps=steps,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Tick data fetcher (production helper)
# ═══════════════════════════════════════════════════════════════════════════════

async def fetch_tick_map_from_rpc(
    rpc_url: str,
    pool_address: str,
    fee_tier: int,
    current_tick: int,
    radius_ticks: int = 100,
) -> TickLiquidityMap:
    """
    Production helper: fetch initialized ticks around current price via RPC.
    Uses eth_call to ticks() and tickBitmap().

    NOTE: This fetches *current* on-chain state, not historical.
    For historical backtesting, cache this map once and reuse, or augment
    with archive-node RPC calls at specific block numbers.
    """
    import aiohttp
    from eth_abi import encode, decode

    tick_spacing = TICK_SPACING[fee_tier]
    compressed = current_tick // tick_spacing
    word_pos = compressed >> 8

    data: Dict[int, TickData] = {}
    tick_selector = b'\xf3\x0d\xba\x93'  # ticks(int24)

    # Fetch a band of initialized ticks
    async with aiohttp.ClientSession() as session:
        for offset in range(-radius_ticks, radius_ticks + 1):
            candidate = current_tick + offset * tick_spacing
            calldata = '0x' + tick_selector.hex() + encode(['int24'], [candidate]).hex()
            payload = {
                'jsonrpc': '2.0', 'id': 1,
                'method': 'eth_call',
                'params': [{'to': pool_address, 'data': calldata}, 'latest']
            }
            try:
                async with session.post(rpc_url, json=payload, headers={'Content-Type': 'application/json'}) as resp:
                    result = await resp.json()
                raw = result.get('result', '0x')
                if len(raw) < 2:
                    continue
                decoded = decode(
                    ['uint128', 'int128', 'uint256', 'uint256', 'int56', 'uint160', 'uint32', 'bool'],
                    bytes.fromhex(raw[2:])
                )
                if decoded[7]:  # initialized
                    data[candidate] = TickData(
                        tick=candidate,
                        liquidity_net=decoded[1],
                        liquidity_gross=decoded[0],
                    )
            except Exception:
                continue

    return TickLiquidityMap(data, tick_spacing)
