"""
tri_arb_retro.py — Retrospective triangular-arb check over swap_monitor's
logged spot-price history (/tmp/swap_monitor.log, ~376K swap events).

IMPORTANT CAVEAT (same honesty the codebase already applies to pairwise
spreads in swap_monitor.py): these are SPOT prices from each pool's
post-swap sqrtPriceX96, forward-filled across time. They ignore price
impact and are NOT live executable quotes — this is a frequency/magnitude
screen to answer "did the price data ever imply a triangular edge",
not a claim that the edge was capturable. Item 1's live QuoterV2 probe
(tri_arb_probe.py) is the real-quote check.

Method: replay the log in order, keep a forward-filled last-known price
per (pair, venue). After each SWAP event, recompute both triangle
directions using the freshest available legs, track the max round-trip
edge seen and how often it beat a conservative round-trip fee floor.
"""

import re
from pathlib import Path

LOG = Path("/tmp/swap_monitor.log")

# token0/token1 address ordering (Uniswap sorts by address value):
# WBTC(0x2f2a25) < WETH(0x82af49) < ARB(0x912ce5) < USDC(0xaf88d0) < USDT(0xfd086b)
# price as logged = token1 per token0

SWAP_RE = re.compile(
    r"^(\d\d:\d\d:\d\d\.\d+).*\[SWAP\] \$\s*([\d,]+)\s+(\w+)/(\w+)\s+(\S+)\s+price=([\d.]+)"
)

# Round-trip fee floor per venue-combo (rough, conservative — 3 legs):
# univ3-500=0.05%, univ3-3000=0.30%, camelot~0.30% (dynamic, use as proxy)
FEE = {"UniV3-500": 0.0005, "UniV3-3000": 0.0030, "UniV3-10000": 0.01, "Camelot": 0.0030}


def _ts_to_seconds(ts: str, day_offset: list) -> float:
    """HH:MM:SS.mmm -> seconds since log start, handling midnight rollover."""
    h, m, rest = ts.split(":")
    s, ms = rest.split(".")
    sec = int(h) * 3600 + int(m) * 60 + int(s) + int(ms) / 1000
    if day_offset and sec + day_offset[0] < day_offset[1]:
        day_offset[0] += 86400  # rolled past midnight
    if not day_offset:
        day_offset.extend([0, sec])
    day_offset[1] = sec + day_offset[0]
    return sec + day_offset[0]


# Max allowed staleness between the freshest and stalest of the 3 legs used
# in a triangle for that snapshot to count as "near-simultaneous". A thinly
# traded pool (e.g. ARB/WETH) can sit unchanged for minutes while the other
# two legs move — treating that as a live edge is exactly the "phantom
# spread" failure mode already documented in swap_monitor.py.
MAX_STALENESS_SEC = 15.0


def iter_snapshots():
    """Stream (timestamp, prices_dict, last_seen_dict) after each SWAP event, forward-filled."""
    prices = {}      # (pair, venue) -> price
    last_seen = {}    # (pair, venue) -> seconds
    day_offset = []
    n = 0
    with LOG.open(errors="ignore") as f:
        for line in f:
            m = SWAP_RE.match(line)
            if not m:
                continue
            n += 1
            ts, usd, t0, t1, venue, price = m.groups()
            t_sec = _ts_to_seconds(ts, day_offset)
            prices[(t0, t1, venue)] = float(price)
            last_seen[(t0, t1, venue)] = t_sec
            yield ts, t_sec, prices, last_seen, n


def best_price(prices, t0, t1, last_seen, now, max_staleness=MAX_STALENESS_SEC):
    """Return list of (venue, price) for a pair, across venues NOT stale, or None."""
    candidates = [
        (v, p) for (a, b, v), p in prices.items()
        if a == t0 and b == t1 and now - last_seen.get((a, b, v), -1e9) <= max_staleness
    ]
    return candidates or None


def triangle_a(prices, last_seen, now):
    """WETH(0x82af49) -> USDC(0xaf88d0) -> ARB(0x912ce5) -> WETH, and reverse."""
    weth_usdc = best_price(prices, "0x82af49", "0xaf88d0", last_seen, now)  # USDC per WETH
    arb_usdc  = best_price(prices, "0x912ce5", "0xaf88d0", last_seen, now)  # USDC per ARB
    weth_arb  = best_price(prices, "0x82af49", "0x912ce5", last_seen, now)  # ARB per WETH
    if not (weth_usdc and arb_usdc and weth_arb):
        return None

    results = []
    for v1, p_wu in weth_usdc:
        for v2, p_au in arb_usdc:
            for v3, p_wa in weth_arb:
                # fwd: WETH->USDC->ARB->WETH
                usdc_amt = 1 * p_wu
                arb_amt = usdc_amt / p_au
                weth_back = arb_amt / p_wa
                fee = FEE.get(v1, 0.003) + FEE.get(v2, 0.003) + FEE.get(v3, 0.003)
                results.append(("A-fwd", weth_back - 1, fee, (v1, v2, v3)))
                # rev: WETH->ARB->USDC->WETH
                arb_amt2 = 1 * p_wa
                usdc_amt2 = arb_amt2 * p_au
                weth_back2 = usdc_amt2 / p_wu
                results.append(("A-rev", weth_back2 - 1, fee, (v3, v2, v1)))
    return results


def triangle_b(prices, last_seen, now):
    """WETH(0x82af49) -> WBTC(0x2f2a25) -> USDC(0xaf88d0) -> WETH, and reverse."""
    wbtc_weth = best_price(prices, "0x2f2a25", "0x82af49", last_seen, now)  # WETH per WBTC
    wbtc_usdc = best_price(prices, "0x2f2a25", "0xaf88d0", last_seen, now)  # USDC per WBTC
    weth_usdc = best_price(prices, "0x82af49", "0xaf88d0", last_seen, now)  # USDC per WETH
    if not (wbtc_weth and wbtc_usdc and weth_usdc):
        return None

    results = []
    for v1, p_bw in wbtc_weth:
        for v2, p_bu in wbtc_usdc:
            for v3, p_wu in weth_usdc:
                # fwd: WETH->WBTC->USDC->WETH
                wbtc_amt = 1 / p_bw
                usdc_amt = wbtc_amt * p_bu
                weth_back = usdc_amt / p_wu
                fee = FEE.get(v1, 0.003) + FEE.get(v2, 0.003) + FEE.get(v3, 0.003)
                results.append(("B-fwd", weth_back - 1, fee, (v1, v2, v3)))
                # rev: WETH->USDC->WBTC->WETH
                usdc_amt2 = 1 * p_wu
                wbtc_amt2 = usdc_amt2 / p_bu
                weth_back2 = wbtc_amt2 * p_bw
                results.append(("B-rev", weth_back2 - 1, fee, (v3, v2, v1)))
    return results


def report(name, total, beats_fee, best, all_net_edges):
    print(f"=== {name} ===")
    print(f"  snapshots with all 3 legs fresh (<{MAX_STALENESS_SEC:.0f}s apart): {total}")
    print(f"  combos where raw edge beat round-trip fee floor: {beats_fee}")
    if all_net_edges:
        s = sorted(all_net_edges)
        n = len(s)
        pct = lambda p: s[min(n - 1, int(n * p))]
        print(f"  net-edge (after fee) distribution: p50={pct(0.50)*100:+.4f}%  "
              f"p95={pct(0.95)*100:+.4f}%  p99={pct(0.99)*100:+.4f}%  max={s[-1]*100:+.4f}%")
    if best:
        ts, edge, fee, label, venues = best
        print(f"  best RAW edge ever: {edge*100:+.4f}% (fee floor {fee*100:.3f}%, "
              f"net {edge*100-fee*100:+.4f}%) at {ts} [{label}] venues={venues}")
    print()


def main():
    best_a = None
    best_b = None
    beats_fee_a = 0
    beats_fee_b = 0
    total_a = 0
    total_b = 0
    net_edges_a = []
    net_edges_b = []
    n = 0

    for ts, t_sec, prices, last_seen, n in iter_snapshots():
        ra = triangle_a(prices, last_seen, t_sec)
        if ra:
            total_a += 1
            best_net = max(edge - fee for _, edge, fee, _ in ra)
            net_edges_a.append(best_net)
            for label, edge, fee, venues in ra:
                if best_a is None or edge > best_a[1]:
                    best_a = (ts, edge, fee, label, venues)
                if edge > fee:
                    beats_fee_a += 1
        rb = triangle_b(prices, last_seen, t_sec)
        if rb:
            total_b += 1
            best_net = max(edge - fee for _, edge, fee, _ in rb)
            net_edges_b.append(best_net)
            for label, edge, fee, venues in rb:
                if best_b is None or edge > best_b[1]:
                    best_b = (ts, edge, fee, label, venues)
                if edge > fee:
                    beats_fee_b += 1

    print(f"Parsed {n} SWAP events from {LOG}\n")
    report("Triangle A (WETH-USDC-ARB)", total_a, beats_fee_a, best_a, net_edges_a)
    report("Triangle B (WETH-WBTC-USDC)", total_b, beats_fee_b, best_b, net_edges_b)


if __name__ == "__main__":
    main()
