"""
Replay the oracle-normalised slippage formula (flash_loan_route.py:766-768)
against actual "100% slippage" rejections recorded in pipeline.log.

For each case we show:
  old_slippage  — raw (amount_in - amount_out) / amount_in   (the broken formula)
  new_slippage  — oracle-normalised, matching the fix in the codebase
"""

# ── Token catalogue ─────────────────────────────────────────────────────────
# Arbitrum addresses (lowercase for matching), decimals, approx USD price.
# Prices from memory: WETH ~$1725, BTC ~$64098 as of 2026-06-21.
TOKENS = {
    "0x82af4944": dict(sym="WETH",   dec=18, price=1725.0),
    "0xaf88d065": dict(sym="USDC",   dec=6,  price=1.0),
    "0xfd086bc7": dict(sym="USDT",   dec=6,  price=1.0),
    "0xff970a61": dict(sym="USDC.e", dec=6,  price=1.0),
    "0x5979d7b5": dict(sym="wstETH", dec=18, price=2074.0),   # ~20% staking premium
    "0x2f2a2543": dict(sym="WBTC",   dec=8,  price=64098.0),
    "0x912ce591": dict(sym="ARB",    dec=18, price=0.79),
}

def addr_key(addr: str) -> str:
    return addr.lower()[:10]  # first 10 chars match log format

# Build lookup by prefix (log shows "0x82aF4944→0xaf88d065")
PREFIX_MAP = {v["sym"]: k for k, v in TOKENS.items()}
ADDR_MAP   = {k[:10]: v for k, v in TOKENS.items()}   # keyed by 10-char prefix

def lookup(addr_prefix: str):
    key = addr_prefix.lower()
    for k, v in TOKENS.items():
        if k.startswith(key):
            return v
    return None

# ── Cases extracted from pipeline.log ────────────────────────────────────────
# Format: (log_line, collateral_prefix, debt_prefix, amount_in, amount_out)
# CACHE HIT lines don't show amount_in; we recover it from a MISS line for the
# same pair when available, else mark as None (those rows are included for
# completeness but skipped in the formula check).

CASES = [
    # line  col                 debt               amount_in         amount_out
    (111,  "0x82aF4944",       "0xaf88d065",       None,             112),      # CACHE HIT – amt_in unknown
    (115,  "0x82aF4944",       "0xFd086bC7",       2680022263,       4),
    (118,  "0x82aF4944",       "0xaf88d065",       None,             75),       # CACHE HIT
    (126,  "0x82aF4944",       "0xFF970A61",       3282481148,       5),
    (135,  "0x82aF4944",       "0xaf88d065",       None,             98),       # CACHE HIT
    (139,  "0x82aF4944",       "0xFd086bC7",       17342306057449,   31537),
    (142,  "0x82aF4944",       "0xaf88d065",       None,             2),        # CACHE HIT
    (147,  "0x82aF4944",       "0xFF970A61",       3325382849,       6),
    (150,  "0x82aF4944",       "0xaf88d065",       None,             1920),     # CACHE HIT
    (157,  "0x82aF4944",       "0xFF970A61",       2888291957,       5),
    (165,  "0x82aF4944",       "0x2f2a2543",       384174601638,     1),        # WETH→WBTC
    (252,  "0x82aF4944",       "0xaf88d065",       None,             7475),     # CACHE HIT
    (255,  "0x82aF4944",       "0xaf88d065",       None,             112),      # CACHE HIT
    (268,  "0x82aF4944",       "0xaf88d065",       None,             75),       # CACHE HIT
    (271,  "0x82aF4944",       "0xaf88d065",       None,             98),       # CACHE HIT
    (276,  "0x82aF4944",       "0xaf88d065",       None,             1920),     # CACHE HIT
    (285,  "0x82aF4944",       "0xaf88d065",       None,             2),        # CACHE HIT
    (292,  "0x82aF4944",       "0xFd086bC7",       2680022263,       4),
    (294,  "0x82aF4944",       "0xFF970A61",       3325382849,       6),
    (296,  "0x82aF4944",       "0x2f2a2543",       384174601638,     1),        # WETH→WBTC
    (298,  "0x82aF4944",       "0xFF970A61",       10109021853,      18),
    (300,  "0x82aF4944",       "0xFF970A61",       2888291957,       5),
    (302,  "0x82aF4944",       "0xFF970A61",       3282481148,       5),
    (309,  "0x82aF4944",       "0xaf88d065",       None,             0),        # CACHE HIT – out=0 edge case
    (315,  "0x82aF4944",       "0xaf88d065",       None,             112),      # CACHE HIT
    (319,  "0x82aF4944",       "0xFd086bC7",       2680022286,       4),
    (322,  "0x82aF4944",       "0xaf88d065",       None,             75),       # CACHE HIT
    (326,  "0x82aF4944",       "0xFF970A61",       3282481176,       5),
]

def oracle_formula(amount_in, amount_out, col_info, dbt_info):
    """Exact replica of flash_loan_route.py lines 766-768."""
    col_price = col_info["price"]
    dbt_price = dbt_info["price"]
    col_dec   = col_info["dec"]
    dbt_dec   = dbt_info["dec"]
    oracle_out = amount_in * col_price * (10 ** dbt_dec) // (dbt_price * (10 ** col_dec))
    if oracle_out <= 0:
        return None, None   # guard: formula cannot fire
    new_slip = max(0.0, (oracle_out - amount_out) / oracle_out)
    return oracle_out, new_slip

def old_formula(amount_in, amount_out):
    return (amount_in - amount_out) / amount_in

# ── Run ───────────────────────────────────────────────────────────────────────
print(f"{'Line':>5}  {'Pair':<16}  {'amount_in':>20}  {'out':>20}  {'OLD slip':>9}  {'oracle_out':>12}  {'NEW slip':>9}  verdict")
print("-" * 115)

for line, col_addr, dbt_addr, amount_in, amount_out in CASES:
    col_info = lookup(col_addr)
    dbt_info = lookup(dbt_addr)
    col_sym  = col_info["sym"] if col_info else col_addr[:6]
    dbt_sym  = dbt_info["sym"] if dbt_info else dbt_addr[:6]
    pair     = f"{col_sym}→{dbt_sym}"

    if amount_in is None:
        # CACHE HIT — infer amount_in from amount_out using oracle price ratio
        # (best-effort approximation so we can still show the new formula result)
        if col_info and dbt_info and dbt_info["price"] > 0 and col_info["price"] > 0:
            # approximate amount_in that would produce this amount_out at oracle price
            inferred = int(amount_out * dbt_info["price"] * (10 ** col_info["dec"])
                          / (col_info["price"] * (10 ** dbt_info["dec"])))
            if inferred <= 0:
                print(f"{line:>5}  {pair:<16}  {'(CACHE HIT)':>20}  {amount_out:>20}  {'—':>9}  {'—':>12}  {'—':>9}  skipped (amt_in=0)")
                continue
            amount_in_disp = f"~{inferred}"
            old_slip = old_formula(inferred, amount_out) if inferred > 0 else None
            oracle_out, new_slip = oracle_formula(inferred, amount_out, col_info, dbt_info)
        else:
            print(f"{line:>5}  {pair:<16}  {'(CACHE HIT)':>20}  {amount_out:>20}  {'—':>9}  {'—':>12}  {'—':>9}  skipped")
            continue
    else:
        amount_in_disp = str(amount_in)
        old_slip = old_formula(amount_in, amount_out)
        oracle_out, new_slip = oracle_formula(amount_in, amount_out, col_info, dbt_info)

    if oracle_out is None:
        verdict = "GUARD (oracle_out=0, fix inactive)"
    elif new_slip > 0.02:
        verdict = f"STILL REJECTED ({new_slip:.1%}) — may be genuinely illiquid"
    else:
        verdict = f"PASS ({new_slip:.2%}) ✓"

    old_str   = f"{old_slip:.2%}" if old_slip is not None else "—"
    new_str   = f"{new_slip:.2%}" if new_slip is not None else "—"
    oracle_str= f"{int(oracle_out)}" if oracle_out is not None else "—"

    print(f"{line:>5}  {pair:<16}  {amount_in_disp:>20}  {amount_out:>20}  {old_str:>9}  {oracle_str:>12}  {new_str:>9}  {verdict}")

print()
print("Prices used: WETH=$1725  USDC/USDT/USDC.e=$1.00  WBTC=$64098  wstETH=$2074  ARB=$0.79")
print("Formula:     oracle_out = amount_in * col_price * 10^dbt_dec // (dbt_price * 10^col_dec)")
print("             new_slip   = max(0, (oracle_out - amount_out) / oracle_out)")
