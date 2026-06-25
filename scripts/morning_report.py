"""
Go-live readiness morning report for the same-block backrun bot.

Run by cron at 12:00 UTC. Reads the overnight SHADOW data from
/tmp/swap_monitor.log and queries Arbitrum (Alchemy, from .env) for live gas +
competition data, then sends a summary to Telegram. Every section is guarded so
one failure still yields a report.

Sections: (1) latency-vs-visibility verdict, (2) arbitrage opportunities seen,
(3) gas + wallet, (4) competition proxy, (5) recommendation.
"""
import os, re, time, statistics, traceback
from collections import defaultdict

import requests
from web3 import Web3
from dotenv import load_dotenv

ROOT = os.path.join(os.path.dirname(__file__), "..")
load_dotenv(os.path.join(ROOT, ".env"))
LOG = "/tmp/swap_monitor.log"

RPC = os.getenv("ARBITRUM_HTTP_URL")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID")
BOT_ADDR = os.getenv("BOT_ADDRESS")

WETH = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
USDT = "0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9"
WBTC = "0x2f2a2543B76A4166549F7aaB2e75Bef0aefC5B0f"
ARB  = "0x912CE59144191C1204E64559FE8253a0e49E6548"
FACTORY  = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
ETH_FEED = "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612"
SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
PAIRS = [(WETH, USDC), (WETH, USDT), (WBTC, WETH), (WBTC, USDC), (ARB, USDC), (ARB, WETH)]
FEES = [500, 3000, 10000]
ARB_GAS_LIMIT = 800_000  # approx per-arb tx ceiling

w3 = Web3(Web3.HTTPProvider(RPC))


def section(title, fn):
    try:
        return fn()
    except Exception as e:
        return f"<b>{title}</b>\n  ⚠ failed: {e}\n  {traceback.format_exc().splitlines()[-1]}"


def latency_visibility():
    lines = open(LOG, errors="ignore").read().splitlines()
    stats = [l for l in lines if "[Stats]" in l]
    if not stats:
        return "<b>1) LATENCY vs VISIBILITY</b>\n  no [Stats] lines yet"
    last = stats[-1]
    first_ts = stats[0].split()[0] if stats else "?"
    last_ts  = last.split()[0]
    m = re.search(r"feed_seen=(\d+)/(\d+)", last)
    lead = re.search(r"lead=([0-9.]+)ms", last)
    seen, total = (int(m.group(1)), int(m.group(2))) if m else (0, 0)
    ratio = (seen / total * 100) if total else 0.0
    lead_str = f"{lead.group(1)}ms median" if lead else "n/a"
    large = re.search(r"large=(\d+)", last)
    verdict = (
        "LATENCY RACE → colocation/Alchemy IS the fix" if ratio >= 50 and lead
        else "VISIBILITY GAP → colocation won't help; swaps not seen as pending"
        if total >= 20 else "INSUFFICIENT DATA (need more large swaps)"
    )
    return (
        f"<b>1) LATENCY vs VISIBILITY</b>\n"
        f"  window: {first_ts} → {last_ts}\n"
        f"  large swaps: {large.group(1) if large else '?'}\n"
        f"  visible-as-pending: <b>{seen}/{total}</b> ({ratio:.0f}%)\n"
        f"  lead time (latency budget): <b>{lead_str}</b>\n"
        f"  → <b>{verdict}</b>"
    )


def opportunities():
    lines = open(LOG, errors="ignore").read().splitlines()
    stats = [l for l in lines if "[Stats]" in l]
    last = stats[-1] if stats else ""
    def g(p, d="?"):
        m = re.search(p, last); return m.group(1) if m else d
    feed_swaps = g(r"feed_swaps=(\d+)")
    feed_fired = g(r"feed\[[a-z]+\]_fired=(\d+)")
    shadow_ok  = g(r"shadow_confirmed=(\d+)")
    scores = [l for l in lines if "[SeqFeed SHADOW] score" in l]
    profitable = sum(1 for l in scores if "PROFITABLE" in l)
    urdiag = [l for l in lines if "UR-DIAG] execute=" in l]
    ur_last = urdiag[-1].split("UR-DIAG]")[-1].strip() if urdiag else "n/a"
    return (
        f"<b>2) ARB OPPORTUNITIES (overnight, shadow)</b>\n"
        f"  feed_swaps={feed_swaps}  feed_fired={feed_fired}  "
        f"shadow_confirmed={shadow_ok}\n"
        f"  SHADOW round-trip scores: {len(scores)} (profitable: {profitable})\n"
        f"  last UR-DIAG: {ur_last}"
    )


def gas_and_wallet():
    gp = w3.eth.gas_price
    # ETH/USD from Chainlink
    feed = w3.eth.contract(address=Web3.to_checksum_address(ETH_FEED),
                           abi=[{"name": "latestRoundData", "outputs": [
                               {"type": "uint80"}, {"type": "int256"}, {"type": "uint256"},
                               {"type": "uint256"}, {"type": "uint80"}],
                               "inputs": [], "stateMutability": "view", "type": "function"}])
    eth_usd = feed.functions.latestRoundData().call()[1] / 1e8
    tx_eth = gp * ARB_GAS_LIMIT / 1e18
    tx_usd = tx_eth * eth_usd
    bal = w3.eth.get_balance(Web3.to_checksum_address(BOT_ADDR)) / 1e18
    return (
        f"<b>3) GAS + WALLET</b>\n"
        f"  gas price: {gp/1e9:.3f} gwei  (ETH≈${eth_usd:,.0f})\n"
        f"  est per-arb tx: {tx_eth:.6f} ETH (~${tx_usd:.3f}) @ {ARB_GAS_LIMIT:,} gas\n"
        f"  BOT wallet: {bal:.5f} ETH (~${bal*eth_usd:,.0f}, ~{int(bal/tx_eth) if tx_eth else 0} txs)"
    )


def competition():
    # Resolve monitored UniV3 pools, then sample recent Swap events; same-block
    # multi-swaps on one pool are a backrun/sandwich signature (proxy for rivals).
    fac = w3.eth.contract(address=Web3.to_checksum_address(FACTORY),
                          abi=[{"name": "getPool", "inputs": [
                              {"type": "address"}, {"type": "address"}, {"type": "uint24"}],
                              "outputs": [{"type": "address"}], "stateMutability": "view",
                              "type": "function"}])
    pools = []
    for a, b in PAIRS:
        for f in FEES:
            try:
                p = fac.functions.getPool(Web3.to_checksum_address(a),
                                          Web3.to_checksum_address(b), f).call()
                if int(p, 16) != 0:
                    pools.append(Web3.to_checksum_address(p))
            except Exception:
                pass
    latest = w3.eth.block_number
    frm = latest - 2000  # ~8 min of Arbitrum
    logs = w3.eth.get_logs({"fromBlock": frm, "toBlock": latest,
                            "address": pools, "topics": [SWAP_TOPIC]})
    by_block_pool = defaultdict(int)
    for lg in logs:
        by_block_pool[(lg["blockNumber"], lg["address"])] += 1
    multi = sum(1 for v in by_block_pool.values() if v >= 2)
    # priority-fee competition: sample recent blocks' base fee
    fh = w3.eth.fee_history(20, "latest", [50, 90])
    base = fh["baseFeePerGas"][-1] / 1e9
    prio = [r[1] / 1e9 for r in fh.get("reward", []) if r]
    prio90 = statistics.mean(prio) if prio else 0
    return (
        f"<b>4) COMPETITION (proxy)</b>\n"
        f"  monitored pools resolved: {len(pools)}\n"
        f"  swaps last ~2000 blk: {len(logs)}  "
        f"same-block multi-swaps: <b>{multi}</b> (backrun signature)\n"
        f"  base fee: {base:.4f} gwei  p90 priority: {prio90:.4f} gwei"
    )


def recommendation():
    try:
        last = [l for l in open(LOG, errors="ignore") if "[Stats]" in l][-1]
        m = re.search(r"feed_seen=(\d+)/(\d+)", last)
        seen, total = (int(m.group(1)), int(m.group(2))) if m else (0, 0)
        ratio = (seen / total) if total else 0
        if total < 20:
            rec = "HOLD — not enough large-swap samples to decide; keep shadow running."
        elif ratio >= 0.5:
            rec = ("OPTIMIZE-THEN-GO-LIVE — swaps ARE visible; bottleneck is the race. "
                   "Move feed WS + submit RPC to Alchemy/us-east, then re-arm "
                   "SWAP_MONITOR_EXECUTE=1. Tighten MIN_BACKRUN_EDGE_PCT if fires miss.")
        else:
            rec = ("HOLD on colocation — VISIBILITY gap (swaps not seen as pending). "
                   "Colocation won't help. Pivot: decode more entrypoints or broaden pairs.")
    except Exception as e:
        rec = f"could not derive: {e}"
    return f"<b>5) RECOMMENDATION</b>\n  {rec}"


def main():
    parts = [
        f"<b>🤖 BOT GO-LIVE REPORT — {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())}</b>",
        section("1) LATENCY vs VISIBILITY", latency_visibility),
        section("2) ARB OPPORTUNITIES", opportunities),
        section("3) GAS + WALLET", gas_and_wallet),
        section("4) COMPETITION", competition),
        section("5) RECOMMENDATION", recommendation),
    ]
    text = "\n\n".join(parts)
    print(text)
    if TG_TOKEN and TG_CHAT:
        try:
            requests.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                          json={"chat_id": TG_CHAT, "text": text, "parse_mode": "HTML"},
                          timeout=20)
        except Exception as e:
            print(f"telegram send failed: {e}")


if __name__ == "__main__":
    main()
