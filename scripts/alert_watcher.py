#!/usr/bin/env python3
"""Watch pipeline log and send Telegram alerts for liquidation events."""
import os, re, time, urllib.request, urllib.parse, sys

LOG_FILE = "/home/ubuntu/defi_flash_bot/logs/pipeline.log"
ENV_FILE = "/home/ubuntu/defi_flash_bot/.env"

def load_telegram():
    token = chat_id = None
    with open(ENV_FILE) as f:
        for line in f:
            if "TELEGRAM_BOT_TOKEN" in line:
                token = line.split("=", 1)[1].strip()
            elif "TELEGRAM_CHAT_ID" in line:
                chat_id = line.split("=", 1)[1].strip()
    return token, chat_id

def send_alert(token, chat_id, text):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = urllib.parse.urlencode({"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}).encode()
    try:
        urllib.request.urlopen(urllib.request.Request(url, data=data), timeout=10)
    except Exception as e:
        print(f"Telegram send failed: {e}", file=sys.stderr)

TOKEN, CHAT_ID = load_telegram()
if not TOKEN or not CHAT_ID:
    print("ERROR: Telegram not configured", file=sys.stderr)
    sys.exit(1)

print(f"Watching {LOG_FILE}...", flush=True)
seen = set()
last_pos = os.path.getsize(LOG_FILE) if os.path.exists(LOG_FILE) else 0

PATTERNS = {
    "LIQUIDATABLE": (r"LIQUIDATABLE (0x[a-fA-F0-9]+) HF=([\d.]+)",
                     lambda m: f"🚨 *LIQUIDATABLE* `{m[1][:10]}…` HF={m[2]}"),
    "GO": (r"\[Pipeline\] GO (0x[a-fA-F0-9]+) HF=([\d.]+) EV≈\$([\d.]+)",
           lambda m: f"🎯 *GO* `{m[1][:10]}…` HF={m[2]} EV=${m[3]}"),
    "BUILT": (r"Built flash loan tx.*borrower=(0x[a-fA-F0-9]+).*est_profit=\$([\d.]+)",
              lambda m: f"📤 *BUILT* `{m[1][:10]}…` profit=${m[2]}"),
    "CONFIRMED": (r"(?:confirmed|tx_hash).*?=(0x[a-fA-F0-9]+)",
                  lambda m: f"✅ *CONFIRMED* tx=`{m[1][:10]}…`"),
    "PROFIT": (r"confirmed=\d+.*profit=\$?([\d.]+)",
               lambda m: f"💰 *PROFIT* ${m[1]}"),

    # Operational — always alert:
    "ERROR":           (r'ERROR|Exception|Traceback', lambda m: "🔴 Pipeline error — check logs"),
    "CIRCUIT_BREAKER": (r'circuit.?breaker|breaker.?trip', lambda m: "🔴 Circuit breaker tripped"),
    "WALLET_LOW":      (r'wallet.?low|balance below|insufficient', lambda m: "⚠️ Wallet balance low"),
}

def should_alert(label: str, line: str, profit: float | None = None) -> bool:
    """
    Decide whether an event deserves a Telegram alert.
    Silent on noise (dust, $0 builds). Loud on real opportunities AND real problems.
    """
    # ALWAYS alert — real opportunities
    if label in ("GO", "CONFIRMED"):
        return True

    # ALWAYS alert — operational problems you must know about
    if label in ("ERROR", "CIRCUIT_BREAKER", "WALLET_LOW", "CRASH"):
        return True

    # CONDITIONAL — only if genuinely profitable
    if label in ("BUILT", "PROFIT"):
        return profit is not None and profit > 1.0

    # NEVER alert — noise (LIQUIDATABLE dust, everything else)
    return False


while True:
    try:
        size = os.path.getsize(LOG_FILE)
        if size > last_pos:
            with open(LOG_FILE) as f:
                f.seek(last_pos)
                for line in f:
                    for label, (pattern, formatter) in PATTERNS.items():
                        m = re.search(pattern, line)
                        if m:
                            key = f"{label}:{m.groups()}"
                            if key in seen:
                                continue
                            seen.add(key)
                            if len(seen) > 500:
                                seen.clear()

                            # Extract profit if the line has one
                            profit = None
                            pm = re.search(r'profit=\$?([0-9]+\.?[0-9]*)', line)
                            if pm:
                                profit = float(pm.group(1))

                            msg = formatter(m)
                            if should_alert(label, line, profit):
                                print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)
                                send_alert(TOKEN, CHAT_ID, msg)
                            else:
                                # Log silently — visible in journalctl, no phone buzz
                                print(f"[silent] {label}: {line.strip()[:80]}", flush=True)
                            break  # one pattern match per line is enough
                last_pos = f.tell()
    except Exception as e:
        print(f"Error: {e}", flush=True)
    time.sleep(2)
