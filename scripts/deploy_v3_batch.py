"""
scripts/deploy_v3_batch.py — Deploy the batch-capable FlashExecutorV3 to Arbitrum.

Deploys the executor that includes executeLiquidationBatch (#4), then re-approves
the routers the bot needs for collateral->debt swaps, and writes the deployment
record. Defaults to a DRY RUN that only estimates gas + USD cost — pass --send to
actually broadcast.

Usage:
    # cost preview only (no broadcast):
    ./venv/bin/python scripts/deploy_v3_batch.py
    # actually deploy:
    ./venv/bin/python scripts/deploy_v3_batch.py --send

Reads BOT_PRIVATE_KEY from .env. Picks a healthy RPC automatically (Alchemy is
capacity-exhausted, so it is skipped if it fails a health check).
"""
from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

from web3 import Web3
from dotenv import load_dotenv

ROOT = Path(__file__).parent.parent
load_dotenv(ROOT / ".env")

ARBITRUM_CHAIN_ID = 42161
BALANCER_VAULT = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"
AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
MIN_PROFIT_THRESHOLD_WEI = 50_000_000_000_000_000  # 0.05 ETH default

# Routers the bot uses for collateral->debt swaps (from prior deployment record).
ROUTERS = {
    "uni_v3":  "0xE592427A0AEce92De3Edee1F18E0157C05861564",
    "camelot": "0x1F721E2E82F6676FCE4eA07A5958cF098D339e18",
}


def _clean(u: str) -> str:
    return u.strip().strip('"').strip("'") if u else u


def pick_rpc(w3_required_chain: int = ARBITRUM_CHAIN_ID) -> str:
    """Return the first RPC that passes a chain-id health check (skips dead Alchemy)."""
    candidates = [
        os.getenv("RPC_PUBLICNODE"), os.getenv("RPC_BLASTAPI"),
        os.getenv("DRPC_RPC_URL"), os.getenv("READ_RPC_SECONDARY"),
        os.getenv("ARBITRUM_HTTP_URL"), os.getenv("ALCHEMY_HTTP_URL"),
    ]
    for url in candidates:
        url = _clean(url)
        if not url:
            continue
        try:
            w3 = Web3(Web3.HTTPProvider(url, request_kwargs={"timeout": 10,
                      "headers": {"User-Agent": "deploy/1.0"}}))
            if w3.is_connected() and w3.eth.chain_id == w3_required_chain:
                return url
        except Exception:
            continue
    print("ERROR: no healthy Arbitrum RPC found")
    sys.exit(1)


def get_compiled_contract():
    json_path = ROOT / "out" / "FlashExecutorV3.sol" / "FlashExecutorV3.json"
    if not json_path.exists():
        import subprocess
        print("Compiling (forge build)...")
        r = subprocess.run(["forge", "build", "--contracts", "contracts/FlashExecutorV3.sol"],
                           cwd=ROOT, capture_output=True, text=True)
        if r.returncode != 0:
            print(f"forge build failed:\n{r.stderr}"); sys.exit(1)
    artifact = json.loads(json_path.read_text())
    # Sanity: the new method must be present in the ABI we're about to deploy.
    names = {f.get("name") for f in artifact["abi"] if f.get("type") == "function"}
    if "executeLiquidationBatch" not in names:
        print("ERROR: compiled ABI lacks executeLiquidationBatch — recompile."); sys.exit(1)
    return artifact["abi"], artifact["bytecode"]["object"]


def main():
    send = "--send" in sys.argv
    key = os.getenv("BOT_PRIVATE_KEY")
    if not key:
        print("ERROR: BOT_PRIVATE_KEY not set"); sys.exit(1)

    rpc = pick_rpc()
    w3 = Web3(Web3.HTTPProvider(rpc, request_kwargs={"timeout": 30,
              "headers": {"User-Agent": "deploy/1.0"}}))
    acct = w3.eth.account.from_key(key)
    bal = w3.eth.get_balance(acct.address)
    abi, bytecode = get_compiled_contract()

    print(f"RPC:       {rpc.split('//')[1].split('.')[0]}")
    print(f"Deployer:  {acct.address}")
    print(f"Balance:   {w3.from_wei(bal,'ether'):.6f} ETH")
    print(f"Mode:      {'SEND (broadcast)' if send else 'DRY RUN (estimate only)'}")

    C = w3.eth.contract(abi=abi, bytecode=bytecode)
    ctor = C.constructor(BALANCER_VAULT, AAVE_POOL, MIN_PROFIT_THRESHOLD_WEI)

    # Gas + cost estimate.
    try:
        gas_est = ctor.estimate_gas({"from": acct.address})
    except Exception as e:
        print(f"WARN: estimate_gas failed ({e}); using 3,000,000 fallback")
        gas_est = 3_000_000
    gas = int(gas_est * 1.2)
    base = w3.eth.get_block("latest").get("baseFeePerGas", w3.to_wei("0.01", "gwei"))
    max_fee = int(base * 2) + w3.to_wei("0.02", "gwei")
    cost_wei = gas * max_fee
    print(f"Gas est:   {gas_est:,} (+20% buffer → {gas:,})")
    print(f"Max fee:   {w3.from_wei(max_fee,'gwei'):.4f} gwei")
    print(f"Max cost:  {w3.from_wei(cost_wei,'ether'):.6f} ETH")

    if not send:
        print("\nDRY RUN — nothing broadcast. Re-run with --send to deploy.")
        print("After deploy: set FLASH_EXECUTOR_V3=<addr> in .env, then re-wire pipeline.")
        return

    if bal < cost_wei:
        print(f"ERROR: balance {w3.from_wei(bal,'ether'):.6f} < max cost "
              f"{w3.from_wei(cost_wei,'ether'):.6f} ETH"); sys.exit(1)

    nonce = w3.eth.get_transaction_count(acct.address)
    tx = ctor.build_transaction({
        "from": acct.address, "nonce": nonce, "gas": gas,
        "maxFeePerGas": max_fee, "maxPriorityFeePerGas": w3.to_wei("0.02", "gwei"),
        "chainId": ARBITRUM_CHAIN_ID, "type": 2,
    })
    signed = w3.eth.account.sign_transaction(tx, key)
    txh = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"\nDeploy tx: {txh.hex()}  — waiting for receipt...")
    rcpt = w3.eth.wait_for_transaction_receipt(txh, timeout=180)
    if rcpt["status"] != 1:
        print(f"❌ DEPLOY FAILED: {rcpt}"); sys.exit(1)
    addr = rcpt["contractAddress"]
    print(f"✅ Deployed FlashExecutorV3 (batch) at {addr}  block={rcpt['blockNumber']} gas={rcpt['gasUsed']:,}")

    # Post-deploy: approve routers.
    inst = w3.eth.contract(address=addr, abi=abi)
    nonce += 1
    for name, raddr in ROUTERS.items():
        rtx = inst.functions.approveRouter(Web3.to_checksum_address(raddr)).build_transaction({
            "from": acct.address, "nonce": nonce, "gas": 80_000,
            "maxFeePerGas": max_fee, "maxPriorityFeePerGas": w3.to_wei("0.02", "gwei"),
            "chainId": ARBITRUM_CHAIN_ID, "type": 2,
        })
        s = w3.eth.account.sign_transaction(rtx, key)
        h = w3.eth.send_raw_transaction(s.raw_transaction)
        w3.eth.wait_for_transaction_receipt(h, timeout=120)
        print(f"   approved router {name}: {raddr}")
        nonce += 1

    record = {
        "contract": "FlashExecutorV3", "variant": "batch",
        "address": addr, "deployer": acct.address, "block": rcpt["blockNumber"],
        "tx_hash": txh.hex(), "balancer_vault": BALANCER_VAULT, "aave_pool": AAVE_POOL,
        "min_profit_threshold_wei": str(MIN_PROFIT_THRESHOLD_WEI),
        "chain_id": ARBITRUM_CHAIN_ID, "routers": ROUTERS,
        "deployed_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "has_executeLiquidationBatch": True,
    }
    out = ROOT / "deployments" / "FlashExecutorV3_batch.json"
    out.write_text(json.dumps(record, indent=2))
    print(f"\nDeployment record: {out}")
    print("NEXT: set FLASH_EXECUTOR_V3=%s in .env, wire batch_liquidation_builder into "
          "pipeline_v3._execute_liquidation, dry-run shadow, then enable." % addr)


if __name__ == "__main__":
    main()
