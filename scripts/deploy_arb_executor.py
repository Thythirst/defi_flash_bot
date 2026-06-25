"""
scripts/deploy_arb_executor.py — Deploy ArbExecutor to Arbitrum mainnet.

Steps performed:
  1. Deploy ArbExecutor(AAVE_POOL)
  2. approveRouter(CAMELOT_ROUTER)
  3. approveRouter(UNIV3_ROUTER)
  4. Print the deployed address (paste into .env as ARB_EXECUTOR_ADDR)

Usage:
    cd ~/defi_flash_bot
    venv/bin/python scripts/deploy_arb_executor.py
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from web3 import Web3

load_dotenv()

# ── Constants ────────────────────────────────────────────────────────────────

ARBITRUM_CHAIN_ID = 42161
AAVE_POOL         = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
CAMELOT_ROUTER    = "0x1F721E2E82F6676FCE4eA07A5958cF098D339e18"
UNIV3_ROUTER      = "0xE592427A0AEce92De3Edee1F18E0157C05861564"

ARTIFACT_PATH = (
    Path(__file__).parent.parent / "out" / "ArbExecutor.sol" / "ArbExecutor.json"
)


def load_artifact():
    with open(ARTIFACT_PATH) as f:
        art = json.load(f)
    abi      = art["abi"]
    bytecode = art["bytecode"]["object"]
    if not bytecode.startswith("0x"):
        bytecode = "0x" + bytecode
    return abi, bytecode


def wait_receipt(w3: Web3, tx_hash: str, label: str) -> dict:
    from web3.exceptions import TransactionNotFound
    print(f"  waiting for {label} ({tx_hash[:12]}…)", end="", flush=True)
    for _ in range(60):
        try:
            receipt = w3.eth.get_transaction_receipt(tx_hash)
        except TransactionNotFound:
            receipt = None
        if receipt:
            status = "ok" if receipt["status"] == 1 else "FAILED"
            print(f" {status} (block {receipt['blockNumber']})")
            if receipt["status"] != 1:
                sys.exit(f"Transaction {label} REVERTED")
            return receipt
        time.sleep(2)
        print(".", end="", flush=True)
    sys.exit(f"Timeout waiting for {label}")


def main():
    private_key = os.getenv("BOT_PRIVATE_KEY")
    if not private_key:
        sys.exit("ERROR: BOT_PRIVATE_KEY not set")

    rpc_url = os.getenv("ARBITRUM_HTTP_URL") or os.getenv("ALCHEMY_HTTP_URL")
    if not rpc_url:
        sys.exit("ERROR: ARBITRUM_HTTP_URL not set")

    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        sys.exit(f"ERROR: Cannot connect to {rpc_url}")

    account = w3.eth.account.from_key(private_key)
    balance = w3.eth.get_balance(account.address)
    print(f"Deployer : {account.address}")
    print(f"Balance  : {w3.from_wei(balance, 'ether'):.6f} ETH")
    print(f"Chain    : {w3.eth.chain_id}")

    if w3.eth.chain_id != ARBITRUM_CHAIN_ID:
        sys.exit(f"ERROR: expected chain 42161, got {w3.eth.chain_id}")

    abi, bytecode = load_artifact()
    print(f"Artifact : {ARTIFACT_PATH.name} ({len(bytecode)//2} bytes)")

    # ── Deploy ────────────────────────────────────────────────────────────────
    print("\n[1/3] Deploying ArbExecutor…")
    contract = w3.eth.contract(abi=abi, bytecode=bytecode)

    base_fee = w3.eth.get_block("latest")["baseFeePerGas"]
    max_fee  = base_fee * 3
    priority = min(int(0.1e9), base_fee)  # 0.1 gwei or base_fee, whichever smaller

    deploy_tx = contract.constructor(
        Web3.to_checksum_address(AAVE_POOL)
    ).build_transaction({
        "from":                 account.address,
        "nonce":                w3.eth.get_transaction_count(account.address),
        "maxFeePerGas":         max_fee,
        "maxPriorityFeePerGas": priority,
        "chainId":              ARBITRUM_CHAIN_ID,
    })

    signed = account.sign_transaction(deploy_tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction).hex()
    receipt = wait_receipt(w3, tx_hash, "deploy")
    executor_address = receipt["contractAddress"]
    print(f"  Deployed: {executor_address}")

    # ── approveRouter: Camelot ────────────────────────────────────────────────
    print("\n[2/3] approveRouter(CAMELOT_ROUTER)…")
    deployed = w3.eth.contract(
        address=Web3.to_checksum_address(executor_address), abi=abi
    )
    nonce = w3.eth.get_transaction_count(account.address)
    base_fee = w3.eth.get_block("latest")["baseFeePerGas"]
    tx = deployed.functions.approveRouter(
        Web3.to_checksum_address(CAMELOT_ROUTER)
    ).build_transaction({
        "from":                 account.address,
        "nonce":                nonce,
        "maxFeePerGas":         base_fee * 3,
        "maxPriorityFeePerGas": priority,
        "chainId":              ARBITRUM_CHAIN_ID,
    })
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction).hex()
    wait_receipt(w3, tx_hash, "approveRouter(Camelot)")

    # ── approveRouter: UniV3 ──────────────────────────────────────────────────
    print("\n[3/3] approveRouter(UNIV3_ROUTER)…")
    nonce = w3.eth.get_transaction_count(account.address)
    base_fee = w3.eth.get_block("latest")["baseFeePerGas"]
    tx = deployed.functions.approveRouter(
        Web3.to_checksum_address(UNIV3_ROUTER)
    ).build_transaction({
        "from":                 account.address,
        "nonce":                nonce,
        "maxFeePerGas":         base_fee * 3,
        "maxPriorityFeePerGas": priority,
        "chainId":              ARBITRUM_CHAIN_ID,
    })
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction).hex()
    wait_receipt(w3, tx_hash, "approveRouter(UniV3)")

    # ── Done ──────────────────────────────────────────────────────────────────
    print(f"""
Done. Add this to .env:

  ARB_EXECUTOR_ADDR={executor_address}

Then verify on Arbiscan:
  https://arbiscan.io/address/{executor_address}
""")


if __name__ == "__main__":
    main()
