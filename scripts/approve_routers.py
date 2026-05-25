"""
scripts/approve_routers.py — Approve DEX routers on deployed FlashExecutorV3.

Usage:
    export BOT_PRIVATE_KEY=0x...
    export FLASH_EXECUTOR_V3=0x...
    python3 scripts/approve_routers.py
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from web3 import Web3
from dotenv import load_dotenv

# ─── Known Arbitrum Router Addresses ────────────────────────
ROUTERS = {
    "uniswap_v3_swaprouter": "0xE592427A0AEce92De3Edee1F18E0157C05861564",
    "camelot_v3_nfpm": "0xB1026b8e7276e7AC75410F1fcbbe21796e8f7526",
    "camelot_v3_router": "0xf5F4496219F31DDb12b336056Fe74D0Bb8405239",
    "sushiswap_v3_router": "0x8A21F6768C1f8075791D08546Dadf6daA0bE16EC",
    "sushiswap_v2_router": "0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506",
}


def get_contract_abi():
    """Read compiled ABI from Forge output."""
    json_path = Path(__file__).parent.parent / "out" / "FlashExecutorV3.sol" / "FlashExecutorV3.json"
    with open(json_path) as f:
        artifact = json.load(f)
    return artifact["abi"]


def approve_routers():
    load_dotenv()
    
    private_key = os.getenv("BOT_PRIVATE_KEY")
    if not private_key:
        print("ERROR: BOT_PRIVATE_KEY not set")
        sys.exit(1)
    
    contract_address = os.getenv("FLASH_EXECUTOR_V3")
    if not contract_address:
        print("ERROR: FLASH_EXECUTOR_V3 not set")
        sys.exit(1)
    
    rpc_url = os.getenv("ARBITRUM_HTTP_URL") or os.getenv("ALCHEMY_HTTP_URL")
    if not rpc_url:
        print("ERROR: RPC URL not set")
        sys.exit(1)
    
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    account = w3.eth.account.from_key(private_key)
    
    abi = get_contract_abi()
    contract = w3.eth.contract(address=contract_address, abi=abi)
    
    print(f"Approving routers for FlashExecutorV3 at {contract_address}")
    print(f"Owner: {account.address}\n")
    
    nonce = w3.eth.get_transaction_count(account.address)
    
    for name, router_addr in ROUTERS.items():
        # Check if already approved
        is_approved = contract.functions.approvedRouters(router_addr).call()
        if is_approved:
            print(f"  ✓ {name}: {router_addr} (already approved)")
            continue
        
        tx = contract.functions.approveRouter(router_addr).build_transaction({
            'from': account.address,
            'nonce': nonce,
            'gas': 100_000,
            'maxFeePerGas': w3.to_wei('0.5', 'gwei'),
            'maxPriorityFeePerGas': w3.to_wei('0.05', 'gwei'),
            'chainId': w3.eth.chain_id,
            'type': 2,
        })
        
        signed = w3.eth.account.sign_transaction(tx, private_key)
        tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)
        print(f"  → {name}: {router_addr} | tx: {tx_hash.hex()}")
        
        receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
        if receipt['status'] == 1:
            print(f"    ✅ Approved (block {receipt['blockNumber']})")
        else:
            print(f"    ❌ FAILED")
        
        nonce += 1
    
    print("\nDone.")


if __name__ == "__main__":
    approve_routers()
