"""
scripts/deploy_v3.py — Deploy FlashExecutorV3 to Arbitrum Mainnet.

Usage:
    export BOT_PRIVATE_KEY=0x...
    export ARBITRUM_HTTP_URL=https://arb-mainnet.g.alchemy.com/v2/...
    python3 scripts/deploy_v3.py

Requirements:
    pip install web3 python-dotenv
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from web3 import Web3
from dotenv import load_dotenv

# ─── Config ─────────────────────────────────────────────────
ARBITRUM_CHAIN_ID = 42161
BALANCER_VAULT = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"
AAVE_POOL = "0x794a61358D6845594F94dc1DB02A252b5b4814aD"
MIN_PROFIT_THRESHOLD_WEI = 50_000_000_000_000_000  # $50 @ $1000 ETH = 0.05 ETH

# ─── Contract Bytecode (to be populated by forge build) ─────
# After running `forge build`, read the compiled output from out/

def get_compiled_contract():
    """Read FlashExecutorV3 compilation output."""
    out_dir = Path(__file__).parent.parent / "out" / "FlashExecutorV3.sol"
    
    # Try standard Forge output path
    json_path = out_dir / "FlashExecutorV3.json"
    if not json_path.exists():
        # Fallback: auto-compile
        print("Contract not compiled. Running forge build...")
        import subprocess
        result = subprocess.run(
            ["forge", "build", "--contracts", "contracts/FlashExecutorV3.sol"],
            cwd=Path(__file__).parent.parent,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print(f"Forge build failed:\n{result.stderr}")
            sys.exit(1)
    
    if not json_path.exists():
        print(f"Could not find compiled contract at {json_path}")
        print("Run: forge build")
        sys.exit(1)
    
    with open(json_path) as f:
        artifact = json.load(f)
    
    return artifact["abi"], artifact["bytecode"]["object"]


def deploy():
    load_dotenv()
    
    private_key = os.getenv("BOT_PRIVATE_KEY")
    if not private_key:
        print("ERROR: BOT_PRIVATE_KEY not set in environment")
        sys.exit(1)
    
    rpc_url = os.getenv("ARBITRUM_HTTP_URL") or os.getenv("ALCHEMY_HTTP_URL")
    if not rpc_url:
        print("ERROR: ARBITRUM_HTTP_URL or ALCHEMY_HTTP_URL not set")
        sys.exit(1)
    
    w3 = Web3(Web3.HTTPProvider(rpc_url))
    if not w3.is_connected():
        print("ERROR: Cannot connect to RPC")
        sys.exit(1)
    
    # Verify we're on Arbitrum
    chain_id = w3.eth.chain_id
    if chain_id != ARBITRUM_CHAIN_ID:
        print(f"WARNING: Connected to chain {chain_id}, expected Arbitrum ({ARBITRUM_CHAIN_ID})")
        response = input("Continue anyway? (y/N): ")
        if response.lower() != 'y':
            sys.exit(1)
    
    account = w3.eth.account.from_key(private_key)
    print(f"Deployer: {account.address}")
    
    balance = w3.eth.get_balance(account.address)
    balance_eth = w3.from_wei(balance, 'ether')
    print(f"Balance: {balance_eth:.6f} ETH")
    
    if balance < w3.to_wei(0.001, 'ether'):
        print("ERROR: Insufficient balance for deployment (need ~0.001 ETH)")
        sys.exit(1)
    
    abi, bytecode = get_compiled_contract()
    
    # Constructor: (address _balancerVault, address _aavePool, uint256 _minProfitThreshold)
    FlashExecutorV3 = w3.eth.contract(abi=abi, bytecode=bytecode)
    
    # Build transaction
    construct_txn = FlashExecutorV3.constructor(
        BALANCER_VAULT,
        AAVE_POOL,
        MIN_PROFIT_THRESHOLD_WEI,
    ).build_transaction({
        'from': account.address,
        'nonce': w3.eth.get_transaction_count(account.address),
        'gas': 3_000_000,
        'maxFeePerGas': w3.to_wei('0.5', 'gwei'),
        'maxPriorityFeePerGas': w3.to_wei('0.05', 'gwei'),
        'chainId': chain_id,
        'type': 2,  # EIP-1559
    })
    
    # Sign and send
    signed_txn = w3.eth.account.sign_transaction(construct_txn, private_key)
    print(f"Deploying FlashExecutorV3 (balancer={BALANCER_VAULT}, minProfit={MIN_PROFIT_THRESHOLD_WEI})...")
    
    tx_hash = w3.eth.send_raw_transaction(signed_txn.rawTransaction)
    print(f"Transaction sent: {tx_hash.hex()}")
    print("Waiting for confirmation...")
    
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
    
    if receipt['status'] == 1:
        contract_address = receipt['contractAddress']
        print(f"\n✅ DEPLOYMENT SUCCESSFUL")
        print(f"Contract address: {contract_address}")
        print(f"Block: {receipt['blockNumber']}")
        print(f"Gas used: {receipt['gasUsed']}")
        print(f"\nNEXT STEPS:")
        print(f"1. Save address to .env: FLASH_EXECUTOR_V3={contract_address}")
        print(f"2. Approve routers: python3 scripts/approve_routers.py")
        print(f"3. Fund with ETH for gas (if needed)")
        print(f"4. Start live executor: python3 scripts/live_executor.py")
        
        # Save deployment info
        deploy_info = {
            "contract": "FlashExecutorV3",
            "address": contract_address,
            "deployer": account.address,
            "block": receipt['blockNumber'],
            "tx_hash": tx_hash.hex(),
            "balancer_vault": BALANCER_VAULT,
            "aave_pool": AAVE_POOL,
            "min_profit_threshold_wei": str(MIN_PROFIT_THRESHOLD_WEI),
            "chain_id": chain_id,
        }
        
        deploy_file = Path(__file__).parent.parent / "deployments" / "FlashExecutorV3.json"
        deploy_file.parent.mkdir(exist_ok=True)
        with open(deploy_file, "w") as f:
            json.dump(deploy_info, f, indent=2)
        print(f"\nDeployment info saved to: {deploy_file}")
        
        return contract_address
    else:
        print(f"\n❌ DEPLOYMENT FAILED")
        print(f"Status: {receipt['status']}")
        print(f"Receipt: {receipt}")
        sys.exit(1)


if __name__ == "__main__":
    deploy()
