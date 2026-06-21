"""Approve Camelot V3 router on FlashExecutorV3."""
import os, sys, json
from pathlib import Path
from dotenv import load_dotenv
from web3 import Web3

project_root = Path("/home/ubuntu/defi_flash_bot")
load_dotenv(dotenv_path=project_root / ".env")

rpc_url = os.getenv("ALCHEMY_HTTP_URL") or os.getenv("ARBITRUM_HTTP_URL")
private_key = os.getenv("BOT_PRIVATE_KEY")
executor_addr = os.getenv("FLASH_EXECUTOR_V3")
camelot_v3 = "0x1F721E2E82F6676FCE4eA07A5958cF098D339e18"

with open(project_root / "out" / "FlashExecutorV3.sol" / "FlashExecutorV3.json") as f:
    build = json.load(f)

w3 = Web3(Web3.HTTPProvider(rpc_url))
account = w3.eth.account.from_key(private_key)
contract = w3.eth.contract(address=w3.to_checksum_address(executor_addr), abi=build["abi"])

already_approved = contract.functions.approvedRouters(camelot_v3).call()
print(f"Already approved: {already_approved}")

if not already_approved:
    print("Approving Camelot V3 router...")
    tx = contract.functions.approveRouter(camelot_v3).build_transaction({
        "from": account.address,
        "nonce": w3.eth.get_transaction_count(account.address),
        "gas": 100000,
        "gasPrice": w3.eth.gas_price,
    })
    signed = account.sign_transaction(tx)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction)
    print(f"Tx sent: {tx_hash.hex()}")
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=60)
    print(f"Status: {'OK' if receipt['status'] else 'FAILED'}")
    approved = contract.functions.approvedRouters(camelot_v3).call()
    print(f"Verified: {approved}")
else:
    print("No action needed.")
