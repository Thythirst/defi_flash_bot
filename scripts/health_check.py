"""Contract health check."""
import os, json, sys
from pathlib import Path
from dotenv import load_dotenv
from web3 import Web3
from web3.providers.rpc import HTTPProvider

project_root = Path("/root/defi_flash_bot/prod")
load_dotenv(dotenv_path=project_root / ".env")

rpc = os.getenv("ALCHEMY_HTTP_URL")
executor = os.getenv("FLASH_EXECUTOR_V3")

w3 = Web3(HTTPProvider(rpc))

abi_path = project_root / "out" / "FlashExecutorV3.sol" / "FlashExecutorV3.json"
with open(abi_path) as f:
    abi = json.load(f)["abi"]

contract = w3.eth.contract(address=executor, abi=abi)
bal = w3.eth.get_balance(executor)
paused = contract.functions.paused().call()
owner = contract.functions.owner().call()
min_profit = contract.functions.minProfitThreshold().call()
deployer = "0x1269800101780229B50919e1e27be62DC6279e9B"
deployer_bal = w3.eth.get_balance(deployer)

print(f"Contract:   {executor}")
print(f"ETH:        {w3.from_wei(bal, 'ether'):.6f}")
print(f"Owner:      {owner}")
print(f"Paused:     {paused}")
print(f"Min profit: {w3.from_wei(min_profit, 'ether')} ETH")
print(f"Deployer:   {w3.from_wei(deployer_bal, 'ether'):.6f} ETH")
print()

routers = [
    ("UniV3",     "0xE592427A0AEce92De3Edee1F18E0157C05861564"),
    ("SushiV3",   "0x8A21F6768c1F8075791D08546dADF6daA0Be16eC"),
    ("CamelotV3", "0xf5f4496219F31dDB12b336056fE74D0bB8405239"),
    ("SushiV2",   "0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506"),
]
for name, addr in routers:
    approved = contract.functions.approvedRouters(addr).call()
    print(f"  {name:10s}: {'✅' if approved else '❌'}")

print(f"\nBlock: {w3.eth.block_number}")
print(f"Gas:   {w3.from_wei(w3.eth.gas_price, 'gwei'):.1f} gwei")
