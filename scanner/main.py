#!/usr/bin/env python3
"""Arbitrum flash-loan arbitrage scanner — production grade v2."""

from __future__ import annotations

import asyncio
import argparse
import json
import logging
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml
from aiohttp import ClientError, ClientSession, ClientTimeout
from dotenv import load_dotenv
from eth_account import Account
from eth_account.datastructures import SignedTransaction
from eth_utils import to_checksum_address
from web3 import AsyncWeb3, Web3
from web3.providers import AsyncWebSocketProvider, HTTPProvider
from web3.exceptions import ContractLogicError, TransactionNotFound

from gas import ArbitrumGasEstimator
from pairs import Pair, PairRegistry, RoutePlan

# ─── Logging ───────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("sentinel")

# ─── Env / Config ────────────────────────────────────────────────
load_dotenv()

ARBITRUM_WSS_URL = os.getenv("ARBITRUM_WSS_URL")
ARBITRUM_HTTP_URL = os.getenv("ARBITRUM_HTTP_URL")
PRIVATE_RPC_URL = os.getenv("PRIVATE_RPC_URL")          # e.g. Flashbots Protect / bloXroute
FLASH_EXECUTOR_ADDRESS = os.getenv("FLASH_EXECUTOR_ADDRESS")
AAVE_POOL_ADDRESS = os.getenv("AAVE_POOL_ADDRESS", "0x794a61358D6845594F94dc1DB02A252b5b4814aD")

ENCRYPTED_KEYSTORE_PATH = os.getenv("KEYSTORE_PATH")
KEYSTORE_PASSWORD = os.getenv("KEYSTORE_PASSWORD")

PROFIT_THRESHOLD_WEI = int(Decimal("0.001") * Decimal(10**18))  # 0.001 ETH
SLIPPAGE_BPS = 50  # 0.5%
BPS_DENOMINATOR = 10000
RECONNECT_MAX_TRIES = 10
RECONNECT_BASE_DELAY = 1.0
CHAIN_ID = 42161

# ─── Minimal DEX Router ABI (V2 style) ────────────────────────────
ROUTER_ABI = [
    {
        "inputs": [
            {"internalType": "uint256", "name": "amountIn", "type": "uint256"},
            {"internalType": "address[]", "name": "path", "type": "address[]"},
        ],
        "name": "getAmountsOut",
        "outputs": [{"internalType": "uint256[]", "name": "amounts", "type": "uint256[]"}],
        "stateMutability": "view",
        "type": "function",
    },
    {
        "inputs": [
            {"internalType": "uint256", "name": "amountIn", "type": "uint256"},
            {"internalType": "uint256", "name": "amountOutMin", "type": "uint256"},
            {"internalType": "address[]", "name": "path", "type": "address[]"},
            {"internalType": "address", "name": "to", "type": "address"},
            {"internalType": "uint256", "name": "deadline", "type": "uint256"},
        ],
        "name": "swapExactTokensForTokens",
        "outputs": [{"internalType": "uint256[]", "name": "amounts", "type": "uint256[]"}],
        "stateMutability": "nonpayable",
        "type": "function",
    },
]

# ─── Secure Signer ─────────────────────────────────────────────────
class SecureSigner:
    """Loads an encrypted keystore (EIP-Scrypt / PBKDF2).
    WARNING: Decrypted key lives in process memory. Migrate to AWS KMS / HSM before mainnet.
    """

    def __init__(self, keystore_path: str, password: str):
        self._account = self._load_keystore(keystore_path, password)
        self.address = self._account.address
        logger.info("Signer loaded for address %s", self.address)

    @staticmethod
    def _load_keystore(path: str, password: str) -> Account:
        keystore_file = Path(path).expanduser()
        if not keystore_file.exists():
            raise FileNotFoundError(f"Keystore not found: {path}")
        with open(keystore_file, "r", encoding="utf-8") as f:
            keystore = json.load(f)
        private_key = Account.decrypt(keystore, password)
        return Account.from_key(private_key)

    def sign_transaction(self, tx_dict: dict) -> SignedTransaction:
        return self._account.sign_transaction(tx_dict)


# ─── Opportunity Dataclass ──────────────────────────────────────
@dataclass
class Opportunity:
    asset: str
    amount: int
    path: List[str]
    dex_routers: List[str]
    min_out: int
    profit_raw: int
    estimated_gas_cost: int
    block_number: int
    block_hash: str


# ─── Arbitrage Scanner ─────────────────────────────────────────────
class ArbitrageScanner:
    def __init__(
        self,
        signer: SecureSigner,
        pair_registry: PairRegistry,
        gas_estimator: ArbitrumGasEstimator,
        dry_run: bool = False,
    ):
        self.signer = signer
        self.pair_registry = pair_registry
        self.gas_estimator = gas_estimator
        self.dry_run = dry_run

        self.w3: Optional[AsyncWeb3] = None
        self.http_w3: Optional[Web3] = None
        self._shutdown = False
        self._latest_block_number = 0
        self._latest_block_hash = ""

        # Load FlashExecutor ABI
        abi_path = Path(__file__).parent / "abis" / "flash_executor.json"
        with open(abi_path, "r") as f:
            self._executor_abi = json.load(f)

    # ─── Connection ──────────────────────────────────────────────
    async def _get_http_w3(self) -> Web3:
        if self.http_w3 is None:
            self.http_w3 = Web3(HTTPProvider(ARBITRUM_HTTP_URL))
        return self.http_w3

    async def _establish_ws(self) -> AsyncWeb3:
        attempt = 0
        while attempt < RECONNECT_MAX_TRIES and not self._shutdown:
            try:
                w3 = AsyncWeb3(AsyncWebSocketProvider(ARBITRUM_WSS_URL))
                connected = await w3.is_connected()
                if connected:
                    logger.info("WebSocket connected (attempt %d)", attempt + 1)
                    return w3
            except Exception as exc:
                attempt += 1
                delay = min(RECONNECT_BASE_DELAY * (2**attempt), 60.0)
                logger.warning("WS connect error (%s). Retrying in %.1fs...", exc, delay)
                await asyncio.sleep(delay)
        raise ConnectionError("Max WebSocket reconnection attempts exceeded")

    # ─── Stale-State Validation ─────────────────────────────────────
    async def _is_stale(self, opp: Opportunity) -> bool:
        if self.w3 is None:
            return True
        current_block = await self.w3.eth.block_number
        if current_block > opp.block_number + 2:
            logger.warning("Stale opp: block drift %d", current_block - opp.block_number)
            return True

        # Re-simulate first leg to detect price drift
        router = self.w3.eth.contract(
            address=to_checksum_address(opp.dex_routers[0]),
            abi=ROUTER_ABI,
        )
        try:
            fresh = await router.functions.getAmountsOut(
                opp.amount,
                opp.path[:2],
            ).call(block_identifier="latest")
        except Exception as exc:
            logger.warning("Stale check failed: %s", exc)
            return True

        if fresh[1] < int(opp.min_out * 0.999):
            logger.warning("Price drifted beyond 0.1%% tolerance")
            return True
        return False

    # ─── Execution ──────────────────────────────────────────────
    async def _send_flash_loan(self, opp: Opportunity) -> Optional[str]:
        if self.w3 is None:
            return None
        if not FLASH_EXECUTOR_ADDRESS:
            logger.error("FLASH_EXECUTOR_ADDRESS not set")
            return None

        flash_executor = self.w3.eth.contract(
            address=to_checksum_address(FLASH_EXECUTOR_ADDRESS),
            abi=self._executor_abi,
        )

        # Build Route struct tuple: (address[] path, uint256 minOut, address[] dexRouters)
        route_tuple = (opp.path, opp.min_out, opp.dex_routers)

        tx = flash_executor.functions.executeFlashLoan(
            opp.asset,
            opp.amount,
            route_tuple,
        ).build_transaction({
            "from": self.signer.address,
            "nonce": await self.w3.eth.get_transaction_count(self.signer.address, "pending"),
            "gasPrice": await self.w3.eth.gas_price,
            "chainId": CHAIN_ID,
        })

        # Estimate real Arbitrum gas cost (L2 + L1)
        try:
            total_cost = await self.gas_estimator.estimate_total_cost(self.w3, tx)
            tx["gas"] = int(await self.w3.eth.estimate_gas(tx) * 1.25)  # 25% headroom
        except ContractLogicError as exc:
            logger.error("Simulation revert: %s", exc)
            return None
        except Exception as exc:
            logger.error("Gas estimation failed: %s", exc)
            return None

        # Final profitability gate after gas
        if opp.profit_raw <= total_cost + PROFIT_THRESHOLD_WEI:
            logger.info(
                "Opp rejected post-gas: profit=%d, gas_cost=%d",
                opp.profit_raw,
                total_cost,
            )
            return None

        # Stale check immediately before signing
        if await self._is_stale(opp):
            logger.info("Opp became stale before signing")
            return None

        if self.dry_run:
            logger.info("DRY_RUN: would broadcast tx for opp profit=%s wei", opp.profit_raw)
            return "dry_run"

        # Sign & broadcast
        signed = self.signer.sign_transaction(tx)
        provider = self.w3.provider
        # Prefer private mempool if configured
        if PRIVATE_RPC_URL:
            # Submit via private RPC (Flashbots Protect, bloXroute, etc.)
            private_w3 = Web3(HTTPProvider(PRIVATE_RPC_URL))
            try:
                tx_hash = private_w3.eth.send_raw_transaction(signed.raw_transaction)
                logger.info("Broadcast via private RPC: %s", tx_hash.hex())
            except Exception as exc:
                logger.error("Private RPC failed, falling back to public: %s", exc)
                tx_hash = await self.w3.eth.send_raw_transaction(signed.raw_transaction)
                logger.info("Broadcast via public RPC: %s", tx_hash.hex())
        else:
            tx_hash = await self.w3.eth.send_raw_transaction(signed.raw_transaction)
            logger.info("Broadcast tx: %s", tx_hash.hex())

        # Poll receipt
        for _ in range(60):
            await asyncio.sleep(0.5)
            try:
                receipt = await self.w3.eth.get_transaction_receipt(tx_hash)
                if receipt:
                    status = receipt.get("status")
                    logger.info(
                        "Receipt: block=%s gasUsed=%s status=%s",
                        receipt.get("blockNumber"),
                        receipt.get("gasUsed"),
                        status,
                    )
                    return tx_hash.hex()
            except TransactionNotFound:
                continue
            except Exception as exc:
                logger.warning("Receipt poll error: %s", exc)

        logger.warning("Tx not mined within timeout: %s", tx_hash.hex())
        return tx_hash.hex()

    # ─── Scanning Logic ────────────────────────────────────────────
    async def scan_opportunity(self, pair: Pair, route_plan: RoutePlan) -> Optional[Opportunity]:
        if self.w3 is None:
            return None

        # Build router contract instances
        router_a = self.w3.eth.contract(
            address=to_checksum_address(route_plan.routers[0]),
            abi=ROUTER_ABI,
        )
        router_b = self.w3.eth.contract(
            address=to_checksum_address(route_plan.routers[1]),
            abi=ROUTER_ABI,
        )

        try:
            # Leg 1: asset -> intermediate on router A
            leg1 = await router_a.functions.getAmountsOut(
                pair.amount_in,
                [pair.asset, pair.intermediate],
            ).call()

            # Leg 2: intermediate -> asset on router B
            leg2 = await router_b.functions.getAmountsOut(
                leg1[1],
                [pair.intermediate, pair.asset],
            ).call()
        except Exception as exc:
            logger.debug("Scan call failed: %s", exc)
            return None

        final_out = leg2[1]
        gross_profit = final_out - pair.amount_in

        if gross_profit < PROFIT_THRESHOLD_WEI:
            return None

        # Compute minOut with slippage on final output
        min_out = int(final_out * (BPS_DENOMINATOR - SLIPPAGE_BPS) // BPS_DENOMINATOR)

        # Estimate gas for FlashExecutor.executeFlashLoan
        flash_executor = self.w3.eth.contract(
            address=to_checksum_address(FLASH_EXECUTOR_ADDRESS),
            abi=self._executor_abi,
        )
        route_tuple = (
            [pair.asset, pair.intermediate, pair.asset],
            min_out,
            route_plan.routers,
        )
        dummy_tx = flash_executor.functions.executeFlashLoan(
            pair.asset,
            pair.amount_in,
            route_tuple,
        ).build_transaction({
            "from": self.signer.address,
            "nonce": 0,
            "gasPrice": 0,
            "chainId": CHAIN_ID,
        })

        try:
            gas_cost = await self.gas_estimator.estimate_total_cost(self.w3, dummy_tx)
        except Exception:
            gas_cost = 0

        net_profit = gross_profit - gas_cost
        if net_profit < PROFIT_THRESHOLD_WEI:
            return None

        return Opportunity(
            asset=pair.asset,
            amount=pair.amount_in,
            path=[pair.asset, pair.intermediate, pair.asset],
            dex_routers=route_plan.routers,
            min_out=min_out,
            profit_raw=net_profit,
            estimated_gas_cost=gas_cost,
            block_number=self._latest_block_number,
            block_hash=self._latest_block_hash,
        )

    # ─── Main Loop ──────────────────────────────────────────────
    async def run(self):
        if not ARBITRUM_WSS_URL:
            logger.error("ARBITRUM_WSS_URL not set")
            return

        while not self._shutdown:
            try:
                self.w3 = await self._establish_ws()
                sub_id = await self.w3.eth.subscribe("newHeads")
                logger.info("Subscribed to newHeads: %s", sub_id)

                async for response in self.w3.socket.process_subscriptions():
                    if self._shutdown:
                        break

                    result = response.get("result")
                    if not result:
                        continue

                    block_number = int(result["number"], 16)
                    block_hash = result["hash"]
                    self._latest_block_number = block_number
                    self._latest_block_hash = block_hash

                    logger.info("New block #%d", block_number)

                    # Scan all registered pairs
                    for pair in self.pair_registry.pairs:
                        for route_plan in self.pair_registry.get_route_plans(pair):
                            opp = await self.scan_opportunity(pair, route_plan)
                            if opp:
                                logger.info(
                                    "Opp found! pair=%s profit=%s wei gas=%s",
                                    pair.name,
                                    opp.profit_raw,
                                    opp.estimated_gas_cost,
                                )
                                await self._send_flash_loan(opp)

            except (ConnectionError, ClientError, asyncio.TimeoutError) as exc:
                logger.error("Connection dropped (%s). Reconnecting...", exc)
                await asyncio.sleep(RECONNECT_BASE_DELAY)
            except Exception as exc:
                logger.exception("Unhandled loop error: %s", exc)
                await asyncio.sleep(RECONNECT_BASE_DELAY)
            finally:
                if self.w3:
                    try:
                        await self.w3.provider.disconnect()
                    except Exception:
                        pass
                self.w3 = None

    def stop(self):
        self._shutdown = True


# ─── Entry Point ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Arbitrum Flash Loan Arbitrage Scanner")
    parser.add_argument("--mode", choices=["live", "backtest"], default="live")
    parser.add_argument("--pairs-config", default="pairs.yaml")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not ENCRYPTED_KEYSTORE_PATH or not KEYSTORE_PASSWORD:
        logger.error("KEYSTORE_PATH and KEYSTORE_PASSWORD required")
        sys.exit(1)

    signer = SecureSigner(ENCRYPTED_KEYSTORE_PATH, KEYSTORE_PASSWORD)
    registry = PairRegistry.from_yaml(args.pairs_config)
    gas_est = ArbitrumGasEstimator()

    if args.mode == "live":
        scanner = ArbitrageScanner(signer, registry, gas_est, dry_run=args.dry_run)
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, scanner.stop)
        try:
            loop.run_until_complete(scanner.run())
        finally:
            loop.close()
    else:
        from backtest import BacktestEngine
        engine = BacktestEngine(registry, gas_est)
        engine.run()


if __name__ == "__main__":
    main()
