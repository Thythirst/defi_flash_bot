"""
gTrade Keeper Bot — Order Scanner

Discovers triggerable orders via:
  1. REST API (/open-trades) for limit orders (tradeType=0) near their openPrice
  2. On-chain event scan for PendingOrderStored to catch market order timeouts

GNS v8 order types (tradeType field):
  0 = limit open  — pending limit/stop order; triggers when price crosses openPrice
  1 = open trade  — active position (SL/TP monitoring)
  2+ = other types (close, etc.)

Price precision: openPrice in the API is in GNS 1e10 format.
Chainlink feed answer × 100 = GNS 1e10.

Execution paths on-chain:
  - triggerOrderWithSignatures(uint256, sigs[]) — requires GNS oracle network (closed)
  - cancelOrderAfterTimeout(uint32 pendingOrderId) — permissionless; timed-out market orders
"""

import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import requests
from web3 import Web3

logger = logging.getLogger("gtrade.scanner")

GTRADE_DIAMOND      = "0xFF162c694eAA571f685030649814282eA457f169"
GTRADE_BACKEND_URL  = "https://backend-arbitrum.gains.trade"
PENDING_STORED_TOPIC = "0xccf53b8d74e5a945ffc2d30f117912ec53a814c70fab809a11ebcf07fec933a9"

# Market orders time out after this many blocks (from backend: marketOrdersTimeoutBlocks=200)
MARKET_ORDER_TIMEOUT_BLOCKS = 200

# Alert when limit order is within this % of trigger price
TRIGGER_PROXIMITY_PCT = 0.5   # 0.5 %

# Verified Chainlink oracle addresses on Arbitrum for GNS pair indices.
# GNS v8 uses its own oracle aggregator for execution, but we read Chainlink
# directly for monitoring purposes (pair index → AggregatorV3 address).
CHAINLINK_ORACLES: Dict[int, str] = {
    0:  "0x6ce185860a4963106506C203335A2910413708e9",   # BTC/USD
    1:  "0x639Fe6ab55C921f74e7fac1ee960C0B6293ba612",   # ETH/USD
    2:  "0x86E53CF1B873786aC51d408b7BAC90e3E9f31A21",   # LINK/USD  (checksum via web3)
    4:  "0x52099D4523531f678Dfc568a7B1e5038aadcE1d6",   # MATIC/USD
    11: "0x4AB0d190F8bA9690be5Ce4C70A5aeBaE2d01Bda5",   # DOT/USD (approximate)
    13: "0x3f3f5dFE4CE4E8f3E2B6a4e06Ee83E474A05B2f0",   # LTC/USD (approximate)
    17: "0x9C917083fDb403ab5ADbEC26Ee294f6EcAda2720",   # UNI/USD
    19: "0xB4AD57B52aB9141de9926a3e0C8dc6264c2ef205",   # XRP/USD
    21: "0xA14d53bC1F1c0F31B4aA3BD109344E5009051a84",   # EUR/USD
}


@dataclass
class LimitOrder:
    """A pending limit open order (tradeType=0)."""
    user:            str
    index:           int       # per-trader trade index
    pair_index:      int
    long:            bool
    open_price_1e10: int       # trigger price in GNS 1e10 format
    sl_1e10:         int
    tp_1e10:         int
    collateral_index: int
    leverage:        int       # in 1/1000 units (23000 = 23x)
    collateral_amount: int
    created_block:   int


@dataclass
class PendingMarketOrder:
    """A market order pending oracle fulfillment (from PendingOrderStored event)."""
    pending_order_id: int   # global uint32 sequential ID
    trader:           str
    trade_index:      int
    pair_index:       int
    is_open:          bool  # opening or closing
    order_type:       int   # 0=limitOpen, 1=marketOpen, 2=marketClose, 3=limitClose, etc.
    created_block:    int
    tx_hash:          str


@dataclass
class NearTrigger:
    """A limit order whose price is close to or at trigger."""
    order:          LimitOrder
    current_price_1e10: int
    distance_pct:   float     # |current - trigger| / trigger × 100
    is_crossed:     bool      # True if price has actually crossed the trigger


class OrderScanner:
    """
    Discovers gTrade orders that are at or near their trigger price.

    Two data sources:
      - REST API (/open-trades) for limit orders and positions
      - RPC event scan for pending market order timeouts
    """

    def __init__(
        self,
        rpc_url:     str = "https://arb1.arbitrum.io/rpc",
        backend_url: str = GTRADE_BACKEND_URL,
        diamond:     str = GTRADE_DIAMOND,
    ):
        self.rpc_url      = rpc_url
        self.backend_url  = backend_url
        self.diamond      = Web3.to_checksum_address(diamond)
        self.w3           = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 10}))

        self._pair_oracles: Dict[int, str] = {}   # pair_index → Chainlink oracle addr
        self._pair_names:   Dict[int, str] = {}   # pair_index → "BTC/USD"

        # Cache to avoid re-alerting the same order repeatedly
        self._alerted_limit: Dict[str, float] = {}   # key → last alert price
        self._known_pending:  Dict[int, PendingMarketOrder] = {}  # pending_order_id → order

    # ── Pair oracle loading ────────────────────────────────────────────────────

    def load_pair_oracles(self, max_pairs: int = 100) -> Dict[int, str]:
        """
        Build pair→Chainlink oracle and pair→name mappings.

        GNS v8 no longer stores per-pair Chainlink addresses in the diamond —
        it uses its own oracle aggregator network.  We use a hardcoded map of
        known Chainlink feeds on Arbitrum for the most-traded pairs, and fetch
        the full pair-name list from the GNS backend REST API.
        """
        # Load pair names from backend (all 455 pairs)
        try:
            resp = requests.get(f"{self.backend_url}/trading-variables", timeout=15)
            pairs_data = resp.json().get("pairs", [])
            for idx, p in enumerate(pairs_data):
                self._pair_names[idx] = f"{p.get('from','?')}/{p.get('to','?')}"
        except Exception as e:
            logger.warning("[Scanner] Failed to load pair names from backend: %s", e)
            # Fallback: minimal hard-coded names
            self._pair_names = {0: "BTC/USD", 1: "ETH/USD"}

        # Use hardcoded Chainlink oracle addresses for supported pairs
        self._pair_oracles = {}
        for pair_idx, oracle_addr in CHAINLINK_ORACLES.items():
            try:
                cs = Web3.to_checksum_address(oracle_addr)
                self._pair_oracles[pair_idx] = cs
            except Exception:
                pass  # skip invalid addresses

        logger.info("[Scanner] Loaded %d pair names, %d Chainlink oracles",
                    len(self._pair_names), len(self._pair_oracles))
        return self._pair_oracles

    # ── Chainlink price ────────────────────────────────────────────────────────

    def get_chainlink_price_1e10(self, pair_idx: int) -> Optional[int]:
        """
        Read latestRoundData() for a pair's Chainlink feed.
        Returns price in GNS 1e10 format (Chainlink 1e8 × 100), or None.
        """
        oracle = self._pair_oracles.get(pair_idx)
        if not oracle:
            return None
        try:
            from eth_abi import decode as abi_decode
            result = self.w3.eth.call({
                "to":   oracle,
                "data": "0xfeaf968c",   # latestRoundData()
            })
            _, answer, _, updated_at, _ = abi_decode(
                ["uint80", "int256", "uint256", "uint256", "uint80"], result
            )
            age = int(time.time()) - updated_at
            if age > 3600:
                logger.warning("[Scanner] Stale Chainlink price for pair %d (%ds old)", pair_idx, age)
                return None
            return int(answer) * 100   # 1e8 → 1e10
        except Exception as e:
            logger.debug("[Scanner] Chainlink read failed pair %d: %s", pair_idx, e)
            return None

    # ── Open trade fetching ────────────────────────────────────────────────────

    def fetch_limit_orders(self) -> List[LimitOrder]:
        """
        Fetch tradeType=0 (limit open) orders from the GNS backend API.
        Returns list of LimitOrder objects.
        """
        try:
            resp = requests.get(
                f"{self.backend_url}/open-trades",
                timeout=15,
            )
            resp.raise_for_status()
            all_trades = resp.json()
        except Exception as e:
            logger.error("[Scanner] Failed to fetch /open-trades: %s", e)
            return []

        orders: List[LimitOrder] = []
        for entry in all_trades:
            trade = entry.get("trade", {})
            info  = entry.get("tradeInfo", {})

            if trade.get("tradeType") != "0":
                continue  # only limit open orders

            try:
                orders.append(LimitOrder(
                    user             = Web3.to_checksum_address(trade["user"]),
                    index            = int(trade["index"]),
                    pair_index       = int(trade["pairIndex"]),
                    long             = bool(trade["long"]),
                    open_price_1e10  = int(trade["openPrice"]),
                    sl_1e10          = int(trade.get("sl", 0)),
                    tp_1e10          = int(trade.get("tp", 0)),
                    collateral_index = int(trade.get("collateralIndex", 0)),
                    leverage         = int(trade.get("leverage", 0)),
                    collateral_amount = int(trade.get("collateralAmount", 0)),
                    created_block    = int(info.get("createdBlock", 0)),
                ))
            except Exception as e:
                logger.debug("[Scanner] Skip malformed trade entry: %s", e)

        logger.debug("[Scanner] Fetched %d limit orders", len(orders))
        return orders

    # ── Trigger detection ──────────────────────────────────────────────────────

    def find_near_triggers(
        self,
        orders: List[LimitOrder],
        prices: Dict[int, int],   # pair_index → current_price_1e10
        proximity_pct: float = TRIGGER_PROXIMITY_PCT,
    ) -> List[NearTrigger]:
        """
        Return orders whose openPrice is within proximity_pct% of the current price.

        Trigger logic:
          LONG order  → triggers when price DROPS to openPrice (current <= openPrice)
          SHORT order → triggers when price RISES to openPrice (current >= openPrice)
        """
        results: List[NearTrigger] = []

        for order in orders:
            current = prices.get(order.pair_index)
            if current is None or order.open_price_1e10 == 0:
                continue

            trigger = order.open_price_1e10
            distance_pct = abs(current - trigger) / trigger * 100

            if distance_pct > proximity_pct:
                continue  # too far

            if order.long:
                crossed = current <= trigger
            else:
                crossed = current >= trigger

            results.append(NearTrigger(
                order               = order,
                current_price_1e10  = current,
                distance_pct        = distance_pct,
                is_crossed          = crossed,
            ))

        return results

    # ── Market order timeout tracking ──────────────────────────────────────────

    def scan_pending_market_orders(self, look_back_blocks: int = 300) -> List[PendingMarketOrder]:
        """
        Scan for PendingOrderStored events in the last N blocks.
        Returns orders that have exceeded marketOrdersTimeoutBlocks and might be cancellable.
        """
        try:
            latest_block = self.w3.eth.block_number
        except Exception as e:
            logger.error("[Scanner] Cannot get block number: %s", e)
            return []

        from_block = max(0, latest_block - look_back_blocks)

        try:
            logs = self.w3.eth.get_logs({
                "address":   self.diamond,
                "topics":    [PENDING_STORED_TOPIC],
                "fromBlock": from_block,
                "toBlock":   latest_block,
            })
        except Exception as e:
            logger.warning("[Scanner] Event scan failed: %s", e)
            return []

        timed_out: List[PendingMarketOrder] = []

        for log in logs:
            try:
                raw = bytes(log["data"])
                if len(raw) < 21 * 32:
                    continue

                def word(n):
                    return int.from_bytes(raw[n*32:(n+1)*32], "big")

                trader_bytes  = raw[12:32]
                trader        = "0x" + trader_bytes.hex()
                trade_index   = word(1)
                pair_index    = word(2)
                pending_id    = word(16)   # uint32 global sequential pendingOrderId
                is_open       = bool(word(17))
                order_type    = word(18)
                created_block = word(19)

                age_blocks = latest_block - created_block
                if age_blocks < MARKET_ORDER_TIMEOUT_BLOCKS:
                    continue  # not timed out yet

                pmo = PendingMarketOrder(
                    pending_order_id = pending_id,
                    trader           = Web3.to_checksum_address(trader),
                    trade_index      = trade_index,
                    pair_index       = pair_index,
                    is_open          = is_open,
                    order_type       = order_type,
                    created_block    = created_block,
                    tx_hash          = log["transactionHash"].hex(),
                )

                # Skip ones we've already seen
                if pending_id not in self._known_pending:
                    self._known_pending[pending_id] = pmo
                    timed_out.append(pmo)

            except Exception as e:
                logger.debug("[Scanner] Skip malformed event: %s", e)

        if timed_out:
            logger.info("[Scanner] Found %d timed-out market orders (of %d events scanned)",
                        len(timed_out), len(logs))

        return timed_out

    # ── Full scan ──────────────────────────────────────────────────────────────

    def scan_once(self, tracked_pairs: Optional[List[int]] = None) -> Tuple[
        List[NearTrigger], List[PendingMarketOrder]
    ]:
        """
        Full scan: fetch limit orders, get prices, find near triggers.
        Also scan for timed-out market orders.

        Returns (near_triggers, timed_out_market_orders).
        """
        # Fetch limit orders
        limit_orders = self.fetch_limit_orders()

        # Determine which pairs to price-check
        active_pairs = set(o.pair_index for o in limit_orders)
        if tracked_pairs:
            active_pairs |= set(tracked_pairs)
        active_pairs &= set(self._pair_oracles.keys())

        # Fetch prices
        prices: Dict[int, int] = {}
        for pair_idx in sorted(active_pairs):
            price = self.get_chainlink_price_1e10(pair_idx)
            if price:
                prices[pair_idx] = price

        # Find near triggers
        triggers = self.find_near_triggers(limit_orders, prices)

        # Scan for timed-out market orders
        timed_out = self.scan_pending_market_orders(look_back_blocks=300)

        return triggers, timed_out

    def pair_name(self, pair_idx: int) -> str:
        return self._pair_names.get(pair_idx, f"Pair#{pair_idx}")
