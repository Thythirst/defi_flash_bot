from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import yaml


@dataclass
class Pair:
    name: str
    asset: str               # Token we flash-loan (e.g. WETH)
    intermediate: str        # Token we arb through (e.g. USDC)
    amount_in: int           # Loan size in wei
    decimals: int = 18


@dataclass
class RoutePlan:
    routers: List[str]       # [buy_router, sell_router]
    description: str = ""


class PairRegistry:
    def __init__(self, pairs: List[Pair]):
        self.pairs = pairs

    @classmethod
    def from_yaml(cls, path: str) -> "PairRegistry":
        data = yaml.safe_load(Path(path).read_text())
        pairs = []
        for item in data.get("pairs", []):
            pairs.append(
                Pair(
                    name=item["name"],
                    asset=item["asset"],
                    intermediate=item["intermediate"],
                    amount_in=int(item["amount_in"]),
                    decimals=item.get("decimals", 18),
                )
            )
        return cls(pairs)

    def get_route_plans(self, pair: Pair) -> List[RoutePlan]:
        # In production, load from yaml. Default two-router cross-DEX arb.
        return [
            RoutePlan(
                routers=[
                    "0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506",  # SushiSwap V2
                    "0xE592427A0AEce92De3E7dee1F18E0157C05861564",  # Uniswap V3 SwapRouter (placeholder; use V2 router if you prefer)
                ],
                description="sushi -> uni",
            ),
            RoutePlan(
                routers=[
                    "0xE592427A0AEce92De3E7dee1F18E0157C05861564",
                    "0x1b02dA8Cb0d097eB8D57A175b88c7D8b47997506",
                ],
                description="uni -> sushi",
            ),
        ]
