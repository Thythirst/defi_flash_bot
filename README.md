# Aave V3 Liquidation Executor

> Production-grade liquidation infrastructure for Aave V3 on Arbitrum and Ethereum Mainnet.

[![Foundry](https://img.shields.io/badge/Built%20with-Foundry-FFBD13.svg)](https://getfoundry.sh/)
[![Python](https://img.shields.io/badge/Python-3.11-blue.svg)](https://www.python.org/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

---

## Overview

This repository contains a complete, open-source toolkit for running an **honest Aave V3 liquidator**:

- **`FlashExecutorV3.sol`** — Hardened Solidity contract for flash-loan liquidations via Balancer V2, with optional Uniswap V3 swap routing.
- **`integrated_executor.py`** — High-performance Python executor with sub-300ms block-to-broadcast latency.
- **`liquidation_monitor.py`** — Standalone real-time borrower health-factor monitor for risk visibility.
- **`backtest_engine.py`** — Historical validation engine with 1.1M+ blocks of validated liquidation data.

**Primary chain:** Arbitrum One  
**Secondary chain:** Ethereum Mainnet (Flashbots multi-relay)

---

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  WebSocket      │────▶│  Priority Queue  │────▶│  Multicall3     │
│  newHeads       │     │  (top 20 at-risk)│     │  Batch Health   │
│  (~150ms)       │     │                  │     │  Check (~50ms)  │
└─────────────────┘     └──────────────────┘     └─────────────────┘
                                                          │
                           ┌──────────────────────────────┘
                           ▼
                    ┌──────────────────┐
                    │  Opportunity     │
                    │  Assessment      │
                    │  (local math)    │
                    └──────────────────┘
                           │
           ┌───────────────┼───────────────┐
           ▼               ▼               ▼
    ┌────────────┐  ┌────────────┐  ┌────────────┐
    │ Pre-built  │  │ Flashbots  │  │ Direct     │
    │ TX Cache   │  │ Multi-Relay│  │ Broadcast  │
    │ (~30ms)    │  │ (mainnet)  │  │ (Arbitrum) │
    └────────────┘  └────────────┘  └────────────┘
```

### Latency Budget

| Stage | Target | Implementation |
|---|---|---|
| Block detection | ~150 ms | WebSocket `eth_subscribe(newHeads)` + HTTP fallback |
| Health check (20 borrowers) | ~50 ms | Multicall3 batch |
| Opportunity assessment | ~20 ms | Local math, no external calls |
| TX build / sign | ~30 ms | Pre-built unsigned cache |
| Broadcast | ~50 ms | Flashbots or direct |
| **Total** | **~300 ms** | End-to-end |

---

## Smart Contract

### `FlashExecutorV3.sol`

- **Flash loan source:** Balancer V2 Vault
- **Liquidation target:** Aave V3 Pool
- **Swap routing:** Uniswap V3 `exactInputSingle`
- **Safety:** Reentrancy guards, slippage checks, owner-only recovery
- **Test coverage:** 8/8 Foundry tests passing (unit + fork)

```bash
forge test
```

### Deployment

```bash
python3 scripts/deploy_v3.py
```

Requires `BOT_PRIVATE_KEY` and `ARBITRUM_HTTP_URL` in `.env`.

---

## Executor

### Quick Start

```bash
# 1. Install dependencies
python3.11 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env with your RPC keys and wallet

# 3. Run in dry-run mode (simulate only)
export DRY_RUN=1
python3 scripts/integrated_executor.py

# 4. Run live (Arbitrum)
export CHAIN=arbitrum
export DRY_RUN=0
python3 scripts/integrated_executor.py
```

### Environment Variables

| Variable | Description |
|---|---|
| `CHAIN` | `arbitrum` or `mainnet` |
| `BOT_PRIVATE_KEY` | Hot wallet with gas ETH |
| `FLASH_EXECUTOR_V3` | Deployed contract address |
| `ARBITRUM_HTTP_URL` / `ARBITRUM_WS_URL` | Arbitrum RPC endpoints |
| `MAINNET_HTTP_URL` / `MAINNET_WS_URL` | Mainnet RPC endpoints |
| `FLASHBOTS_AUTH_KEY` | Separate auth key for Flashbots (mainnet only) |
| `MIN_PROFIT_USD` | Profit threshold (default: $50 Arb, $500 mainnet) |
| `DRY_RUN` | `1` to simulate without broadcasting |

---

## Liquidation Monitor

Standalone tool for tracking borrower health factors — useful for DAO treasury monitoring, risk dashboards, and community alerts.

```bash
python3 -m scanner.liquidation_monitor
```

Outputs a sorted health-factor leaderboard of all tracked borrowers.

---

## Backtest Engine

Validates strategy profitability before live deployment.

### Aave V3 Liquidation Backtest

```bash
python3 -m scanner.liquidation_backtest \
  --from-block 463000000 \
  --to-block 464100000 \
  --rpc-url $ARBITRUM_HTTP_URL
```

### Cross-Pool Arbitrage Backtest

```bash
python3 run_backtest.py \
  --strategy WETH_USDC_CROSS_FEE \
  --from-block 464995000 \
  --to-block 464996000
```

### Historical Results (Arbitrum, 1.1M blocks)

| Metric | Value |
|---|---|
| Total liquidation events | 11 |
| Profitable (simulated) | 7 (77.8%) |
| Best single liquidation | block 464,200,724 → 37.59 ETH net |
| Average net profit (winners) | ~5.68 ETH |

*All pool addresses factory-verified via live `eth_call`. See `liquidation_backtest.csv` and `liquidation_backtest.json` for per-event detail.*

---

## Project Structure

```
.
├── src/
│   └── FlashExecutorV3.sol          # Flash-loan liquidation contract
├── tests/
│   └── FlashExecutorV3.t.sol        # Foundry test suite
├── scripts/
│   ├── integrated_executor.py       # Production executor
│   ├── deploy_v3.py                 # Contract deployment
│   ├── approve_routers.py           # DEX router approvals
│   └── dry_run.py                   # Dry-run validation
├── scanner/
│   ├── liquidation_monitor.py       # Health-factor monitor
│   ├── liquidation_backtest.py      # Historical liquidation backtest
│   ├── backtest_engine.py           # Cross-pool arbitrage backtest
│   ├── websocket_monitor.py         # WebSocket block subscription
│   ├── flashbots_relay.py           # Flashbots bundle submission
│   ├── multicall_batch.py           # Multicall3 batching
│   ├── priority_queue.py            # At-risk borrower queue
│   ├── chains.py                    # Chain-agnostic config
│   └── v3_math.py                   # Exact EVM TickMath in Python
├── infra/
│   └── aws_setup.sh                 # AWS us-east-1 deployment script
├── pairs.yaml                       # Strategy + pool configuration
├── foundry.toml                     # Foundry config
└── requirements.txt                 # Python dependencies
```

---

## Deployment

### AWS us-east-1 (Recommended)

```bash
# Launch EC2 c6i.large or better in us-east-1
curl -sL https://raw.githubusercontent.com/Thythirst/defi_flash_bot/main/infra/aws_setup.sh | bash
```

See `infra/aws_setup.sh` for full setup including kernel tuning, Python 3.11, Foundry, and systemd service.

---

## Security

- `.env` and private keys are **excluded from git** (see `.gitignore`).
- The bot wallet should be a **hot wallet** with only gas ETH — never store large balances.
- `FlashExecutorV3` has not yet undergone a third-party audit. Use at your own risk.

---

## Grant Application

This project is applying for an **Aave Grant** to fund:
- Third-party audit of `FlashExecutorV3.sol`
- Extended historical backtesting (6M+ blocks)
- Mainnet deployment and live dry-run validation
- Open-source documentation and community onboarding

See [`GRANT_APPLICATION.md`](GRANT_APPLICATION.md) for full details.

---

## License

MIT — see [LICENSE](LICENSE) for details.

---

## Contact

- **GitHub:** [@Thythirst](https://github.com/Thythirst)
- **Email:** fsmuchina@gmail.com
- **Telegram:** @RealOg
