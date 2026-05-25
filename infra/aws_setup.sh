#!/bin/bash
# infra/aws_setup.sh — Production deployment script for AWS us-east-1
#
# Sets up an EC2 instance optimized for MEV liquidation bot:
# - Ubuntu 22.04 LTS
# - Low-latency kernel tuning
# - Python 3.11 + dependencies
# - Foundry for contract compilation
# - Systemd service for auto-restart
#
# Usage:
#   1. Launch EC2 t3.large or better in us-east-1
#   2. SSH in: ssh -i key.pem ubuntu@<ip>
#   3. Run: curl -sL https://raw.githubusercontent.com/.../aws_setup.sh | bash
#   4. Configure .env and start service

set -euo pipefail

BOT_DIR="/opt/liquidation-bot"
SERVICE_NAME="liquidation-bot"
USER="bot"

echo "=== AWS MEV Bot Setup ==="
echo "Region: us-east-1 (closest to Arbitrum sequencer + Ethereum builders)"

# ─── System Updates ─────────────────────────────────────────
echo "[1/8] Updating system..."
sudo apt-get update && sudo apt-get upgrade -y

# ─── Low-Latency Kernel Tuning ──────────────────────────────
echo "[2/8] Tuning kernel for low latency..."

# Increase network buffers
sudo sysctl -w net.core.rmem_max=134217728
sudo sysctl -w net.core.wmem_max=134217728
sudo sysctl -w net.ipv4.tcp_rmem="4096 87380 134217728"
sudo sysctl -w net.ipv4.tcp_wmem="4096 65536 134217728"
sudo sysctl -w net.core.netdev_max_backlog=30000
sudo sysctl -w net.ipv4.tcp_congestion_control=bbr

# Reduce CPU scheduling latency
sudo sysctl -w kernel.sched_min_granularity_ns=10000000
sudo sysctl -w kernel.sched_wakeup_granularity_ns=15000000

# Persist settings
cat <<EOF | sudo tee /etc/sysctl.d/99-mev-tuning.conf
net.core.rmem_max=134217728
net.core.wmem_max=134217728
net.ipv4.tcp_rmem=4096 87380 134217728
net.ipv4.tcp_wmem=4096 65536 134217728
net.core.netdev_max_backlog=30000
net.ipv4.tcp_congestion_control=bbr
kernel.sched_min_granularity_ns=10000000
kernel.sched_wakeup_granularity_ns=15000000
EOF

# ─── Install Dependencies ───────────────────────────────────
echo "[3/8] Installing dependencies..."
sudo apt-get install -y \
    build-essential \
    libssl-dev \
    zlib1g-dev \
    libbz2-dev \
    libreadline-dev \
    libsqlite3-dev \
    llvm \
    libncurses5-dev \
    libncursesw5-dev \
    xz-utils \
    tk-dev \
    libffi-dev \
    liblzma-dev \
    python3-openssl \
    git \
    curl \
    jq \
    htop \
    tmux

# ─── Install Python 3.11 ────────────────────────────────────
echo "[4/8] Installing Python 3.11..."
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt-get update
sudo apt-get install -y python3.11 python3.11-venv python3.11-dev python3-pip

# ─── Create Bot User ────────────────────────────────────────
echo "[5/8] Creating bot user..."
sudo useradd -r -s /bin/false -d "$BOT_DIR" "$USER" || true
sudo mkdir -p "$BOT_DIR"
sudo chown "$USER:$USER" "$BOT_DIR"

# ─── Clone/Setup Repository ─────────────────────────────────
echo "[6/8] Setting up bot repository..."
# NOTE: Replace with your actual repo URL or SCP the code manually
# sudo -u "$USER" git clone https://github.com/yourusername/defi_flash_bot.git "$BOT_DIR/repo"

# For now, assume code is copied manually or via CI
sudo mkdir -p "$BOT_DIR/repo"
sudo chown -R "$USER:$USER" "$BOT_DIR/repo"

# Create virtual environment
sudo -u "$USER" bash -c "
    cd $BOT_DIR/repo
    python3.11 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip wheel
    pip install web3 eth-account eth-utils eth-abi python-dotenv aiohttp websockets
"

# ─── Install Foundry ────────────────────────────────────────
echo "[7/8] Installing Foundry..."
curl -L https://foundry.paradigm.xyz | bash
export PATH="$HOME/.foundry/bin:$PATH"
foundryup

# ─── Systemd Service ────────────────────────────────────────
echo "[8/8] Creating systemd service..."

sudo tee /etc/systemd/system/${SERVICE_NAME}.service <<EOF
[Unit]
Description=Aave V3 Liquidation Bot
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$BOT_DIR/repo/prod
Environment=PYTHONUNBUFFERED=1
EnvironmentFile=$BOT_DIR/repo/prod/.env
ExecStart=$BOT_DIR/repo/prod/venv/bin/python scripts/integrated_executor.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

# Security hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=$BOT_DIR/repo/prod

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"

# ─── Environment Template ───────────────────────────────────
sudo -u "$USER" tee "$BOT_DIR/repo/prod/.env" <<'EOF'
# Chain: "arbitrum" or "mainnet"
CHAIN=arbitrum

# RPC endpoints (replace with your credentials)
ARBITRUM_HTTP_URL=https://arb-mainnet.g.alchemy.com/v2/YOUR_KEY
ARBITRUM_WS_URL=wss://arb-mainnet.g.alchemy.com/v2/YOUR_KEY
MAINNET_HTTP_URL=https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY
MAINNET_WS_URL=wss://eth-mainnet.g.alchemy.com/v2/YOUR_KEY

# Bot wallet (hot wallet with gas ETH)
BOT_PRIVATE_KEY=0x...

# Deployed contract
FLASH_EXECUTOR_V3=0x...

# Flashbots auth key (mainnet only — separate from bot wallet)
FLASHBOTS_AUTH_KEY=0x...

# Telegram alerts (optional)
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=

# Profit threshold
MIN_PROFIT_USD=50.0

# Dry run mode: set to "1" to simulate only
DRY_RUN=0
EOF

sudo chmod 600 "$BOT_DIR/repo/prod/.env"

# ─── Done ───────────────────────────────────────────────────
echo ""
echo "=== Setup Complete ==="
echo ""
echo "Next steps:"
echo "  1. Edit $BOT_DIR/repo/prod/.env with your credentials"
echo "  2. Deploy contract: cd $BOT_DIR/repo/prod && ./venv/bin/python scripts/deploy_v3.py"
echo "  3. Start bot: sudo systemctl start $SERVICE_NAME"
echo "  4. View logs: sudo journalctl -u $SERVICE_NAME -f"
echo ""
echo "Performance tuning applied:"
echo "  - BBR congestion control"
echo "  - Increased socket buffers"
echo "  - CPU scheduler tuned for low latency"
echo ""
echo "Recommended instance types:"
echo "  - t3.large  ($0.083/hr) — minimum viable"
echo "  - c6i.large ($0.085/hr) — better CPU, consistent performance"
echo "  - c6i.xlarge ($0.17/hr) — recommended for mainnet"
echo ""
