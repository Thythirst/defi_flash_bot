#!/usr/bin/env bash
# Deploy script for MEV services
# Usage: deploy.sh [all|service1,service2,...] [dry-run]
set -euo pipefail

SERVICES="${1:-all}"
DRY_RUN="${2:-false}"
PROD_DIR="/root/defi_flash_bot/prod"
VENV_PYTHON="$PROD_DIR/venv/bin/python"
LOG_DIR="$PROD_DIR/logs"
HEALTH_URL="http://localhost:8080/health"

# ── Service definitions ──────────────────────────────────────────
# Order matters: critical services (with hot wallet) are deployed
# sequentially; stateless services can restart in parallel.

declare -A SERVICE_GROUPS=(
  # Group 0: infrastructure — deploy first, all at once
  [oracle-service]="infra"
  [mempool-intel]="infra"
  [risk-engine]="infra"
  [mev-monitor]="infra"
  [aave-indexer]="infra"

  # Group 1: scanners — deploy after infra, parallel
  [cex-deviation]="scanner"
  [dex-arb-scanner]="scanner"

  # Group 2: critical — deploy LAST, one at a time, with health checks
  [execution-engine]="critical"
  [liquidation-dryrun]="critical"
)
declare -A SERVICE_HEALTH_ENDPOINTS=(
  [execution-engine]="/health"
  [liquidation-dryrun]="/health"
)

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
warn() { echo "[$(date '+%H:%M:%S')] ⚠  $*" >&2; }
err()  { echo "[$(date '+%H:%M:%S')] ❌ $*" >&2; }
ok()   { echo "[$(date '+%H:%M:%S')] ✅ $*"; }

health_check() {
  local svc=$1
  local endpoint=${SERVICE_HEALTH_ENDPOINTS[$svc]:-/health}
  local url="http://localhost:8080${endpoint}"

  for i in $(seq 1 10); do
    if curl -sf "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

restart_service() {
  local svc=$1
  if [[ "$DRY_RUN" == "true" ]]; then
    log "[DRY-RUN] Would restart: $svc"
    return 0
  fi
  log "Restarting: $svc"
  systemctl restart "$svc"
}

deploy_group() {
  local group=$1
  local pids=()

  for svc in "${!SERVICE_GROUPS[@]}"; do
    [[ "${SERVICE_GROUPS[$svc]}" == "$group" ]] || continue

    if systemctl is-active --quiet "$svc" 2>/dev/null; then
      restart_service "$svc" &
      pids+=($!)
    else
      warn "$svc not active — skipping"
    fi
  done

  for pid in "${pids[@]}"; do
    wait "$pid" || warn "Background restart failed (pid=$pid)"
  done
}

deploy_critical() {
  for svc in "${!SERVICE_GROUPS[@]}"; do
    [[ "${SERVICE_GROUPS[$svc]}" == "critical" ]] || continue

    if ! systemctl is-active --quiet "$svc" 2>/dev/null; then
      warn "$svc not active — skipping"
      continue
    fi

    restart_service "$svc"
    sleep 2

    if health_check "$svc"; then
      ok "$svc healthy after restart"
    else
      err "$svc FAILED health check after restart — ROLLBACK REQUIRED"
      return 1
    fi
  done
  return 0
}

# ── Main ─────────────────────────────────────────────────────────

mkdir -p "$LOG_DIR"
log "Deploy started — mode=$( [[ "$DRY_RUN" == "true" ]] && echo 'DRY-RUN' || echo 'LIVE' )"

# Ensure venv is current (no requirements drift)
if [[ "$DRY_RUN" != "true" ]]; then
  log "Syncing Python deps..."
  "$VENV_PYTHON" -m pip install -r "$PROD_DIR/requirements.txt" --quiet 2>&1 | tail -1
fi

# Phase 1: infrastructure (parallel)
log "Phase 1: Infrastructure"
deploy_group infra

# Phase 2: scanners (parallel)
log "Phase 2: Scanners"
deploy_group scanner

# Phase 3: critical (sequential with health checks)
log "Phase 3: Critical services"
if deploy_critical; then
  ok "All critical services healthy"
else
  err "Critical service health check FAILED"
  exit 1
fi

ok "Deploy complete"
