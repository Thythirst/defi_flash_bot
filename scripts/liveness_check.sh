#!/bin/bash
# Confirms each module's tracked block is actually advancing, not just "started"
# NOTE: Uses the pipeline log file, not journalctl (stdout goes to log file)

LOG_FILE="/home/ubuntu/defi_flash_bot/logs/pipeline.log"

for module in "AaveBase" "CompoundV3" "AsyncRPC"; do
  B1=$(grep "$module" "$LOG_FILE" | tail -1)
  sleep 65
  B2=$(grep "$module" "$LOG_FILE" | tail -1)
  echo "$module: $([ "$B1" != "$B2" ] && echo "ADVANCING" || echo "⚠️ NO NEW ACTIVITY")"
done
