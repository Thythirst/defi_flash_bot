"""
services/watchlist/__init__.py — Progressive Watchlist Architecture.

Components:
  bootstrap.py          — One-time/daily address loading + getUserAccountData filtering
  manager.py            — Per-block HF refresh + Redis ZSET maintenance
  oracle_monitor.py     — Chainlink AnswerUpdated → price deviation → watchlist refresh
  competitor_monitor.py — LiquidationCall tracking → competitor database
  metrics.py            — Watchlist size, HF distribution, RPC health, coverage

Redis schema:
  arb:watchlist:active           ZSET    score=health_factor ascending
  arb:watchlist:meta             HASH    bootstrap_ts, last_refresh_block, total_refreshed
  arb:watchlist:collateral:{sym} SET     users with collateral in {sym}
  arb:watchlist:competitors      ZSET    score=liquidation_count
  arb:watchlist:liquidations     STREAM  liquidation events for dashboard
  arb:watchlist:metrics          HASH    refresh_latency_ms, watchlist_size, prune_count
  arb:watchlist:oracle:feeds     HASH    feed_address -> last_answer, last_block
"""
