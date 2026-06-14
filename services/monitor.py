"""
services/monitor.py — Lightweight monitoring dashboard.

Collects metrics from Redis, serves HTTP dashboard (JSON API + HTML).
Zero external dependencies beyond aiohttp.

Endpoints:
  GET  /              — HTML dashboard
  GET  /health        — JSON health check
  GET  /api/overview  — JSON summary
  GET  /api/services  — JSON service status
  GET  /api/pnl       — JSON daily P&L breakdown
  GET  /api/oracle    — JSON current prices
  GET  /api/risk      — JSON risk engine state
  GET  /api/forecast  — JSON liquidation forecast
  GET  /api/mempool   — JSON mempool stats

Redis reads:
  arb:metrics:daily:{date}    — daily P&L
  price:aggregate:{sym}       — current prices
  price:meta:{sym}            — oracle metadata
  risk:state:{strategy}       — strategy status
  risk:limits                 — current limits
  forecast:ranking            — liquidation forecast
  mempool:stats:{minute}      — mempool activity
  aave:liquidatable           — liquidatable count
  aave:checkpoint             — indexer progress

Usage:
  python -m services.monitor --port 8080
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, Optional

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import redis.asyncio as redis
from aiohttp import web
from dotenv import load_dotenv
load_dotenv(dotenv_path=project_root / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | monitor | %(message)s",
)
logger = logging.getLogger("monitor")

# ────────────────────────────────────────────────────────────────
# HTML Dashboard
# ────────────────────────────────────────────────────────────────

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="refresh" content="30">
<title>MEV Monitor — {hostname}</title>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ background: #0d1117; color: #c9d1d9; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', monospace; padding: 20px; }}
  h1 {{ color: #58a6ff; font-size: 20px; margin-bottom: 5px; }}
  .subtitle {{ color: #8b949e; font-size: 12px; margin-bottom: 20px; }}
  .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; }}
  .card {{ background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 16px; }}
  .card h2 {{ color: #58a6ff; font-size: 14px; margin-bottom: 10px; border-bottom: 1px solid #30363d; padding-bottom: 6px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
  td {{ padding: 4px 8px; border-bottom: 1px solid #21262d; }}
  td:first-child {{ color: #8b949e; width: 120px; }}
  td:last-child {{ text-align: right; font-weight: 600; }}
  .positive {{ color: #3fb950; }}
  .negative {{ color: #f85149; }}
  .warning {{ color: #d2991d; }}
  .info {{ color: #58a6ff; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; }}
  .badge-active {{ background: #1a3a1a; color: #3fb950; }}
  .badge-inactive {{ background: #3a1a1a; color: #f85149; }}
  .badge-paused {{ background: #3a3a1a; color: #d2991d; }}
  .section {{ font-size: 11px; color: #484f58; margin-top: 16px; text-align: center; }}
</style>
</head>
<body>
<h1>⚡ MEV Monitoring Dashboard</h1>
<div class="subtitle">{hostname} | {now} | Auto-refresh 30s</div>

<div class="grid">
  <div class="card">
    <h2>📊 Daily P&amp;L</h2>
    <table>
      {pnl_rows}
    </table>
  </div>

  <div class="card">
    <h2>🔧 Services</h2>
    <table>
      {service_rows}
    </table>
  </div>

  <div class="card">
    <h2>💰 Oracle Prices</h2>
    <table>
      {price_rows}
    </table>
  </div>

  <div class="card">
    <h2>🛡 Risk Engine</h2>
    <table>
      {risk_rows}
    </table>
  </div>

  <div class="card">
    <h2>📈 Indexer</h2>
    <table>
      {indexer_rows}
    </table>
  </div>

  <div class="card">
    <h2>🔮 Forecast</h2>
    <table>
      <tr><td>Liquidatable</td><td>{liq_count:,}</td></tr>
      <tr><td>At-risk (&lt;5%)</td><td class="warning">{at_risk:,}</td></tr>
      <tr><td>Total users tracked</td><td>{user_count:,}</td></tr>
    </table>
  </div>

  <div class="card">
    <h2>📡 Mempool (last hour)</h2>
    <table>
      {mempool_rows}
    </table>
  </div>

  <div class="card">
    <h2>🖥 System</h2>
    <table>
      {system_rows}
    </table>
  </div>
</div>

<div class="section">Hermes MEV Stack · {uptime}</div>
</body>
</html>"""


# ────────────────────────────────────────────────────────────────
# Monitor Service
# ────────────────────────────────────────────────────────────────

class MonitorService:
    """Collects metrics and serves HTTP dashboard."""

    def __init__(self, redis_url: str = "redis://localhost:6379", port: int = 8080):
        self.redis_url = redis_url
        self.port = port
        self.redis: Optional[redis.Redis] = None
        self.start_time = time.time()

    async def connect(self):
        self.redis = redis.from_url(self.redis_url, decode_responses=True)
        await self.redis.ping()
        logger.info("Monitor connected to Redis")

    # ── Data collectors ─────────────────────────────────────────

    async def _get_pnl_data(self) -> dict:
        date = time.strftime("%Y-%m-%d")
        data = await self.redis.hgetall(f"arb:metrics:daily:{date}")

        # Also check per-strategy hashes
        rows = {}
        for strategy in ["liq", "dex_arb"]:
            sub = await self.redis.hgetall(f"arb:metrics:daily:{date}:{strategy}")
            if sub:
                for k, v in sub.items():
                    data[f"{strategy}_{k}"] = v

        total_profit = 0.0
        total_gas = 0.0
        for k, v in data.items():
            if k.endswith("_profit") and not k.startswith("best_"):
                total_profit += int(v) / 1e18 if v else 0
            elif k == "gas_spent":
                total_gas += int(v) / 1e18 if v else 0

        return {
            "profit_eth": round(total_profit, 4),
            "gas_eth": round(total_gas, 4),
            "net_eth": round(total_profit - total_gas, 4),
            "trades": int(data.get("mined", 0)),
            "attempts": int(data.get("attempts", 0)),
            "best_single": int(data.get("best_single", 0)) / 1e18 if data.get("best_single") else 0,
            "raw": data,
        }

    async def _get_service_status(self) -> dict:
        import subprocess
        services = {
            "liquidation": "liquidation-dryrun",
            "dex_arb": "dex-arb-scanner",
            "cex_dev": "cex-deviation",
            "aave_indexer": "aave-indexer",
            "oracle": "oracle-service",
            "risk": "risk-engine",
            "mempool": "mempool-intel",
            "execution": "execution-engine",
        }
        result = {}
        for name, unit in services.items():
            try:
                proc = subprocess.run(
                    ["systemctl", "is-active", unit],
                    capture_output=True, text=True, timeout=5
                )
                result[name] = proc.stdout.strip()
            except Exception:
                result[name] = "unknown"
        return result

    async def _get_oracle_prices(self) -> dict:
        prices = {}
        for sym in ["ETH", "BTC", "LINK", "ARB", "USDC", "USDT", "DAI"]:
            val = await self.redis.get(f"price:aggregate:{sym}")
            if val:
                meta = await self.redis.hgetall(f"price:meta:{sym}")
                prices[sym] = {
                    "price": round(float(val), 2),
                    "sources": meta.get("sources", "?"),
                    "deviation": float(meta.get("deviation_max", 0)),
                }
        return prices

    async def _get_risk_state(self) -> dict:
        limits = await self.redis.hgetall("risk:limits")
        killswitch = await self.redis.get("risk:killswitch") or "0"
        states = {}
        for s in ["liquidation", "dex_arb", "cex_deviation"]:
            data = await self.redis.hgetall(f"risk:state:{s}")
            states[s] = data
        return {
            "limits": limits,
            "killswitch": killswitch == "1",
            "strategies": states,
        }

    async def _get_indexer_status(self) -> dict:
        cp = await self.redis.hgetall("aave:checkpoint")
        liq = await self.redis.zcard("aave:liquidatable")
        user_count = 0
        for rk in await self.redis.keys("aave:reserve:*:users"):
            user_count += await self.redis.scard(rk)
        return {
            "last_block": int(cp.get("last_block", "0")),
            "liquidatable": liq,
            "users_tracked": user_count,
        }

    async def _get_mempool_stats(self) -> dict:
        # Aggregate last hour of stats
        totals = {"total_tx": 0, "oracle_updates": 0, "liquidations": 0,
                   "large_swaps": 0, "flash_loans": 0}
        for minute_offset in range(60):
            ts = time.time() - minute_offset * 60
            key = time.strftime("%Y-%m-%dT%H:%M", time.gmtime(ts))
            data = await self.redis.hgetall(f"mempool:stats:{key}")
            for k in totals:
                totals[k] += int(data.get(k, 0))
        return totals

    async def _get_system_info(self) -> dict:
        import subprocess
        # Memory
        mem = {}
        try:
            proc = subprocess.run(["free", "-m"], capture_output=True, text=True, timeout=5)
            lines = proc.stdout.strip().split("\n")
            if len(lines) > 1:
                parts = lines[1].split()
                mem = {"total": int(parts[1]), "used": int(parts[2]), "free": int(parts[3])}
        except Exception:
            mem = {"total": 0, "used": 0, "free": 0}

        # Uptime
        uptime_seconds = int(time.time() - self.start_time)
        uptime_str = f"{uptime_seconds // 3600}h {(uptime_seconds % 3600) // 60}m"

        return {"memory": mem, "uptime": uptime_str}

    # ── Dashboard render ────────────────────────────────────────

    async def _render_dashboard(self) -> str:
        pnl = await self._get_pnl_data()
        services = await self._get_service_status()
        prices = await self._get_oracle_prices()
        risk = await self._get_risk_state()
        indexer = await self._get_indexer_status()
        mempool = await self._get_mempool_stats()
        system = await self._get_system_info()

        import socket
        hostname = socket.gethostname()
        now = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())

        # P&L rows
        profit_class = "positive" if pnl["net_eth"] >= 0 else "negative"
        pnl_rows = f"""
        <tr><td>Gross Profit</td><td class="positive">{pnl['profit_eth']:.4f} ETH</td></tr>
        <tr><td>Gas Spent</td><td class="negative">-{pnl['gas_eth']:.4f} ETH</td></tr>
        <tr><td><b>Net P&amp;L</b></td><td class="{profit_class}"><b>{pnl['net_eth']:+.4f} ETH</b></td></tr>
        <tr><td>Trades</td><td>{pnl['trades']} mined / {pnl['attempts']} attempts</td></tr>
        <tr><td>Best Single</td><td class="positive">{pnl['best_single']:.4f} ETH</td></tr>
        """

        # Service rows
        service_rows = ""
        for name, status in services.items():
            badge = "active" if status == "active" else "inactive"
            service_rows += f'<tr><td>{name}</td><td><span class="badge badge-{badge}">{status}</span></td></tr>\n'

        # Price rows
        price_rows = ""
        for sym in ["ETH", "BTC", "LINK", "ARB", "USDC"]:
            if sym in prices:
                p = prices[sym]
                dev_class = "positive" if p["deviation"] < 1 else "warning"
                price_rows += f'<tr><td>{sym}</td><td>${p["price"]:,.2f} <span class="{dev_class}">({p["deviation"]:.2f}%)</span> {p["sources"]}</td></tr>\n'

        # Risk rows
        ks = risk.get("killswitch", False)
        risk_rows = f"""
        <tr><td>Killswitch</td><td><span class="badge badge-{'inactive' if ks else 'active'}">{'🔴 ON' if ks else '🟢 OFF'}</span></td></tr>
        <tr><td>Max Trade</td><td>{risk.get('limits', {}).get('max_trade_eth', '?')} ETH</td></tr>
        <tr><td>Daily Loss Cap</td><td>{risk.get('limits', {}).get('daily_loss_cap_eth', '?')} ETH</td></tr>
        """

        # Indexer rows
        idx = indexer
        liq_count = idx.get("liquidatable", 0)
        indexer_rows = f"""
        <tr><td>Block</td><td>{idx['last_block']:,}</td></tr>
        <tr><td>Users Tracked</td><td>{idx['users_tracked']:,}</td></tr>
        <tr><td>Liquidatable</td><td class="{'warning' if liq_count > 0 else 'info'}">{liq_count:,}</td></tr>
        """

        # Mempool rows
        mp = mempool
        mempool_rows = f"""
        <tr><td>Total TX</td><td>{mp['total_tx']:,}</td></tr>
        <tr><td>Oracles</td><td>{mp['oracle_updates']}</td></tr>
        <tr><td>Liquidations</td><td class="warning">{mp['liquidations']}</td></tr>
        <tr><td>Large Swaps</td><td>{mp['large_swaps']}</td></tr>
        """

        # System rows
        sys = system
        system_rows = f"""
        <tr><td>Memory</td><td>{sys['memory']['used']}M / {sys['memory']['total']}M ({sys['memory']['free']}M free)</td></tr>
        <tr><td>Monitor Uptime</td><td>{sys['uptime']}</td></tr>
        """

        # At-risk count
        at_risk = await self.redis.zcount("forecast:ranking", "-inf", "5")

        return DASHBOARD_HTML.format(
            hostname=hostname,
            now=now,
            pnl_rows=pnl_rows,
            service_rows=service_rows,
            price_rows=price_rows,
            risk_rows=risk_rows,
            indexer_rows=indexer_rows,
            liq_count=liq_count,
            at_risk=at_risk,
            user_count=idx["users_tracked"],
            mempool_rows=mempool_rows,
            system_rows=system_rows,
            uptime=sys["uptime"],
        )

    # ── HTTP Handlers ───────────────────────────────────────────

    async def handle_index(self, request):
        html = await self._render_dashboard()
        return web.Response(text=html, content_type="text/html")

    async def handle_health(self, request):
        services = await self._get_service_status()
        all_ok = all(s == "active" for s in services.values())
        return web.json_response({
            "status": "ok" if all_ok else "degraded",
            "services": services,
            "uptime": int(time.time() - self.start_time),
        })

    async def handle_api_overview(self, request):
        pnl = await self._get_pnl_data()
        services = await self._get_service_status()
        prices = await self._get_oracle_prices()
        risk = await self._get_risk_state()
        indexer = await self._get_indexer_status()
        system = await self._get_system_info()
        return web.json_response({
            "pnl": pnl,
            "services": services,
            "prices": prices,
            "risk": risk,
            "indexer": indexer,
            "system": system,
        })

    async def handle_api_services(self, request):
        return web.json_response(await self._get_service_status())

    async def handle_api_pnl(self, request):
        return web.json_response(await self._get_pnl_data())

    async def handle_api_oracle(self, request):
        return web.json_response(await self._get_oracle_prices())

    async def handle_api_risk(self, request):
        return web.json_response(await self._get_risk_state())

    async def handle_api_forecast(self, request):
        count = await self.redis.zcard("forecast:ranking")
        at_risk = await self.redis.zcount("forecast:ranking", "-inf", "5")
        top = await self.redis.zrange("forecast:ranking", 0, 9, withscores=True)
        return web.json_response({"count": count, "at_risk": at_risk, "top": top})

    async def handle_api_mempool(self, request):
        return web.json_response(await self._get_mempool_stats())

    # ── Start server ────────────────────────────────────────────

    async def start(self):
        await self.connect()

        app = web.Application()
        app.router.add_get("/", self.handle_index)
        app.router.add_get("/health", self.handle_health)
        app.router.add_get("/api/overview", self.handle_api_overview)
        app.router.add_get("/api/services", self.handle_api_services)
        app.router.add_get("/api/pnl", self.handle_api_pnl)
        app.router.add_get("/api/oracle", self.handle_api_oracle)
        app.router.add_get("/api/risk", self.handle_api_risk)
        app.router.add_get("/api/forecast", self.handle_api_forecast)
        app.router.add_get("/api/mempool", self.handle_api_mempool)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self.port)
        await site.start()

        logger.info("Monitor dashboard: http://0.0.0.0:%d", self.port)
        logger.info("API: http://0.0.0.0:%d/api/overview", self.port)

        # Keep running
        while True:
            await asyncio.sleep(3600)

    async def stop(self):
        if self.redis:
            await self.redis.aclose()


# ─── CLI ──────────────────────────────────────────────────────

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="MEV Monitoring Dashboard")
    parser.add_argument("--redis", default="redis://localhost:6379")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    svc = MonitorService(redis_url=args.redis, port=args.port)
    try:
        await svc.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        await svc.stop()


if __name__ == "__main__":
    asyncio.run(main())
