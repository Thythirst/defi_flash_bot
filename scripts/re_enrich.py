#!/usr/bin/env python3
"""Re-enrich backtest_full2.db using public arb1 RPC."""
import asyncio, sqlite3, sys
from datetime import datetime
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider

DB = "/home/ubuntu/defi_flash_bot/backtest_full2.db"
RPC = "https://arb1.arbitrum.io/rpc"
CONCURRENCY = 15
LEGACY_GAS_MULT = 1.5
CURRENT_GAS_MULT = 2.0
CURRENT_PRIORITY = 0.05
PROFIT_FLOOR = 2.0

async def main():
    w3 = AsyncWeb3(AsyncHTTPProvider(RPC))
    conn = sqlite3.connect(DB)
    
    rows = conn.execute("""
        SELECT rowid, tx_hash, block_number, collateral_asset, debt_asset,
               debt_to_cover, collateral_amount
        FROM liquidations 
        WHERE base_fee = '0' AND CAST(block_number AS INTEGER) >= 50000000
    """).fetchall()
    total = len(rows)
    print(f"[{datetime.now():%H:%M:%S}] Re-enriching {total:,} rows with {CONCURRENCY} concurrent...")
    
    sem = asyncio.Semaphore(CONCURRENCY)
    completed = 0
    updated = 0
    lock = asyncio.Lock()
    
    async def enrich_one(rowid, tx_hash, block_num, coll_asset, debt_asset, dtc, coll_amt):
        nonlocal completed, updated
        async with sem:
            try:
                block_num = int(block_num)
                block = await w3.eth.get_block(block_num)
                tx = await w3.eth.get_transaction(tx_hash)
                
                bf = block.get('baseFeePerGas', 0) if hasattr(block, 'get') else dict(block).get('baseFeePerGas', 0)
                gp = tx.get('maxFeePerGas') or tx.get('gasPrice', 0) if hasattr(tx, 'get') else 0
                
                if bf <= 0:
                    async with lock:
                        completed += 1
                    return
                
                our_legacy = int(bf * LEGACY_GAS_MULT)
                priority = int(bf * CURRENT_PRIORITY)
                our_current = int(bf * CURRENT_GAS_MULT) + priority
                would_win_legacy = 1 if our_legacy >= gp else 0
                would_win_current = 1 if our_current >= gp else 0
                
                conn.execute("""
                    UPDATE liquidations SET 
                        base_fee=?, winner_gas_price=?,
                        our_bid_legacy=?, our_bid_current=?,
                        would_win_legacy=?, would_win_current=?
                    WHERE rowid=?
                """, (str(bf), str(gp), str(our_legacy), str(our_current),
                      would_win_legacy, would_win_current, rowid))
                
                async with lock:
                    completed += 1
                    updated += 1
                    if completed % 500 == 0:
                        conn.commit()
                        print(f"[{datetime.now():%H:%M:%S}] {completed:,}/{total:,} ({completed*100//total}%) enriched, {updated:,} updated")
            except Exception as e:
                async with lock:
                    completed += 1
    
    tasks = [enrich_one(*r) for r in rows]
    await asyncio.gather(*tasks)
    conn.commit()
    
    # Stats
    total_rows, enriched, wins = conn.execute("""
        SELECT COUNT(*), 
               COUNT(CASE WHEN base_fee NOT IN ('0','') THEN 1 END),
               COUNT(CASE WHEN would_win_current=1 THEN 1 END)
        FROM liquidations
    """).fetchone()
    
    win_rate = wins / enriched * 100 if enriched else 0
    print(f"\n[{datetime.now():%H:%M:%S}] Done. {enriched:,}/{total_rows:,} enriched, {wins:,} would_win ({win_rate:.1f}%)")
    conn.close()

asyncio.run(main())
