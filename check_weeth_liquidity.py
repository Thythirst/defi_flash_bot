import subprocess, json

rpc_line = subprocess.check_output("grep '^BASE_RPC_URL=' /home/ubuntu/defi_flash_bot/.env | head -1", shell=True).decode().strip()
rpc = rpc_line.split('=', 1)[1]

weETH = '0x04c0599ae5A44757c0af6f9ec3b93da8976c150a'
WETH = '0x4200000000000000000000000000000000000006'
wstETH = '0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452'

bal_selector = '0x70a08231'

def get_balance(token, owner):
    data = bal_selector[2:] + owner[2:].lower().rjust(64,'0')
    p = {'jsonrpc':'2.0','id':1,'method':'eth_call','params':[{'to':token,'data':'0x'+data},'latest']}
    r = subprocess.check_output(['curl','-s',rpc,'-H','Content-Type: application/json','-d',json.dumps(p)], timeout=15).decode()
    return int(json.loads(r)['result'], 16)

eth_price = 1900  # approximate

print('=== Uni V3 weETH/WETH Pool Balances ===')
for fee, pool in [(100,'0xb1419a7f9e8c6e434b1d05377e0dbc4154e3de78'),
                   (500,'0x33dfd66802cc936a58a0b25b5e4f792c1ca2312e'),
                   (3000,'0x06b80b12048a37f3762a0015a80ac0bb37c4e539'),
                   (10000,'0xfa038a1d7f6b68e29b02136939eac4c81b612d60')]:
    wb = get_balance(WETH, pool) / 1e18
    web = get_balance(weETH, pool) / 1e18
    tvl = (wb + web) * eth_price
    print(f'  Fee {fee:5d}: WETH={wb:>10.6f} weETH={web:>10.6f} TVL=${tvl:>12,.0f}')

print()
print('=== Searching for ANY weETH liquidity on Base DEXes ===')

# Try weETH/wstETH on Uni V3
factory = '0x33128a8fC17869897dcE68Ed026d694621f6FDfD'
for fee in [100, 500, 3000, 10000]:
    data = '1698ee82' + weETH[2:].rjust(64,'0') + wstETH[2:].rjust(64,'0') + format(fee,'064x')
    p = {'jsonrpc':'2.0','id':1,'method':'eth_call','params':[{'to':factory,'data':'0x'+data},'latest']}
    r = subprocess.check_output(['curl','-s',rpc,'-H','Content-Type: application/json','-d',json.dumps(p)], timeout=15).decode()
    raw = json.loads(r).get('result','')
    pool_addr = '0x' + raw[-40:] if raw else ''
    if pool_addr != '0x0000000000000000000000000000000000000000':
        wb = get_balance(WETH, pool_addr) / 1e18
        web = get_balance(weETH, pool_addr) / 1e18
        ws = get_balance(wstETH, pool_addr) / 1e18
        print(f'  weETH/wstETH fee={fee}: pool={pool_addr} WETH={wb:.4f} weETH={web:.4f} wstETH={ws:.4f}')

# Try weETH on Aerodrome with any pair
aero_factory = '0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A'
for other in [WETH, wstETH]:
    other_name = 'WETH' if other == WETH else 'wstETH'
    for ts in [1, 100, 200]:
        data = 'd8917c1d' + weETH[2:].rjust(64,'0') + other[2:].rjust(64,'0') + format(ts & 0xFFFFFF,'064x')
        p = {'jsonrpc':'2.0','id':1,'method':'eth_call','params':[{'to':aero_factory,'data':'0x'+data},'latest']}
        r = subprocess.check_output(['curl','-s',rpc,'-H','Content-Type: application/json','-d',json.dumps(p)], timeout=15).decode()
        raw = json.loads(r).get('result','')
        pool_addr = '0x' + raw[-40:] if raw else ''
        if pool_addr != '0x0000000000000000000000000000000000000000':
            wb = get_balance(WETH, pool_addr) / 1e18
            web = get_balance(weETH, pool_addr) / 1e18
            ws = get_balance(wstETH, pool_addr) / 1e18
            print(f'  Aerodrome weETH/{other_name} ts={ts}: pool={pool_addr} WETH={wb:.4f} weETH={web:.4f} wstETH={ws:.4f}')

# Summary
print()
print('=== VERDICT ===')
print(f'weETH total supply on Base: ~28,000')
print(f'weETH/WETH liquidity: check pool balances above')
print(f'If TVL < $100K, a $875K liquidation swap is NOT viable on Base DEXes')
