#!/usr/bin/env python3
"""
Test _rpc_call's new three-layer defense against QuickNode text/plain 429.
Simulates responses via a mock HTTP server, then verifies the retry/fallback chain.
"""
import asyncio
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from aiohttp import web

# Mock QuickNode that returns text/plain 429
MOCK_PORT = 18799
MOCK_URL = f"http://localhost:{MOCK_PORT}/qn"
CHAINSTACK_URL = f"http://localhost:{MOCK_PORT}/cs"
PUBLIC_ARB_URL = f"http://localhost:{MOCK_PORT}/pa"

request_counts = {"qn": 0, "cs": 0, "pa": 0}
all_passed = True

async def handler(request):
    """Mock RPC endpoints."""
    path = request.path
    body = await request.text()
    body_json = json.loads(body)
    
    if "/qn" in path:
        request_counts["qn"] += 1
        # Simulate QuickNode text/plain 429: HTTP 200 with Content-Type: text/plain
        # Body is a bare error object (not JSON-RPC envelope)
        return web.Response(
            status=200,
            content_type="text/plain",
            text=json.dumps({
                "code": -32007,
                "message": "50/second request limit reached - reduce calls per second or upgrade your account at quicknode.com/billing/plan"
            })
        )
    elif "/cs" in path:
        request_counts["cs"] += 1
        # Chainstack: normal response
        return web.json_response({
            "jsonrpc": "2.0",
            "id": body_json.get("id", 1),
            "result": "0x1c093a3f"
        })
    elif "/pa" in path:
        request_counts["pa"] += 1
        # PublicArb: normal response
        return web.json_response({
            "jsonrpc": "2.0",
            "id": body_json.get("id", 1),
            "result": "0x1c093a40"
        })
    return web.json_response({"error": "unknown endpoint"})

async def test():
    global all_passed
    print("=== Test: QuickNode text/plain 429 → fallback chain ===")
    
    # Start mock server
    app = web.Application()
    app.router.add_post("/qn", handler)
    app.router.add_post("/cs", handler)
    app.router.add_post("/pa", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "localhost", MOCK_PORT)
    await site.start()
    
    # Import live_executor and test _rpc_call directly
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "live_executor",
        Path(__file__).parent.parent / "scripts" / "live_executor.py"
    )
    module = importlib.util.module_from_spec(spec)
    
    # We need to mock the class constructor — just test _rpc_call logic directly
    # by creating a minimal mock
    from unittest.mock import MagicMock, patch, AsyncMock
    import os
    
    # Set env vars for the test
    os.environ["QUICKNODE_HTTP_URL"] = MOCK_URL
    os.environ["CHAINSTACK_ARBITRUM_HTTP_URL"] = CHAINSTACK_URL
    os.environ["PUBLIC_ARBITRUM_RPC"] = PUBLIC_ARB_URL
    os.environ["ARBITRUM_HTTP_URL"] = MOCK_URL
    
    # Reload the module to pick up env vars
    import importlib
    spec.loader.exec_module(module)
    
    print(f"\n1. QuickNode responses before: {request_counts['qn']}")
    print(f"2. Testing _rpc_call with mocked executor...")
    
    # Test 1: Simulate get_latest_block call through _rpc_call
    # The QuickNode mock returns bare {"code": -32007, "message": "..."}
    # This should now enter the retry loop and fall back to Chainstack
    
    # We can't easily instantiate the full class, so let's test the 
    # response parsing logic directly by importing the patched function
    
    # Instead, test by making actual HTTP calls through the patched _rpc_call
    # Create a lightweight test class
    
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent / ".env")
    
    sys.path.insert(0, str(Path(__file__).parent.parent))
    
    # Read the patched source and compile it
    import py_compile
    compiled = py_compile.compile(
        str(Path(__file__).parent.parent / "scripts" / "live_executor.py"),
        doraise=True
    )
    
    # Import the patched module
    import scripts.live_executor as le
    
    # Create a test instance with mock endpoints
    class TestExecutor:
        SERVICE_NAME = "test"
        def _init_rpc_metrics(self):
            self.SERVICE_NAME = "test"
            self._rpc_redis_metrics_key = "rpc:metrics:test"
        
        def __init__(self):
            self._init_rpc_metrics()
            self.rpc_urls = [MOCK_URL, CHAINSTACK_URL, PUBLIC_ARB_URL]
            import collections
            self.rpc_metrics = collections.defaultdict(int)
            self._rpc_window = []
        
        async def _rpc_call(self, method, params, retries=2):
            return await le.LiveExecutor._rpc_call(self, method, params, retries)
        
        def _provider_label(self, url):
            return le.LiveExecutor._provider_label(self, url)
    
    # Monkey-patch the _rpc_call method into our test class
    test_exec = TestExecutor()
    
    print("\n3. Calling _rpc_call('eth_blockNumber', [])...")
    try:
        result = await test_exec._rpc_call("eth_blockNumber", [])
        print(f"   SUCCESS: result={result}")
        print(f"   QuickNode calls: {request_counts['qn']} (should be 1 — hit, then retried)")
        print(f"   Chainstack calls: {request_counts['cs']} (should be 1 — fallback succeeded)")
        print(f"   PublicArb calls: {request_counts['pa']} (should be 0 — Chainstack succeeded)")
        
        # Verify
        checks = []
        checks.append(("QuickNode was tried", request_counts['qn'] >= 1))
        checks.append(("Chainstack fallback worked", request_counts['cs'] == 1))
        checks.append(("PublicArb not needed", request_counts['pa'] == 0))
        checks.append(("Result contains block number", "result" in result and result["result"].startswith("0x")))
        
        for desc, passed in checks:
            status = "✓" if passed else "✗"
            print(f"   {status} {desc}")
            if not passed:
                global all_passed
                all_passed = False
        
    except Exception as e:
        print(f"   FAILED: {e}")
        print(f"   QuickNode calls: {request_counts['qn']}")
        print(f"   Chainstack calls: {request_counts['cs']}")
        all_passed = False
    
    # Cleanup
    await site.stop()
    await runner.cleanup()
    
    if all_passed:
        print("\n✓ ALL CHECKS PASSED — QuickNode bare error enters retry/fallback chain")
    else:
        print("\n✗ SOME CHECKS FAILED")
    
    return all_passed

if __name__ == "__main__":
    success = asyncio.run(test())
    sys.exit(0 if success else 1)
