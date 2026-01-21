#!/usr/bin/env python3
"""
test_rate_limit.py - Rate Limiting Testing Script
Usage: python test_rate_limit.py <target_url>
"""

import sys
import time
import requests
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor

def send_request(url):
    try:
        resp = requests.get(url, timeout=5)
        return resp.status_code
    except:
        return None

def test_rate_limit(base_url):
    print(f"=== Rate Limiting Test ===")
    print(f"Target: {base_url}\n")
    
    test_url = urljoin(base_url, "/api/data")
    
    # Test 1: Rapid sequential requests
    print("[*] Testing rapid sequential requests (50 requests)...")
    codes = []
    start = time.time()
    for _ in range(50):
        try:
            resp = requests.get(test_url, timeout=5)
            codes.append(resp.status_code)
        except:
            codes.append(None)
    elapsed = time.time() - start
    
    rate_limited = codes.count(429)
    success = codes.count(200)
    
    print(f"    Completed in {elapsed:.2f}s")
    print(f"    200 OK: {success}, 429 Rate Limited: {rate_limited}")
    
    if rate_limited == 0 and success > 40:
        print("[WARNING] No rate limiting detected!")
    else:
        print(f"[OK] Rate limiting active after ~{success} requests")
    
    # Test 2: Concurrent requests
    print("\n[*] Testing concurrent requests (20 parallel)...")
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(executor.map(send_request, [test_url] * 20))
    
    rate_limited = results.count(429)
    success = results.count(200)
    
    print(f"    200 OK: {success}, 429 Rate Limited: {rate_limited}")
    
    if rate_limited == 0:
        print("[WARNING] No rate limiting on concurrent requests!")
    else:
        print("[OK] Concurrent rate limiting working")
    
    print(f"\n=== Results ===")
    print("Review the numbers above to assess rate limiting effectiveness.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <target_url>")
        sys.exit(1)
    test_rate_limit(sys.argv[1])
