#!/usr/bin/env python3
"""
test_cors.py - CORS Misconfiguration Testing Script
Usage: python test_cors.py <target_url>
"""

import sys
import requests
from urllib.parse import urljoin

def test_cors(base_url):
    print(f"=== CORS Misconfiguration Test ===")
    print(f"Target: {base_url}\n")
    
    issues = []
    test_endpoints = ["/api/data", "/api/user", "/"]
    
    malicious_origins = [
        "https://evil.com",
        "https://attacker.com",
        "null",
        "https://example.com.evil.com",
    ]
    
    for endpoint in test_endpoints:
        url = urljoin(base_url, endpoint)
        
        for origin in malicious_origins:
            try:
                headers = {"Origin": origin}
                resp = requests.get(url, headers=headers, timeout=10)
                
                acao = resp.headers.get("Access-Control-Allow-Origin", "")
                acac = resp.headers.get("Access-Control-Allow-Credentials", "")
                
                # Check if origin is reflected
                if acao == origin:
                    print(f"[!] Origin reflected: {endpoint}")
                    print(f"    Origin: {origin}")
                    print(f"    ACAO: {acao}")
                    issues.append((endpoint, origin, "reflected"))
                    
                    if acac.lower() == "true":
                        print(f"    [CRITICAL] Credentials allowed!")
                        issues.append((endpoint, origin, "credentials"))
                        
                # Check for wildcard with credentials
                elif acao == "*" and acac.lower() == "true":
                    print(f"[!] Wildcard with credentials: {endpoint}")
                    issues.append((endpoint, "*", "wildcard_creds"))
                    
                # Check for null origin acceptance
                elif origin == "null" and acao == "null":
                    print(f"[!] Null origin accepted: {endpoint}")
                    issues.append((endpoint, "null", "null_accepted"))
                    
            except requests.exceptions.RequestException:
                pass
    
    print(f"\n=== Results ===")
    if issues:
        print(f"[WARNING] Found {len(issues)} CORS misconfigurations.")
    else:
        print("[OK] No obvious CORS misconfigurations detected.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <target_url>")
        sys.exit(1)
    test_cors(sys.argv[1])
