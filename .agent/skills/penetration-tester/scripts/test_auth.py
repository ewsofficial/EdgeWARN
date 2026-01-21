#!/usr/bin/env python3
"""
test_auth.py - Authentication Bypass Testing Script
Usage: python test_auth.py <target_url>
"""

import sys
import requests
from urllib.parse import urljoin

def test_auth(base_url):
    print(f"=== Authentication Bypass Test ===")
    print(f"Target: {base_url}\n")
    
    issues = []
    
    # Endpoints that should require auth
    protected_endpoints = [
        "/api/admin",
        "/api/user/profile",
        "/api/settings",
        "/api/data",
        "/admin",
        "/dashboard",
    ]
    
    print("[*] Testing for missing authentication...")
    for endpoint in protected_endpoints:
        url = urljoin(base_url, endpoint)
        try:
            resp = requests.get(url, timeout=10)
            
            # If we get 200 without auth, that's a problem
            if resp.status_code == 200:
                print(f"[!] No auth required: {endpoint} (HTTP 200)")
                issues.append(("no_auth", endpoint))
            elif resp.status_code not in [401, 403, 302]:
                print(f"[?] Unexpected status: {endpoint} (HTTP {resp.status_code})")
                
        except requests.exceptions.RequestException:
            pass
    
    # Test weak token patterns
    print("\n[*] Testing weak session patterns...")
    weak_tokens = [
        {"Authorization": "Bearer admin"},
        {"Authorization": "Bearer test"},
        {"Authorization": "Bearer null"},
        {"Cookie": "session=admin"},
        {"Cookie": "session=1"},
        {"X-Auth-Token": "admin"},
    ]
    
    for headers in weak_tokens:
        url = urljoin(base_url, "/api/admin")
        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                print(f"[!] Weak token accepted: {headers}")
                issues.append(("weak_token", str(headers)))
        except requests.exceptions.RequestException:
            pass
    
    print(f"\n=== Results ===")
    if issues:
        print(f"[WARNING] Found {len(issues)} authentication issues.")
    else:
        print("[OK] No obvious authentication bypass detected.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <target_url>")
        sys.exit(1)
    test_auth(sys.argv[1])
