---
name: penetration-tester
description: Perform penetration testing on codebase to identify security vulnerabilities.
---

# Penetration Tester Skill

This skill provides tools and scripts for security testing the EdgeWARN codebase.

## Attack Patterns Covered

| Category | Attack Pattern | Script |
|----------|----------------|--------|
| Secrets | Hardcoded credentials, API keys, tokens | `scan_secrets.sh` |
| Dependencies | Known CVEs in npm/pip packages | `check_dependencies.sh` |
| SQL Injection | SQL query manipulation | `test_sqli.py` |
| Command Injection | Shell command injection | `test_cmdi.py` |
| XSS | Cross-Site Scripting | `test_xss.py` |
| Path Traversal | Directory traversal (`../`) | `test_path_traversal.py` |
| SSRF | Server-Side Request Forgery | `test_ssrf.py` |
| Auth Bypass | Weak tokens, missing auth | `test_auth.py` |
| Rate Limiting | Brute force, DoS | `test_rate_limit.py` |
| CORS | Misconfigured CORS headers | `test_cors.py` |

## Instructions

### 1. Static Analysis (No Server Required)

```bash
# Scan for hardcoded secrets
./scripts/scan_secrets.sh /path/to/project

# Check for vulnerable dependencies
./scripts/check_dependencies.sh /path/to/project
```

### 2. Dynamic Testing (Requires Running Server)

Start the API server first, then run tests against it:

```bash
# Set target URL
export TARGET_URL="http://localhost:3000"

# Run individual tests
python scripts/test_sqli.py $TARGET_URL
python scripts/test_xss.py $TARGET_URL
python scripts/test_cmdi.py $TARGET_URL
python scripts/test_path_traversal.py $TARGET_URL
python scripts/test_ssrf.py $TARGET_URL
python scripts/test_auth.py $TARGET_URL
python scripts/test_rate_limit.py $TARGET_URL
python scripts/test_cors.py $TARGET_URL
```

## Script Details

### `scan_secrets.sh`
Searches source files for patterns matching API keys, passwords, tokens, and other secrets using regex.

### `check_dependencies.sh`
Runs `npm audit` for Node.js and `pip-audit` for Python to identify known vulnerabilities.

### `test_sqli.py`
Injects SQL payloads (`' OR 1=1 --`, `'; DROP TABLE--`) into query parameters and form fields.

### `test_cmdi.py`
Tests for command injection using shell metacharacters (`;`, `|`, `` ` ``, `$()`).

### `test_xss.py`
Injects XSS payloads (`<script>alert(1)</script>`, event handlers) and checks for reflection.

### `test_path_traversal.py`
Attempts directory traversal (`../../../etc/passwd`) on file-serving endpoints.

### `test_ssrf.py`
Tests URL parameters with internal IPs (`127.0.0.1`, `169.254.169.254`) and localhost.

### `test_auth.py`
Tests for missing authentication, weak session tokens, and authorization bypass.

### `test_rate_limit.py`
Sends rapid requests to test rate limiting configuration.

### `test_cors.py`
Checks `Access-Control-Allow-Origin` headers for overly permissive configurations.
