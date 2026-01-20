#!/bin/bash
# scan_secrets.sh - Scan for hardcoded secrets in source files
# Usage: ./scan_secrets.sh /path/to/project

set -e

TARGET_DIR="${1:-.}"

echo "=== Secret Scanner ==="
echo "Scanning: $TARGET_DIR"
echo ""

# Define patterns to search for
PATTERNS=(
    # API Keys
    'api[_-]?key\s*[:=]\s*["\047][A-Za-z0-9_\-]{16,}["\047]'
    'apikey\s*[:=]\s*["\047][A-Za-z0-9_\-]{16,}["\047]'
    
    # Passwords
    'password\s*[:=]\s*["\047][^"\047]{4,}["\047]'
    'passwd\s*[:=]\s*["\047][^"\047]{4,}["\047]'
    'pwd\s*[:=]\s*["\047][^"\047]{4,}["\047]'
    
    # Tokens
    'token\s*[:=]\s*["\047][A-Za-z0-9_\-\.]{16,}["\047]'
    'bearer\s+[A-Za-z0-9_\-\.]{20,}'
    
    # AWS
    'AKIA[0-9A-Z]{16}'
    'aws[_-]?secret[_-]?access[_-]?key'
    
    # Private Keys
    '-----BEGIN (RSA |DSA |EC |OPENSSH )?PRIVATE KEY-----'
    
    # Generic secrets
    'secret\s*[:=]\s*["\047][^"\047]{8,}["\047]'
    'credentials\s*[:=]'
)

FOUND=0

for pattern in "${PATTERNS[@]}"; do
    MATCHES=$(grep -rniE "$pattern" "$TARGET_DIR" \
        --include="*.js" --include="*.ts" --include="*.py" \
        --include="*.json" --include="*.yaml" --include="*.yml" \
        --include="*.env" --include="*.config" --include="*.conf" \
        --exclude-dir=node_modules --exclude-dir=.git --exclude-dir=__pycache__ \
        2>/dev/null || true)
    
    if [ -n "$MATCHES" ]; then
        echo "[!] Pattern: $pattern"
        echo "$MATCHES"
        echo ""
        FOUND=1
    fi
done

if [ $FOUND -eq 0 ]; then
    echo "[OK] No secrets detected."
else
    echo "[WARNING] Potential secrets found above. Review manually."
fi
