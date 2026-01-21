#!/bin/bash
# check_dependencies.sh - Check for vulnerable dependencies
# Usage: ./check_dependencies.sh /path/to/project

set -e

TARGET_DIR="${1:-.}"

echo "=== Dependency Vulnerability Scanner ==="
echo "Scanning: $TARGET_DIR"
echo ""

cd "$TARGET_DIR"

# Check for Node.js dependencies
if [ -f "package.json" ]; then
    echo "[*] Found package.json - Running npm audit..."
    npm audit --json 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    vulns = data.get('vulnerabilities', {})
    if vulns:
        print(f'[!] Found {len(vulns)} vulnerable packages:')
        for pkg, info in vulns.items():
            severity = info.get('severity', 'unknown')
            print(f'    - {pkg}: {severity}')
    else:
        print('[OK] No npm vulnerabilities found.')
except:
    print('[?] Could not parse npm audit output.')
" || echo "[?] npm audit failed or not available."
    echo ""
fi

# Check for Python dependencies
if [ -f "requirements.txt" ] || [ -f "setup.py" ] || [ -f "pyproject.toml" ]; then
    echo "[*] Found Python project - Checking dependencies..."
    
    if command -v pip-audit &> /dev/null; then
        echo "[*] Running pip-audit..."
        pip-audit 2>/dev/null || echo "[?] pip-audit encountered an error."
    elif command -v safety &> /dev/null; then
        echo "[*] Running safety check..."
        safety check 2>/dev/null || echo "[?] safety check encountered an error."
    else
        echo "[*] Running pip check (basic)..."
        pip check 2>/dev/null || echo "[?] pip check found issues."
        echo "[TIP] Install pip-audit for better vulnerability scanning: pip install pip-audit"
    fi
    echo ""
fi

echo "=== Scan Complete ==="
