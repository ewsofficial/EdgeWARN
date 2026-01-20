#!/bin/bash
# check_outdated.sh - Check for outdated dependencies
# Usage: ./check_outdated.sh /path/to/project

set -e

TARGET_DIR="${1:-.}"
cd "$TARGET_DIR"

echo "# Dependency Status Report"
echo ""
echo "**Directory:** $TARGET_DIR"
echo "**Generated:** $(date)"
echo ""

# Node.js
if [ -f "package.json" ]; then
    echo "## Node.js Dependencies"
    echo ""
    
    if command -v npm &> /dev/null; then
        echo "### Outdated Packages"
        echo "\`\`\`"
        npm outdated 2>/dev/null || echo "All packages up to date"
        echo "\`\`\`"
        echo ""
        
        echo "### Security Audit"
        echo "\`\`\`"
        npm audit --audit-level=moderate 2>/dev/null || echo "No vulnerabilities found"
        echo "\`\`\`"
    else
        echo "npm not available"
    fi
    echo ""
fi

# Python
if [ -f "requirements.txt" ] || [ -f "pyproject.toml" ]; then
    echo "## Python Dependencies"
    echo ""
    
    echo "### Outdated Packages"
    echo "\`\`\`"
    pip list --outdated 2>/dev/null | head -20 || echo "Could not check"
    echo "\`\`\`"
    echo ""
    
    echo "### Dependency Conflicts"
    echo "\`\`\`"
    pip check 2>/dev/null || echo "No conflicts"
    echo "\`\`\`"
fi
