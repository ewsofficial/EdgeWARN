#!/bin/bash
# analyze_structure.sh - Analyze project structure and generate report
# Usage: ./analyze_structure.sh /path/to/project

set -e

TARGET_DIR="${1:-.}"

echo "=== Project Structure Analysis ==="
echo "Directory: $TARGET_DIR"
echo "Generated: $(date)"
echo ""

echo "## Directory Tree"
echo "\`\`\`"
find "$TARGET_DIR" -type d \
    -not -path "*/.git/*" \
    -not -path "*/node_modules/*" \
    -not -path "*/__pycache__/*" \
    -not -path "*/.next/*" \
    | head -50
echo "\`\`\`"
echo ""

echo "## File Count by Extension"
echo "| Extension | Count |"
echo "|-----------|-------|"
find "$TARGET_DIR" -type f \
    -not -path "*/.git/*" \
    -not -path "*/node_modules/*" \
    -not -path "*/__pycache__/*" \
    | sed 's/.*\.//' | sort | uniq -c | sort -rn | head -15 \
    | while read count ext; do
        echo "| .$ext | $count |"
    done
echo ""

echo "## Large Files (>100 lines)"
echo "| Lines | File |"
echo "|-------|------|"
find "$TARGET_DIR" -type f \( -name "*.py" -o -name "*.js" -o -name "*.ts" \) \
    -not -path "*/.git/*" \
    -not -path "*/node_modules/*" \
    -exec wc -l {} \; 2>/dev/null \
    | awk '$1 > 100 {print $1, $2}' | sort -rn | head -10 \
    | while read lines file; do
        echo "| $lines | $file |"
    done
echo ""

echo "## Entry Points"
echo "\`\`\`"
find "$TARGET_DIR" -type f \( -name "main.py" -o -name "run.py" -o -name "index.js" -o -name "server.js" -o -name "app.js" \) \
    -not -path "*/node_modules/*" 2>/dev/null || echo "No standard entry points found"
echo "\`\`\`"
