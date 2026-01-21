#!/bin/bash
# analyze_logs.sh - Search logs for error patterns
# Usage: ./analyze_logs.sh /path/to/logs

set -e

LOG_DIR="${1:-.}"

echo "=== Log Analysis ==="
echo "Directory: $LOG_DIR"
echo ""

echo "## Error Summary"
echo ""

# Count errors by type
echo "### Error Counts"
echo "| Pattern | Count |"
echo "|---------|-------|"
grep -riE "(error|exception|failed|critical)" "$LOG_DIR" 2>/dev/null \
    | grep -oiE "(error|exception|failed|critical)" \
    | sort | uniq -ci | sort -rn \
    | while read count pattern; do
        echo "| $pattern | $count |"
    done
echo ""

echo "### Recent Errors (last 20)"
echo "\`\`\`"
grep -riE "(error|exception|failed)" "$LOG_DIR" 2>/dev/null | tail -20 || echo "No errors found"
echo "\`\`\`"
echo ""

echo "### Stack Traces"
echo "\`\`\`"
grep -riE "traceback|stack trace" "$LOG_DIR" -A5 2>/dev/null | head -30 || echo "No stack traces found"
echo "\`\`\`"
