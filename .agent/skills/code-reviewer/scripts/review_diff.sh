#!/bin/bash
# review_diff.sh - Generate a code review report from git diff
# Usage: ./review_diff.sh [base_branch]

BASE_BRANCH="${1:-main}"
CURRENT_BRANCH=$(git branch --show-current)

echo "# Code Review Report"
echo ""
echo "**Comparing:** $BASE_BRANCH → $CURRENT_BRANCH"
echo "**Generated:** $(date)"
echo ""

echo "## Summary"
echo ""
echo "| Metric | Value |"
echo "|--------|-------|"

FILES_CHANGED=$(git diff --name-only $BASE_BRANCH..HEAD | wc -l)
INSERTIONS=$(git diff --stat $BASE_BRANCH..HEAD | tail -1 | grep -oE '[0-9]+ insertion' | grep -oE '[0-9]+' || echo 0)
DELETIONS=$(git diff --stat $BASE_BRANCH..HEAD | tail -1 | grep -oE '[0-9]+ deletion' | grep -oE '[0-9]+' || echo 0)
COMMITS=$(git rev-list --count $BASE_BRANCH..HEAD)

echo "| Files Changed | $FILES_CHANGED |"
echo "| Insertions | $INSERTIONS |"
echo "| Deletions | $DELETIONS |"
echo "| Commits | $COMMITS |"
echo ""

echo "## Files Changed"
echo ""
git diff --name-status $BASE_BRANCH..HEAD | while read status file; do
    case $status in
        A) echo "- ➕ **Added:** $file" ;;
        M) echo "- 📝 **Modified:** $file" ;;
        D) echo "- ❌ **Deleted:** $file" ;;
        R*) echo "- 🔄 **Renamed:** $file" ;;
    esac
done
echo ""

echo "## Commit History"
echo ""
git log --oneline $BASE_BRANCH..HEAD | while read line; do
    echo "- $line"
done
echo ""

echo "## Review Checklist"
echo ""
echo "- [ ] Code follows project style"
echo "- [ ] No debug statements left"
echo "- [ ] Error handling present"
echo "- [ ] Tests added/updated"
echo "- [ ] Documentation updated"
