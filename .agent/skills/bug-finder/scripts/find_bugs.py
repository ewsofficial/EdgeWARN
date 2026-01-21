#!/usr/bin/env python3
"""
find_bugs.py - Static analysis to find common bug patterns
Usage: python find_bugs.py /path/to/project
"""

import os
import sys
import re

BUG_PATTERNS = [
    # Python patterns
    (r'except\s*:', 'Bare except clause - catches all exceptions', '.py'),
    (r'except\s+Exception\s*:', 'Catching broad Exception', '.py'),
    (r'==\s*None', 'Use "is None" instead of "== None"', '.py'),
    (r'!=\s*None', 'Use "is not None" instead of "!= None"', '.py'),
    (r'print\s*\(', 'Debug print statement left in code', '.py'),
    (r'TODO|FIXME|HACK|XXX', 'Unresolved TODO/FIXME comment', '.py'),
    (r'password\s*=\s*["\'][^"\']+["\']', 'Hardcoded password', '.py'),
    
    # JavaScript patterns
    (r'console\.log\s*\(', 'Debug console.log left in code', '.js'),
    (r'==\s*null(?!\s*=)', 'Use === for null comparison', '.js'),
    (r'==\s*undefined', 'Use === for undefined comparison', '.js'),
    (r'var\s+\w+', 'Use let/const instead of var', '.js'),
    (r'TODO|FIXME|HACK|XXX', 'Unresolved TODO/FIXME comment', '.js'),
    (r'catch\s*\(\s*\w*\s*\)\s*\{\s*\}', 'Empty catch block', '.js'),
]

def scan_file(filepath):
    """Scan a file for bug patterns."""
    issues = []
    ext = os.path.splitext(filepath)[1]
    
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
    except:
        return issues
    
    for i, line in enumerate(lines, 1):
        for pattern, message, file_ext in BUG_PATTERNS:
            if ext == file_ext and re.search(pattern, line, re.IGNORECASE):
                issues.append({
                    'line': i,
                    'pattern': message,
                    'content': line.strip()[:60]
                })
    
    return issues

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} /path/to/project")
        sys.exit(1)
    
    project_dir = sys.argv[1]
    all_issues = []
    
    for root, dirs, files in os.walk(project_dir):
        dirs[:] = [d for d in dirs if d not in ['node_modules', '.git', '__pycache__']]
        
        for file in files:
            if file.endswith(('.py', '.js')):
                filepath = os.path.join(root, file)
                issues = scan_file(filepath)
                if issues:
                    rel_path = os.path.relpath(filepath, project_dir)
                    all_issues.append((rel_path, issues))
    
    print("# Bug Pattern Scan Results\n")
    
    if not all_issues:
        print("**No issues found!**")
        return
    
    total = sum(len(issues) for _, issues in all_issues)
    print(f"**Found {total} potential issues in {len(all_issues)} files**\n")
    
    for filepath, issues in all_issues:
        print(f"## {filepath}")
        for issue in issues:
            print(f"- Line {issue['line']}: {issue['pattern']}")
            print(f"  ```{issue['content']}```")
        print()

if __name__ == "__main__":
    main()
