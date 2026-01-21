---
name: bug-finder
description: Identify and diagnose bugs, errors, and issues in the codebase.
---

# Bug Finder Skill

This skill helps identify, diagnose, and fix bugs in the EdgeWARN codebase.

## Capabilities

| Capability | Description |
|------------|-------------|
| Error Analysis | Parse error messages and stack traces |
| Log Investigation | Search logs for error patterns |
| Code Inspection | Find common bug patterns in code |
| Runtime Debugging | Debug running processes |
| Regression Detection | Identify changes that introduced bugs |

## Common Bug Patterns to Check

### JavaScript/Node.js
- Uncaught promise rejections
- Undefined/null reference errors
- Async/await without try-catch
- Memory leaks (event listeners, closures)
- Race conditions in async code

### Python
- Unhandled exceptions
- Type errors (None handling)
- Import errors and circular dependencies
- File handle leaks
- Threading issues

## Instructions

### 1. Analyze Error Messages

When given an error, extract:
- Error type/name
- Error message
- File and line number
- Stack trace

### 2. Search for Related Code

```bash
# Find where the error originates
grep -rn "ErrorClassName" src/

# Find related function calls
grep -rn "functionName" src/ --include="*.py" --include="*.js"
```

### 3. Check Recent Changes

```bash
# See recent commits that might have caused the issue
git log --oneline -20

# Check changes to a specific file
git log -p --follow -5 path/to/file.py

# Find when a line was last changed
git blame path/to/file.py
```

### 4. Common Fixes Checklist

- [ ] Check for null/undefined values
- [ ] Verify async operations have error handling
- [ ] Confirm file paths are correct
- [ ] Check environment variables are set
- [ ] Verify dependencies are installed
- [ ] Check for typos in variable/function names
- [ ] Ensure proper data types are used
- [ ] Verify API responses match expected format

### 5. Debugging Commands

```bash
# Node.js debugging
node --inspect src/run.js

# Python debugging
python -m pdb src/run.py

# Check process status
ps aux | grep -E "node|python"

# Check open files/ports
lsof -i :3000
```

## Bug Report Template

When documenting a bug:

```markdown
## Bug Description
Brief description of the issue

## Steps to Reproduce
1. Step one
2. Step two
3. Step three

## Expected Behavior
What should happen

## Actual Behavior
What actually happens

## Error Output
```
[paste error/stack trace here]
```

## Environment
- OS: 
- Node version:
- Python version:

## Root Cause
[Analysis of why this happened]

## Fix
[Description of the fix]
```
