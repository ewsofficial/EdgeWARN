---
name: refactoring-assistant
description: Help refactor code for better readability, maintainability, and adherence to best practices.
---

# Refactoring Assistant Skill

This skill helps improve code quality through systematic refactoring.

## Capabilities

| Capability | Description |
|------------|-------------|
| Code Smell Detection | Identify anti-patterns and bad practices |
| DRY Principle | Find and eliminate code duplication |
| Function Extraction | Break down large functions |
| Naming Improvements | Suggest better variable/function names |
| Structure Optimization | Improve code organization |

## Code Smells to Detect

### Functions
- [ ] Too long (>50 lines)
- [ ] Too many parameters (>4)
- [ ] Deep nesting (>3 levels)
- [ ] Multiple responsibilities

### Variables
- [ ] Single-letter names (except loop counters)
- [ ] Unclear abbreviations
- [ ] Magic numbers/strings
- [ ] Unused variables

### Structure
- [ ] Duplicate code blocks
- [ ] Large files (>500 lines)
- [ ] Circular dependencies
- [ ] God objects/modules

## Refactoring Patterns

### Extract Function
```python
# Before
def process():
    # validation
    if not data:
        raise ValueError("Empty")
    if not isinstance(data, list):
        raise TypeError("Must be list")
    # processing...

# After  
def validate(data):
    if not data:
        raise ValueError("Empty")
    if not isinstance(data, list):
        raise TypeError("Must be list")

def process():
    validate(data)
    # processing...
```

### Replace Magic Numbers
```javascript
// Before
if (status === 200) { ... }

// After
const HTTP_OK = 200;
if (status === HTTP_OK) { ... }
```

### Simplify Conditionals
```python
# Before
if x > 0:
    if y > 0:
        if z > 0:
            do_something()

# After
if x > 0 and y > 0 and z > 0:
    do_something()
```

## Checklist Before Refactoring

1. [ ] Tests exist and pass
2. [ ] Understand current behavior
3. [ ] Make small, incremental changes
4. [ ] Run tests after each change
5. [ ] Commit frequently
