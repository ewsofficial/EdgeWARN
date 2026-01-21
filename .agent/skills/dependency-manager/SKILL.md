---
name: dependency-manager
description: Manage project dependencies, resolve conflicts, and keep packages up to date.
---

# Dependency Manager Skill

This skill helps manage project dependencies across Python and Node.js.

## Capabilities

| Capability | Description |
|------------|-------------|
| Dependency Audit | Check for outdated or vulnerable packages |
| Version Management | Update dependencies safely |
| Conflict Resolution | Resolve version conflicts |
| Cleanup | Remove unused dependencies |

## Commands

### Node.js
```bash
# Check outdated packages
npm outdated

# Update packages
npm update

# Check for security issues
npm audit
npm audit fix

# Remove unused
npx depcheck
```

### Python
```bash
# List installed
pip list

# Check outdated
pip list --outdated

# Generate requirements
pip freeze > requirements.txt

# Check conflicts
pip check
```

## Best Practices

- [ ] Pin major versions in production
- [ ] Use lock files (package-lock.json, requirements.txt)
- [ ] Audit dependencies regularly
- [ ] Remove unused dependencies
- [ ] Test after updates
