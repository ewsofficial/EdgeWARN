---
name: code-reviewer
description: Review code changes, provide feedback, and ensure quality standards.
---

# Code Reviewer Skill

This skill helps review code changes and provide constructive feedback.

## Review Checklist

### Functionality
- [ ] Code works as intended
- [ ] Edge cases handled
- [ ] Error handling present
- [ ] No regressions introduced

### Code Quality
- [ ] Follows project style
- [ ] DRY principle applied
- [ ] Clear naming conventions
- [ ] Appropriate comments

### Security
- [ ] No hardcoded secrets
- [ ] Input validation present
- [ ] SQL injection prevented
- [ ] XSS prevention in place

### Performance
- [ ] No obvious bottlenecks
- [ ] Efficient algorithms
- [ ] Appropriate caching

### Testing
- [ ] Tests added/updated
- [ ] Tests pass
- [ ] Edge cases tested

## Feedback Template

```markdown
## Summary
Brief overview of the changes

## Positives
- Good things about the code

## Suggestions
- Improvements to consider

## Required Changes
- Must-fix issues before merge
```

## Commands

```bash
# View changes
git diff main..feature-branch

# View specific file changes
git diff main..feature-branch -- path/to/file

# Review commit history
git log --oneline main..feature-branch
```
