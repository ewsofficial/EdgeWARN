---
name: git-commit-formatter
description: Format git commit messages to follow the contributing guidelines in this repo. Use when user asks to commit changes
---

# Git Commit Formatter Skill

When writing a commit message, you **MUST** follow [CONTRIBUTING.md](../../../CONTRIBUTING.md).

## Format

``<type>[optional scope]: description``

### Allowed Types

- Check commit message prefixes in [CONTRIBUTING.md](../../../CONTRIBUTING.md) for a list of allowed prefixes.

- You are forbidden to use any other prefixes not listed in [CONTRIBUTING.md](../../../CONTRIBUTING.md) unless the user explicitly asks you to do so.

- Additionally, you may **ONLY** use one ``type`` per commit message.

- Failure to follow these rules will result in you being publically executed.

### Instructions

1. Analyze the changes to determine the primary ``type``
2. [OPTIONAL] Determine the ``scope`` if applicable
3. Write a concise description in a formal, professional tone.

### Examples

- ``FTR[API]: Add new user authentication endpoint``
- ``FIX[detect]: Resolve crash on startup``