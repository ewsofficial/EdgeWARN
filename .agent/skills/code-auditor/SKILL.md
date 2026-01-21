---
name: code-auditor
description: Audit code changes against project policies and best practices. Use this when reviewing code or preparing a pull request.
---

# Code Auditor Skill

This skill is designed to audit code changes and ensure compliance with the repository's [CONTRIBUTING.md](../../../CONTRIBUTING.md) and general best practices.

## Policies Enforced

The following policies are strictly enforced based on the project's contribution guidelines:

1.  **Branch Naming Convention**:
    -   General usage: `<yourname>/<branch-name>` (e.g., `jdoe/login-page`).
    -   Prebuilds: `version-test/<version-number>` (e.g., `version-test/1.0.0`).

2.  **Security and Safety**:
    -   **No Sensitive Data**: Code must NOT contain passwords, API keys, tokens, or PII.
    -   **No Malicious Code**: Code must be free of backdoors, malware, or harmful logic.
    -   **Licensing**: Only contribute code that you have the right to submit.

3.  **Code Style**:
    -   Must follow the existing code style of the project.
    -   Linters and formatters should be run before committing.

4.  **Testing**:
    -   Contributions should not negatively affect project functionality.
    -   New features should ideally have accompanying tests (implied by `TST` prefix in contributing guide).

## Considerations

When performing a code audit, consider the following:

-   **Consistency**: Does the new code match the style and patterns of the existing codebase?
-   **Readability**: Is the code easy to understand? Are variable names descriptive?
-   **Maintainability**: Is the code modular? Are complex sections well-documented?
-   **Performance**: Are there any obvious performance bottlenecks (e.g., unnecessary loops, expensive operations)?
-   **Documentation**: key changes should be documented.

## Instructions

To perform a code audit:

1.  **Check Branch Name**: Verify if the current branch follows the naming convention (`<yourname>/<branch-name>` or `version-test/<version-number>`).
2.  **Scan for Secrets**: specificially look for hardcoded API keys, passwords, or tokens.
3.  **Review Code Changes**:
    -   Read through the diffs.
    -   Check for style violations.
    -   Ensure no "dead code" is introduced (unless marked for future use).
    -   Verify that no malicious or harmful code is present.
4.  **Verify Tests**: Check if tests were added or updated for new functionality.
5.  **Check Commit Messages** (if applicable): Ensure commit messages would follow the prefixes defined in `CONTRIBUTING.md` (ADD, FTR, FIX, etc.).
6.  **Report Findings**: Summarize any violations or concerns found during the audit.