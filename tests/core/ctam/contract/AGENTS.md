# AGENTS Guide for `tests/core/ctam/contract/`

## Purpose
Phase 0 contract tests for the CTAM internal API artifacts in `docs/ctam/`: the
JSON Schemas, the OpenAPI v1 document, the pointer allowlist, and the limits
table. No runtime code is exercised, so these fail on a documentation edit.

## Agent guidance
- Schemas are validated by this repository's own walker in
  `src/common/config/loader.py`, not by `jsonschema`, which is deliberately
  absent. Only the keywords in `_KNOWN_SCHEMA_KEYWORDS` are available: no `$ref`,
  `oneOf`, `format`, `minLength`, or `patternProperties`. Reaching for one is a
  startup error, not a silently unenforced constraint.
- Patterns in `docs/ctam/schema/` must end in `\Z`. The walker uses `re.search`,
  and Python's `$` also matches before a trailing newline, so a `$`-anchored
  allowlist would admit `/modules/Foo\n`. Inline patterns in the OpenAPI document
  use `$` instead, because that file is also read by tooling with no `\Z`; a test
  requires each to be a schema pattern with only the anchor swapped.
- `test_pointer_allowlist.py` holds `TABLE`, which tags each pointer with the
  layer that must reject it. Rows marked `HOST` are expected to pass the schema
  pattern. Phase 3's validator should import `TABLE` and assert those rows are
  rejected there rather than restating the cases.
- A limit lives in `docs/ctam/internal-api-limits.md` and nowhere else. If a
  schema or example restates one, tie the two in `test_limits_contract.py` so the
  copies cannot drift.
- Adding a route means declaring what its payload is via `x-edgewarn-data-schema`
  or `x-edgewarn-item-schema`. Skipping validation requires an entry in a
  recorded-exception list with a reason, which is checked for staleness.
