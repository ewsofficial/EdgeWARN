# CTAM internal API

CTAM runs a reserved built-in StormCast adapter followed by independently
installed external modules. External modules are discovered only from
`ctam_modules/<module-id>/module.toml`; importing `EdgeWARN.ctam` does not
register or execute any module.

StormCast is bundled because its published motion is consumed by later tracking
cycles. It runs through the same host-owned cycle boundary as external modules.
The `stormcast` module ID and `StormCast` output key are reserved and cannot be
installed externally.

The former in-process `AnalysisModule`, registry, and grid-module conventions
are retired. A grid analysis must use the cycle-scoped external API rather than
returning an in-memory `_grid_outputs` object.

See [module-manifest.md](module-manifest.md) for discovery and declaration,
[internal-api.md](internal-api.md) for the private loopback contract, and the
checked-in OpenAPI/schema documents for request and response validation.
