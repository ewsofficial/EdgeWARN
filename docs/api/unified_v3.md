# Unified EdgeWARN API v3

The public service is started with `npm run api` and serves both EdgeWARN and
EWMRS products from one configured base directory. Its default port is `5000`.

`GET /api/v3/openapi.json` is the authoritative machine-readable contract.
All v3 JSON collections use `{ "data": [], "meta": { "nextCursor", "requestId" } }`;
single JSON resources use `{ "data": {}, "meta": { "requestId" } }`.
Errors use `application/problem+json`.

## Runtime configuration

- Canonical base directory: `--base-dir <path>` or `EDGEWARN_BASE_DIR`
- Compatibility aliases for one migration release: `--base_dir <path>` and
  `BASE_DIR`; conflicting settings fail startup
- `PORT` sets the service port; `npm run debug:api` uses debug port `3001`
- `ALLOWED_ORIGINS` is a comma-separated exact browser-origin allowlist.
  Credentials are not enabled for this read-only API.
- `TRUST_PROXY_IPS` configures trusted reverse proxies. Production rejects the
  ambiguous `TRUST_PROXY=true` form.

## Primary resources

- Analysis: `/api/v3/cells`, `/storm-snapshots`, `/alert-snapshots`,
  `/alerts`, `/observations/metar`
- Renders: `/api/v3/render-products/{productId}/snapshots/{timestamp}` with
  `/image` and `/tiles` representations
- Radar: `/api/v3/radar-sites`
- RAP: `/api/v3/models/rap/layers`
- WPC: `/api/v3/analyses/wpc/surface`
- Styles: `/api/v3/styles/colormaps`
- Infrastructure: `/health/live`, `/health/ready`

Canonical render IDs use lower-kebab-case, such as `comp-ref-qc`, `qpe-01h`,
and `goes-abi-c13`. The product catalog preserves the mapping to runtime
folders and legacy file prefixes.

## Migration

The prior `/api/v2`, `/renders`, `/nexrad`, `/rap`, `/wpc`, `/colormaps`,
`/health`, and `/healthz` paths are compatibility adapters on the same
process. They retain legacy bodies/representations and include `Deprecation:
true` plus a link to this API contract. New clients should use v3; no data
route redirects are issued.
