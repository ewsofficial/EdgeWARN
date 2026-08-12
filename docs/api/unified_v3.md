# Unified EdgeWARN API v3

The public service is started with `npm run api` and serves both EdgeWARN and
EWMRS products from one configured base directory. Its default port is `5000`.

`GET /api/v3/openapi.json` is the authoritative machine-readable contract.
All v3 JSON collections use `{ "data": [], "meta": { "nextCursor" } }`;
single JSON resources use `{ "data": {}, "meta": {} }`. Per-request correlation
is available through the `X-Request-Id` response header rather than the body,
so cacheable JSON responses keep a stable body and support conditional `GET`
(`ETag`/`If-None-Match` → `304`).
Errors use `application/problem+json`.

## Runtime configuration

- Canonical base directory: `--base-dir <path>` or `EDGEWARN_BASE_DIR`
- Compatibility aliases for one migration release: `--base_dir <path>` and
  `BASE_DIR`; conflicting settings fail startup
- `PORT` sets the service port; `npm run debug:api` uses debug port `3001`
- `ALLOWED_ORIGINS` is a comma-separated exact browser-origin allowlist.
  Credentials are not enabled for this read-only API.
- `TRUST_PROXY_IPS` configures trusted reverse proxies. Production rejects the
  ambiguous `TRUST_PROXY=true` form. Only set this when a stripping reverse
  proxy removes client-supplied `X-Forwarded-For`/`X-Forwarded-Proto` headers
  before forwarding; on a directly exposed host, enabling trust lets clients
  spoof forwarded headers and bypass per-client rate limits.

## Primary resources

- Analysis: `/api/v3/cells`, `/storm-snapshots`, `/alert-snapshots`,
  `/alerts`, `/observations/metar`
- Renders: `/api/v3/render-products/{productId}/snapshots/{timestamp}/chunks`
  lists sparse float16 value chunks; `/chunks/{x}/{y}` returns the binary payload.
  The historical `/image` and `/tiles` resources remain PNG-only compatibility
  endpoints and never relabel a binary chunk as an image.
- Radar: `/api/v3/radar-sites`
- RAP: `/api/v3/models/rap/layers`
- WPC: `/api/v3/analyses/wpc/surface`
- Styles: `/api/v3/styles/colormaps`
- Infrastructure: `/health/live`, `/health/ready`

Canonical render IDs use lower-kebab-case, such as `comp-ref-qc`, `qpe-01h`,
and `goes-abi-c13`. The product catalog preserves the mapping to runtime
folders and legacy file prefixes.

## EWMRS binary chunks

MRMS and GOES ABI renders publish one-channel float16 value chunks. They are
gzip-compressed `chunk_{x}_{y}.f16.gz` files under
`<BASE_DIR>/gui/<product>/<timestamp>/chunks/`. `NaN` is the no-data value;
gzip uses deterministic metadata and the API sends `Content-Encoding: gzip`.
Clients apply the published product colormap to the scalar values; GOES RGB
composites are derived client-side from the raw ABI channel chunks. Chunks
retain top-to-bottom row order and a bottom-left chunk-grid origin.

Fetch the `/chunks` listing first. It provides the grid, format descriptor,
and the authoritative sparse coordinate list—missing coordinates are fully
transparent chunks, not a request to synthesize pixels. The payload endpoint
sets `X-EWMRS-Format-Version`, `X-Data-Type`, `X-Pixel-Format`, chunk width and
height, grid origin, and pixel-row-order headers. Verify that the response
length equals `width * height * channels * 2` before creating a `Uint16Array`
or `Float16Array`; responses
are immutable and support ETag conditional GET and HEAD.

```js
const listing = await (await fetch(chunkListUrl)).json();
const response = await fetch(chunkUrl);
const bytes = new Uint16Array(await response.arrayBuffer());
if (bytes.byteLength !== 350 * 350) throw new Error('invalid float16 scalar chunk');
// Interpret as float16 (or upload as half-float); grid y=0 is the bottom row.
```

These float16 value chunks are distinct from RAP `data.u16` scalar arrays and NEXRAD
`.bin.gz` products, which have their own metadata and decoders.

## Migration

The prior `/api/v2`, `/renders`, `/nexrad`, `/rap`, `/wpc`, `/colormaps`,
`/health`, and `/healthz` paths are compatibility adapters on the same
process. They retain legacy bodies/representations and include `Deprecation:
true` plus a link to this API contract. New clients should use v3; no data
route redirects are issued.
