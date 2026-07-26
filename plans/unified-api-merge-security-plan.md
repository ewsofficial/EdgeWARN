# Unified EdgeWARN API Merge and Security Plan

**Audit baseline:** commit `28beff7242495170ad4cc34d22d74f0b3316e931`
on branch `version-test/3.0.0`

**Package version:** `2.7.0`

**Status:** planning and audit only; the unified API described here has not been
implemented

**Target:** one Express service, one runtime configuration, and one versioned
public API that preserves all current EdgeWARN and EWMRS data products

## 1. Outcome

Replace the independently started EdgeWARN and EWMRS HTTP services with one
service and one `/api/v3` resource model. The merged service will:

- preserve every current cell, storm snapshot, alert, METAR, render, tile,
  NEXRAD, RAP, WPC, colormap, and RAP-mapping capability;
- use nouns and path parameters for resource identity instead of action names
  such as `fetch`, `download`, and `get-items`;
- keep the existing routes as compatibility adapters for a defined migration
  period;
- share configuration, middleware, validation, file access, caching, errors,
  logging, and process startup code;
- close the security gaps in section 7 before the old standalone servers are
  retired.

This is intentionally an incompatible API-version change. The new response
schema and endpoint naming belong under `/api/v3`; the existing `/api/v2`
EdgeWARN contract must not be silently changed.

## 2. Evidence reviewed

The audit covered the current implementation rather than relying only on the
API documentation:

- `src/EdgeWARN/api/server.js`, `config.js`, all v2 routers, and both API
  utilities;
- `src/EWMRS/api/server.js`, all render, NEXRAD, RAP, WPC, and colormap
  routers;
- all Jest/Supertest API tests under `tests/api`;
- `docs/api/api_endpoints.md`, `api_implementation.md`,
  `ewmrs_api_endpoints.md`, and `data_keys.md`;
- `package.json`, the installed top-level dependency tree, and the related
  security/correctness findings already recorded in
  `plans/performance-implementation-roadmap.md` and
  `plans/runtime-correctness-race-condition-remediation-plan.md`.

Baseline verification:

- `npm test -- --runInBand`: **14 suites and 204 tests passed**.
- Installed production packages resolved locally to Express `4.22.2`, CORS
  `2.8.6`, Helmet `8.1.0`, compression `1.8.1`, express-rate-limit `8.5.2`,
  dotenv `16.6.1`, lru-cache `11.2.5`, and Morgan `1.10.1`.
- A current npm advisory query was attempted but could not be completed because
  sending repository dependency metadata to the public npm audit service was
  not authorized. Dependency vulnerability status is therefore **not
  established by this audit** and remains an explicit release gate.

The green tests prove the documented current contracts, not the absence of the
security and architecture issues below. Several missing adversarial cases are
called out in section 10.

## 3. Current API inventory

### 3.1 EdgeWARN service

The EdgeWARN service defaults to port `5000`, forks up to four workers, reads
from `<BASE_DIR>/data`, and currently exposes:

| Current route | Behavior |
| --- | --- |
| `GET /` | Service banner and version |
| `GET /health` | Minimal process liveness |
| `GET /api/v2` | v2 route discovery |
| `GET /api/v2/features/cells[?id=]` | List cell IDs or return one cell |
| `GET /api/v2/features/timestamps[?timestamp=]` | List storm times or return one storm-cell snapshot |
| `GET /api/v2/features/alerts/official[?id=\|timestamp=]` | List official-alert times, return a snapshot, or return one alert |
| `GET /api/v2/features/alerts/edgewarn[?id=\|timestamp=]` | List EdgeWARN-alert times, return a snapshot, or return one alert |
| `GET /api/v2/data/metar[?timestamp=]` | List hourly times or return METAR observations |
| `GET /robots.txt` | Crawler policy |
| `/features/*`, `/data/*` | Removed-v1 `410 Gone` responses |

The name `features/timestamps` hides the actual resource: it returns storm-cell
snapshots. The optional query parameters also make a collection route serve
three different resource modes.

### 3.2 EWMRS service

The EWMRS service defaults to port `3003`, reads mainly from
`<BASE_DIR>/gui` and `<BASE_DIR>/wpc`, and currently exposes:

| Current route | Behavior |
| --- | --- |
| `GET /` | Service metadata, including absolute runtime paths |
| `GET /healthz` | Minimal process liveness |
| `GET /renders/get-items` | List known render products present on disk |
| `GET /renders/fetch?product=` | List render timestamps |
| `GET /renders/download?product=&timestamp=` | Return a legacy flat PNG |
| `GET /renders/tile?product=&timestamp=` | List sparse tiles |
| `GET /renders/tile?product=&timestamp=&x=&y=` | Return one PNG tile |
| `GET /renders/tile-info?product=` | Return grid metadata and timestamps |
| `GET /nexrad` | List radar sites with data |
| `GET /nexrad/:site` | Return elevation-to-timestamp availability |
| `GET /nexrad/:site/:timestamp/:elevation?product=` | Return a gzip binary field |
| `GET /rap/layers` | List RAP layer folders |
| `GET /rap/mappings` | Return RAP layer/colormap mappings |
| `GET /rap/fetch?layer=` | List RAP timestamps |
| `GET /rap/metadata?layer=&timestamp=` | Return RAP decoding metadata |
| `GET /rap/data?layer=&timestamp=` | Return a raw Uint16 array |
| `GET /wpc/fetch?type=sfc` | List WPC surface-analysis timestamps |
| `GET /wpc/download?type=sfc&timestamp=` | Return WPC GeoJSON |
| `GET /colormaps` | Return colormap definitions |

The EWMRS root advertises only a subset of those routes. The route names also
mix actions (`fetch`, `download`, `get-items`), formats (`data`), and resources
(`layers`, `colormaps`).

## 4. Naming and contract rules for `/api/v3`

The following rules prevent the merged API from reproducing the current
inconsistencies:

1. Use plural nouns for collections and stable resource IDs in path segments.
2. Use `lower-kebab-case` for URI words and public product/layer slugs.
   Scientific identifiers such as `KTLH` and `DBZH` retain their conventional
   uppercase representation.
3. Use path parameters for identity and query parameters only for filtering,
   pagination, or representation selection.
4. Keep the operational timestamp key `YYYYMMDD-HHMMSS` as a URL-safe resource
   ID. Return a parsed ISO-8601 `validTime` alongside it in JSON responses.
5. Use `camelCase` for new JSON properties. Storage names such as `tile_grid`,
   folder names, and filename prefixes must not define the public schema.
6. Return JSON collections as:

   ```json
   {
     "data": [],
     "meta": {
       "nextCursor": null,
       "requestId": "..."
     }
   }
   ```

7. Return one JSON resource under `{ "data": ... }`. Binary, PNG, and GeoJSON
   representations keep their native media types rather than being wrapped.
8. Return errors as RFC 9457-style `application/problem+json` documents with
   `type`, `title`, `status`, `detail`, `instance`, and `requestId`.
9. Default list pagination to `100` items with a maximum of `1000`. Preserve
   newest-first ordering for time collections.
10. Advertise the contract with an OpenAPI document at
    `GET /api/v3/openapi.json`; do not maintain a hand-written endpoint array
    in server code.

### 4.1 Stable product catalog

Introduce one data catalog that separates public identifiers from storage:

```json
{
  "id": "goes-abi-c13",
  "legacyId": "GOES_ABI_C13",
  "storageDirectory": "GOES_ABI_C13",
  "legacyFilePrefix": "GOES_ABI_C13_BrightnessTemp",
  "representation": "png-tiles",
  "colormapId": "goes-ir"
}
```

The catalog must be consumed by both the renderer and API, or generated from
one checked source with a parity test. This replaces the drift-prone
`PRODUCT_MAPPING` in `routes/renders.js`, the separate unused `GUI_SUBDIRS`
array in `EWMRS/api/server.js`, and overlapping product definitions in
`EWMRS/render/config.py`.

Examples of canonical IDs:

| Legacy storage/API ID | `/api/v3` ID |
| --- | --- |
| `CompRefQC` | `comp-ref-qc` |
| `QPE_01H` | `qpe-01h` |
| `GOES_ABI_C13` | `goes-abi-c13` |
| `GOES_RGB_TrueColor` | `goes-rgb-true-color` |
| `Temperature_2m` | `temperature-2m` |

Legacy identifiers remain accepted only by compatibility routes. The catalog
must reject collisions when two storage names normalize to the same slug.

## 5. Proposed unified endpoints

### 5.1 Service and health

| New route | Purpose |
| --- | --- |
| `GET /` | Minimal product banner with links to `/api/v3` and documentation; never expose filesystem paths |
| `GET /api/v3` | API version, capability links, and OpenAPI link |
| `GET /api/v3/openapi.json` | Machine-readable contract |
| `GET /health/live` | Process liveness only |
| `GET /health/ready` | Readiness of the configured runtime roots and required indexes |
| `GET /robots.txt` | Existing crawler policy |

Health routes remain outside the version prefix because they are deployment
infrastructure contracts. Readiness must be bounded and must not recursively
scan runtime trees.

### 5.2 EdgeWARN analysis resources

| New route | Response/capability |
| --- | --- |
| `GET /api/v3/cells` | Paginated cell summaries or IDs |
| `GET /api/v3/cells/:cellId` | One persisted cell |
| `GET /api/v3/storm-snapshots` | Available storm snapshot times |
| `GET /api/v3/storm-snapshots/:timestamp` | Storm cells at one valid time |
| `GET /api/v3/alert-snapshots?source=official\|edgewarn` | Available alert snapshot times |
| `GET /api/v3/alert-snapshots/:timestamp?source=official\|edgewarn` | Alert summaries at one valid time |
| `GET /api/v3/alerts/:alertId?source=official\|edgewarn` | One alert by ID |
| `GET /api/v3/observations/metar` | Available METAR observation hours |
| `GET /api/v3/observations/metar/:timestamp` | METAR observations for the containing hour |

`source` is a filter rather than a path hierarchy because official and
EdgeWARN alerts are the same resource types backed by separate providers. It
is required on these routes until IDs are globally namespaced.

The METAR response must expose both the requested time and the actual hourly
observation key so a request for `12:35:00` is not mislabeled as if a
minute-specific file existed.

### 5.3 Raster render resources

| New route | Response/capability |
| --- | --- |
| `GET /api/v3/render-products` | Available catalog products |
| `GET /api/v3/render-products/:productId` | Product metadata, representation, grid, and links |
| `GET /api/v3/render-products/:productId/snapshots` | Available render times |
| `GET /api/v3/render-products/:productId/snapshots/:timestamp/image` | Legacy flat PNG when present |
| `GET /api/v3/render-products/:productId/snapshots/:timestamp/tiles` | Sparse valid tile coordinates and grid |
| `GET /api/v3/render-products/:productId/snapshots/:timestamp/tiles/:x/:y` | One PNG tile |

This separates tile listing from tile retrieval and removes the dual-mode
`/renders/tile` contract. Product metadata absorbs the useful part of
`tile-info`; available timestamps remain their own paginated collection.

### 5.4 NEXRAD resources

| New route | Response/capability |
| --- | --- |
| `GET /api/v3/radar-sites` | Radar sites with available fields |
| `GET /api/v3/radar-sites/:siteId/availability` | Elevations, valid times, and available products |
| `GET /api/v3/radar-sites/:siteId/scans/:timestamp/elevations/:elevation/products/:productId` | One gzip-compressed polar field |

The availability response should include products per elevation/time instead
of requiring a client to guess from the global allowlist. This is additive to
the old EWMRS behavior and does not change the stored binary layout.

### 5.5 RAP model resources

| New route | Response/capability |
| --- | --- |
| `GET /api/v3/models/rap/layers` | Available RAP layer catalog |
| `GET /api/v3/models/rap/layers/:layerId/snapshots` | Available valid times |
| `GET /api/v3/models/rap/layers/:layerId/snapshots/:timestamp/metadata` | Decoding metadata |
| `GET /api/v3/models/rap/layers/:layerId/snapshots/:timestamp/data` | Raw little-endian Uint16 data |
| `GET /api/v3/models/rap/layer-mappings` | RAP layer-to-colormap mappings |

The raw data response retains all existing decode headers. Metadata values used
in headers must be validated and length-bounded before being passed to
`res.set`.

### 5.6 WPC and style resources

| New route | Response/capability |
| --- | --- |
| `GET /api/v3/analyses/wpc/surface` | Available surface-analysis times |
| `GET /api/v3/analyses/wpc/surface/:timestamp` | Surface-analysis GeoJSON |
| `GET /api/v3/styles/colormaps` | Colormap definitions |

The only supported WPC type is already expressed by the path, so the redundant
`type=sfc` parameter disappears.

## 6. Complete legacy migration map

Compatibility adapters must call the same service/repository functions as v3;
they must not keep a second copy of file access and validation logic.

| Existing contract | New contract | Compatibility behavior |
| --- | --- | --- |
| `GET /api/v2` | `GET /api/v3` | Keep the current v2 discovery document unchanged |
| `GET /api/v2/features/cells` | `GET /api/v3/cells` | Preserve bare ID array |
| `GET /api/v2/features/cells?id=:id` | `GET /api/v3/cells/:id` | Preserve passthrough cell JSON |
| `GET /api/v2/features/timestamps` | `GET /api/v3/storm-snapshots` | Preserve bare timestamp array |
| `GET /api/v2/features/timestamps?timestamp=:time` | `GET /api/v3/storm-snapshots/:time` | Preserve snapshot passthrough |
| `GET /api/v2/features/alerts/:source` | `GET /api/v3/alert-snapshots?source=:source` | Preserve bare timestamp array |
| `GET /api/v2/features/alerts/:source?timestamp=:time` | `GET /api/v3/alert-snapshots/:time?source=:source` | Preserve bare alert array and missing-file `[]` behavior |
| `GET /api/v2/features/alerts/:source?id=:id` | `GET /api/v3/alerts/:id?source=:source` | Preserve stored `feature` unwrap |
| `GET /api/v2/data/metar` | `GET /api/v3/observations/metar` | Preserve bare timestamp array |
| `GET /api/v2/data/metar?timestamp=:time` | `GET /api/v3/observations/metar/:time` | Preserve `{type,timestamp,data}` |
| `GET /renders/get-items` | `GET /api/v3/render-products` | Return legacy product IDs |
| `GET /renders/fetch?product=:id` | `GET /api/v3/render-products/:id/snapshots` | Preserve bare timestamp array |
| `GET /renders/download?product=:id&timestamp=:time` | `GET /api/v3/render-products/:id/snapshots/:time/image` | Preserve PNG and old file-prefix lookup |
| `GET /renders/tile?product=:id&timestamp=:time` | `GET /api/v3/render-products/:id/snapshots/:time/tiles` | Preserve current tile-list JSON shape |
| `GET /renders/tile?product=:id&timestamp=:time&x=:x&y=:y` | `GET /api/v3/render-products/:id/snapshots/:time/tiles/:x/:y` | Preserve PNG and status codes |
| `GET /renders/tile-info?product=:id` | `GET /api/v3/render-products/:id` plus snapshots | Preserve current combined object |
| `GET /nexrad` | `GET /api/v3/radar-sites` | Preserve bare site array |
| `GET /nexrad/:site` | `GET /api/v3/radar-sites/:site/availability` | Preserve elevation-to-time object |
| `GET /nexrad/:site/:time/:elevation?product=:product` | `GET /api/v3/radar-sites/:site/scans/:time/elevations/:elevation/products/:product` | Preserve gzip body and download headers |
| `GET /rap/layers` | `GET /api/v3/models/rap/layers` | Preserve legacy folder-name array |
| `GET /rap/mappings` | `GET /api/v3/models/rap/layer-mappings` | Preserve mapping JSON |
| `GET /rap/fetch?layer=:layer` | `GET /api/v3/models/rap/layers/:layer/snapshots` | Preserve bare timestamp array |
| `GET /rap/metadata?layer=:layer&timestamp=:time` | `GET /api/v3/models/rap/layers/:layer/snapshots/:time/metadata` | Preserve metadata JSON |
| `GET /rap/data?layer=:layer&timestamp=:time` | `GET /api/v3/models/rap/layers/:layer/snapshots/:time/data` | Preserve bytes and decode headers |
| `GET /wpc/fetch?type=sfc` | `GET /api/v3/analyses/wpc/surface` | Preserve bare timestamp array |
| `GET /wpc/download?type=sfc&timestamp=:time` | `GET /api/v3/analyses/wpc/surface/:time` | Preserve GeoJSON |
| `GET /colormaps` | `GET /api/v3/styles/colormaps` | Preserve colormap array |
| `GET /health`, `GET /healthz` | `GET /health/live` | Keep both aliases during migration |
| `GET /robots.txt` | `GET /robots.txt` | Preserve the existing crawler policy |
| EdgeWARN and EWMRS `GET /` | Unified `GET /` | Return one non-sensitive capability document |
| `/features/*`, `/data/*` | None | Keep current `410 Gone` behavior |

The two old root documents cannot both exist at one host path. Their operational
function is retained by one unified root that advertises both domains; the
EWMRS absolute `base_dir` and `gui_dir` fields are deliberately removed.

Compatibility responses should add:

- `Deprecation: true`;
- `Sunset: <agreed HTTP date>` once a removal release is scheduled;
- a `Link: </api/v3/openapi.json>; rel="deprecation"` or migration-doc link.

Do not use redirects for data routes. Redirects can alter caching, binary
downloads, and clients that do not follow redirects. Adapters should translate
parameters and response envelopes in process.

## 7. Security audit

### 7.1 Threat model

Assume the API is reachable from the public Internet behind zero or more
reverse proxies. The weather products are currently read-only and generally
public, but runtime files may be malformed because of partial writes, local
operator mistakes, or compromise of an ingest/render worker. Startup
configuration is trusted; request parameters and runtime file contents are
not.

Authentication is not currently present. That is acceptable only while every
public route remains intentionally public and read-only. Any future mutation,
administrative diagnostics, cache purge, reload, or filesystem inspection
route must be placed behind an authenticated operator boundary and must not be
added to the public router by default.

### 7.2 Existing strengths to preserve

- Helmet and compression are enabled in both services.
- EdgeWARN bounds strict JSON request bodies and places rate limiting before
  parsing.
- EdgeWARN production errors are redacted by a global error handler.
- EdgeWARN JSON filenames are allowlisted and realpath-checked.
- WPC GeoJSON reads perform a realpath containment check.
- NEXRAD sites, products, elevations, and timestamps have narrow validators;
  NEXRAD timestamps also validate real calendar values.
- EWMRS render product access is restricted to `PRODUCT_MAPPING`.
- Current tests cover many traversal strings, reserved filenames, one WPC
  symlink escape, and one EdgeWARN cell symlink escape.

### 7.3 Findings and required remediation

#### High

| ID | Finding | Evidence and impact | Required change |
| --- | --- | --- | --- |
| SEC-H1 | Render PNG/tile, RAP metadata/data, and NEXRAD binary routes rely on lexical containment and then `access`/`stat` plus `sendFile`; they do not consistently reject symlinks escaping their allowed roots. | `renders.js:262-270,360-369`; `rap.js:246-296`; `nexrad/filesystem.js:15-24,132-133`. If a runtime writer or local account can place a symlink at an expected filename, a remote request can read a matching file outside the runtime root. The separate check/send steps also leave a replacement race. | Build one safe file repository that opens a validated regular file beneath a pre-opened/canonical root, rejects symlinks, bounds size, and streams the opened handle. Add escape and swap-race tests for every file representation. |
| SEC-H2 | Mutable indexes and payloads are read while Python writers can rewrite them in place. | Existing runtime audit H5/H6 identifies partial JSON and NEXRAD files; current API readers parse or serve them immediately. Repeated requests can expose corrupt data or amplify parse failures. | Coordinate with atomic writer publication: temporary file, validate, flush, atomic replace, publish index last. Readers must size-check and handle malformed/in-progress artifacts as bounded `503` responses without caching them. |

#### Medium

| ID | Finding | Evidence and impact | Required change |
| --- | --- | --- | --- |
| SEC-M1 | EWMRS uses unrestricted `cors()` while EdgeWARN has a separate, more restrictive policy. EdgeWARN also enables credentials even when development mode reflects arbitrary origins. | `EWMRS/api/server.js:182-185`; `EdgeWARN/api/server.js:98-118`. CORS is not authentication, but the merged browser trust policy is undefined and unnecessarily broad. | One allowlist parser, same-origin by default, `credentials: false` for this read-only API, exact normalized origins, `Vary: Origin`, and startup failure or explicit warning for unsafe production configuration. |
| SEC-M2 | The EdgeWARN health limiter bypass trusts the public `x-internal-check: true` header. | `EdgeWARN/api/server.js:143-149`; explicitly exercised by `tests/api/test_server.js`. Any client can bypass both limiters for `/health`. | Remove the header bypass. Give liveness a small dedicated limiter or bypass only at a trusted ingress/network layer. Readiness receives a stricter limiter because it touches disk. |
| SEC-M3 | EdgeWARN's four workers each use the default in-memory rate-limit store. EWMRS has another independent limiter policy. | `EdgeWARN/api/server.js:130-168,259-284`; EWMRS defaults differ at `server.js:71-80,196-209`. Effective limits multiply across workers and reset on restart. | Define limits by route cost and enforce them in one shared store or at the trusted ingress. Keep an in-process safety limiter as defense in depth. |
| SEC-M4 | Proxy handling is inconsistent and EdgeWARN's rate-limit `trustProxy` boolean can disagree with Express's `trust proxy` setting. | `EdgeWARN/api/server.js:121-157`; EWMRS does not configure proxy trust. Incorrect deployment can collapse clients to one key or trust unintended forwarding data. | Parse one explicit proxy allowlist, set Express once, use `req.ip` only after that setting, reject ambiguous `TRUST_PROXY=true` production configuration, and add forwarded-header tests. |
| SEC-M5 | Directory and JSON reads are unbounded, and several expensive listings repeat nested filesystem scans on every request. | Alert/METAR `readdir`; NEXRAD site/elevation/file scans; unrestricted `readFile`/`JSON.parse`; EdgeWARN caching occurs only after a full read/parse. This permits CPU, memory, file-descriptor, and event-loop pressure when runtime trees grow or contain oversized artifacts. | Maximum artifact sizes by type, paginated/bounded listings, short-lived cached immutable indexes, concurrency limits, request timeouts, and route-cost-specific rate limits. Prefer producer-written indexes over recursive request-time discovery. |
| SEC-M6 | EWMRS returns internal exception messages in some `500` bodies and its root returns absolute runtime paths. | `wpc.js:73-80,134-138`; `colormaps.js:21-27`; `EWMRS/api/server.js:221-227`. These disclose filesystem and parser details. | Central error middleware with production redaction; log details server-side with request IDs. Remove `base_dir` and `gui_dir` from all public responses. |
| SEC-M7 | Timestamp and integer validation is inconsistent. Most timestamp validators check only shape; render timestamps accept any safe-looking segment; tile coordinates use permissive `parseInt`. | `EdgeWARN/api/utils/validation.js:17-34`; `renders.js:55-62,346-358`; existing runtime finding L3. Values such as impossible dates or `0junk` can pass some paths. | One strict timestamp parser with calendar validation and optional minute/hour alignment rules; one full-string bounded integer parser; reject repeated query values and unknown query keys consistently. |
| SEC-M8 | Internal JSON controls some response headers without a common validation boundary. | RAP `metadata.units`, grid, and scale values feed `X-*` headers in `rap.js:90-126`. Malformed metadata can throw during header construction or create very large headers. | Schema-validate internal JSON, coerce only finite bounded numeric values, restrict units to a short printable string, and omit invalid optional headers. |

#### Low / hardening

| ID | Finding | Evidence and impact | Required change |
| --- | --- | --- | --- |
| SEC-L1 | Errors and not-found behavior use several incompatible shapes and sometimes convert all file errors to `404`. | EdgeWARN alerts swallow any ID-file exception; other routers use `{error}`, `{error,message,details}`, or nested `{success,error}`. This hides corruption operationally and encourages client-specific parsing. | Typed repository errors plus one v3 problem-details mapper. Compatibility adapters retain legacy bodies while logs retain the true cause. |
| SEC-L2 | EWMRS has request logging but EdgeWARN does not; neither service establishes a request ID and redaction policy. | `EWMRS/api/server.js:184`; no shared logging middleware. | Structured access logs with generated/validated request IDs, duration, route template, status, bytes, and no raw authorization headers or unbounded query values. |
| SEC-L3 | The EdgeWARN configuration module creates a large directory tree as an import side effect. | `EdgeWARN/api/config.js:106-148`. Tests importing the app touch the default runtime tree and log its absolute path. | Pure config parsing; explicit startup validation; create only service-owned required directories when an operator opts in. The read-only API should normally fail readiness rather than mutate missing producer directories. |
| SEC-L4 | HSTS/CSP and cache behavior are not environment- and representation-specific. | EdgeWARN forces one-year subdomain HSTS; EWMRS uses defaults; cache headers vary or are absent. | One documented Helmet policy applied at the HTTPS ingress/application as appropriate. Classify public cacheability, set immutable caching for timestamped artifacts, short caching for indexes, `no-store` for health/errors, ETags, and `Vary` headers. |
| SEC-L5 | Root metadata hard-codes versions and endpoint lists in multiple files. | EdgeWARN repeats `2.7.0`; EWMRS's endpoint list is incomplete. This becomes stale reconnaissance data without being reliable discovery. | Read version from one package/build source and generate capability links from the OpenAPI contract. Masking `2.7.0` as `2.x` is not a substitute for patching dependencies. |

### 7.4 Dependency and platform gate

Before implementation merge:

- run an authorized production and development dependency advisory scan against
  the lockfile;
- review direct and transitive packages, not only `npm ls --depth=0`;
- define the supported Node major version (the baseline tests ran on Node
  `22.23.1`);
- fail CI on accepted severity thresholds with a documented exception process;
- generate an SBOM and pin deployment to the lockfile;
- run a secret scan and license check in CI.

No claim about dependency vulnerability count should be made until this gate
has run successfully.

## 8. Target implementation architecture

Use a neutral top-level API package rather than making EWMRS a child of
EdgeWARN or vice versa:

```text
src/api/
├── app.js                         # Express construction; no listen side effect
├── server.js                      # One process/cluster entry point
├── config/
│   ├── index.js                   # Pure validated runtime config
│   └── product-catalog.json       # Public IDs to storage metadata
├── middleware/
│   ├── cors.js
│   ├── errors.js
│   ├── logging.js
│   ├── rateLimit.js
│   ├── requestId.js
│   └── security.js
├── repositories/
│   ├── artifactRepository.js      # Contained, size-bounded JSON/file access
│   └── indexRepository.js         # Cached and schema-checked indexes
├── services/
│   ├── alerts.js
│   ├── cells.js
│   ├── metar.js
│   ├── nexrad.js
│   ├── rap.js
│   ├── renders.js
│   ├── stormSnapshots.js
│   └── wpc.js
├── routes/
│   ├── v3/
│   └── compatibility/
├── schemas/                       # Request and on-disk response validators
└── openapi/
    └── v3.yaml
```

Key boundaries:

- Routers parse HTTP inputs and format representations; they never construct
  runtime paths.
- Services implement resource behavior independent of legacy/v3 response
  shapes.
- Repositories alone know filesystem layouts and enforce containment,
  symlink, size, and schema rules.
- Compatibility routers call services and translate output; they do not import
  old routers.
- `createApp(config)` is side-effect free and testable. `server.js` owns
  listening and optional worker management.

### 8.1 One runtime configuration

Normalize on:

- CLI: `--base-dir` and `--base-dir=<path>`;
- environment: `EDGEWARN_BASE_DIR`;
- one `PORT`, one debug flag (`--debug-server`), one CORS allowlist, one proxy
  allowlist, and one family of rate-limit variables.

For one release, accept `--base_dir` and `BASE_DIR` as deprecated aliases. Fail
startup if canonical and alias values disagree. Always `path.resolve` the
chosen base, derive `data`, `gui`, and `wpc` roots once, and inject the immutable
result into the app.

Do not silently create all ingest directories from the HTTP service. Readiness
should report which producer roots are unavailable.

### 8.2 Redundancy to remove

| Duplicate/current code | Consolidation |
| --- | --- |
| Rate-limit CLI/integer parsing in both servers | One validated config parser |
| Two compression filters | One middleware factory |
| Two CORS policies | One fail-safe policy |
| Two health contracts | Liveness/readiness routers plus aliases |
| EdgeWARN, RAP, NEXRAD, and WPC path-containment variants | One artifact repository |
| Repeated `fileExists`/`directoryExists` helpers | Repository stat/open operations |
| EdgeWARN, RAP, NEXRAD, render, and WPC timestamp regexes | Shared strict parsers with alignment options |
| Product mapping, GUI subdirectory list, and renderer config | One catalog with parity validation |
| Repeated index JSON old/new normalization | One schema-aware index loader |
| Route-local `try/catch` and error bodies | Typed errors and central mapper |
| Hard-coded version strings and endpoint arrays | Package/build metadata and OpenAPI links |
| Two start scripts and server lifecycles | `npm run api` with deprecated wrapper scripts |

## 9. Phased implementation

### Phase 0 — Freeze and specify the current contract

- [ ] Turn every row in section 6 into a black-box compatibility test,
  including status, headers, ordering, missing-file behavior, and response
  shape.
- [ ] Add binary/PNG byte-for-byte fixtures for render, tile, NEXRAD, and RAP
  routes.
- [ ] Record the current cache headers and accepted old/new index formats.
- [ ] Write and validate the `/api/v3` OpenAPI document before adding routes.
- [ ] Define the canonical product/layer slug catalog and collision test.
- [ ] Obtain a dependency advisory scan or explicitly approve a time-bounded
  release exception.

Exit gate: every existing functional route is represented in the compatibility
matrix and v3 OpenAPI; no undocumented response mode remains.

### Phase 1 — Build the secure shared foundation

- [ ] Add pure unified configuration with canonical/deprecated aliases and
  conflict detection.
- [ ] Add request IDs, structured logs, production-redacted problem details,
  JSON `404`, and explicit `405`/`Allow` behavior.
- [ ] Add one CORS, Helmet, compression, proxy, timeout, and rate-limit stack.
- [ ] Implement safe artifact/index repositories with realpath/open-handle
  containment, regular-file checks, type-specific maximum sizes, and schema
  validation.
- [ ] Add bounded caches keyed by canonical path plus file identity/mtime; do
  not cache errors or partially published files.
- [ ] Coordinate the atomic-publication prerequisites from the runtime
  correctness plan.

Exit gate: adversarial repository tests in section 10 pass before a public v3
route uses the new readers.

### Phase 2 — Extract services without changing old contracts

- [ ] Move cells, storm snapshots, alerts, and METAR behavior into services.
- [ ] Move render product/index/tile behavior into a render service backed by
  the canonical catalog.
- [ ] Move NEXRAD discovery and binary resolution into a radar service.
- [ ] Move RAP layer, mapping, metadata, and data behavior into a model service.
- [ ] Move WPC and colormap behavior into analysis/style services.
- [ ] Rewire existing routers through these services while keeping golden
  compatibility tests green.

Exit gate: old routers contain only HTTP translation and all filesystem path
construction is in repositories.

### Phase 3 — Add `/api/v3`

- [ ] Implement service discovery and the OpenAPI route.
- [ ] Implement analysis, observation, render, radar, model, analysis, and
  style routers from section 5.
- [ ] Add collection envelopes, pagination, canonical IDs, `validTime`,
  problem details, conditional GET, and representation-specific cache policy.
- [ ] Add liveness and readiness; retain bounded legacy health aliases.
- [ ] Add contract tests generated or checked against OpenAPI.

Exit gate: each old capability has both a passing legacy test and a passing v3
test against the same fixture.

### Phase 4 — Switch to one server

- [ ] Add `npm run api` and `npm run debug:api`.
- [ ] Make `api:edgewarn` and `api:ewmrs` temporary wrappers that warn and start
  the unified service, or retain thin proxy processes only if deployment
  sequencing requires them.
- [ ] Use one port (default `5000` unless deployment owners choose otherwise).
- [ ] Update reverse proxy, container, service, health-check, firewall, and
  monitoring configuration.
- [ ] Update README, INSTALLATION, AGENTS, API docs, and all client examples.
- [ ] Run a shadow/canary deployment against a copied runtime tree and compare
  legacy responses byte-for-byte.

Exit gate: production needs one HTTP process/service definition and all known
consumers use either v3 or a compatibility route on that service.

### Phase 5 — Deprecate and remove duplication

- [ ] Publish migration documentation and a measured sunset date based on
  access logs, not an arbitrary release count.
- [ ] Add `Deprecation`, `Sunset`, and `Link` headers.
- [ ] Monitor route-template usage without logging sensitive/raw IDs.
- [ ] Remove standalone server entry points only after legacy traffic reaches
  the agreed threshold.
- [ ] Remove compatibility routers in a later major release.
- [ ] Delete obsolete helpers, mappings, documentation, and tests only after
  their replacements are proven.

Exit gate: no supported consumer depends on removed routes, no second server
can be started accidentally, and searches find only the canonical config,
catalog, middleware, and file-access implementations.

## 10. Verification plan

### 10.1 Contract and functionality

- Every row in section 6 gets a fixture-backed legacy/v3 pair.
- Assert list ordering, old/new index compatibility, empty-list fallbacks,
  alert feature unwrapping, METAR hour mapping, tile-grid fallback, sparse tile
  filtering, RAP decode headers, NEXRAD content disposition, and WPC GeoJSON.
- Compare binary and image payload hashes, not only status and content type.
- Validate OpenAPI examples and response schemas in CI.
- Test Windows and POSIX path handling.

### 10.2 Security

- Traversal corpus: encoded separators, mixed separators, dot segments,
  repeated query values, control characters, reserved names, Unicode
  confusables, oversized segments, and null bytes.
- Symlink corpus for every JSON, PNG, gzip, and Uint16 route: file symlink,
  directory symlink, root replacement, dangling symlink, and an attempted
  check/use swap.
- Impossible dates, non-aligned RAP/WPC times, `0junk`, exponent/hex integers,
  negative values, huge integers, and duplicate parameters.
- Oversized/malformed JSON indexes, excessive tile arrays, invalid header
  metadata, truncated gzip/binary files, and files changing during a request.
- CORS allow/deny/preflight tests, `Vary: Origin`, no credential reflection,
  trusted/untrusted proxy chains, spoofed internal-check headers, and
  multi-worker rate-limit behavior.
- Slow-client, range-request, high-concurrency listing, and event-loop-delay
  tests under bounded resource budgets.
- Production error tests proving no stack, absolute path, raw exception, or
  internal filename is returned.
- Authorized dependency, SBOM, secret, static analysis, and baseline dynamic
  scan before release.

### 10.3 Operational

- Readiness distinguishes missing producer directories from process failure.
- Graceful shutdown drains open file streams and stops accepting new requests.
- Cache invalidation observes atomically replaced indexes without serving
  mixed generations.
- Metrics cover latency, status, bytes, cache outcome, repository errors,
  limiter decisions, open streams, and event-loop delay by route template.
- Canary comparison runs against realistic `data`, `gui`, and `wpc` trees,
  including timestamps being published while requests are active.

## 11. Completion criteria

The merge is complete only when all of the following are proven:

1. One deployable Node service serves `/api/v3`, health, and every required
   compatibility route.
2. Every current EdgeWARN and EWMRS capability in sections 3 and 6 has passing
   old/new contract evidence.
3. Public v3 paths follow the naming and schema rules in section 4.
4. No router constructs arbitrary filesystem paths or directly uses unchecked
   `readFile`, `readdir`, `stat`, `access`, or `sendFile`.
5. Symlink, traversal, size, malformed-artifact, CORS, proxy, rate-limit, error
   disclosure, and partial-publication tests pass.
6. One canonical base-directory setting, product catalog, validation library,
   middleware stack, error model, and server lifecycle remain.
7. Documentation, OpenAPI, package scripts, deployment, monitoring, and client
   migration guidance match the running service.
8. An authorized dependency audit and the full Jest suite pass on the exact
   lockfile/build being released.

Until all eight criteria are met, this document describes planned work rather
than a completed API merge.
