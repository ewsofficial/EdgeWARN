## 2026-01-12 - API Security Hardening (Helmet & CORS)
**Vulnerability:** The API lacked standard security headers (exposing `X-Powered-By: Express` and missing CSP/HSTS) and had an overly permissive CORS configuration (`*`).
**Learning:** Adding security headers like HSTS via `helmet` can break applications if they are deployed in HTTP-only environments or behind proxies that handle SSL termination without proper forwarding headers.
**Prevention:**
1. Used `helmet` but explicitly disabled HSTS (`hsts: false`) to prevent locking out HTTP clients.
2. Refactored CORS to check for `ALLOWED_ORIGINS` environment variable, falling back to `*` only if undefined. This secures the app where possible (production) while maintaining backward compatibility for existing deployments.
