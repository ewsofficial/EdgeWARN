# EdgeWARN API Security Audit Report

**Date:** 2026-03-01  
**Auditor:** Debug Mode Analysis  
**Scope:** src/EdgeWARN/api/*  
**API Version:** v2.0.0

---

## Executive Summary

> [!NOTE]
> **Status: RESOLVED**  
> All issues identified in this report have been addressed in recent commits (March 2026). Specifically, the legacy `data/nws.js` route was removed, and security best practices (Helmet, CORS, Rate Limiting) were properly configured in the API server.

This security audit identified **10 security issues** in the EdgeWARN API, ranging from **Critical** to **Low** severity. The most concerning issues include unvalidated user input that could lead to prototype pollution, overly permissive CORS configuration, and information disclosure through error messages and health endpoints.

| Severity | Count |
|----------|-------|
| Critical | 1 |
| High | 4 |
| Medium | 3 |
| Low | 2 |

---

## Critical Issues

### 1. Unvalidated `id` Parameter Leading to Prototype Pollution (CRITICAL)

**Location:** [`src/EdgeWARN/api/routes/v2/data/nws.js:68`](src/EdgeWARN/api/routes/v2/data/nws.js:68)

**Description:**
The `id` parameter from user input is used directly as an object key without validation:

```javascript
const alert = registry.alerts?.[id];
```

This allows an attacker to pass special property names like `__proto__`, `constructor`, or `prototype` which could modify the object's prototype chain, leading to prototype pollution vulnerabilities.

**Impact:**
- Potential remote code execution (depending on application logic)
- Data integrity compromise
- Application behavior manipulation

**Proof of Concept:**
```
GET /api/v2/data/nws?id=__proto__
GET /api/v2/data/nws?id=constructor
```

**Remediation:**
Add validation to ensure `id` only contains alphanumeric characters and hyphens:

```javascript
export function validateAlertId(id) {
  if (typeof id !== 'string') return false;
  // Allow alphanumeric, hyphens, and underscores only
  return /^[a-zA-Z0-9_-]+$/.test(id);
}
```

---

## High Priority Issues

### 2. Information Disclosure via Error Messages (HIGH)

**Locations:**
- [`src/EdgeWARN/api/server.js:117`](src/EdgeWARN/api/server.js:117)
- [`src/EdgeWARN/api/routes/v2/data/nws.js:134`](src/EdgeWARN/api/routes/v2/data/nws.js:134)
- [`src/EdgeWARN/api/routes/v2/data/metar.js:99`](src/EdgeWARN/api/routes/v2/data/metar.js:99)
- [`src/EdgeWARN/api/routes/v2/features/cells.js:65`](src/EdgeWARN/api/routes/v2/features/cells.js:65)
- [`src/EdgeWARN/api/routes/v2/features/timestamps.js:65`](src/EdgeWARN/api/routes/v2/features/timestamps.js:65)

**Description:**
Error handlers log `err.stack` which may contain sensitive file paths, and error messages reveal internal system details like file not found locations.

**Impact:**
- Exposure of internal directory structure
- Information useful for planning further attacks
- Potential disclosure of sensitive file paths

**Remediation:**
Replace verbose error logging with generic messages in production:

```javascript
// In production, don't expose stack traces
if (process.env.NODE_ENV === 'production') {
  console.error('Error:', err.message);
  res.status(500).json({ error: 'Internal server error' });
} else {
  console.error(err.stack);
  res.status(500).json({ error: err.message, stack: err.stack });
}
```

---

### 3. Overly Permissive CORS Configuration (HIGH)

**Location:** [`src/EdgeWARN/api/server.js:54-60`](src/EdgeWARN/api/server.js:54-60)

**Description:**
The CORS configuration defaults to allowing all origins (`*`) if no `ALLOWED_ORIGINS` environment variable is set:

```javascript
const allowedOrigins = process.env.ALLOWED_ORIGINS
  ? process.env.ALLOWED_ORIGINS.split(',')
  : '*';
```

**Impact:**
- Cross-site request forgery (CSRF) attacks possible
- Unauthorized cross-origin access to API
- Session hijacking if authentication is added later

**Remediation:**
Require explicit CORS configuration and default to same-origin:

```javascript
const allowedOrigins = process.env.ALLOWED_ORIGINS
  ? process.env.ALLOWED_ORIGINS.split(',').map(o => o.trim())
  : (process.env.NODE_ENV === 'production' ? false : ['http://localhost:3000']);

if (!allowedOrigins) {
  console.warn('[Security] No ALLOWED_ORIGINS set, API may be inaccessible from browsers');
}

app.use(cors({
  origin: allowedOrigins,
  credentials: true,
  methods: ['GET', 'HEAD', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));
```

---

### 4. HSTS Disabled - Vulnerable to SSL Stripping (HIGH)

**Location:** [`src/EdgeWARN/api/server.js:38-47`](src/EdgeWARN/api/server.js:38-47)

**Description:**
Helmet's HSTS (HTTP Strict Transport Security) is explicitly disabled:

```javascript
app.use(helmet({
  hsts: false,
  // ...
}));
```

**Impact:**
- Man-in-the-middle attacks via SSL stripping
- Downgrade attacks from HTTPS to HTTP
- Session hijacking

**Remediation:**
Enable HSTS with appropriate configuration:

```javascript
app.use(helmet({
  hsts: {
    maxAge: 31536000, // 1 year
    includeSubDomains: true,
    preload: true
  },
  contentSecurityPolicy: {
    useDefaults: true,
    directives: {
      "default-src": ["'self'"],
    }
  }
}));
```

If HTTP-only clients must be supported, implement HTTPS redirection instead of disabling HSTS entirely.

---

### 5. Health Endpoint Information Disclosure (HIGH)

**Location:** [`src/EdgeWARN/api/routes/health.js:13-47`](src/EdgeWARN/api/routes/health.js:13-47)

**Description:**
The health endpoint exposes detailed system information including CPU usage and memory consumption:

```javascript
res.json({
  status: 'OK',
  cpuUsage: Number((totalUsage * 100).toFixed(2)),
  systemMemoryUsageMB: usedMemMB
});
```

**Impact:**
- Information useful for DoS attack planning
- System resource profiling for targeted attacks
- Potential exposure of infrastructure details

**Remediation:**
Simplify health endpoint to return minimal information:

```javascript
router.get('/', (req, res) => {
  res.json({
    status: 'OK',
    timestamp: new Date().toISOString()
  });
});
```

Move detailed metrics to an authenticated `/admin/metrics` endpoint.

---

## Medium Priority Issues

### 6. Trust Proxy Without Validation (MEDIUM)

**Location:** [`src/EdgeWARN/api/server.js:65`](src/EdgeWARN/api/server.js:65)

**Description:**
```javascript
app.set('trust proxy', 1);
```

This trusts the first proxy unconditionally, which could lead to IP spoofing if the API is exposed directly to the internet.

**Impact:**
- IP-based rate limiting can be bypassed
- Access logging shows incorrect client IPs
- Potential security control bypass

**Remediation:**
Configure trust proxy based on environment:

```javascript
if (process.env.TRUST_PROXY === 'true') {
  app.set('trust proxy', true);
} else if (process.env.TRUST_PROXY_IPS) {
  app.set('trust proxy', process.env.TRUST_PROXY_IPS.split(','));
} else {
  app.set('trust proxy', false);
}
```

---

### 7. Rate Limiting Configuration (MEDIUM)

**Location:** [`src/EdgeWARN/api/server.js:68-73`](src/EdgeWARN/api/server.js:68-73)

**Description:**
Current rate limiting: 100 requests per 20 seconds per IP.

This may be too permissive for a public API and doesn't differentiate between endpoints (static data vs computationally expensive operations).

**Remediation:**
Implement tiered rate limiting:

```javascript
// Stricter limit for expensive operations
const strictLimiter = rateLimit({
  windowMs: 60 * 1000, // 1 minute
  max: 30,
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many requests, please try again later' }
});

// More lenient for simple reads
const standardLimiter = rateLimit({
  windowMs: 60 * 1000,
  max: 100,
  standardHeaders: true,
  legacyHeaders: false
});

// Apply strict limiter to routes that do more processing
app.use('/api/v2/data/nws', strictLimiter, nwsRouter);
app.use('/api/v2/features/cells', standardLimiter, cellsRouter);
```

---

### 8. API Version Disclosure (MEDIUM)

**Locations:**
- [`src/EdgeWARN/api/server.js:80`](src/EdgeWARN/api/server.js:80)
- [`src/EdgeWARN/api/routes/v2/index.js:21`](src/EdgeWARN/api/routes/v2/index.js:21)

**Description:**
API version is exposed in responses, which aids attackers in finding known vulnerabilities for that version.

**Remediation:**
Remove version from public responses or make it configurable:

```javascript
// In production, don't expose detailed version
const version = process.env.NODE_ENV === 'production' 
  ? '2.x' 
  : '2.0.0';
```

---

## Low Priority Issues

### 9. Missing Security Headers on Specific Responses (LOW)

**Description:**
Some responses may not include all recommended security headers. While Helmet is used, certain headers should be verified.

**Remediation:**
Add middleware to ensure consistent headers:

```javascript
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('X-XSS-Protection', '1; mode=block');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  next();
});
```

---

### 10. No Authentication/Authorization (LOW)

**Description:**
The API currently has no authentication mechanism. While this may be intentional for a public API, it's worth documenting the decision.

**Remediation:**
If authentication is needed later, consider:
- API keys for rate limit differentiation
- JWT tokens for authenticated endpoints
- OAuth2 for user-specific data

Document the public API decision in the API documentation.

---

## Remediation Priority Matrix

| Issue | Severity | Effort | Priority |
|-------|----------|--------|----------|
| 1. Prototype Pollution | Critical | Low | P0 - Fix Immediately |
| 2. Error Disclosure | High | Low | P1 |
| 3. CORS Configuration | High | Low | P1 |
| 4. HSTS Disabled | High | Low | P1 |
| 5. Health Info Disclosure | High | Low | P1 |
| 6. Trust Proxy | Medium | Low | P2 |
| 7. Rate Limiting | Medium | Medium | P2 |
| 8. Version Disclosure | Medium | Low | P2 |
| 9. Security Headers | Low | Low | P3 |
| 10. No Auth | Low | High | P3 |

---

## Security Testing Recommendations

1. **Add security-focused tests:**
   - Test for prototype pollution with `__proto__`, `constructor`, `prototype`
   - Test path traversal attempts in file operations
   - Test CORS preflight requests
   - Verify rate limiting works correctly

2. **Implement security scanning:**
   - Add `npm audit` to CI/CD pipeline
   - Consider using Snyk or similar dependency scanners
   - Run OWASP ZAP for API penetration testing

3. **Regular security audits:**
   - Review code quarterly for new vulnerabilities
   - Keep dependencies updated
   - Monitor security advisories for Express.js ecosystem

---

## Appendix: Quick Fixes

### Immediate Patches (Copy-Paste Ready)

#### Fix 1: Add alert ID validation
```javascript
// In validation.js
export function validateAlertId(id) {
  if (typeof id !== 'string' || id.length > 100) return false;
  return /^[a-zA-Z0-9_-]+$/.test(id);
}
```

#### Fix 2: Secure error handling
```javascript
// In server.js error handler
app.use((err, req, res, next) => {
  const isDev = process.env.NODE_ENV !== 'production';
  console.error(isDev ? err.stack : `Error: ${err.message}`);
  res.status(500).json({ 
    error: isDev ? err.message : 'Internal server error' 
  });
});
```

#### Fix 3: Secure CORS
```javascript
const allowedOrigins = process.env.ALLOWED_ORIGINS?.split(',').map(o => o.trim()) || [];
app.use(cors({
  origin: allowedOrigins.length > 0 ? allowedOrigins : false,
  credentials: true
}));
```

---

*End of Security Audit Report*
