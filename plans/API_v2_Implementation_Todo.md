# API v2 Implementation Checklist

This document provides step-by-step tasks for implementing the EdgeWARN API v2.

## Pre-Implementation

- [ ] Review the main implementation plan at [`plans/API_v2_Implementation_Plan.md`](plans/API_v2_Implementation_Plan.md:1)
- [ ] Ensure all existing tests pass: `npm test`

---

## Phase 1: Update Validation Utilities

**File:** [`src/EdgeWARN/api/utils/validation.js`](src/EdgeWARN/api/utils/validation.js:1)

Add the following new validation functions:

```javascript
/**
 * Validate timestamp format for v2 API (YYYYMMDD-HHMMSS)
 * @param {string} timestamp - Timestamp string
 * @returns {boolean} True if valid format
 */
export function validateTimestampV2(timestamp) {
  if (!timestamp) return false;
  // Format: YYYYMMDD-HHMMSS (same as existing validateTimestamp)
  const regex = /^\d{8}-\d{6}$/;
  return regex.test(timestamp);
}

/**
 * Validate mutual exclusion - ensures two parameters are not both present
 * @param {object} params - Object containing query parameters
 * @param {string} key1 - First parameter name
 * @param {string} key2 - Second parameter name
 * @returns {boolean} True if valid (not both present)
 */
export function validateMutualExclusion(params, key1, key2) {
  const hasKey1 = params[key1] !== undefined && params[key1] !== '';
  const hasKey2 = params[key2] !== undefined && params[key2] !== '';
  return !(hasKey1 && hasKey2);
}
```

---

## Phase 2: Create v2 Route Files

### 2.1 Create Directory Structure

```bash
mkdir -p src/EdgeWARN/api/routes/v2/features
mkdir -p src/EdgeWARN/api/routes/v2/data
```

### 2.2 Create v2 Index Router

**File:** `src/EdgeWARN/api/routes/v2/index.js`

```javascript
import express from 'express';
import cellsRouter from './features/cells.js';
import timestampsRouter from './features/timestamps.js';
import nwsRouter from './data/nws.js';
import metarRouter from './data/metar.js';

const router = express.Router();

// Mount v2 feature routes
router.use('/features/cells', cellsRouter);
router.use('/features/timestamps', timestampsRouter);

// Mount v2 data routes
router.use('/data/nws', nwsRouter);
router.use('/data/metar', metarRouter);

// Root v2 endpoint
router.get('/', (req, res) => {
  res.json({
    message: 'EdgeWARN API v2',
    version: '2.0.0',
    endpoints: {
      features: {
        cells: '/api/v2/features/cells[?id={int}]',
        timestamps: '/api/v2/features/timestamps[?timestamp={YYYYMMDD-HHMMSS}]'
      },
      data: {
        nws: '/api/v2/data/nws[?timestamp={YYYYMMDD-HHMMSS}|id={alert_id}]',
        metar: '/api/v2/data/metar[?timestamp={YYYYMMDD-HHMMSS}]'
      }
    }
  });
});

export default router;
```

### 2.3 Create Features - Cells Endpoint

**File:** `src/EdgeWARN/api/routes/v2/features/cells.js`

**Requirements:**
- GET endpoint at `/`
- Without `?id` parameter: Return list of cell IDs from [`cell_index.json`](src/EdgeWARN/api/config.js:74)
- With `?id={int}` parameter: Return specific cell data from `{id}.json`
- Use existing utilities: [`validateCellId()`](src/EdgeWARN/api/utils/validation.js:31), [`readIndexFile()`](src/EdgeWARN/api/utils/fileReader.js:80), [`readJsonFileSafe()`](src/EdgeWARN/api/utils/fileReader.js:35)
- Use config: [`apiConfig.CELL_DIR`](src/EdgeWARN/api/config.js:74), [`apiConfig.STORMCELL_DIR`](src/EdgeWARN/api/config.js:73)
- Cache headers: 5 seconds for list, 60 seconds for individual cells

**Response formats:**
- List: `[1, 2, 3, 5, 8]`
- Single cell: Full cell JSON object

### 2.4 Create Features - Timestamps Endpoint

**File:** `src/EdgeWARN/api/routes/v2/features/timestamps.js`

**Requirements:**
- GET endpoint at `/`
- Without `?timestamp` parameter: Return list of timestamps from [`stormcell_index.json`](src/EdgeWARN/api/config.js:73)
- With `?timestamp={YYYYMMDD-HHMMSS}`: Return stormcell list for that timestamp from `stormcells_{timestamp}.json`
- Use existing utilities: [`validateTimestampV2()`](src/EdgeWARN/api/utils/validation.js:1), [`readIndexFile()`](src/EdgeWARN/api/utils/fileReader.js:80), [`readJsonFileSafe()`](src/EdgeWARN/api/utils/fileReader.js:35)
- Use config: [`apiConfig.STORMCELL_DIR`](src/EdgeWARN/api/config.js:73)
- Cache headers: 5 seconds for list, 3600 seconds for stormcell data

**Response formats:**
- List: `["20260123-150000", "20260123-143000"]`
- Single timestamp: Stormcells JSON object

### 2.5 Create Data - NWS Endpoint

**File:** `src/EdgeWARN/api/routes/v2/data/nws.js`

**Requirements:**
- GET endpoint at `/`
- No parameters: Return list of timestamps (scan directory for `nws_snapshot_{timestamp}.json` files)
- With `?timestamp={YYYYMMDD-HHMMSS}`: Return NWS snapshot for that timestamp
- With `?id={alert_id}`: Return specific alert from [`alerts_registry.json`](src/EdgeWARN/api/routes/data/download.js:49)
- **Validation:** Return 400 if both `timestamp` and `id` are provided (use [`validateMutualExclusion()`](src/EdgeWARN/api/utils/validation.js:1))
- Use config: [`apiConfig.NWS_DIR`](src/EdgeWARN/api/config.js:76)
- Cache headers: 5 seconds for list, 60 seconds for data

**Response formats:**
- List: `["20260123-150000", "20260123-143000"]`
- Snapshot: `{ "timestamp": "...", "count": 5, "alerts": [...] }`
- Single alert: Alert object from registry

### 2.6 Create Data - METAR Endpoint

**File:** `src/EdgeWARN/api/routes/v2/data/metar.js`

**Requirements:**
- GET endpoint at `/`
- No parameters: Return list of timestamps from METAR files (format: `METAR_YYYYMMDD-HHz.json`)
- With `?timestamp={YYYYMMDD-HHMMSS}`: Return METAR data for that timestamp
- Use existing utilities: [`validateTimestampV2()`](src/EdgeWARN/api/utils/validation.js:1), [`readJsonFileSafe()`](src/EdgeWARN/api/utils/fileReader.js:35)
- Use config: [`apiConfig.METAR_DIR`](src/EdgeWARN/api/config.js:75)
- Note: METAR files use hourly format - extract hour from timestamp
- Cache headers: 5 seconds for list, 60 seconds for data

**Response formats:**
- List: `["20260123-150000", "20260123-140000"]`
- Single timestamp: `{ "timestamp": "...", "data": {...} }`

---

## Phase 3: Update Server Configuration

**File:** [`src/EdgeWARN/api/server.js`](src/EdgeWARN/api/server.js:1)

Add v2 router import and mounting:

```javascript
// Add import near top (after existing imports)
import v2Router from './routes/v2/index.js';

// Add route mounting (after v1 routes)
app.use('/api/v2', v2Router);
```

---

## Phase 4: Testing

### 4.1 Create Test Directory Structure

```bash
mkdir -p tests/api/v2/features
mkdir -p tests/api/v2/data
```

### 4.2 Create Tests for Cells Endpoint

**File:** `tests/api/v2/features/cells.test.js`

Test cases to implement:
- `GET /api/v2/features/cells` returns array of cell IDs
- `GET /api/v2/features/cells?id=123` returns specific cell
- `GET /api/v2/features/cells?id=invalid` returns 400
- `GET /api/v2/features/cells?id=99999` returns 404 for non-existent cell

### 4.3 Create Tests for Timestamps Endpoint

**File:** `tests/api/v2/features/timestamps.test.js`

Test cases to implement:
- `GET /api/v2/features/timestamps` returns array of timestamps
- `GET /api/v2/features/timestamps?timestamp=20260123-150000` returns stormcells
- `GET /api/v2/features/timestamps?timestamp=invalid` returns 400
- `GET /api/v2/features/timestamps?timestamp=20991231-000000` returns 404

### 4.4 Create Tests for NWS Endpoint

**File:** `tests/api/v2/data/nws.test.js`

Test cases to implement:
- `GET /api/v2/data/nws` returns array of timestamps
- `GET /api/v2/data/nws?timestamp=20260123-150000` returns snapshot
- `GET /api/v2/data/nws?id=urn:oid:...` returns specific alert
- `GET /api/v2/data/nws?timestamp=...&id=...` returns 400 (mutual exclusion)
- `GET /api/v2/data/nws?timestamp=invalid` returns 400

### 4.5 Create Tests for METAR Endpoint

**File:** `tests/api/v2/data/metar.test.js`

Test cases to implement:
- `GET /api/v2/data/metar` returns array of timestamps
- `GET /api/v2/data/metar?timestamp=20260123-150000` returns METAR data
- `GET /api/v2/data/metar?timestamp=invalid` returns 400
- `GET /api/v2/data/metar?timestamp=20991231-000000` returns 404

### 4.6 Run All Tests

```bash
npm test
```

Ensure all tests pass before proceeding.

---

## Phase 5: Documentation Updates

### 5.1 Update API Documentation

**File:** [`docs/API.md`](docs/API.md:1)

Add new section for API v2:
- Add v2 endpoints section after v1 documentation
- Include request/response examples for all v2 endpoints
- Document query parameters and validation rules
- Add migration guide from v1 to v2

### 5.2 Update Changelog

**File:** [`CHANGELOG.md`](CHANGELOG.md:1)

Add entry for v2 API addition:
```markdown
## [Unreleased]

### Added
- New API v2 endpoints with RESTful design:
  - GET /api/v2/features/cells
  - GET /api/v2/features/timestamps
  - GET /api/v2/data/nws
  - GET /api/v2/data/metar
```

---

## Implementation Order

1. **Start with validation utilities** - These are dependencies for all routes
2. **Create v2 route files** - Follow the order: cells → timestamps → metar → nws
3. **Update server.js** - Mount the v2 router
4. **Write tests** - Test each endpoint as you implement it
5. **Update documentation** - Document what you've built

---

## Code Patterns to Follow

### Route Handler Pattern

```javascript
import express from 'express';
import apiConfig from '../../config.js';
import { readJsonFileSafe, readIndexFile } from '../../utils/fileReader.js';
import { validateXxx } from '../../utils/validation.js';

const router = express.Router();

router.get('/', async (req, res) => {
  const { param } = req.query;
  
  // If specific resource requested
  if (param) {
    // Validate parameter
    if (!validateXxx(param)) {
      return res.status(400).json({ error: 'Invalid parameter' });
    }
    
    try {
      res.set('Cache-Control', 'public, max-age=60');
      const data = await readJsonFileSafe(dir, filename);
      res.json(data);
    } catch (err) {
      if (err.code === 'ENOENT') {
        return res.status(404).json({ error: 'Not found' });
      }
      console.error('Error:', err);
      res.status(500).json({ error: 'Internal server error' });
    }
    return;
  }
  
  // List all resources
  try {
    res.set('Cache-Control', 'public, max-age=5');
    const indexData = await readIndexFile(indexPath);
    res.json(indexData.items || []);
  } catch (err) {
    if (err.code === 'ENOENT') {
      return res.json([]);
    }
    console.error('Error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

export default router;
```

---

## Post-Implementation Checklist

- [ ] All v2 endpoints respond correctly
- [ ] All tests pass
- [ ] v1 endpoints still work (backward compatibility)
- [ ] Documentation is updated
- [ ] Changelog is updated
- [ ] Code review completed
