# EdgeWARN API v2 Technical Implementation

This document describes the technical implementation of the EdgeWARN API v2, including the server architecture, middleware, routing, and utility functions.

## Server Architecture

The EdgeWARN API is built with Node.js and Express.js, using a modular architecture. The server is designed to be scalable, secure, and efficient.

### File Structure

```
src/EdgeWARN/api/
├── server.js                    # Main server entry point
├── config.js                    # Configuration and directory setup
├── robots.txt                   # Robots.txt for search engines
├── routes/
│   ├── health.js                # Health check route
│   └── v2/
│       ├── index.js             # API v2 router
│       ├── features/
│       │   ├── cells.js         # Cells endpoint
│       │   ├── alerts.js        # Alerts endpoint
│       │   └── timestamps.js    # Timestamps endpoint
│       └── data/
│           └── metar.js         # METAR endpoint
└── utils/
    ├── fileReader.js            # File reading and caching utilities
    └── validation.js            # Input validation utilities
```

## Server Configuration

### server.js

The main server entry point that sets up the Express application with all middleware and routes.

**Key Features**:
- Cluster support for multi-core processing
- Security middleware (Helmet)
- Compression middleware
- CORS configuration
- Rate limiting
- Error handling
- Route mounting

**Cluster Configuration**:
The server uses Node.js clustering to take advantage of multiple CPU cores. By default, it forks up to 4 workers.

```javascript
const numCPUs = Math.min(os.cpus().length, 4);
```

### config.js

Handles application configuration, including:
- Base directory detection
- Directory paths for data storage
- Debug server mode
- Port configuration
- Directory validation and creation

**Base Directory Detection Order**:
1. CLI argument `--base-dir`
2. Environment variable `EDGEWARN_BASE_DIR`
3. Platform-specific defaults:
   - Windows: `C:\EdgeWARN_input`
   - Linux/macOS: User home directory, then common locations

## Routing

### v2/index.js

The main API v2 router that mounts all feature and data routes.

```javascript
import express from 'express';
import cellsRouter from './features/cells.js';
import timestampsRouter from './features/timestamps.js';
import alertsRouter from './features/alerts.js';
import metarRouter from './data/metar.js';

const router = express.Router();

router.use('/features/cells', cellsRouter);
router.use('/features/timestamps', timestampsRouter);
router.use('/features/alerts', alertsRouter);
router.use('/data/metar', metarRouter);
```

### Feature Routes

#### cells.js

Handles cell data retrieval.

**Key Functions**:
- `GET /api/v2/features/cells` - Returns cell IDs or specific cell data
- Validates cell ID parameter
- Caches cell data for 60 seconds
- Reads data from `cell_index.json` and individual cell files

#### alerts.js

Handles alert data retrieval for both official NWS alerts and EdgeWARN alerts.

**Key Functions**:
- `GET /api/v2/features/alerts/official` - Returns official NWS alerts
- `GET /api/v2/features/alerts/edgewarn` - Returns EdgeWARN alerts
- Handles both `id` and `timestamp` query parameters
- Validates input parameters
- Caches alert data for 60 seconds

#### timestamps.js

Handles timestamp data retrieval and storm cell data by timestamp.

**Key Functions**:
- `GET /api/v2/features/timestamps` - Returns available timestamps
- `GET /api/v2/features/timestamps?timestamp={YYYYMMDD-HHMMSS}` - Returns storm cell data for specific timestamp
- Reads from `stormcell_index.json`
- Caches data for 5 seconds (timestamps) or 1 hour (stormcell data)

### Data Routes

#### metar.js

Handles METAR weather observation retrieval.

**Key Functions**:
- `GET /api/v2/data/metar` - Returns available timestamps
- `GET /api/v2/data/metar?timestamp={YYYYMMDD-HHMMSS}` - Returns METAR data for specific timestamp
- Handles hourly METAR files
- Formats timestamps from `YYYYMMDD-HH` to `YYYYMMDD-HHMMSS`

### Health Route

#### health.js

Simple health check endpoint.

```javascript
router.get('/', (req, res) => {
  res.json({
    status: 'OK',
    timestamp: new Date().toISOString()
  });
});
```

## Utility Functions

### fileReader.js

Provides safe file reading with path traversal protection and caching.

**Key Functions**:
- `isSafeFilename(name)` - Validates filename to prevent path traversal
- `readJsonFileSafe(dir, name, options)` - Reads JSON file with traversal protection
- `readIndexFile(indexPath)` - Reads index files with shorter cache TTL

**Caching**:
Uses `lru-cache` with:
- Max 500 items
- Default TTL: 1 minute (60,000 ms)
- Max size: 40 MB per worker

Index files (cell_index.json, stormcell_index.json) have a shorter TTL of 5 seconds.

### validation.js

Provides input validation utilities.

**Key Functions**:
- `validateResourceType(type)` - Validates resource type
- `validateTimestamp(timestamp)` and `validateTimestampV2(timestamp)` - Validates timestamp format
- `validateMutualExclusion(params, key1, key2)` - Ensures parameters are mutually exclusive
- `validateCellId(id)` - Validates cell ID is a positive integer
- `validateAlertId(id)` - Validates alert ID format and prevents prototype pollution

## Middleware

### Security (Helmet)

```javascript
app.use(helmet({
  hsts: {
    maxAge: 31536000, // 1 year
    includeSubDomains: true
  },
  contentSecurityPolicy: {
    useDefaults: true,
    directives: {
      "default-src": ["'self'"],
    }
  }
}));
```

### CORS

```javascript
const allowedOrigins = process.env.ALLOWED_ORIGINS
  ? process.env.ALLOWED_ORIGINS.split(',').map(o => o.trim())
  : (process.env.NODE_ENV === 'production' ? [] : ['http://localhost:3000', 'http://localhost:8080']);

app.use(cors({
  origin: allowedOrigins.length > 0 ? allowedOrigins : false,
  credentials: true,
  methods: ['GET', 'HEAD', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));
```

### Rate Limiting

```javascript
const limiter = rateLimit({
  windowMs: parseInt(process.env.RATE_LIMIT_WINDOW_MS, 10) || 60 * 1000, // 1 minute
  max: parseInt(process.env.RATE_LIMIT_MAX, 10) || 60, // 60 requests
  standardHeaders: true,
  legacyHeaders: false,
  message: { error: 'Too many requests, please try again later' },
  // Skip rate limiting for health checks from internal monitoring
  skip: (req) => {
    return req.path === '/health' && req.headers['x-internal-check'] === 'true';
  },
  // Custom key generator to handle proxy environments
  keyGenerator: (req) => {
    // ...
  }
});
```

## Performance Optimizations

1. **Clustering**: Takes advantage of multi-core processors
2. **Caching**: LRU cache for frequent requests
3. **Compression**: Gzip compression for responses
4. **Efficient File Reading**: Optimized with fs.promises and cache
5. **Path Traversal Protection**: Validates filenames and paths

## Error Handling

```javascript
app.use((err, req, res, next) => {
  const isDev = process.env.NODE_ENV !== 'production';
  console.error(isDev ? err.stack : `Error: ${err.message}`);
  res.status(500).json({
    error: isDev ? err.message : 'Internal server error'
  });
});
```

Error responses are sanitized in production to prevent information leakage.

## Environment Variables

Key environment variables:
- `PORT`: Server port
- `NODE_ENV`: Environment (development/production)
- `ALLOWED_ORIGINS`: CORS allowed origins
- `RATE_LIMIT_WINDOW_MS`: Rate limit window
- `RATE_LIMIT_MAX`: Rate limit maximum requests
- `TRUST_PROXY`: Whether to trust proxy headers
- `TRUST_PROXY_IPS`: Specific proxy IPs to trust
- `EDGEWARN_BASE_DIR`: Base directory for data

## Debug Server Mode

The server can be run in debug mode using:

```bash
npm run debug
```

Debug mode uses port 3001 instead of 5000 and provides more detailed error messages.

## Deployment

The API can be deployed using:

1. **Production**: `npm start`
2. **Development**: `npm run dev` (with watch mode)
3. **Debug**: `npm run debug`
