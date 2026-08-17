import cors from 'cors';

export function createCors(allowedOrigins, policy = {}) {
  const allowed = new Set(allowedOrigins);
  return cors({
    origin(origin, callback) {
      if (!origin) return callback(null, false);
      return callback(null, allowed.has(origin) ? origin : false);
    },
    credentials: policy.credentials ?? false,
    methods: policy.methods ?? ['GET', 'HEAD', 'OPTIONS'],
    allowedHeaders: policy.allowed_headers ?? ['Content-Type', 'X-Request-Id'],
    maxAge: policy.max_age ?? 600
  });
}
