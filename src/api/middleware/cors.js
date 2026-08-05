import cors from 'cors';

export function createCors(allowedOrigins) {
  const allowed = new Set(allowedOrigins);
  return cors({
    origin(origin, callback) {
      if (!origin) return callback(null, false);
      return callback(null, allowed.has(origin) ? origin : false);
    },
    credentials: false,
    methods: ['GET', 'HEAD', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'X-Request-Id'],
    maxAge: 600
  });
}
