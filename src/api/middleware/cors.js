import cors from 'cors';

export function createCors(allowedOrigins, policy) {
  const allowed = new Set(allowedOrigins);
  return cors({
    origin(origin, callback) {
      if (!origin) return callback(null, false);
      return callback(null, allowed.has(origin) ? origin : false);
    },
    credentials: policy.credentials,
    methods: policy.methods,
    allowedHeaders: policy.allowed_headers,
    maxAge: policy.max_age
  });
}
