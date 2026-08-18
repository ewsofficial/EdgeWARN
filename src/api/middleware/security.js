import compression from 'compression';
import helmet from 'helmet';

export function securityMiddleware(policy) {
  return [
    helmet({
      contentSecurityPolicy: { useDefaults: true, directives: { defaultSrc: [policy.csp_default_src ?? "'self'"] } },
      strictTransportSecurity: { maxAge: policy.hsts_max_age_seconds }
    }),
    compression({
      filter(req, res) {
        const type = res.getHeader('Content-Type');
        const skipMedia = policy.compression_skip_media ?? 'image/*';
        const mediaPattern = new RegExp(`^${skipMedia.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace('\\*', '.*')}$`, 'i');
        return !(typeof type === 'string' && mediaPattern.test(type)) && compression.filter(req, res);
      }
    })
  ];
}

export function requestTimeout(timeoutMs) {
  return (req, res, next) => {
    res.setTimeout(timeoutMs, () => {
      if (!res.headersSent) res.status(503).json({ error: 'Request timed out' });
    });
    next();
  };
}
