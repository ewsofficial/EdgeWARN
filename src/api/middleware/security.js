import compression from 'compression';
import helmet from 'helmet';

export function securityMiddleware() {
  return [
    helmet({ contentSecurityPolicy: { useDefaults: true, directives: { defaultSrc: ["'self'"] } } }),
    compression({
      filter(req, res) {
        const type = res.getHeader('Content-Type');
        return !(typeof type === 'string' && /^image\//i.test(type)) && compression.filter(req, res);
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
