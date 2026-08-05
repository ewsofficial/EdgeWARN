function templatePattern(template) {
  return new RegExp(`^${template.replace(/[.*+?^${}()|[\]\\]/g, '\\$&').replace(/\\\{[^}]+\\\}/g, '[^/]+')}$`);
}

export function createAccessLog(routeTemplates = []) {
  const templates = routeTemplates.map((template) => ({ template, pattern: templatePattern(template) }));
  return (req, res, next) => {
    const started = process.hrtime.bigint();
    res.on('finish', () => {
      const pathname = new URL(req.originalUrl, 'http://edgewarn.invalid').pathname;
      const route = templates.find((candidate) => candidate.pattern.test(pathname))?.template || req.route?.path || 'unmatched';
      console.info(JSON.stringify({
        event: 'api_access', requestId: req.requestId, method: req.method,
        route, status: res.statusCode,
        bytes: Number(res.getHeader('content-length') || 0), durationMs: Number(process.hrtime.bigint() - started) / 1e6
      }));
    });
    next();
  };
}
