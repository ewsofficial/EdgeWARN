// Route-family gating (decomposition Phase 3): requests whose required
// service is not active fail with a structured SERVICE_NOT_ENABLED error
// instead of silently serving stale artifacts. Degraded services still serve.

export function createServiceGate({ serviceRegistry, service, respond }) {
  return (req, res, next) => {
    if (!serviceRegistry) return next();
    const { state, heartbeat } = serviceRegistry.stateFor(service);
    if (state === 'active' || state === 'degraded') return next();

    const lastSeen = heartbeat ? heartbeat.updatedAt.toISOString() : null;
    return respond(req, res, {
      code: 'SERVICE_NOT_ENABLED',
      message: 'Required service is not active',
      service,
      state,
      lastSeen,
    });
  };
}

export function problemJsonResponder(apiConfig) {
  return (req, res, error) => {
    res.status(503).type('application/problem+json').json({
      type: 'about:blank',
      title: 'Service Not Enabled',
      status: 503,
      detail: `Required service '${error.service}' is ${error.state}`,
      instance: req.originalUrl,
      requestId: req.requestId,
      code: error.code,
      service: error.service,
      state: error.state,
      lastSeen: error.lastSeen,
    });
  };
}

export function legacyEnvelopeResponder() {
  // The documented compatibility envelope from
  // plans/realtime-runner-decomposition-plan.md.
  return (req, res, error) => {
    res.status(503).json({
      success: false,
      error: {
        code: error.code,
        message: error.message,
        service: error.service,
        state: error.state,
        last_seen: error.lastSeen,
      },
    });
  };
}
