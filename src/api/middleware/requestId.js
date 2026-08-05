import crypto from 'crypto';

const REQUEST_ID = /^[A-Za-z0-9_-]{8,128}$/;

export function requestId(req, res, next) {
  const supplied = req.get('x-request-id');
  req.requestId = supplied && REQUEST_ID.test(supplied) ? supplied : crypto.randomUUID();
  res.set('X-Request-Id', req.requestId);
  next();
}
