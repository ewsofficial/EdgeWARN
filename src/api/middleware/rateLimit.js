import rateLimit from 'express-rate-limit';

export function createRateLimiters({ perSecond, perMinute }) {
  const build = (windowMs, max) => max > 0 ? rateLimit({ windowMs, max, standardHeaders: true, legacyHeaders: false, message: { error: 'Too many requests, please try again later' } }) : null;
  return [build(1_000, perSecond), build(60_000, perMinute)].filter(Boolean);
}
