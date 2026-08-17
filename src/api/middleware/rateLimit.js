import rateLimit from 'express-rate-limit';

export function createRateLimiters({ perSecond, perMinute, perSecondWindowMs = 1_000, perMinuteWindowMs = 60_000, standardHeaders = true, legacyHeaders = false }) {
  const build = (windowMs, max) => max > 0 ? rateLimit({ windowMs, max, standardHeaders, legacyHeaders, message: { error: 'Too many requests, please try again later' } }) : null;
  return [build(perSecondWindowMs, perSecond), build(perMinuteWindowMs, perMinute)].filter(Boolean);
}
