import rateLimit from 'express-rate-limit';

export function createRateLimiters({ perSecond, perMinute, perSecondWindowMs, perMinuteWindowMs, standardHeaders, legacyHeaders }) {
  const build = (windowMs, max) => max > 0 ? rateLimit({ windowMs, max, standardHeaders, legacyHeaders, message: { error: 'Too many requests, please try again later' } }) : null;
  return [build(perSecondWindowMs, perSecond), build(perMinuteWindowMs, perMinute)].filter(Boolean);
}
