const TIMESTAMP = /^\d{8}-\d{6}$/;
const CELL_ID = /^[1-9][0-9]*$/;
const ALERT_ID = /^[A-Za-z0-9_.:-]{1,200}$/;
const LAYER_ID = /^[A-Za-z0-9_.-]{1,128}$/;

export function timestamp(value) {
  if (typeof value !== 'string' || !TIMESTAMP.test(value)) return null;
  const year = Number(value.slice(0, 4)); const month = Number(value.slice(4, 6)); const day = Number(value.slice(6, 8));
  const hour = Number(value.slice(9, 11)); const minute = Number(value.slice(11, 13)); const second = Number(value.slice(13, 15));
  const date = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
  return date.getUTCFullYear() === year && date.getUTCMonth() === month - 1 && date.getUTCDate() === day && date.getUTCHours() === hour && date.getUTCMinutes() === minute && date.getUTCSeconds() === second ? date.toISOString() : null;
}

export const isCellId = (value) => typeof value === 'string' && CELL_ID.test(value);
export const isAlertId = (value) => typeof value === 'string' && ALERT_ID.test(value) && !['__proto__', 'constructor', 'prototype'].includes(value);
export const isLayerId = (value) => typeof value === 'string' && LAYER_ID.test(value) && !value.includes('..');
export function page(items, { cursor, limit = 100 } = {}) {
  const safeLimit = Number.isInteger(limit) && limit > 0 ? Math.min(limit, 1000) : 100;
  const start = cursor ? Math.max(0, items.indexOf(cursor) + 1) : 0;
  const data = items.slice(start, start + safeLimit);
  return { data, nextCursor: start + safeLimit < items.length ? data.at(-1) : null };
}
