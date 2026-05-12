const NEXRAD_SITE_PATTERN = /^[A-Z0-9]{4}$/;
const NEXRAD_TIMESTAMP_PATTERN = /^\d{8}-\d{6}$/;
const NEXRAD_ELEVATION_PATTERN = /^\d{1,3}(?:\.\d{1,2})?$/;

export const ALLOWED_NEXRAD_PRODUCTS = new Set([
  'DBZH',
  'VRADH',
  'WRADH',
  'PHIDP',
  'CCORH',
  'RHOHV',
  'ZDR'
]);

function hasTraversalChars(value) {
  return value.includes('..') || value.includes('/') || value.includes('\\');
}

export function normalizeNexradSite(site) {
  return typeof site === 'string' ? site.trim().toUpperCase() : '';
}

export function isSafeNexradSite(site) {
  if (typeof site !== 'string') {
    return false;
  }

  const normalized = normalizeNexradSite(site);
  return normalized.length === 4 && !hasTraversalChars(normalized) && NEXRAD_SITE_PATTERN.test(normalized);
}

function isValidDateParts(year, month, day, hour, minute, second) {
  const date = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
  return date.getUTCFullYear() === year &&
    date.getUTCMonth() === month - 1 &&
    date.getUTCDate() === day &&
    date.getUTCHours() === hour &&
    date.getUTCMinutes() === minute &&
    date.getUTCSeconds() === second;
}

export function isSafeNexradTimestamp(timestamp) {
  if (typeof timestamp !== 'string' || hasTraversalChars(timestamp) || !NEXRAD_TIMESTAMP_PATTERN.test(timestamp)) {
    return false;
  }

  const year = Number.parseInt(timestamp.slice(0, 4), 10);
  const month = Number.parseInt(timestamp.slice(4, 6), 10);
  const day = Number.parseInt(timestamp.slice(6, 8), 10);
  const hour = Number.parseInt(timestamp.slice(9, 11), 10);
  const minute = Number.parseInt(timestamp.slice(11, 13), 10);
  const second = Number.parseInt(timestamp.slice(13, 15), 10);

  return isValidDateParts(year, month, day, hour, minute, second);
}

export function isSafeNexradElevation(elevation) {
  if (typeof elevation !== 'string') {
    return false;
  }

  const normalized = elevation.trim();
  if (!normalized || normalized.length > 8 || hasTraversalChars(normalized)) {
    return false;
  }

  if (!NEXRAD_ELEVATION_PATTERN.test(normalized)) {
    return false;
  }

  const numeric = Number.parseFloat(normalized);
  return Number.isFinite(numeric) && numeric >= 0;
}

export function parseNexradElevationNumber(elevation) {
  return Number.parseFloat(elevation);
}

export function isAllowedNexradProduct(product) {
  return typeof product === 'string' && ALLOWED_NEXRAD_PRODUCTS.has(product);
}
