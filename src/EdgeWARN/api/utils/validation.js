/**
 * Input validation utilities for API endpoints
 */

/**
 * Validate resource type parameter
 * @param {string} type - Resource type ("cell" or "list")
 * @returns {boolean} True if valid
 */
export function validateResourceType(type) {
  return type === 'cell' || type === 'list';
}

/**
 * Validate timestamp format (YYYYMMDD-HHMMSS)
 * @param {string} timestamp - Timestamp string
 * @returns {boolean} True if valid format
 */
export function validateTimestamp(timestamp) {
  if (!timestamp) return false;
  // Format: YYYYMMDD-HHMMSS
  const regex = /^\d{8}-\d{6}$/;
  return regex.test(timestamp);
}

/**
 * Validate timestamp format for v2 API (YYYYMMDD-HHMMSS)
 * @param {string} timestamp - Timestamp string
 * @returns {boolean} True if valid format
 */
export function validateTimestampV2(timestamp) {
  if (!timestamp) return false;
  // Format: YYYYMMDD-HHMMSS (same as existing validateTimestamp)
  const regex = /^\d{8}-\d{6}$/;
  return regex.test(timestamp);
}

/**
 * Validate mutual exclusion - ensures two parameters are not both present
 * @param {object} params - Object containing query parameters
 * @param {string} key1 - First parameter name
 * @param {string} key2 - Second parameter name
 * @returns {boolean} True if valid (not both present)
 */
export function validateMutualExclusion(params, key1, key2) {
  const hasKey1 = params[key1] !== undefined && params[key1] !== '';
  const hasKey2 = params[key2] !== undefined && params[key2] !== '';
  return !(hasKey1 && hasKey2);
}

/**
 * Validate cell ID (must be positive integer)
 * @param {string|number} id - Cell ID
 * @returns {boolean} True if valid
 */
export function validateCellId(id) {
  const num = parseInt(id, 10);
  return !isNaN(num) && num > 0 && num.toString() === id.toString();
}

/**
 * Validate alert ID (must be alphanumeric with hyphens, underscores, dots, and colons)
 * Prevents prototype pollution by rejecting special property names
 * @param {string} id - Alert ID
 * @returns {boolean} True if valid
 */
export function validateAlertId(id) {
  if (typeof id !== 'string' || id.length === 0 || id.length > 200) return false;
  // Reject prototype pollution attempts
  if (id === '__proto__' || id === 'constructor' || id === 'prototype') return false;
  // Allow alphanumeric, hyphens, underscores, dots, and colons (for URN-style IDs like urn:oid:...)
  return /^[a-zA-Z0-9_.:-]+$/.test(id);
}
