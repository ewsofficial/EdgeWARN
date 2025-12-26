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
 * Validate cell ID (must be positive integer)
 * @param {string|number} id - Cell ID
 * @returns {boolean} True if valid
 */
export function validateCellId(id) {
  const num = parseInt(id, 10);
  return !isNaN(num) && num > 0 && num.toString() === id.toString();
}
