import { describe, expect, it } from '@jest/globals';

import {
  isAllowedNexradProduct,
  isSafeNexradElevation,
  isSafeNexradSite,
  isSafeNexradTimestamp,
  normalizeNexradSite,
} from '../../../src/EWMRS/api/routes/nexrad/validation.js';

describe('EWMRS NEXRAD validation helpers', () => {
  it('normalizes and validates safe sites', () => {
    expect(normalizeNexradSite('ktlh')).toBe('KTLH');
    expect(isSafeNexradSite('KTLH')).toBe(true);
    expect(isSafeNexradSite('ktlh')).toBe(true);
  });

  it('rejects traversal and malformed site values', () => {
    expect(isSafeNexradSite('../')).toBe(false);
    expect(isSafeNexradSite('KT/L')).toBe(false);
    expect(isSafeNexradSite('KTLH..')).toBe(false);
    expect(isSafeNexradSite('KTL')).toBe(false);
  });

  it('validates timestamps with real calendar bounds', () => {
    expect(isSafeNexradTimestamp('20260512-004336')).toBe(true);
    expect(isSafeNexradTimestamp('20261312-004336')).toBe(false);
    expect(isSafeNexradTimestamp('20260230-004336')).toBe(false);
    expect(isSafeNexradTimestamp('../20260512-004336')).toBe(false);
  });

  it('validates elevations conservatively', () => {
    expect(isSafeNexradElevation('0.5')).toBe(true);
    expect(isSafeNexradElevation('12')).toBe(true);
    expect(isSafeNexradElevation('+0.5')).toBe(false);
    expect(isSafeNexradElevation('0.5/../x')).toBe(false);
    expect(isSafeNexradElevation('0.5e2')).toBe(false);
    expect(isSafeNexradElevation('')).toBe(false);
  });

  it('only accepts exact allowlisted products', () => {
    expect(isAllowedNexradProduct('DBZH')).toBe(true);
    expect(isAllowedNexradProduct('dbzh')).toBe(false);
    expect(isAllowedNexradProduct('DBZH.bin.gz')).toBe(false);
    expect(isAllowedNexradProduct('../DBZH')).toBe(false);
  });
});
