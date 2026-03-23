import { describe, it, expect } from '@jest/globals';
import { validateAlertId, validateTimestamp, validateTimestampV2 } from '../../../src/EdgeWARN/api/utils/validation.js';

function validateTimestampBefore(timestamp) {
  if (!timestamp) return false;
  const regex = /^\d{8}-\d{6}$/;
  return regex.test(timestamp);
}

function validateAlertIdBefore(id) {
  if (typeof id !== 'string' || id.length === 0 || id.length > 200) return false;
  if (id === '__proto__' || id === 'constructor' || id === 'prototype') return false;
  return /^[a-zA-Z0-9_.:-]+$/.test(id);
}

function benchmark(fn, values, iterations) {
  const startTimeNs = process.hrtime.bigint();
  for (let i = 0; i < iterations; i += 1) {
    for (const value of values) {
      fn(value);
    }
  }
  return Number(process.hrtime.bigint() - startTimeNs) / 1_000_000;
}

describe('Performance benchmarks for validation helpers', () => {
  it('benchmarks timestamp validation before vs after pre-compiled regex', () => {
    const inputs = ['20231015-143000', '20230101-000000', '20231231-235959', 'bad'];
    const iterations = 150000;

    const beforeMs = benchmark(validateTimestampBefore, inputs, iterations);
    const afterMs = benchmark(validateTimestamp, inputs, iterations);

    expect(validateTimestamp('20231015-143000')).toBe(true);
    expect(validateTimestampV2('20231015-143000')).toBe(true);
    expect(validateTimestamp('bad')).toBe(false);
    expect(Number.isFinite(beforeMs)).toBe(true);
    expect(Number.isFinite(afterMs)).toBe(true);

    const improvementPct = beforeMs > 0 ? ((beforeMs - afterMs) / beforeMs) * 100 : 0;
    console.log(`[benchmark] validateTimestamp before=${beforeMs.toFixed(2)}ms after=${afterMs.toFixed(2)}ms improvement=${improvementPct.toFixed(2)}%`);
  });

  it('benchmarks alert id validation before vs after pre-compiled regex', () => {
    const inputs = ['urn:oid:2.49.0.1.840.0.2406210827.1', 'NWS-ALERT_2023', 'simpleid', '__proto__', 'invalid@id'];
    const iterations = 150000;

    const beforeMs = benchmark(validateAlertIdBefore, inputs, iterations);
    const afterMs = benchmark(validateAlertId, inputs, iterations);

    expect(validateAlertId('urn:oid:2.49.0.1.840.0.2406210827.1')).toBe(true);
    expect(validateAlertId('__proto__')).toBe(false);
    expect(validateAlertId('invalid@id')).toBe(false);
    expect(Number.isFinite(beforeMs)).toBe(true);
    expect(Number.isFinite(afterMs)).toBe(true);

    const improvementPct = beforeMs > 0 ? ((beforeMs - afterMs) / beforeMs) * 100 : 0;
    console.log(`[benchmark] validateAlertId before=${beforeMs.toFixed(2)}ms after=${afterMs.toFixed(2)}ms improvement=${improvementPct.toFixed(2)}%`);
  });
});
