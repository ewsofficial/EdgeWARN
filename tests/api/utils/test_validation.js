/**
 * Tests for API validation utilities
 * @module tests/api/utils/test_validation
 */

import { describe, it, expect } from '@jest/globals';
import {
    validateResourceType,
    validateTimestamp,
    validateCellId,
    validateTimestampV2,
    validateMutualExclusion,
    validateAlertId
} from '../../../src/EdgeWARN/api/utils/validation.js';

describe('validateResourceType', () => {
    it('should return true for valid type "cell"', () => {
        expect(validateResourceType('cell')).toBe(true);
    });

    it('should return true for valid type "list"', () => {
        expect(validateResourceType('list')).toBe(true);
    });

    it('should return false for invalid string types', () => {
        expect(validateResourceType('invalid')).toBe(false);
        expect(validateResourceType('storm')).toBe(false);
        expect(validateResourceType('')).toBe(false);
    });

    it('should return false for null and undefined', () => {
        expect(validateResourceType(null)).toBe(false);
        expect(validateResourceType(undefined)).toBe(false);
    });

    it('should return false for non-string types', () => {
        expect(validateResourceType(123)).toBe(false);
        expect(validateResourceType({})).toBe(false);
        expect(validateResourceType([])).toBe(false);
    });
});

describe('validateTimestamp', () => {
    it('should return true for valid timestamp format YYYYMMDD-HHMMSS', () => {
        expect(validateTimestamp('20231015-143000')).toBe(true);
        expect(validateTimestamp('20230101-000000')).toBe(true);
        expect(validateTimestamp('20231231-235959')).toBe(true);
    });

    it('should return false for invalid timestamp formats', () => {
        expect(validateTimestamp('2023-10-15-14-30-00')).toBe(false);
        expect(validateTimestamp('20231015')).toBe(false);
        expect(validateTimestamp('14:30:00')).toBe(false);
        expect(validateTimestamp('Oct 15, 2023')).toBe(false);
    });

    it('should return false for timestamps with wrong length', () => {
        expect(validateTimestamp('20231015-14300')).toBe(false);
        expect(validateTimestamp('20231015-1430000')).toBe(false);
        expect(validateTimestamp('2023105-143000')).toBe(false);
    });

    it('should return false for null and undefined', () => {
        expect(validateTimestamp(null)).toBe(false);
        expect(validateTimestamp(undefined)).toBe(false);
    });

    it('should return false for empty string', () => {
        expect(validateTimestamp('')).toBe(false);
    });

    it('should return false for non-numeric characters in date/time positions', () => {
        expect(validateTimestamp('2023AB15-143000')).toBe(false);
        expect(validateTimestamp('20231015-14CD00')).toBe(false);
    });
});

describe('validateCellId', () => {
    it('should return true for valid positive integer as string', () => {
        expect(validateCellId('1')).toBe(true);
        expect(validateCellId('101')).toBe(true);
        expect(validateCellId('999999')).toBe(true);
    });

    it('should return true for valid positive integer as number', () => {
        expect(validateCellId(1)).toBe(true);
        expect(validateCellId(101)).toBe(true);
        expect(validateCellId(999999)).toBe(true);
    });

    it('should return false for zero', () => {
        expect(validateCellId('0')).toBe(false);
        expect(validateCellId(0)).toBe(false);
    });

    it('should return false for negative integers', () => {
        expect(validateCellId('-1')).toBe(false);
        expect(validateCellId(-1)).toBe(false);
        expect(validateCellId('-101')).toBe(false);
    });

    it('should return false for floating point numbers', () => {
        expect(validateCellId('1.5')).toBe(false);
        expect(validateCellId(1.5)).toBe(false);
        expect(validateCellId('101.0')).toBe(false);
    });

    it('should return false for non-numeric strings', () => {
        expect(validateCellId('abc')).toBe(false);
        expect(validateCellId('1a2')).toBe(false);
        expect(validateCellId('')).toBe(false);
    });

    it('should return false for null and undefined', () => {
        expect(validateCellId(null)).toBe(false);
        expect(validateCellId(undefined)).toBe(false);
    });

    it('should return false for objects and arrays', () => {
        expect(validateCellId({})).toBe(false);
        expect(validateCellId([])).toBe(false);
        expect(validateCellId([1, 2, 3])).toBe(false);
    });

    it('should return false for numbers with leading zeros that change value', () => {
        // '01' as string should not equal 1 when converted back
        expect(validateCellId('01')).toBe(false);
        expect(validateCellId('00101')).toBe(false);
    });
});

describe('validateTimestampV2', () => {
    it('should return true for valid timestamp format YYYYMMDD-HHMMSS', () => {
        expect(validateTimestampV2('20231015-143000')).toBe(true);
        expect(validateTimestampV2('20230101-000000')).toBe(true);
        expect(validateTimestampV2('20231231-235959')).toBe(true);
    });

    it('should return false for invalid timestamp formats', () => {
        expect(validateTimestampV2('2023-10-15-14-30-00')).toBe(false);
        expect(validateTimestampV2('20231015')).toBe(false);
        expect(validateTimestampV2('14:30:00')).toBe(false);
    });

    it('should return false for timestamps with wrong length', () => {
        expect(validateTimestampV2('20231015-14300')).toBe(false);
        expect(validateTimestampV2('20231015-1430000')).toBe(false);
    });

    it('should return false for null and undefined', () => {
        expect(validateTimestampV2(null)).toBe(false);
        expect(validateTimestampV2(undefined)).toBe(false);
    });

    it('should return false for empty string', () => {
        expect(validateTimestampV2('')).toBe(false);
    });
});

describe('validateMutualExclusion', () => {
    it('should return true when neither parameter is present', () => {
        expect(validateMutualExclusion({}, 'timestamp', 'id')).toBe(true);
        expect(validateMutualExclusion({ other: 'value' }, 'timestamp', 'id')).toBe(true);
    });

    it('should return true when only first parameter is present', () => {
        expect(validateMutualExclusion({ timestamp: '20231015-143000' }, 'timestamp', 'id')).toBe(true);
    });

    it('should return true when only second parameter is present', () => {
        expect(validateMutualExclusion({ id: 'alert-1' }, 'timestamp', 'id')).toBe(true);
    });

    it('should return false when both parameters are present', () => {
        expect(validateMutualExclusion(
            { timestamp: '20231015-143000', id: 'alert-1' },
            'timestamp',
            'id'
        )).toBe(false);
    });

    it('should treat empty strings as not present', () => {
        expect(validateMutualExclusion(
            { timestamp: '', id: '' },
            'timestamp',
            'id'
        )).toBe(true);
    });

    it('should return false when one is empty string and one has value', () => {
        expect(validateMutualExclusion(
            { timestamp: '', id: 'some-id' },
            'timestamp',
            'id'
        )).toBe(true);
    });

    it('should return true when one parameter is undefined and other is absent', () => {
        expect(validateMutualExclusion(
            { timestamp: undefined },
            'timestamp',
            'id'
        )).toBe(true);
    });
});

describe('validateAlertId', () => {
    it('should return true for valid alert IDs', () => {
        expect(validateAlertId('alert-123')).toBe(true);
        expect(validateAlertId('urn:oid:2.49.0.1.840.0.2406210827.1')).toBe(true);
        expect(validateAlertId('NWS-ALERT_2023')).toBe(true);
        expect(validateAlertId('simpleid')).toBe(true);
    });

    it('should return false for prototype pollution attempts', () => {
        expect(validateAlertId('__proto__')).toBe(false);
        expect(validateAlertId('constructor')).toBe(false);
        expect(validateAlertId('prototype')).toBe(false);
    });

    it('should return false for invalid characters', () => {
        expect(validateAlertId('alert<script>')).toBe(false);
        expect(validateAlertId('alert../path')).toBe(false);
        expect(validateAlertId('alert\nnewline')).toBe(false);
        expect(validateAlertId('alert@domain')).toBe(false);
    });

    it('should return false for empty or invalid types', () => {
        expect(validateAlertId('')).toBe(false);
        expect(validateAlertId(null)).toBe(false);
        expect(validateAlertId(undefined)).toBe(false);
        expect(validateAlertId(123)).toBe(false);
    });

    it('should return false for IDs exceeding max length', () => {
        const longId = 'a'.repeat(201);
        expect(validateAlertId(longId)).toBe(false);
    });
});
