/**
 * Tests for API file reading utilities
 * @module tests/api/utils/test_fileReader
 */

import { describe, it, expect, beforeEach, afterEach } from '@jest/globals';
import fs from 'fs';
import path from 'path';
import os from 'os';
import {
    isSafeFilename,
    readJsonFileSafe,
    readIndexFile
} from '../../../src/EdgeWARN/api/utils/fileReader.js';

describe('isSafeFilename', () => {
    it('should return true for safe JSON filenames', () => {
        expect(isSafeFilename('stormcells_20231015-143000.json')).toBe(true);
        expect(isSafeFilename('101.json')).toBe(true);
        expect(isSafeFilename('cell_index.json')).toBe(true);
    });

    it('should return false for path traversal attempts', () => {
        expect(isSafeFilename('../etc/passwd')).toBe(false);
        expect(isSafeFilename('..\\windows\\system32')).toBe(false);
        expect(isSafeFilename('file/../../../etc/passwd')).toBe(false);
    });

    it('should return false for non-JSON extensions', () => {
        expect(isSafeFilename('data.txt')).toBe(false);
        expect(isSafeFilename('script.js')).toBe(false);
        expect(isSafeFilename('file.exe')).toBe(false);
        expect(isSafeFilename('noextension')).toBe(false);
    });

    it('should return false for null and undefined', () => {
        expect(isSafeFilename(null)).toBe(false);
        expect(isSafeFilename(undefined)).toBe(false);
    });

    it('should return false for empty string', () => {
        expect(isSafeFilename('')).toBe(false);
    });
});

describe('readJsonFileSafe', () => {
    let tempDir;
    let testFile;

    beforeEach(async () => {
        tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'edgewarn-test-'));
        testFile = path.join(tempDir, 'test.json');
    });

    afterEach(async () => {
        try {
            await fs.promises.rm(tempDir, { recursive: true, force: true });
        } catch (e) {
            // Ignore cleanup errors
        }
    });

    it('should successfully read valid JSON file', async () => {
        const testData = { id: 1, name: 'Test Cell', features: [] };
        await fs.promises.writeFile(testFile, JSON.stringify(testData));

        const result = await readJsonFileSafe(tempDir, 'test.json');
        expect(result).toEqual(testData);
    });

    it('should throw EINVAL error for path traversal attempts', async () => {
        await expect(readJsonFileSafe(tempDir, '../etc/passwd.json')).rejects.toThrow('Invalid filename');
        await expect(readJsonFileSafe(tempDir, '..\\windows\\system32.json')).rejects.toThrow('Invalid filename');
    });

    it('should throw EACCES error for paths outside allowed directory', async () => {
        const outsideDir = path.join(os.tmpdir(), 'outside');
        await expect(readJsonFileSafe(tempDir, outsideDir)).rejects.toThrow();
    });

    it('should throw ENOENT error for missing files', async () => {
        await expect(readJsonFileSafe(tempDir, 'nonexistent.json')).rejects.toThrow();
    });

    it('should throw error for invalid JSON', async () => {
        await fs.promises.writeFile(testFile, '{ invalid json }');
        await expect(readJsonFileSafe(tempDir, 'test.json')).rejects.toThrow();
    });

    it('should return cached result on second call', async () => {
        const testData = { id: 1, cached: true };
        await fs.promises.writeFile(testFile, JSON.stringify(testData));

        // First call
        const result1 = await readJsonFileSafe(tempDir, 'test.json');
        expect(result1).toEqual(testData);

        // Modify file
        await fs.promises.writeFile(testFile, JSON.stringify({ id: 2, cached: false }));

        // Second call should return cached result
        const result2 = await readJsonFileSafe(tempDir, 'test.json');
        expect(result2).toEqual(testData);
    });

    it('should handle nested JSON structures', async () => {
        const testData = {
            features: [
                { id: 1, properties: { name: 'Cell 1' } },
                { id: 2, properties: { name: 'Cell 2' } }
            ],
            metadata: {
                timestamp: '20231015-143000',
                count: 2
            }
        };
        await fs.promises.writeFile(testFile, JSON.stringify(testData));

        const result = await readJsonFileSafe(tempDir, 'test.json');
        expect(result).toEqual(testData);
    });
});

describe('readIndexFile', () => {
    let tempDir;
    let indexFile;

    beforeEach(async () => {
        tempDir = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'edgewarn-index-test-'));
        indexFile = path.join(tempDir, 'stormcell_index.json');
    });

    afterEach(async () => {
        try {
            await fs.promises.rm(tempDir, { recursive: true, force: true });
        } catch (e) {
            // Ignore cleanup errors
        }
    });

    it('should successfully read index file', async () => {
        const indexData = {
            timestamps: ['20231015-143000', '20231015-144000', '20231015-145000'],
            lastUpdated: '2023-10-15T14:50:00Z'
        };
        await fs.promises.writeFile(indexFile, JSON.stringify(indexData));

        const result = await readIndexFile(indexFile);
        expect(result).toEqual(indexData);
    });

    it('should return cached result on second call', async () => {
        const indexData = { timestamps: ['20231015-143000'] };
        await fs.promises.writeFile(indexFile, JSON.stringify(indexData));

        // First call
        const result1 = await readIndexFile(indexFile);
        expect(result1).toEqual(indexData);

        // Modify file
        await fs.promises.writeFile(indexFile, JSON.stringify({ timestamps: ['20231015-150000'] }));

        // Second call should return cached result (index files use cache)
        const result2 = await readIndexFile(indexFile);
        expect(result2).toEqual(indexData);
    });

    it('should throw error for missing index file', async () => {
        const nonExistentFile = path.join(tempDir, 'nonexistent_index.json');
        await expect(readIndexFile(nonExistentFile)).rejects.toThrow();
    });

    it('should throw error for invalid JSON', async () => {
        await fs.promises.writeFile(indexFile, '{ invalid json content }');
        await expect(readIndexFile(indexFile)).rejects.toThrow();
    });

    it('should handle cell index format', async () => {
        const cellIndexFile = path.join(tempDir, 'cell_index.json');
        const cellIndexData = {
            cellIds: [101, 102, 103, 104],
            lastUpdated: '2023-10-15T14:50:00Z'
        };
        await fs.promises.writeFile(cellIndexFile, JSON.stringify(cellIndexData));

        const result = await readIndexFile(cellIndexFile);
        expect(result.cellIds).toEqual([101, 102, 103, 104]);
    });
});
