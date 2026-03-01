/**
 * Setup file for API v2 route tests
 * This file handles proper module path resolution for v2 routes
 */

import { jest } from '@jest/globals';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Set up test environment
process.env.NODE_ENV = 'test';
