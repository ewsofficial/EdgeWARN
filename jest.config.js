/**
 * Jest configuration for EdgeWARN API tests
 */

export default {
    // Test environment
    testEnvironment: 'node',

    // Coverage configuration
    coverageDirectory: 'coverage',
    collectCoverageFrom: [
        'src/EdgeWARN/api/**/*.js',
        '!src/EdgeWARN/api/**/*.test.js',
        '!src/EdgeWARN/api/config.js'
    ],
    coverageThreshold: {
        global: {
            branches: 70,
            functions: 70,
            lines: 70,
            statements: 70
        }
    },

    // Test file patterns
    testMatch: [
        '**/tests/api/**/*.test.js',
        '**/tests/api/**/*.spec.js'
    ],

    // Module handling for ES modules
    transform: {},
    extensionsToTreatAsEsm: ['.js'],
    moduleNameMapper: {
        '^(\\.{1,2}/.*)\\.js$': '$1'
    },

    // Setup files
    setupFilesAfterEnv: [],

    // Verbose output
    verbose: true,

    // Clear mocks between tests
    clearMocks: true,

    // Restore mocks after each test
    restoreMocks: true,

    // Test timeout (10 seconds)
    testTimeout: 10000
};
