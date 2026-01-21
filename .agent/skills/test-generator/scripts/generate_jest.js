#!/usr/bin/env node
/**
 * generate_jest.js - Generate Jest test stubs for JavaScript functions
 * Usage: node generate_jest.js <source_file.js>
 */

const fs = require('fs');
const path = require('path');

function extractExports(content) {
    const exports = [];

    // Match module.exports = { ... }
    const moduleExportsMatch = content.match(/module\.exports\s*=\s*\{([^}]+)\}/);
    if (moduleExportsMatch) {
        const items = moduleExportsMatch[1].split(',').map(s => s.trim().split(':')[0].trim());
        exports.push(...items.filter(Boolean));
    }

    // Match export function name()
    const exportFuncMatches = content.matchAll(/export\s+(?:async\s+)?function\s+(\w+)/g);
    for (const match of exportFuncMatches) {
        exports.push(match[1]);
    }

    // Match exports.name = 
    const exportsMatches = content.matchAll(/exports\.(\w+)\s*=/g);
    for (const match of exportsMatches) {
        exports.push(match[1]);
    }

    return [...new Set(exports)];
}

function generateTest(funcName) {
    return `
describe('${funcName}', () => {
    test('should work with valid input', () => {
        // Arrange
        const input = null; // TODO: set test value
        
        // Act
        const result = ${funcName}(input);
        
        // Assert
        expect(result).toBeDefined();
    });

    test('should handle edge cases', () => {
        // TODO: implement edge case test
    });

    test('should throw on invalid input', () => {
        expect(() => ${funcName}(null)).toThrow();
    });
});
`;
}

function main() {
    const args = process.argv.slice(2);
    if (args.length < 1) {
        console.log('Usage: node generate_jest.js <source_file.js>');
        process.exit(1);
    }

    const filepath = args[0];
    if (!fs.existsSync(filepath)) {
        console.log(`File not found: ${filepath}`);
        process.exit(1);
    }

    const content = fs.readFileSync(filepath, 'utf8');
    const exports = extractExports(content);
    const moduleName = path.basename(filepath, '.js');

    console.log(`/**`);
    console.log(` * Tests for ${moduleName} module`);
    console.log(` */`);
    console.log();
    console.log(`const { ${exports.join(', ')} } = require('./${moduleName}');`);
    console.log();

    for (const func of exports) {
        console.log(generateTest(func));
    }
}

main();
