---
name: test-generator
description: Generate unit tests, integration tests, and test fixtures for the codebase.
---

# Test Generator Skill

This skill helps create comprehensive tests for EdgeWARN components.

## Capabilities

| Capability | Description |
|------------|-------------|
| Unit Tests | Generate tests for individual functions |
| Integration Tests | Create tests for component interactions |
| API Tests | Generate endpoint test suites |
| Mock Generation | Create mocks and fixtures |
| Coverage Analysis | Identify untested code paths |

## Test Frameworks

### Python
- `pytest` - Primary testing framework
- `unittest.mock` - Mocking library
- `pytest-cov` - Coverage reporting

### JavaScript
- `jest` - Testing framework
- `supertest` - HTTP testing
- `sinon` - Mocking library

## Instructions

### 1. Analyze Function to Test

Identify:
- Input parameters and types
- Return values and types
- Side effects
- Edge cases
- Error conditions

### 2. Generate Test Cases

For each function, create tests for:
- [ ] Normal/happy path
- [ ] Edge cases (empty, null, max values)
- [ ] Error handling
- [ ] Boundary conditions

## Test Templates

### Python Unit Test
```python
import pytest
from module import function_to_test

class TestFunctionName:
    def test_normal_case(self):
        result = function_to_test(valid_input)
        assert result == expected_output
    
    def test_edge_case_empty(self):
        result = function_to_test([])
        assert result == []
    
    def test_error_handling(self):
        with pytest.raises(ValueError):
            function_to_test(invalid_input)
```

### JavaScript Unit Test
```javascript
const { functionToTest } = require('./module');

describe('functionToTest', () => {
    test('normal case', () => {
        const result = functionToTest(validInput);
        expect(result).toEqual(expectedOutput);
    });
    
    test('edge case - empty', () => {
        const result = functionToTest([]);
        expect(result).toEqual([]);
    });
    
    test('error handling', () => {
        expect(() => functionToTest(invalidInput)).toThrow();
    });
});
```

### API Endpoint Test
```javascript
const request = require('supertest');
const app = require('../server');

describe('GET /api/endpoint', () => {
    test('returns 200 with valid request', async () => {
        const response = await request(app)
            .get('/api/endpoint')
            .query({ param: 'value' });
        
        expect(response.status).toBe(200);
        expect(response.body).toHaveProperty('data');
    });
    
    test('returns 400 with invalid request', async () => {
        const response = await request(app)
            .get('/api/endpoint');
        
        expect(response.status).toBe(400);
    });
});
```

## Running Tests

```bash
# Python
pytest tests/ -v
pytest --cov=src tests/

# JavaScript
npm test
npm run test:coverage
```
