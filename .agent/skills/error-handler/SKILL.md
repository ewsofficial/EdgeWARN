---
name: error-handler
description: Implement consistent error handling patterns across the codebase.
---

# Error Handler Skill

This skill helps implement robust error handling.

## Error Handling Patterns

### JavaScript/Node.js
```javascript
// Async/await
async function doWork() {
  try {
    const result = await riskyOperation();
    return result;
  } catch (err) {
    console.error('Error:', err.message);
    throw new CustomError('Operation failed', err);
  }
}

// Express middleware
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({ error: err.message });
});
```

### Python
```python
# Try/except with logging
import logging

def do_work():
    try:
        result = risky_operation()
        return result
    except ValueError as e:
        logging.error(f"Value error: {e}")
        raise
    except Exception as e:
        logging.exception("Unexpected error")
        raise RuntimeError("Operation failed") from e
```

## Custom Error Classes

### JavaScript
```javascript
class AppError extends Error {
  constructor(message, code, statusCode = 500) {
    super(message);
    this.code = code;
    this.statusCode = statusCode;
  }
}
```

### Python
```python
class AppError(Exception):
    def __init__(self, message, code, status_code=500):
        super().__init__(message)
        self.code = code
        self.status_code = status_code
```

## Best Practices

- [ ] Never swallow errors silently
- [ ] Log errors with context
- [ ] Use specific exception types
- [ ] Provide user-friendly messages
- [ ] Include error codes for debugging
