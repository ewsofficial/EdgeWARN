---
name: api-designer
description: Design RESTful APIs with consistent patterns and best practices.
---

# API Designer Skill

This skill helps design clean, consistent REST APIs.

## REST Conventions

| Method | Purpose | Example |
|--------|---------|---------|
| GET | Read | `GET /api/users` |
| POST | Create | `POST /api/users` |
| PUT | Update (full) | `PUT /api/users/1` |
| PATCH | Update (partial) | `PATCH /api/users/1` |
| DELETE | Remove | `DELETE /api/users/1` |

## Response Format

### Success
```json
{
  "success": true,
  "data": { ... },
  "meta": { "timestamp": "..." }
}
```

### Error
```json
{
  "success": false,
  "error": {
    "code": "INVALID_INPUT",
    "message": "Description"
  }
}
```

## Status Codes

| Code | Meaning |
|------|---------|
| 200 | OK |
| 201 | Created |
| 400 | Bad Request |
| 401 | Unauthorized |
| 403 | Forbidden |
| 404 | Not Found |
| 500 | Server Error |

## Endpoint Template

```javascript
router.get('/resource', async (req, res) => {
  try {
    const data = await getData();
    res.json({ success: true, data });
  } catch (err) {
    res.status(500).json({ 
      success: false, 
      error: { message: err.message } 
    });
  }
});
```
