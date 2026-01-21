---
name: database-assistant
description: Help with database design, queries, and data management.
---

# Database Assistant Skill

This skill helps with database operations and data management.

## Capabilities

| Capability | Description |
|------------|-------------|
| Schema Design | Design database schemas |
| Query Optimization | Write efficient queries |
| Migration Creation | Create database migrations |
| Data Validation | Ensure data integrity |

## Common Patterns

### File-Based Storage (EdgeWARN)
```python
# Read JSON data
import json
with open('data.json', 'r') as f:
    data = json.load(f)

# Write JSON data
with open('data.json', 'w') as f:
    json.dump(data, f, indent=2)
```

### Query Optimization Tips

- Index frequently queried fields
- Use pagination for large datasets
- Cache repeated queries
- Batch write operations

## Data Validation

```python
def validate_data(data):
    required = ['id', 'timestamp', 'value']
    for field in required:
        if field not in data:
            raise ValueError(f"Missing: {field}")
    return True
```

## Backup Commands

```bash
# Copy data directory
cp -r data/ backup/data_$(date +%Y%m%d)/

# Compress backup
tar -czf backup.tar.gz data/
```
