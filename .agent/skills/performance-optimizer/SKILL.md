---
name: performance-optimizer
description: Analyze and optimize code performance, identify bottlenecks, and improve efficiency.
---

# Performance Optimizer Skill

This skill helps identify performance bottlenecks and optimize code.

## Capabilities

| Capability | Description |
|------------|-------------|
| Profiling | CPU and memory profiling |
| Bottleneck Detection | Identify slow operations |
| Optimization Suggestions | Recommend performance improvements |
| Caching Strategies | Implement efficient caching |
| Async Optimization | Improve concurrent operations |

## Performance Checklist

### General
- [ ] Avoid unnecessary loops
- [ ] Use efficient data structures (Set, Map vs Array)
- [ ] Cache expensive computations
- [ ] Lazy load when possible
- [ ] Batch operations where applicable

### JavaScript/Node.js
- [ ] Use streams for large files
- [ ] Implement connection pooling
- [ ] Use async/parallel processing
- [ ] Minimize JSON.parse/stringify
- [ ] Optimize regex patterns

### Python
- [ ] Use generators for large datasets
- [ ] Leverage multiprocessing for CPU-bound tasks
- [ ] Use asyncio for I/O-bound tasks
- [ ] Profile with cProfile
- [ ] Use NumPy for numerical operations

## Profiling Commands

### Python
```bash
# CPU profiling
python -m cProfile -s cumulative script.py

# Memory profiling
python -m memory_profiler script.py

# Line-by-line profiling
kernprof -l -v script.py
```

### Node.js
```bash
# CPU profiling
node --prof script.js
node --prof-process isolate-*.log > profile.txt

# Memory usage
node --expose-gc --inspect script.js
```

## Optimization Patterns

### Caching
```python
from functools import lru_cache

@lru_cache(maxsize=128)
def expensive_function(param):
    return result
```

### Batch Processing
```javascript
// Instead of individual calls
for (const item of items) {
    await db.insert(item);
}

// Use batch insert
await db.insertMany(items);
```

### Lazy Loading
```python
# Load only when needed
def get_heavy_data():
    if not hasattr(get_heavy_data, '_cache'):
        get_heavy_data._cache = load_heavy_data()
    return get_heavy_data._cache
```
