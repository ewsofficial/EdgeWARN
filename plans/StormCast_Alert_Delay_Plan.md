# StormCast Alert Generation Delay Plan

## Objective
Modify the StormCast alert system to only generate stormcast alert polygons after the storm cell has been successfully tracked and forecasted for at least 15 minutes.

## Current Implementation Analysis
- StormCast is a CTAM module that runs forecasts on storm cells
- It generates 0-30 minute forecast polygons
- Alerts are created immediately when a successful forecast produces a polygon
- Cell history is stored in JSON files with full cell state including timestamps
- Historical data is loaded during forecast generation to improve accuracy

## Proposed Changes

### 1. Modify StormCast.run() Method
**Location:** `src/EdgeWARN/core/ctam/modules/StormCast/__init__.py`

In the `run()` method, after loading historical points:
- Calculate the time duration between the earliest and latest historical timestamps
- If duration >= 15 minutes, set `can_generate_alerts = True` in the module results
- Store this flag in `storm_entry["modules"]["StormCast"]["can_generate_alerts"]`

### 2. Modify StormCast.alerts() Method
**Location:** `src/EdgeWARN/core/ctam/modules/StormCast/__init__.py`

In the `alerts()` method:
- Check if `result.get("can_generate_alerts")` is True
- Only generate and return AlertPayload if the flag is set
- Return None otherwise

## Implementation Details

### Duration Calculation Logic
```python
# After loading unique_points (historical + current)
if len(unique_points) >= 2:
    first_ts = parse_ts(unique_points[0]["ts"])
    last_ts = parse_ts(unique_points[-1]["ts"])
    duration_min = (last_ts - first_ts).total_seconds() / 60
    if duration_min >= 15:
        storm_entry["modules"][self.name]["can_generate_alerts"] = True
```

### Alert Generation Logic
```python
def alerts(self, storm_entry: Dict[str, Any]) -> Optional[List[AlertPayload]]:
    result = storm_entry.get("modules", {}).get(self.name, {})
    
    if result.get("status") != "success" or not result.get("can_generate_alerts"):
        return None
    
    # ... existing polygon extraction and alert creation
```

## Assumptions
- "Successful generation" is interpreted as the storm cell having a tracking history of at least 15 minutes
- The pipeline runs frequently enough that continuous tracking implies successful forecasts
- Duration is calculated from first historical timestamp to current timestamp

## Testing Considerations
- Verify alerts are not generated for new storm cells (< 15 min history)
- Confirm alerts generate normally for established cells (>= 15 min history)
- Check edge cases: cells with intermittent tracking, timestamp parsing failures
- Monitor performance impact of duration calculations

## Files to Modify
- `src/EdgeWARN/core/ctam/modules/StormCast/__init__.py`

## Risk Assessment
- Low risk: Changes are localized to StormCast module
- Backward compatible: Existing functionality preserved for established cells
- Performance: Minimal impact (duration calculation on already loaded data)