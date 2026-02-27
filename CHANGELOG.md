# Changelog for Version ``2.0.0``

## Additions
- **Predictive Storm Tracking**: Implemented a core Kalman Filter motion tracking system to continuously predict and associate storm cells over time, replacing static overlap tracking. Includes configurable state dynamics to handle missed scans and a 6-minute Time-To-Live (TTL) for predictions.
- **Storm Lineage Detection Engine**: Introduced a storm merge/split tracking module to maintain continuity when storm cells intersect or fragment. Supported by comprehensive unit and integration tests.
- **Warm Rain Probability Integration**: Added ingestion and processing logic for Warm Rain Probability, returning the maximum (`max`) expected probability value across precipitation features.
- **Tracking Documentation**: Added comprehensive documentation, PRDs, and implementation plans covering tracking updates, lineage continuity, and termination logic.

## Changes
- **Hybrid Assignment Workflow**: Refactored the tracking pipeline configuration to natively integrate Hybrid Assignment logic (Hungarian spatial assignment + Lineage analysis). 
- **Pipeline Execution Optimization**: Optimized the execution pipeline by instituting concurrent execution for downloading integrations and moving METAR/NWS data ingestion to a background process, significantly reducing total cycle latency.
- **Metric Key Conventions**: Updated system-wide metric aggregation key tags, replacing former `p100` designations with the clearer `max` convention.
- **Repository Maintenance**: Cleaned up the file structure and removed obsolete project plan documents and legacy PRDs.

## Fixes
- **NWS Alert Deduplication**: Addressed code review issues within NWS data feed logic, improving deduplication logic to accurately filter out redundant event alerts.
- **StormCast Dropout Prevention**: Modified `StormCast` history mechanics to load historical state directly from persistent files rather than caching locally, preventing data dropouts during stateless backend executions.
- **TimingTracker Race Condition**: Resolved an identified thread-safety race condition within concurrent system tracking instances.