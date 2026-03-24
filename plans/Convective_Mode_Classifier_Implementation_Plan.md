# Convective Mode Classifier Implementation Plan

## Objective
Implement an MVP convective mode classifier that assigns each active storm object to one of three classes:
- `SUPERCELL`
- `LINEAR`
- `DISORGANIZED`

The MVP must be:
- object-based
- morphology-first
- persistence-aware
- interpretable and lightweight

## Pipeline Placement Decision

### Decision
Implement the classifier as a **new CTAM cell module** (`ConvectiveMode`) that runs at the **start of CTAM cell-module processing**, after integration has completed.

### Why this placement
- Integration already enriches storm cells with ProbSevere, MRMS-derived products, and RAP context.
- CTAM is the existing extension surface for per-cell analytic modules.
- CTAM runner preloads history for active cell IDs, which supports hysteresis and temporal persistence logic.
- This preserves separation of concerns:
  - integration = data enrichment
  - CTAM = analytic interpretation

## Output Contract (Minimal)

The module output should include only:

```json
{
  "label": "SUPERCELL | LINEAR | DISORGANIZED",
  "confidence_scores": {
    "SUPERCELL": 0.00,
    "LINEAR": 0.00,
    "DISORGANIZED": 0.00
  }
}
```

### Output rules
- `confidence_scores` must be normalized and sum to approximately 1.0.
- `label` must be the argmax of `confidence_scores`.
- In degraded data conditions, maintain this same schema and encode uncertainty by flattening score separation.

## Module Architecture

Create:
- `src/EdgeWARN/ctam/modules/ConvectiveMode/__init__.py`
- `src/EdgeWARN/ctam/modules/ConvectiveMode/convective_mode_module.py`
- `src/EdgeWARN/ctam/modules/ConvectiveMode/features.py`
- `src/EdgeWARN/ctam/modules/ConvectiveMode/decision.py`
- `src/EdgeWARN/ctam/modules/ConvectiveMode/stability.py`
- `src/EdgeWARN/ctam/modules/ConvectiveMode/quality.py`
- `src/EdgeWARN/ctam/modules/ConvectiveMode/config.py`

## Decision Policy

Use hierarchical decision order:
1. `LINEAR` gate first
2. `SUPERCELL` gate second
3. `DISORGANIZED` residual fallback

### LINEAR gate feature families
- object aspect ratio
- compactness penalty
- contiguous line length
- exterior linearity in neighborhood ring

### SUPERCELL gate feature families
- low-level azimuthal shear magnitude
- low-level azimuthal shear persistence
- mid-level rotational support
- rotation-track support
- RAP shear/SRH as supporting context (not dominant)

### DISORGANIZED rule
Assign when neither LINEAR nor SUPERCELL gate meets required evidence thresholds.

## Temporal Stability and Hysteresis

Implement rolling per-cell state:
- recent feature window
- recent class score window
- entry counters
- exit counters
- current stable label

### Hysteresis behavior
- organized class entry requires multiple confirming scans
- organized class exit also requires persistence to avoid flip-flop
- maintain conservative SUPERCELL promotion thresholds

## Data Quality and Degraded-Mode Behavior

Implement explicit missing-data handling:
- Missing rotation family: block new SUPERCELL promotions unless strong prior persistence exists.
- Missing neighborhood-linearity family: allow LINEAR only with reduced confidence separation.
- Missing RAP context: continue using radar+morphology with confidence flattening.
- Severe incompleteness: default to DISORGANIZED with low-confidence spread.

## Verification Strategy (Operational)

### 1) Classification quality
Using labeled historical cases, compute:
- 3x3 confusion matrix
- per-class precision/recall/F1
- macro-F1

### 2) Probability quality
Using confidence score outputs, compute:
- one-vs-rest Brier Score per class
- reliability diagrams per class
- Brier Skill Score versus climatology
- optional multicategory RPS/RPSS

### 3) Temporal stability quality
For tracked objects, compute:
- label flips per storm-hour
- mean class dwell time
- time-to-stable-label after initiation
- transition confusion (true evolution vs jitter)

### 4) Object-structure quality (LINEAR emphasis)
Evaluate object continuity and structure using case-based object diagnostics and time-domain object verification methods.

## Validation Dataset Design

Evaluate across:
- discrete supercell days
- QLCS/broken-line days
- pulse and multicell days
- transition days
- merger/debris days

Include diverse:
- regions/radars
- beam geometry regimes
- initiation and mature phases

## Acceptance Criteria (MVP)
- Macro-F1 meets or exceeds agreed baseline.
- Positive skill vs climatology on probability metrics for key classes.
- Reliability curves show no strong overconfidence in high-probability bins.
- Flip-rate remains below operational threshold for mature objects.
- No hard failures under common missing-data scenarios.

## Delivery Plan

### Milestone A: Scaffolding + config
- create module package skeleton
- add threshold config file
- emit minimal schema output

### Milestone B: Feature + decision implementation
- implement morphology/line/rotation/environment feature synthesis
- implement hierarchical gate logic

### Milestone C: Stability + quality hardening
- implement rolling windows and hysteresis
- implement degraded-mode behavior

### Milestone D: Verification harness
- add replay-style integration tests
- generate classification/probability/stability reports

### Milestone E: Rollout
- run in shadow mode
- regional threshold tuning
- enable operational output

## Files Planned for Future Implementation
- `src/EdgeWARN/ctam/modules/ConvectiveMode/*`
- `config/convective_mode.yaml`
- `tests/integration/` replay harness and scenario fixtures
- optional docs in `docs/core/`
