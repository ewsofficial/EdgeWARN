"""
Lineage detection module for storm cell merge/split tracking.

This module provides functionality to detect and track storm cell lineage events
including merges (multiple parents -> single child) and splits (single parent -> multiple children).
"""

from .events import LineageEvent, LineageResult, MergeEvent, SplitEvent
from .buffer import LineageBuffer
from .spatial import (
    calculate_overlap_ratio,
    build_spatial_index,
    select_dominant_parent,
    select_dominant_child,
)
from .detector import LineageDetector, detect_lineage_events

__all__ = [
    'LineageEvent',
    'LineageResult',
    'MergeEvent',
    'SplitEvent',
    'LineageBuffer',
    'calculate_overlap_ratio',
    'build_spatial_index',
    'select_dominant_parent',
    'select_dominant_child',
    'LineageDetector',
    'detect_lineage_events',
]
