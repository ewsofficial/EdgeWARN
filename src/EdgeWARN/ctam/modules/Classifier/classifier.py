from typing import Any, Dict, Optional

from ...interface import AnalysisModule


class ClassifierModule(AnalysisModule):
    """Skeleton CTAM module for future storm classification logic."""

    @property
    def name(self) -> str:
        return "Classifier"

    def run(
        self,
        storm_entry: Dict[str, Any],
        environment: Optional[Dict[str, Any]] = None,
        history_cache: Optional[Any] = None,
    ) -> None:
        return None
