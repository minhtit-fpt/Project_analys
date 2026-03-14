"""
Progress Reporter Interface
Provides a callback-based mechanism for reporting progress to external listeners (e.g., GUI).
"""

from typing import Callable, Optional
from dataclasses import dataclass
from enum import Enum


class ExecutionStage(Enum):
    """Enumeration of execution stages."""
    INITIALIZING = "Initializing"
    AUTHENTICATING = "Authenticating with Google Drive"
    FETCHING_MARKETS = "Fetching markets"
    PROCESSING_SYMBOLS = "Processing symbols"
    SAVING_DATA = "Saving data"
    UPLOADING = "Uploading to Google Drive"
    COMPLETED = "Completed"
    ERROR = "Error"


@dataclass
class ProgressInfo:
    """Data class containing progress information."""
    stage: ExecutionStage
    stage_progress: float  # 0.0 to 1.0 for current stage
    overall_progress: float  # 0.0 to 1.0 for overall workflow
    message: str
    current_item: Optional[str] = None
    total_items: Optional[int] = None
    completed_items: Optional[int] = None


class ProgressReporter:
    """
    Progress reporter that allows core logic to report progress
    without knowing about the GUI implementation.
    
    Uses callback pattern to decouple business logic from presentation.
    """
    
    def __init__(self):
        self._callbacks: list[Callable[[ProgressInfo], None]] = []
        self._stage_weights = {
            ExecutionStage.INITIALIZING: 0.05,
            ExecutionStage.AUTHENTICATING: 0.05,
            ExecutionStage.FETCHING_MARKETS: 0.10,
            ExecutionStage.PROCESSING_SYMBOLS: 0.60,
            ExecutionStage.SAVING_DATA: 0.10,
            ExecutionStage.UPLOADING: 0.10,
            ExecutionStage.COMPLETED: 0.00,
            ExecutionStage.ERROR: 0.00,
        }
        self._stage_start_points = self._calculate_stage_start_points()
        self._current_stage = ExecutionStage.INITIALIZING
    
    def _calculate_stage_start_points(self) -> dict:
        """Calculate the starting point for each stage in overall progress."""
        start_points = {}
        cumulative = 0.0
        for stage, weight in self._stage_weights.items():
            start_points[stage] = cumulative
            cumulative += weight
        return start_points
    
    def add_callback(self, callback: Callable[[ProgressInfo], None]):
        """Register a callback to receive progress updates."""
        self._callbacks.append(callback)
    
    def _notify(self, info: ProgressInfo):
        """Notify all registered callbacks."""
        for callback in self._callbacks:
            try:
                callback(info)
            except Exception:
                pass  # Don't let callback errors affect core logic
    
    def _calculate_overall_progress(self, stage: ExecutionStage, stage_progress: float) -> float:
        """Calculate overall progress based on current stage and its progress."""
        start = self._stage_start_points.get(stage, 0.0)
        weight = self._stage_weights.get(stage, 0.0)
        return min(1.0, start + (weight * stage_progress))
    
    def report(self, stage: ExecutionStage, stage_progress: float, message: str,
               current_item: str = None, total_items: int = None, completed_items: int = None):
        """
        Report progress to all registered callbacks.
        
        Args:
            stage: Current execution stage
            stage_progress: Progress within current stage (0.0 to 1.0)
            message: Status message to display
            current_item: Optional current item being processed
            total_items: Optional total number of items
            completed_items: Optional number of completed items
        """
        self._current_stage = stage
        overall_progress = self._calculate_overall_progress(stage, stage_progress)
        
        info = ProgressInfo(
            stage=stage,
            stage_progress=stage_progress,
            overall_progress=overall_progress,
            message=message,
            current_item=current_item,
            total_items=total_items,
            completed_items=completed_items
        )
        self._notify(info)
    
    def report_error(self, message: str):
        """Report an error."""
        info = ProgressInfo(
            stage=ExecutionStage.ERROR,
            stage_progress=0.0,
            overall_progress=self._calculate_overall_progress(self._current_stage, 0.0),
            message=message
        )
        self._notify(info)
    
    def report_completion(self, message: str = "Process completed successfully!"):
        """Report successful completion."""
        info = ProgressInfo(
            stage=ExecutionStage.COMPLETED,
            stage_progress=1.0,
            overall_progress=1.0,
            message=message
        )
        self._notify(info)
