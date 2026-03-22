"""CalR Guard: preflight validation and curation for calorimetry exports.

This package intentionally focuses on the painful front-end of the workflow:
file validation, metadata scaffolding, QC flagging, and variable/time-window
exclusions before the data enters CalR or downstream analysis.
"""

from .models import ExclusionRule, PipelineResult, ProjectMetadata, ValidationIssue
from .pipeline import run_pipeline

__all__ = [
    "ExclusionRule",
    "ProjectMetadata",
    "ValidationIssue",
    "PipelineResult",
    "run_pipeline",
]

__version__ = "0.3.0"
