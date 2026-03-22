from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pandas as pd


@dataclass(slots=True)
class ValidationIssue:
    severity: str
    code: str
    title: str
    description: str
    file: str = ""
    context: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "code": self.code,
            "title": self.title,
            "description": self.description,
            "file": self.file,
            "context": self.context,
        }


@dataclass(slots=True)
class ExclusionRule:
    subject_id: str
    variable: str
    start_hour: float
    end_hour: float
    reason: str = "Manual exclusion"

    def to_dict(self) -> dict[str, Any]:
        return {
            "subject_id": self.subject_id,
            "variable": self.variable,
            "start_hour": self.start_hour,
            "end_hour": self.end_hour,
            "reason": self.reason,
        }


@dataclass(slots=True)
class ProjectMetadata:
    project_title: str = ""
    investigator: str = ""
    system: str = ""
    species: str = "Mouse"
    strain: str = ""
    sex: str = ""
    diet_kcal_per_g: float | None = None
    light_start_zt: float | None = 0.0
    light_end_zt: float | None = 12.0
    ambient_temperature_c: float | None = None
    acclimation_hours: float | None = 24.0
    treatment: str = ""
    notes: str = ""

    @classmethod
    def from_mapping(cls, data: dict[str, Any] | None) -> "ProjectMetadata":
        if not data:
            return cls()
        allowed = {field.name for field in cls.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        filtered = {k: v for k, v in data.items() if k in allowed}
        return cls(**filtered)

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_title": self.project_title,
            "investigator": self.investigator,
            "system": self.system,
            "species": self.species,
            "strain": self.strain,
            "sex": self.sex,
            "diet_kcal_per_g": self.diet_kcal_per_g,
            "light_start_zt": self.light_start_zt,
            "light_end_zt": self.light_end_zt,
            "ambient_temperature_c": self.ambient_temperature_c,
            "acclimation_hours": self.acclimation_hours,
            "treatment": self.treatment,
            "notes": self.notes,
        }


@dataclass
class FileAnalysis:
    path: Path
    filename: str
    suffix: str
    delimiter: str | None
    original_headers: list[str]
    original_df: pd.DataFrame
    vendor: str
    mapping: dict[str, str]
    validations: list[ValidationIssue]
    standardized_df: pd.DataFrame
    cleaned_vendor_df: pd.DataFrame | None = None


@dataclass
class PipelineResult:
    files: list[FileAnalysis]
    standardized_df: pd.DataFrame
    subject_sheet_df: pd.DataFrame
    qc_flags_df: pd.DataFrame
    suggested_exclusions_df: pd.DataFrame
    mapping_review_df: pd.DataFrame
    exclusion_log_df: pd.DataFrame
    validation_df: pd.DataFrame
    project_metadata: ProjectMetadata
    bundle_summary: dict[str, Any]
    output_dir: Path | None = None
    output_zip: Path | None = None
