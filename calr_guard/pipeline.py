from __future__ import annotations

from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from .io_utils import parse_datetime_series, parse_number_series, read_table
from .models import ExclusionRule, FileAnalysis, PipelineResult, ProjectMetadata, ValidationIssue
from .vendor import STANDARD_VARIABLES, detect_vendor, infer_mapping, merge_mapping

NUMERIC_VARS = ["vo2", "vco2", "ee", "rer", "feed", "drink", "wheel", "activity"]
SUBJECT_SHEET_BASE_COLUMNS = [
    "project_title",
    "investigator",
    "system",
    "species",
    "strain",
    "sex",
    "diet_kcal_per_g",
    "light_start_zt",
    "light_end_zt",
    "ambient_temperature_c",
    "acclimation_hours",
    "treatment",
    "notes",
    "subject_id",
    "group",
    "start_mass_g",
    "end_mass_g",
    "lean_mass_g",
    "fat_mass_g",
    "treatment_subject",
    "notes_subject",
]


def _fragile_name(name: str) -> bool:
    return any(ch in name for ch in ["/", "\\", ";", ","])


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()


def _build_validation_df(validations: Iterable[ValidationIssue]) -> pd.DataFrame:
    data = [issue.to_dict() for issue in validations]
    if not data:
        return pd.DataFrame(columns=["severity", "code", "title", "description", "file", "context"])
    return pd.DataFrame(data)


def _validate_subject_names(series: pd.Series, filename: str) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    if series.empty:
        return issues
    bad_subjects = sorted({str(x) for x in series.dropna().astype(str) if _fragile_name(str(x))})
    if bad_subjects:
        issues.append(
            ValidationIssue(
                severity="warn",
                code="fragile_subject_names",
                title="Subject names contain fragile characters",
                description=(
                    "Some subject IDs contain slash, backslash, semicolon, or comma. "
                    f"Examples: {', '.join(bad_subjects[:5])}"
                ),
                file=filename,
            )
        )
    return issues


def _validate_dataframe(
    *,
    df: pd.DataFrame,
    headers: list[str],
    filename: str,
    vendor: str,
    mapping: dict[str, str],
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []

    if _fragile_name(filename):
        issues.append(
            ValidationIssue(
                severity="warn",
                code="fragile_filename",
                title="Filename contains fragile characters",
                description=(
                    f"{filename} contains slash, backslash, semicolon, or comma. "
                    "Those names often cause downstream trouble."
                ),
                file=filename,
            )
        )
    else:
        issues.append(
            ValidationIssue(
                severity="ok",
                code="filename_ok",
                title="Filename check passed",
                description=f"{filename} does not contain obvious fragile characters.",
                file=filename,
            )
        )

    duplicate_headers = [h for i, h in enumerate(headers) if headers.index(h) != i]
    if duplicate_headers:
        issues.append(
            ValidationIssue(
                severity="error",
                code="duplicate_headers",
                title="Duplicate headers detected",
                description=f"Duplicate columns: {', '.join(sorted(set(duplicate_headers)))}",
                file=filename,
            )
        )

    unnamed = [h for h in headers if not str(h).strip() or str(h).lower().startswith("unnamed")]
    if unnamed:
        issues.append(
            ValidationIssue(
                severity="warn",
                code="unnamed_columns",
                title="Unnamed columns detected",
                description=f"Blank or auto-generated columns found: {', '.join(unnamed)}",
                file=filename,
            )
        )

    if df.empty:
        issues.append(
            ValidationIssue(
                severity="error",
                code="no_rows",
                title="No data rows",
                description="The file was read but contains no non-empty rows.",
                file=filename,
            )
        )
        return issues

    if not mapping.get("timestamp"):
        issues.append(
            ValidationIssue(
                severity="error",
                code="missing_timestamp",
                title="Missing time column",
                description="Could not infer a timestamp/date/time column.",
                file=filename,
            )
        )
    if not mapping.get("subject"):
        issues.append(
            ValidationIssue(
                severity="warn",
                code="missing_subject",
                title="Missing subject column",
                description="Could not infer a subject column. Subject IDs will fall back to the filename stem.",
                file=filename,
            )
        )

    if vendor == "TSE":
        header_set = set(headers)
        if ({"VO2(1)", "VO2(2)"} & header_set) and "VO2(3)" not in header_set:
            issues.append(
                ValidationIssue(
                    severity="error",
                    code="tse_normalized_vo2",
                    title="Likely TSE normalized VO2 export",
                    description="Detected VO2(1) and/or VO2(2) without VO2(3). Prefer uncorrected VO2(3).",
                    file=filename,
                )
            )
        if ({"VCO2(1)", "VCO2(2)"} & header_set) and "VCO2(3)" not in header_set:
            issues.append(
                ValidationIssue(
                    severity="error",
                    code="tse_normalized_vco2",
                    title="Likely TSE normalized VCO2 export",
                    description="Detected VCO2(1) and/or VCO2(2) without VCO2(3). Prefer uncorrected VCO2(3).",
                    file=filename,
                )
            )

    if mapping.get("timestamp"):
        ts = parse_datetime_series(df[mapping["timestamp"]])
        bad_ts = int(ts.isna().sum())
        if bad_ts:
            issues.append(
                ValidationIssue(
                    severity="warn",
                    code="timestamp_parse_fail",
                    title="Some timestamps failed to parse",
                    description=f"{bad_ts} row(s) could not be interpreted as valid timestamps.",
                    file=filename,
                    context={"bad_rows": bad_ts},
                )
            )

        interval_diffs: list[float] = []
        interval_medians: list[float] = []
        if mapping.get("subject") and mapping["subject"] in df.columns:
            subject_groups = df.assign(_parsed_ts=ts).groupby(mapping["subject"], dropna=False)
            for _, subject_df in subject_groups:
                valid_ts = subject_df["_parsed_ts"].dropna().sort_values()
                if len(valid_ts) >= 3:
                    diffs = valid_ts.diff().dropna().dt.total_seconds() / 60.0
                    diffs = diffs[diffs > 0]
                    if not diffs.empty:
                        median_interval = float(diffs.median())
                        interval_medians.append(median_interval)
                        irregular = diffs[(diffs - median_interval).abs() > abs(median_interval) * 0.15]
                        interval_diffs.extend(irregular.tolist())
        else:
            valid_ts = ts.dropna().sort_values()
            if len(valid_ts) >= 3:
                diffs = valid_ts.diff().dropna().dt.total_seconds() / 60.0
                diffs = diffs[diffs > 0]
                if not diffs.empty:
                    median_interval = float(diffs.median())
                    interval_medians.append(median_interval)
                    irregular = diffs[(diffs - median_interval).abs() > abs(median_interval) * 0.15]
                    interval_diffs.extend(irregular.tolist())

        if interval_medians:
            median_interval = float(pd.Series(interval_medians).median())
            if interval_diffs:
                issues.append(
                    ValidationIssue(
                        severity="warn",
                        code="irregular_interval",
                        title="Irregular interval spacing",
                        description=(
                            f"Median interval is about {median_interval:.2f} min, but "
                            f"{len(interval_diffs)} interval(s) differ by more than 15%."
                        ),
                        file=filename,
                        context={"median_interval_min": round(median_interval, 3)},
                    )
                )
            else:
                issues.append(
                    ValidationIssue(
                        severity="ok",
                        code="regular_interval",
                        title="Regular interval spacing",
                        description=f"Median interval is about {median_interval:.2f} min.",
                        file=filename,
                        context={"median_interval_min": round(median_interval, 3)},
                    )
                )

    if mapping.get("subject") and mapping["subject"] in df.columns:
        issues.extend(_validate_subject_names(df[mapping["subject"]], filename))

    for variable in NUMERIC_VARS:
        col = mapping.get(variable, "")
        if not col or col not in df.columns:
            continue
        series = parse_number_series(df[col])
        bad_numeric = int(series.isna().sum())
        if bad_numeric and bad_numeric < len(series):
            issues.append(
                ValidationIssue(
                    severity="warn",
                    code=f"{variable}_numeric_parse_partial",
                    title=f"Some {variable} values are non-numeric",
                    description=f"{bad_numeric} row(s) in '{col}' could not be parsed as numbers.",
                    file=filename,
                    context={"column": col, "bad_rows": bad_numeric},
                )
            )
        if variable in {"feed", "drink", "ee", "vo2", "vco2"} and not series.dropna().empty:
            negative = int((series.dropna() < 0).sum())
            if negative:
                issues.append(
                    ValidationIssue(
                        severity="warn",
                        code=f"negative_{variable}",
                        title=f"Negative {variable} values detected",
                        description=f"{negative} row(s) in '{col}' are negative and deserve manual review.",
                        file=filename,
                        context={"column": col, "negative_rows": negative},
                    )
                )

    return issues


def _standardize_frame(
    *,
    df: pd.DataFrame,
    filename: str,
    vendor: str,
    mapping: dict[str, str],
) -> pd.DataFrame:
    stem = Path(filename).stem
    n = len(df)
    out = pd.DataFrame(
        {
            "source_file": [filename] * n,
            "vendor": [vendor] * n,
            "original_row_number": range(1, n + 1),
        }
    )

    if mapping.get("subject") and mapping["subject"] in df.columns:
        subject = df[mapping["subject"]].fillna("").astype(str).str.strip().replace({"": stem})
    else:
        subject = pd.Series([stem] * n, index=df.index)
    out["subject_id"] = subject

    if mapping.get("timestamp") and mapping["timestamp"] in df.columns:
        raw_timestamp = df[mapping["timestamp"]].fillna("").astype(str).str.strip()
        parsed_timestamp = parse_datetime_series(raw_timestamp)
    else:
        raw_timestamp = pd.Series([""] * n, index=df.index)
        parsed_timestamp = pd.to_datetime(pd.Series([pd.NaT] * n))

    out["raw_timestamp"] = raw_timestamp
    out["timestamp"] = parsed_timestamp.dt.strftime("%Y-%m-%dT%H:%M:%S").fillna("")
    out["_timestamp_dt"] = parsed_timestamp

    for variable in NUMERIC_VARS:
        col = mapping.get(variable, "")
        if col and col in df.columns:
            out[variable] = parse_number_series(df[col])
        else:
            out[variable] = pd.Series([float("nan")] * n, dtype="float64")

    out["hours_since_start"] = pd.Series([float("nan")] * n, dtype="float64")
    out["exclude_reason"] = ""
    out["excluded_variables"] = ""
    out["qc_food_spike"] = 0
    return out


def _generate_subject_sheet(subject_ids: Iterable[str], project: ProjectMetadata) -> pd.DataFrame:
    rows = []
    for subject_id in sorted(set(map(str, subject_ids))):
        rows.append(
            {
                **project.to_dict(),
                "subject_id": subject_id,
                "group": "",
                "start_mass_g": "",
                "end_mass_g": "",
                "lean_mass_g": "",
                "fat_mass_g": "",
                "treatment_subject": "",
                "notes_subject": "",
            }
        )
    return pd.DataFrame(rows, columns=SUBJECT_SHEET_BASE_COLUMNS)


def _read_subject_sheet(subject_sheet: str | Path | pd.DataFrame | None) -> pd.DataFrame | None:
    if subject_sheet is None:
        return None
    if isinstance(subject_sheet, pd.DataFrame):
        df = subject_sheet.copy()
    else:
        df, _, _ = read_table(subject_sheet)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.fillna("")
    return df


def _merge_subject_sheet(generated_df: pd.DataFrame, subject_sheet: str | Path | pd.DataFrame | None) -> pd.DataFrame:
    user_df = _read_subject_sheet(subject_sheet)
    if user_df is None or user_df.empty:
        return generated_df.copy()
    if "subject_id" not in user_df.columns:
        raise ValueError("Subject sheet must include a 'subject_id' column.")

    generated = generated_df.copy().fillna("").astype(object)
    user = user_df.copy().fillna("").astype(object)
    generated["subject_id"] = generated["subject_id"].astype(str).str.strip()
    user["subject_id"] = user["subject_id"].astype(str).str.strip()
    user = user[user["subject_id"] != ""]

    for col in SUBJECT_SHEET_BASE_COLUMNS:
        if col not in generated.columns:
            generated[col] = ""
        if col not in user.columns:
            user[col] = ""

    extra_cols = [col for col in user.columns if col not in generated.columns]
    for col in extra_cols:
        generated[col] = ""

    generated = generated.set_index("subject_id", drop=False)
    user = user.drop_duplicates(subset=["subject_id"], keep="last").set_index("subject_id", drop=False)

    overlap_cols = [col for col in generated.columns if col in user.columns and col != "subject_id"]
    for col in overlap_cols:
        replacement = user[col].reindex(generated.index).fillna("")
        mask = replacement.astype(str).str.strip().ne("")
        generated.loc[mask, col] = replacement.loc[mask]

    for col in extra_cols:
        generated[col] = user[col].reindex(generated.index).fillna("")

    ordered = SUBJECT_SHEET_BASE_COLUMNS + [col for col in generated.columns if col not in SUBJECT_SHEET_BASE_COLUMNS]
    return generated.reset_index(drop=True)[ordered]


def _robust_threshold(values: pd.Series, multiplier: float) -> tuple[float | None, str]:
    cleaned = values.dropna().astype(float)
    if cleaned.empty:
        return None, "none"
    median = float(cleaned.median())
    mad = float((cleaned - median).abs().median())
    if mad > 0:
        return median + multiplier * 1.4826 * mad, "median+MAD"
    q1 = float(cleaned.quantile(0.25))
    q3 = float(cleaned.quantile(0.75))
    iqr = q3 - q1
    if iqr > 0:
        return q3 + multiplier * iqr, "Q3+IQR"
    return None, "none"


def _flag_food_spikes(df: pd.DataFrame, multiplier: float) -> pd.DataFrame:
    if df.empty or "feed" not in df.columns:
        return pd.DataFrame(columns=["subject_id", "timestamp", "hours_since_start", "observed_value", "threshold", "method", "source_file", "original_row_number"])

    flags: list[dict[str, Any]] = []
    for subject_id, subject_df in df.groupby("subject_id", dropna=False):
        threshold, method = _robust_threshold(subject_df["feed"], multiplier)
        if threshold is None:
            continue
        mask = subject_df["feed"].notna() & (subject_df["feed"].astype(float) > threshold)
        if not mask.any():
            continue
        flagged = subject_df.loc[mask, ["subject_id", "timestamp", "hours_since_start", "feed", "source_file", "original_row_number"]].copy()
        flagged["observed_value"] = flagged["feed"]
        flagged["threshold"] = threshold
        flagged["method"] = method
        flags.extend(flagged.drop(columns=["feed"]).to_dict("records"))

    if not flags:
        return pd.DataFrame(columns=["subject_id", "timestamp", "hours_since_start", "observed_value", "threshold", "method", "source_file", "original_row_number"])
    return pd.DataFrame(flags).sort_values(["subject_id", "hours_since_start", "timestamp"], kind="stable")


def _apply_qc_flags(df: pd.DataFrame, qc_flags_df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if qc_flags_df.empty:
        return out
    key = qc_flags_df[["source_file", "original_row_number"]].drop_duplicates()
    marker = key.assign(_qc=1)
    out = out.merge(marker, on=["source_file", "original_row_number"], how="left")
    out["qc_food_spike"] = out["_qc"].fillna(0).astype(int)
    out = out.drop(columns=["_qc"])
    return out


def _estimate_subject_interval_hours(df: pd.DataFrame, subject_id: str) -> float | None:
    subject_df = df.loc[df["subject_id"].astype(str) == str(subject_id), ["hours_since_start"]].copy()
    hours = subject_df["hours_since_start"].dropna().astype(float).sort_values().drop_duplicates()
    if len(hours) < 2:
        return None
    diffs = hours.diff().dropna()
    diffs = diffs[diffs > 0]
    if diffs.empty:
        return None
    return float(diffs.median())


def _generate_suggested_exclusions(df: pd.DataFrame, qc_flags_df: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "subject_id",
        "variable",
        "start_hour",
        "end_hour",
        "reason",
        "source_file",
        "n_flagged_points",
        "peak_observed_value",
        "threshold",
        "method",
    ]
    if df.empty or qc_flags_df.empty:
        return pd.DataFrame(columns=columns)

    suggestions: list[dict[str, Any]] = []
    flagged = qc_flags_df.sort_values(["subject_id", "source_file", "hours_since_start", "timestamp"], kind="stable")
    for (subject_id, source_file), group in flagged.groupby(["subject_id", "source_file"], dropna=False):
        group = group.copy().sort_values(["hours_since_start", "timestamp"], kind="stable")
        interval = _estimate_subject_interval_hours(df, str(subject_id))
        gap_tolerance = interval * 1.25 if interval and interval > 0 else 0.0

        segment_rows: list[dict[str, Any]] = []
        previous_hour: float | None = None
        for row in group.to_dict("records"):
            current_hour = row.get("hours_since_start")
            current_hour = None if pd.isna(current_hour) else float(current_hour)
            if not segment_rows:
                segment_rows = [row]
                previous_hour = current_hour
                continue
            contiguous = False
            if current_hour is not None and previous_hour is not None:
                contiguous = (current_hour - previous_hour) <= max(gap_tolerance, 1e-9)
            if contiguous:
                segment_rows.append(row)
            else:
                suggestions.append(_segment_to_suggestion(subject_id, source_file, segment_rows, interval))
                segment_rows = [row]
            previous_hour = current_hour
        if segment_rows:
            suggestions.append(_segment_to_suggestion(subject_id, source_file, segment_rows, interval))

    out = pd.DataFrame(suggestions)
    if out.empty:
        return pd.DataFrame(columns=columns)
    return out[columns].sort_values(["subject_id", "start_hour", "end_hour"], kind="stable")


def _segment_to_suggestion(subject_id: Any, source_file: Any, rows: list[dict[str, Any]], interval: float | None) -> dict[str, Any]:
    hours = [float(row["hours_since_start"]) for row in rows if row.get("hours_since_start") is not None and not pd.isna(row.get("hours_since_start"))]
    start_hour = min(hours) if hours else 0.0
    end_hour = max(hours) if hours else 0.0
    padding = (interval / 2.0) if interval and interval > 0 else 0.0
    threshold = max(float(row.get("threshold", float("nan"))) for row in rows)
    observed = max(float(row.get("observed_value", float("nan"))) for row in rows)
    method = next((str(row.get("method")) for row in rows if row.get("method")), "")
    return {
        "subject_id": str(subject_id),
        "variable": "feed",
        "start_hour": round(max(0.0, start_hour - padding), 3),
        "end_hour": round(end_hour + padding, 3),
        "reason": "Suggested exclusion from QC feed spike flags",
        "source_file": str(source_file),
        "n_flagged_points": len(rows),
        "peak_observed_value": observed,
        "threshold": threshold,
        "method": method,
    }


def _apply_exclusions(
    *,
    standardized_df: pd.DataFrame,
    file_records: list[FileAnalysis],
    exclusions: list[ExclusionRule],
) -> tuple[pd.DataFrame, list[FileAnalysis], pd.DataFrame]:
    if not exclusions:
        empty = pd.DataFrame(columns=["subject_id", "variable", "start_hour", "end_hour", "reason"])
        for record in file_records:
            record.cleaned_vendor_df = record.original_df.copy()
        return standardized_df.copy(), file_records, empty

    tidy = standardized_df.copy()
    file_map = {record.filename: record for record in file_records}
    original_copies = {record.filename: record.original_df.copy() for record in file_records}

    for rule in exclusions:
        match = (
            tidy["subject_id"].astype(str) == str(rule.subject_id)
        ) & tidy["hours_since_start"].notna() & (
            tidy["hours_since_start"].astype(float).between(rule.start_hour, rule.end_hour, inclusive="both")
        )
        target_variables = NUMERIC_VARS if rule.variable == "all" else [rule.variable]
        for variable in target_variables:
            if variable in tidy.columns:
                tidy.loc[match, variable] = pd.NA

        matched_rows = tidy.loc[match, ["source_file", "original_row_number"]].copy()
        if not matched_rows.empty:
            current_reason = tidy.loc[match, "exclude_reason"].fillna("")
            current_vars = tidy.loc[match, "excluded_variables"].fillna("")
            tidy.loc[match, "exclude_reason"] = current_reason.mask(current_reason.eq(""), rule.reason).where(
                current_reason.eq(""), current_reason + "; " + rule.reason
            )
            tidy.loc[match, "excluded_variables"] = current_vars.mask(current_vars.eq(""), rule.variable).where(
                current_vars.eq(""), current_vars + "; " + rule.variable
            )

        for source_file, group_df in matched_rows.groupby("source_file"):
            record = file_map[source_file]
            original_df = original_copies[source_file]
            row_idx = group_df["original_row_number"].astype(int) - 1
            variables = target_variables
            for variable in variables:
                mapped_column = record.mapping.get(variable, "")
                if mapped_column and mapped_column in original_df.columns:
                    original_df.loc[row_idx, mapped_column] = ""
            original_copies[source_file] = original_df

    for record in file_records:
        record.cleaned_vendor_df = original_copies[record.filename]

    exclusion_df = pd.DataFrame([rule.to_dict() for rule in exclusions])
    return tidy, file_records, exclusion_df


def _apply_mapping_override(
    filename: str,
    default_mapping: dict[str, str],
    mapping_overrides: dict[str, Any] | None,
) -> dict[str, str]:
    if not mapping_overrides:
        return default_mapping
    override = None
    if filename in mapping_overrides and isinstance(mapping_overrides[filename], dict):
        override = mapping_overrides[filename]
    elif "global" in mapping_overrides and isinstance(mapping_overrides["global"], dict):
        override = mapping_overrides["global"]
    elif any(key in STANDARD_VARIABLES for key in mapping_overrides.keys()):
        override = mapping_overrides
    return merge_mapping(default_mapping, override)


def _build_mapping_review_df(file_records: list[FileAnalysis]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for record in file_records:
        for variable in STANDARD_VARIABLES:
            if variable == "vendor":
                continue
            source_column = record.mapping.get(variable, "")
            rows.append(
                {
                    "file": record.filename,
                    "vendor": record.vendor,
                    "variable": variable,
                    "source_column": source_column,
                    "status": "mapped" if source_column else "missing",
                }
            )
    if not rows:
        return pd.DataFrame(columns=["file", "vendor", "variable", "source_column", "status"])
    return pd.DataFrame(rows)


def _coerce_exclusions(raw: list[dict[str, Any]] | None) -> list[ExclusionRule]:
    if not raw:
        return []
    rules: list[ExclusionRule] = []
    for item in raw:
        try:
            rules.append(
                ExclusionRule(
                    subject_id=str(item["subject_id"]),
                    variable=str(item.get("variable", "all")),
                    start_hour=float(item["start_hour"]),
                    end_hour=float(item["end_hour"]),
                    reason=str(item.get("reason", "Manual exclusion")),
                )
            )
        except Exception as exc:  # pragma: no cover - defensive
            raise ValueError(f"Invalid exclusion rule: {item!r}") from exc
    return rules


def run_pipeline(
    input_paths: list[str | Path],
    *,
    project_metadata: ProjectMetadata | dict[str, Any] | None = None,
    mapping_overrides: dict[str, Any] | None = None,
    exclusions: list[ExclusionRule] | list[dict[str, Any]] | None = None,
    qc_multiplier: float = 6.0,
    subject_sheet: str | Path | pd.DataFrame | None = None,
) -> PipelineResult:
    if not input_paths:
        raise ValueError("At least one input file is required.")

    project = project_metadata if isinstance(project_metadata, ProjectMetadata) else ProjectMetadata.from_mapping(project_metadata)
    coerced_exclusions = exclusions if exclusions and isinstance(exclusions[0], ExclusionRule) else _coerce_exclusions(exclusions)  # type: ignore[index]

    file_records: list[FileAnalysis] = []
    all_validations: list[ValidationIssue] = []
    standardized_frames: list[pd.DataFrame] = []

    for path_like in input_paths:
        path = Path(path_like)
        df, headers, delimiter = read_table(path)
        vendor = detect_vendor(headers)
        base_mapping = infer_mapping(headers, vendor)
        mapping = _apply_mapping_override(path.name, base_mapping, mapping_overrides)
        validations = _validate_dataframe(df=df, headers=headers, filename=path.name, vendor=vendor, mapping=mapping)
        standardized = _standardize_frame(df=df, filename=path.name, vendor=vendor, mapping=mapping)

        file_record = FileAnalysis(
            path=path,
            filename=path.name,
            suffix=path.suffix.lower(),
            delimiter=delimiter,
            original_headers=headers,
            original_df=df,
            vendor=vendor,
            mapping=mapping,
            validations=validations,
            standardized_df=standardized,
        )
        file_records.append(file_record)
        all_validations.extend(validations)
        standardized_frames.append(standardized)

    combined = pd.concat(standardized_frames, ignore_index=True) if standardized_frames else pd.DataFrame()
    if not combined.empty and combined["_timestamp_dt"].notna().any():
        start_ts = combined.loc[combined["_timestamp_dt"].notna(), "_timestamp_dt"].min()
        delta_hours = (combined["_timestamp_dt"] - start_ts).dt.total_seconds() / 3600.0
        combined["hours_since_start"] = delta_hours.round(3)
    else:
        combined["hours_since_start"] = pd.Series(range(len(combined)), dtype="float64")

    subject_sheet_df = _generate_subject_sheet(combined["subject_id"].dropna().astype(str).tolist(), project)
    subject_sheet_df = _merge_subject_sheet(subject_sheet_df, subject_sheet)

    qc_flags = _flag_food_spikes(combined, qc_multiplier)
    combined = _apply_qc_flags(combined, qc_flags)
    suggested_exclusions = _generate_suggested_exclusions(combined, qc_flags)
    combined, file_records, exclusion_df = _apply_exclusions(
        standardized_df=combined,
        file_records=file_records,
        exclusions=coerced_exclusions,
    )

    validation_df = _build_validation_df(all_validations)
    mapping_review_df = _build_mapping_review_df(file_records)
    severity_counts = (
        validation_df["severity"].value_counts().to_dict() if not validation_df.empty else {"ok": 0, "warn": 0, "error": 0}
    )
    summary = {
        "tool": "CalR Guard",
        "version": "0.3.0",
        "input_file_count": len(file_records),
        "input_files": [record.filename for record in file_records],
        "subjects": int(combined["subject_id"].nunique()) if not combined.empty else 0,
        "rows": int(len(combined)),
        "validation_issue_count": int((validation_df["severity"] != "ok").sum()) if not validation_df.empty else 0,
        "validation_error_count": int(severity_counts.get("error", 0)),
        "validation_warn_count": int(severity_counts.get("warn", 0)),
        "validation_ok_count": int(severity_counts.get("ok", 0)),
        "qc_flag_count": int(len(qc_flags)),
        "suggested_exclusion_count": int(len(suggested_exclusions)),
        "exclusion_rule_count": int(len(exclusion_df)),
        "vendor_counts": combined["vendor"].value_counts().to_dict() if not combined.empty else {},
    }

    return PipelineResult(
        files=file_records,
        standardized_df=combined,
        subject_sheet_df=subject_sheet_df,
        qc_flags_df=qc_flags,
        suggested_exclusions_df=suggested_exclusions,
        mapping_review_df=mapping_review_df,
        exclusion_log_df=exclusion_df,
        validation_df=validation_df,
        project_metadata=project,
        bundle_summary=summary,
    )
