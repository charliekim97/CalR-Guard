from __future__ import annotations

import html
import json
import re
import shutil
from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import pandas as pd

from .io_utils import write_dataframe_preserve_type
from .models import PipelineResult


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat() if not pd.isna(value) else None
    if pd.isna(value):
        return None
    return value


def _tidy_export(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "_timestamp_dt" in out.columns:
        out = out.drop(columns=["_timestamp_dt"])
    return out


def _safe_slug(text: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", str(text)).strip("_")
    return cleaned or "subject"


def _df_to_html(df: pd.DataFrame, *, max_rows: int = 200) -> str:
    if df.empty:
        return "<p class='muted'>No rows.</p>"
    preview = df.head(max_rows).copy()
    return preview.to_html(index=False, border=0, classes="dataframe compact")


def _write_qc_plots(result: PipelineResult, outdir: Path) -> list[dict[str, str]]:
    plot_dir = outdir / "qc_plots"
    plot_dir.mkdir(parents=True, exist_ok=True)
    tidy = _tidy_export(result.standardized_df)
    if tidy.empty or "feed" not in tidy.columns or "hours_since_start" not in tidy.columns:
        return []

    metadata: list[dict[str, str]] = []
    for subject_id, subject_df in tidy.groupby("subject_id", dropna=False):
        subject_df = subject_df.copy()
        subject_df = subject_df[subject_df["feed"].notna() & subject_df["hours_since_start"].notna()].sort_values("hours_since_start")
        if subject_df.empty:
            continue
        flagged = result.qc_flags_df.loc[result.qc_flags_df["subject_id"].astype(str) == str(subject_id)].copy()

        fig, ax = plt.subplots(figsize=(8, 4.5))
        ax.plot(subject_df["hours_since_start"], subject_df["feed"], marker="o", linewidth=1)
        if not flagged.empty:
            ax.scatter(flagged["hours_since_start"], flagged["observed_value"], marker="x", s=60)
        ax.set_title(f"Feed QC: {subject_id}")
        ax.set_xlabel("Hours since start")
        ax.set_ylabel("Feed")
        ax.grid(True, alpha=0.25)
        fig.tight_layout()

        filename = f"{_safe_slug(str(subject_id))}_feed_qc.png"
        output_file = plot_dir / filename
        fig.savefig(output_file, dpi=150)
        plt.close(fig)
        metadata.append({"subject_id": str(subject_id), "path": str(output_file.relative_to(outdir))})
    return metadata


def _write_review_report(result: PipelineResult, outdir: Path, qc_plot_meta: list[dict[str, str]]) -> Path:
    summary = result.bundle_summary
    validation_df = result.validation_df.copy()
    severity_counts = validation_df["severity"].value_counts().to_dict() if not validation_df.empty else {}
    tidy_preview = _tidy_export(result.standardized_df)

    cards = [
        ("Input files", summary.get("input_file_count", 0)),
        ("Rows", summary.get("rows", 0)),
        ("Subjects", summary.get("subjects", 0)),
        ("Errors", severity_counts.get("error", 0)),
        ("Warnings", severity_counts.get("warn", 0)),
        ("QC flags", summary.get("qc_flag_count", 0)),
        ("Suggested exclusions", summary.get("suggested_exclusion_count", 0)),
        ("Manual exclusions", summary.get("exclusion_rule_count", 0)),
    ]
    cards_html = "".join(
        f"<div class='card'><div class='label'>{html.escape(str(label))}</div><div class='value'>{html.escape(str(value))}</div></div>"
        for label, value in cards
    )

    input_files_html = "".join(f"<li>{html.escape(str(name))}</li>" for name in summary.get("input_files", []))
    plot_html = "".join(
        f"<div class='plot'><h3>{html.escape(item['subject_id'])}</h3><img src='{html.escape(item['path'])}' alt='QC plot for {html.escape(item['subject_id'])}'></div>"
        for item in qc_plot_meta
    )

    report_html = f"""
<!DOCTYPE html>
<html lang='en'>
<head>
  <meta charset='utf-8'>
  <title>CalR Guard review report</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 24px; color: #1f2937; }}
    h1, h2, h3 {{ margin-bottom: 0.4rem; }}
    .muted {{ color: #6b7280; }}
    .cards {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 12px; margin: 16px 0 24px; }}
    .card {{ border: 1px solid #d1d5db; border-radius: 10px; padding: 14px; background: #f9fafb; }}
    .label {{ font-size: 0.85rem; color: #6b7280; }}
    .value {{ font-size: 1.5rem; font-weight: 700; margin-top: 6px; }}
    table.dataframe {{ border-collapse: collapse; width: 100%; font-size: 0.92rem; margin: 12px 0 24px; }}
    table.dataframe th, table.dataframe td {{ border: 1px solid #e5e7eb; padding: 6px 8px; text-align: left; vertical-align: top; }}
    table.dataframe thead th {{ background: #f3f4f6; }}
    .plot-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(360px, 1fr)); gap: 18px; }}
    .plot img {{ width: 100%; border: 1px solid #e5e7eb; border-radius: 8px; }}
    ul {{ margin-top: 8px; }}
    .footer {{ margin-top: 32px; font-size: 0.9rem; color: #6b7280; }}
  </style>
</head>
<body>
  <h1>CalR Guard review report</h1>
  <p class='muted'>Human-readable summary for sanity checking before data goes downstream. Because sending a professor a zip full of JSON is a good way to test their patience.</p>

  <div class='cards'>{cards_html}</div>

  <h2>Input files</h2>
  <ul>{input_files_html}</ul>

  <h2>Validation report</h2>
  {_df_to_html(validation_df)}

  <h2>Mapping review</h2>
  {_df_to_html(result.mapping_review_df)}

  <h2>Suggested exclusions from QC flags</h2>
  {_df_to_html(result.suggested_exclusions_df)}

  <h2>Manual exclusions applied</h2>
  {_df_to_html(result.exclusion_log_df)}

  <h2>QC feed spike flags</h2>
  {_df_to_html(result.qc_flags_df)}

  <h2>Subject metadata sheet preview</h2>
  {_df_to_html(result.subject_sheet_df, max_rows=100)}

  <h2>Standardized output preview</h2>
  {_df_to_html(tidy_preview, max_rows=100)}

  <h2>QC plots</h2>
  <div class='plot-grid'>{plot_html or "<p class='muted'>No QC plots generated.</p>"}</div>

  <div class='footer'>Generated by CalR Guard {html.escape(str(summary.get('version', '')))}.</div>
</body>
</html>
"""
    output_file = outdir / "review_report.html"
    output_file.write_text(report_html, encoding="utf-8")
    return output_file


def write_bundle(result: PipelineResult, outdir: str | Path, *, zip_output: bool = True) -> tuple[Path, Path | None]:
    out_path = Path(outdir)
    out_path.mkdir(parents=True, exist_ok=True)

    tidy = _tidy_export(result.standardized_df)
    tidy.to_csv(out_path / "cleaned_tidy.csv", index=False)

    result.subject_sheet_df.to_csv(out_path / "metadata_sheet.csv", index=False)
    result.subject_sheet_df.to_excel(out_path / "metadata_sheet.xlsx", index=False)

    result.qc_flags_df.to_csv(out_path / "qc_flags.csv", index=False)
    result.suggested_exclusions_df.to_csv(out_path / "suggested_exclusions.csv", index=False)
    result.exclusion_log_df.to_csv(out_path / "exclusion_log.csv", index=False)
    result.validation_df.to_csv(out_path / "validation_report.csv", index=False)
    result.mapping_review_df.to_csv(out_path / "mapping_review.csv", index=False)

    with (out_path / "validation_report.json").open("w", encoding="utf-8") as f:
        json.dump(result.validation_df.to_dict(orient="records"), f, indent=2, default=_json_default)

    with (out_path / "suggested_exclusions.json").open("w", encoding="utf-8") as f:
        json.dump(result.suggested_exclusions_df.to_dict(orient="records"), f, indent=2, default=_json_default)

    with (out_path / "project_metadata.json").open("w", encoding="utf-8") as f:
        json.dump(result.project_metadata.to_dict(), f, indent=2, default=_json_default)

    with (out_path / "bundle_summary.json").open("w", encoding="utf-8") as f:
        json.dump(result.bundle_summary, f, indent=2, default=_json_default)

    mapping_payload = {
        record.filename: {
            "vendor": record.vendor,
            "mapping": record.mapping,
            "source_suffix": record.suffix,
        }
        for record in result.files
    }
    with (out_path / "mapping.json").open("w", encoding="utf-8") as f:
        json.dump(mapping_payload, f, indent=2, default=_json_default)

    vendor_dir = out_path / "vendor_preserved"
    vendor_dir.mkdir(exist_ok=True)
    for record in result.files:
        cleaned = record.cleaned_vendor_df if record.cleaned_vendor_df is not None else record.original_df
        base_name = record.path.stem + "_cleaned"
        if record.suffix in {".xlsx", ".xls"}:
            output_file = vendor_dir / f"{base_name}.xlsx"
        else:
            output_file = vendor_dir / f"{base_name}{record.suffix or '.csv'}"
        write_dataframe_preserve_type(cleaned, output_file, delimiter=record.delimiter)

    qc_plot_meta = _write_qc_plots(result, out_path)
    _write_review_report(result, out_path, qc_plot_meta)

    readme = out_path / "README_BUNDLE.txt"
    readme.write_text(
        "CalR Guard export bundle\n\n"
        "Files:\n"
        "- cleaned_tidy.csv: normalized analysis-friendly table with QC and exclusion annotations\n"
        "- vendor_preserved/*: original vendor columns preserved, with requested exclusions blanked at the source column level\n"
        "- metadata_sheet.csv/xlsx: editable subject and project metadata scaffold\n"
        "- qc_flags.csv: suspicious feeding spikes flagged for review\n"
        "- suggested_exclusions.csv/json: candidate feed exclusion windows derived from QC spikes\n"
        "- exclusion_log.csv: manual variable/time-window exclusions applied\n"
        "- validation_report.csv/json: preflight warnings and errors\n"
        "- mapping_review.csv / mapping.json: inferred column mapping per source file\n"
        "- review_report.html: human-readable summary for quick review\n"
        "- qc_plots/*: subject-level feed QC plots\n"
        "- bundle_summary.json: top-line counts\n\n"
        "Note: vendor_preserved exports are designed for manual review and downstream preprocessing.\n"
        "Whether a given file can be loaded directly into CalR still depends on the original manufacturer export format.\n",
        encoding="utf-8",
    )

    zip_path: Path | None = None
    if zip_output:
        zip_path = Path(shutil.make_archive(str(out_path), "zip", root_dir=out_path))

    result.output_dir = out_path
    result.output_zip = zip_path
    return out_path, zip_path
