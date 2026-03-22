from __future__ import annotations

from pathlib import Path

import pandas as pd

from calr_guard.exporter import write_bundle
from calr_guard.pipeline import run_pipeline


def test_pipeline_detects_tse_and_qc_flag(tmp_path: Path) -> None:
    demo = Path(__file__).resolve().parents[1] / "examples" / "demo_tse_like.csv"
    result = run_pipeline([demo])

    assert result.bundle_summary["input_file_count"] == 1
    assert result.bundle_summary["subjects"] == 2
    assert result.standardized_df["vendor"].eq("TSE").all()
    assert len(result.qc_flags_df) == 1
    assert len(result.suggested_exclusions_df) == 1
    assert result.validation_df["severity"].isin(["ok", "warn", "error"]).all()

    outdir, zip_path = write_bundle(result, tmp_path / "bundle", zip_output=True)
    assert (outdir / "cleaned_tidy.csv").exists()
    assert (outdir / "metadata_sheet.xlsx").exists()
    assert (outdir / "mapping.json").exists()
    assert (outdir / "mapping_review.csv").exists()
    assert (outdir / "suggested_exclusions.csv").exists()
    assert (outdir / "review_report.html").exists()
    assert (outdir / "qc_plots").exists()
    assert zip_path is not None and zip_path.exists()


def test_variable_time_window_exclusion_blanks_feed(tmp_path: Path) -> None:
    demo = Path(__file__).resolve().parents[1] / "examples" / "demo_tse_like.csv"
    result = run_pipeline(
        [demo],
        exclusions=[
            {
                "subject_id": "Mouse_01",
                "variable": "feed",
                "start_hour": 0.5,
                "end_hour": 0.5,
                "reason": "artifact",
            }
        ],
    )

    excluded = result.standardized_df.loc[
        (result.standardized_df["subject_id"] == "Mouse_01")
        & (result.standardized_df["hours_since_start"] == 0.5)
    ]
    assert excluded["feed"].isna().all()
    assert excluded["exclude_reason"].eq("artifact").all()

    write_bundle(result, tmp_path / "bundle", zip_output=False)
    vendor_clean = tmp_path / "bundle" / "vendor_preserved" / "demo_tse_like_cleaned.csv"
    content = vendor_clean.read_text(encoding="utf-8")
    assert ",,0.03,12.2" in content or ",,0.03,12.2\n" in content


def test_subject_sheet_merge(tmp_path: Path) -> None:
    demo = Path(__file__).resolve().parents[1] / "examples" / "demo_tse_like.csv"
    subject_sheet = tmp_path / "subject_sheet.csv"
    pd.DataFrame(
        [
            {"subject_id": "Mouse_01", "group": "Control", "start_mass_g": 24.1, "room": "A1"},
            {"subject_id": "Mouse_02", "group": "Treatment", "start_mass_g": 26.3, "room": "A2"},
        ]
    ).to_csv(subject_sheet, index=False)

    result = run_pipeline([demo], subject_sheet=subject_sheet)
    merged = result.subject_sheet_df.set_index("subject_id")
    assert merged.loc["Mouse_01", "group"] == "Control"
    assert str(merged.loc["Mouse_02", "room"]) == "A2"
