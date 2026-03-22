from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any

import gradio as gr

from calr_guard.exporter import write_bundle
from calr_guard.pipeline import run_pipeline


def _parse_optional_json(text: str) -> dict[str, Any] | list[dict[str, Any]] | None:
    text = (text or "").strip()
    if not text:
        return None
    return json.loads(text)


def run_app_pipeline(
    files: list[str] | None,
    subject_sheet_file: str | None,
    project_title: str,
    investigator: str,
    system: str,
    species: str,
    strain: str,
    sex: str,
    diet_kcal_per_g: float | None,
    light_start_zt: float | None,
    light_end_zt: float | None,
    ambient_temperature_c: float | None,
    acclimation_hours: float | None,
    treatment: str,
    notes: str,
    qc_multiplier: float,
    mapping_json: str,
    exclusions_json: str,
):
    if not files:
        raise gr.Error("Upload at least one data file.")

    project_metadata = {
        "project_title": project_title,
        "investigator": investigator,
        "system": system,
        "species": species,
        "strain": strain,
        "sex": sex,
        "diet_kcal_per_g": diet_kcal_per_g,
        "light_start_zt": light_start_zt,
        "light_end_zt": light_end_zt,
        "ambient_temperature_c": ambient_temperature_c,
        "acclimation_hours": acclimation_hours,
        "treatment": treatment,
        "notes": notes,
    }
    try:
        mapping_overrides = _parse_optional_json(mapping_json)
    except Exception as exc:
        raise gr.Error(f"Invalid mapping JSON: {exc}") from exc
    try:
        exclusions = _parse_optional_json(exclusions_json)
    except Exception as exc:
        raise gr.Error(f"Invalid exclusions JSON: {exc}") from exc

    result = run_pipeline(
        files,
        project_metadata=project_metadata,
        mapping_overrides=mapping_overrides if isinstance(mapping_overrides, dict) else None,
        exclusions=exclusions if isinstance(exclusions, list) else None,
        qc_multiplier=qc_multiplier,
        subject_sheet=subject_sheet_file or None,
    )

    workdir = Path(tempfile.mkdtemp(prefix="calr_guard_"))
    bundle_dir, zip_path = write_bundle(result, workdir / "bundle", zip_output=True)
    assert zip_path is not None

    validation_df = result.validation_df.copy()
    mapping_df = result.mapping_review_df.copy()
    preview_df = result.standardized_df.drop(columns=["_timestamp_dt"], errors="ignore").head(200).copy()
    subject_df = result.subject_sheet_df.copy()
    qc_df = result.qc_flags_df.copy()
    suggested_df = result.suggested_exclusions_df.copy()
    report_path = bundle_dir / "review_report.html"

    summary = result.bundle_summary
    md = (
        f"### CalR Guard summary\n"
        f"- input files: **{summary['input_file_count']}**\n"
        f"- rows: **{summary['rows']}**\n"
        f"- subjects: **{summary['subjects']}**\n"
        f"- validation errors: **{summary.get('validation_error_count', 0)}**\n"
        f"- validation warnings: **{summary.get('validation_warn_count', 0)}**\n"
        f"- QC feed flags: **{summary['qc_flag_count']}**\n"
        f"- suggested exclusions: **{summary.get('suggested_exclusion_count', 0)}**\n"
        f"- manual exclusion rules applied: **{summary['exclusion_rule_count']}**"
    )
    return md, validation_df, mapping_df, subject_df, suggested_df, qc_df, preview_df, str(report_path), str(zip_path)


with gr.Blocks(title="CalR Guard") as demo:
    gr.Markdown(
        "# CalR Guard\n"
        "Preflight validation, metadata scaffolding, QC flagging, and variable/time-window exclusions for calorimetry exports.\n"
        "The point is to clean up the boring failures before CalR ever sees the files. Humanity somehow keeps tripping over delimiters and bad timestamps, so here we are."
    )

    with gr.Tab("1. Upload & project"):
        files = gr.File(label="Input files", file_count="multiple", file_types=[".csv", ".tsv", ".txt", ".xlsx", ".xls"], type="filepath")
        subject_sheet_file = gr.File(label="Optional existing metadata sheet (.csv/.xlsx)", file_types=[".csv", ".tsv", ".txt", ".xlsx", ".xls"], type="filepath")
        with gr.Row():
            project_title = gr.Textbox(label="Project title")
            investigator = gr.Textbox(label="Investigator")
            system = gr.Textbox(label="System / instrument")
        with gr.Row():
            species = gr.Textbox(label="Species", value="Mouse")
            strain = gr.Textbox(label="Strain")
            sex = gr.Dropdown(label="Sex", choices=["", "Female", "Male", "Mixed"], value="")
        with gr.Row():
            diet_kcal_per_g = gr.Number(label="Diet kcal/g", value=None)
            light_start_zt = gr.Number(label="Light start ZT", value=0)
            light_end_zt = gr.Number(label="Light end ZT", value=12)
        with gr.Row():
            ambient_temperature_c = gr.Number(label="Ambient temperature C", value=None)
            acclimation_hours = gr.Number(label="Acclimation hours", value=24)
            treatment = gr.Textbox(label="Treatment")
        notes = gr.Textbox(label="Notes", lines=3)

    with gr.Tab("2. Optional overrides"):
        gr.Markdown(
            "**Mapping override JSON** example:\n"
            "```json\n{\n  \"global\": {\"timestamp\": \"Datetime\", \"subject\": \"Subject\", \"feed\": \"Feed\"}\n}\n```\n\n"
            "**Exclusions JSON** example:\n"
            "```json\n[\n  {\"subject_id\": \"Mouse_01\", \"variable\": \"feed\", \"start_hour\": 0.4, \"end_hour\": 0.6, \"reason\": \"Feeder artifact\"}\n]\n```"
        )
        mapping_json = gr.Textbox(label="Mapping override JSON", lines=10)
        exclusions_json = gr.Textbox(label="Exclusions JSON", lines=10)
        qc_multiplier = gr.Slider(label="QC feed spike multiplier", minimum=2.0, maximum=12.0, step=0.5, value=6.0)

    run_btn = gr.Button("Run CalR Guard", variant="primary")

    with gr.Tab("3. Results"):
        summary_md = gr.Markdown()
        validation_df = gr.Dataframe(label="Validation report")
        mapping_df = gr.Dataframe(label="Mapping review")
        subject_df = gr.Dataframe(label="Merged subject metadata sheet")
        suggested_df = gr.Dataframe(label="Suggested exclusions from QC flags")
        qc_df = gr.Dataframe(label="QC feed spike flags")
        preview_df = gr.Dataframe(label="Preview of standardized tidy output")
        report_file = gr.File(label="Download review report (HTML)")
        bundle_file = gr.File(label="Download export bundle (.zip)")

    run_btn.click(
        fn=run_app_pipeline,
        inputs=[
            files,
            subject_sheet_file,
            project_title,
            investigator,
            system,
            species,
            strain,
            sex,
            diet_kcal_per_g,
            light_start_zt,
            light_end_zt,
            ambient_temperature_c,
            acclimation_hours,
            treatment,
            notes,
            qc_multiplier,
            mapping_json,
            exclusions_json,
        ],
        outputs=[summary_md, validation_df, mapping_df, subject_df, suggested_df, qc_df, preview_df, report_file, bundle_file],
    )


if __name__ == "__main__":  # pragma: no cover
    demo.launch(server_name="127.0.0.1", server_port=7860, show_error=True)
