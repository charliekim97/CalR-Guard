from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from .exporter import write_bundle
from .io_utils import read_json_or_yaml
from .pipeline import run_pipeline


def _load_exclusions(path: str | None) -> list[dict[str, Any]] | None:
    if not path:
        return None
    payload = read_json_or_yaml(path)
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict) and "exclusions" in payload and isinstance(payload["exclusions"], list):
        return payload["exclusions"]
    raise ValueError("Exclusions file must be a list or contain an 'exclusions' list.")


def _load_config(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    payload = read_json_or_yaml(path)
    if not isinstance(payload, dict):
        raise ValueError("Config file must be a JSON or YAML object.")
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CalR Guard: preflight validation and curation for calorimetry exports")
    parser.add_argument("--input", nargs="+", required=True, help="One or more input files (.csv, .tsv, .txt, .xlsx)")
    parser.add_argument("--outdir", required=True, help="Directory where the export bundle will be written")
    parser.add_argument("--config", help="Optional JSON/YAML config with project_metadata, mapping_overrides, exclusions, qc_multiplier")
    parser.add_argument("--mapping", help="Optional JSON/YAML mapping override file")
    parser.add_argument("--project", help="Optional JSON/YAML project metadata file")
    parser.add_argument("--subject-sheet", help="Optional CSV/XLSX subject metadata sheet to merge into generated metadata")
    parser.add_argument("--exclusions", help="Optional JSON/YAML exclusion rules file")
    parser.add_argument("--qc-multiplier", type=float, default=None, help="Robust multiplier used to flag feed spikes (default 6.0)")
    parser.add_argument("--no-zip", action="store_true", help="Do not also create a .zip archive of the output bundle")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    config = _load_config(args.config)
    mapping_overrides = read_json_or_yaml(args.mapping) if args.mapping else config.get("mapping_overrides")
    project_metadata = read_json_or_yaml(args.project) if args.project else config.get("project_metadata")
    exclusions = _load_exclusions(args.exclusions) if args.exclusions else config.get("exclusions")
    qc_multiplier = args.qc_multiplier if args.qc_multiplier is not None else config.get("qc_multiplier", 6.0)
    subject_sheet = args.subject_sheet or config.get("subject_sheet")

    result = run_pipeline(
        args.input,
        project_metadata=project_metadata,
        mapping_overrides=mapping_overrides,
        exclusions=exclusions,
        qc_multiplier=float(qc_multiplier),
        subject_sheet=subject_sheet,
    )
    outdir, zip_path = write_bundle(result, args.outdir, zip_output=not args.no_zip)

    payload = {
        "output_dir": str(outdir),
        "output_zip": str(zip_path) if zip_path else None,
        "summary": result.bundle_summary,
    }
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
