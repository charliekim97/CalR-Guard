from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Any

import pandas as pd


def detect_delimiter(sample: str) -> str:
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", "\t", ";", "|"])
        return dialect.delimiter
    except csv.Error:
        # TSV is common in instrument exports. Otherwise fall back to comma.
        tab_count = sample.count("\t")
        comma_count = sample.count(",")
        return "\t" if tab_count > comma_count else ","


def read_text_like(path: Path) -> tuple[pd.DataFrame, list[str], str]:
    raw = path.read_text(encoding="utf-8", errors="replace")
    first_chunk = raw[:8192]
    delimiter = detect_delimiter(first_chunk)
    reader = csv.reader(io.StringIO(raw), delimiter=delimiter)
    try:
        header_row = next(reader)
    except StopIteration:
        return pd.DataFrame(), [], delimiter
    original_headers = [str(h).strip() for h in header_row]
    df = pd.read_csv(
        io.StringIO(raw),
        sep=delimiter,
        engine="python",
        dtype=str,
        keep_default_na=False,
    )
    df.columns = [str(c).strip() for c in df.columns]
    return df, original_headers, delimiter


def read_excel_like(path: Path) -> tuple[pd.DataFrame, list[str]]:
    df = pd.read_excel(path, dtype=str)
    df = df.fillna("")
    headers = [str(c).strip() for c in df.columns]
    df.columns = headers
    return df, headers


def read_table(path: str | Path) -> tuple[pd.DataFrame, list[str], str | None]:
    resolved = Path(path)
    suffix = resolved.suffix.lower()
    if suffix in {".csv", ".tsv", ".txt"}:
        df, headers, delimiter = read_text_like(resolved)
        return df, headers, delimiter
    if suffix in {".xlsx", ".xls"}:
        df, headers = read_excel_like(resolved)
        return df, headers, None
    raise ValueError(f"Unsupported file type: {resolved.suffix}")


def parse_number_series(series: pd.Series) -> pd.Series:
    if series.empty:
        return series.astype(float)
    cleaned = series.astype(str).str.replace(",", "", regex=False).str.strip()
    cleaned = cleaned.replace({"": pd.NA, "NA": pd.NA, "nan": pd.NA, "None": pd.NA})
    return pd.to_numeric(cleaned, errors="coerce")


def parse_datetime_series(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.to_datetime(series)
    cleaned = series.astype(str).str.strip().replace({"": pd.NA, "NA": pd.NA, "nan": pd.NA})
    return pd.to_datetime(cleaned, errors="coerce")


def write_dataframe_preserve_type(df: pd.DataFrame, path: Path, delimiter: str | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    suffix = path.suffix.lower()
    if suffix in {".xlsx", ".xls"}:
        df.to_excel(path, index=False)
        return
    sep = delimiter or ("\t" if suffix in {".tsv", ".txt"} else ",")
    df.to_csv(path, index=False, sep=sep)


def read_json_or_yaml(path: str | Path) -> dict[str, Any]:
    import json
    import yaml

    text = Path(path).read_text(encoding="utf-8")
    suffix = Path(path).suffix.lower()
    if suffix == ".json":
        return json.loads(text)
    return yaml.safe_load(text)
