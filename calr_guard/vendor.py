from __future__ import annotations

import re
from typing import Iterable

STANDARD_VARIABLES = [
    "subject",
    "timestamp",
    "vo2",
    "vco2",
    "ee",
    "rer",
    "feed",
    "drink",
    "wheel",
    "activity",
]

_MATCHERS: dict[str, list[re.Pattern[str]]] = {
    "subject": [
        re.compile(r"^subject$", re.I),
        re.compile(r"^subject[_ ]?id$", re.I),
        re.compile(r"^animal$", re.I),
        re.compile(r"^animal[_ ]?id$", re.I),
        re.compile(r"^cage$", re.I),
        re.compile(r"^cage[_ ]?id$", re.I),
        re.compile(r"^id$", re.I),
    ],
    "timestamp": [
        re.compile(r"^datetime$", re.I),
        re.compile(r"^date[_ ]?time$", re.I),
        re.compile(r"^timestamp$", re.I),
        re.compile(r"^date time$", re.I),
        re.compile(r"^time$", re.I),
        re.compile(r"^date$", re.I),
    ],
    "vo2": [
        re.compile(r"^VO2\(3\)$", re.I),
        re.compile(r"^vo2$", re.I),
        re.compile(r"^vo2\b", re.I),
    ],
    "vco2": [
        re.compile(r"^VCO2\(3\)$", re.I),
        re.compile(r"^vco2$", re.I),
        re.compile(r"^vco2\b", re.I),
    ],
    "ee": [
        re.compile(r"^ee$", re.I),
        re.compile(r"energy.?expenditure", re.I),
        re.compile(r"kcal", re.I),
        re.compile(r"heat", re.I),
    ],
    "rer": [re.compile(r"^rer$", re.I)],
    "feed": [
        re.compile(r"^feed$", re.I),
        re.compile(r"^food$", re.I),
        re.compile(r"food.?intake", re.I),
        re.compile(r"feed.?intake", re.I),
    ],
    "drink": [
        re.compile(r"^drink$", re.I),
        re.compile(r"^water$", re.I),
        re.compile(r"water.?intake", re.I),
        re.compile(r"drink.?intake", re.I),
    ],
    "wheel": [re.compile(r"wheel", re.I)],
    "activity": [
        re.compile(r"xytot", re.I),
        re.compile(r"locomotor", re.I),
        re.compile(r"activity", re.I),
        re.compile(r"ped.?meters?", re.I),
    ],
}


def _find_header(headers: Iterable[str], patterns: list[re.Pattern[str]]) -> str:
    header_list = list(headers)
    for pattern in patterns:
        for header in header_list:
            if pattern.search(str(header).strip()):
                return str(header)
    return ""


def detect_vendor(headers: list[str]) -> str:
    joined = " | ".join(map(str, headers))
    if re.search(r"VO2\(3\)|VCO2\(3\)|H\(3\)|XT\+YT", joined, re.I):
        return "TSE"
    if re.search(r"Promethion|Exp|Exd|Animal_ID|PedMeters|CageTemp", joined, re.I):
        return "Sable"
    if re.search(r"Oxymax|O2|CO2|Subject|Cage", joined, re.I) and re.search(r"Date|Time|VO2", joined, re.I):
        return "Columbus"
    return "Unknown"


def infer_mapping(headers: list[str], vendor: str | None = None) -> dict[str, str]:
    mapping: dict[str, str] = {"vendor": vendor or detect_vendor(headers)}
    for variable in STANDARD_VARIABLES:
        mapping[variable] = _find_header(headers, _MATCHERS[variable])

    # Prefer the uncorrected TSE columns when they exist.
    if "VO2(3)" in headers:
        mapping["vo2"] = "VO2(3)"
    if "VCO2(3)" in headers:
        mapping["vco2"] = "VCO2(3)"
    return mapping


def merge_mapping(base: dict[str, str], override: dict[str, str] | None) -> dict[str, str]:
    merged = dict(base)
    if not override:
        return merged
    for key, value in override.items():
        if key in STANDARD_VARIABLES and value is not None:
            merged[key] = str(value)
    return merged
