#!/usr/bin/env python3


from pathlib import Path
import re
from typing import List, Tuple, Dict

TESTS_DIR = "final_tests/benchmark_cycling_tests"  # <- to adapt
OUTPUT_FILENAME = "tests_summary.txt"

BACKENDS: List[Tuple[str, str]] = [
    ("postgis", "PostGIS"),
    ("sedona", "Sedona"),
    ("spacetime", "SpaceTime"),
]

YAML_GLOB = "*.yaml"

SEPARATOR = "=" * 80
SUB_SEP   = "-" * 80


def read_text(p: Path) -> str:
    if not p.exists():
        return ""
    try:
        return p.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return p.read_text(encoding="latin-1")


def extract_test_key(yaml_path: Path) -> str:
    return yaml_path.stem


def natural_sort_key(s: str):
    return [int(text) if text.isdigit() else text.lower()
            for text in re.split(r"(\d+)", s)]


def main() -> None:
    root = Path(TESTS_DIR).expanduser().resolve()
    if not root.exists():
        raise SystemExit(f"No folder : {root}")

    yaml_files = sorted(root.glob(YAML_GLOB), key=lambda p: natural_sort_key(p.name))

    out_path = root / OUTPUT_FILENAME
    lines: List[str] = []

    lines.append(SEPARATOR)
    lines.append(f"summary test in — folder: {root}")
    lines.append(SEPARATOR)
    lines.append("")

    if not yaml_files:
        lines.append("no yaml found.")
    else:
        for yml in yaml_files:
            key = extract_test_key(yml)

            lines.append(SEPARATOR)
            lines.append(f"TEST: {key}")
            lines.append(SEPARATOR)
            lines.append("")
            # YAML
            lines.append(f"--- YAML ({yml.name}) ---")
            lines.append(read_text(yml).rstrip())
            lines.append("")

            for backend_key, backend_label in BACKENDS:
                candidate = root / f"{key}_{backend_key}.txt"
                lines.append(SUB_SEP)
                lines.append(f"{backend_label} ({candidate.name})")
                lines.append(SUB_SEP)
                if candidate.exists():
                    content = read_text(candidate).rstrip()
                    lines.append(content if content else "[empty file]")
                else:
                    lines.append("[no file]")
                lines.append("")
            lines.append("")

    out_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    print(f"OK: summary wrote at {out_path}")


if __name__ == "__main__":
    main()