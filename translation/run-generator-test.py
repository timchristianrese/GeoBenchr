#!/usr/bin/env python3
"""
Usage:
  python3 run-generator-test.py --tech sedona postgis spacetime
"""

from __future__ import annotations
import argparse
import logging
import sys
from pathlib import Path
from typing import Callable, Dict, List


TEST_DIR = Path(__file__).resolve().parent / "final_tests" / "benchmark_cycling_tests" / "pdf_tests"


ROOT = Path(__file__).resolve().parent
SCRIPT_DIR = ROOT / "script"
sys.path.append(str(SCRIPT_DIR))

try:
    from queryConfigGenerator import QueryConfig
    from sedonaGenerator import SedonaGenerator
    from spaceTimeGenerator import SpaceTimeGenerator
except ImportError as exc:
    print("Import error in Sedona/SpaceTime generator:", exc)
    sys.exit(1)

try:
    from postGISGenerator import PostGISGenerator
except Exception as exc:
    print("Import error in PostGISGenerator:", exc)
    sys.exit(1)

GENERATOR_CLASS: Dict[str, Callable[[QueryConfig], object]] = {
    "sedona": SedonaGenerator,
    "postgis": PostGISGenerator,
    "spacetime": SpaceTimeGenerator,
}

# ----------------------------------------------------------------------
# Utilities
# ----------------------------------------------------------------------
def load_cfg(yml_path: Path) -> QueryConfig:
    import yaml
    data = yaml.safe_load(yml_path.read_text())
    root = data[0] if isinstance(data, list) else data
    return QueryConfig.from_dict(root)

def generate(cfg: QueryConfig, tech: str) -> str:
    gen_cls = GENERATOR_CLASS[tech]
    gen = gen_cls(cfg)
    return str(gen.generate()).strip()

def write_txt(path: Path, content: str) -> None:
    path.write_text(content + ("\n" if not content.endswith("\n") else ""), encoding="utf-8")

def is_yaml(p: Path) -> bool:
    return p.suffix.lower() in {".yml", ".yaml"}

def is_result_file(p: Path) -> bool:
    return p.name.endswith("_result.yaml") or p.name.endswith("_result.yml")

def list_base_yaml_files(tst_dir: Path) -> List[Path]:
    return [
        p for p in sorted(tst_dir.iterdir())
        if p.is_file() and is_yaml(p) and not is_result_file(p) and "spacetime" not in p.stem.lower()
    ]

def find_spacetime_variant(base_yml: Path) -> Path | None:
    stem_no_ext = base_yml.with_suffix("")
    candidates = [
        stem_no_ext.with_name(stem_no_ext.name + "_spacetime").with_suffix(".yaml"),
        stem_no_ext.with_name(stem_no_ext.name + "_spacetime").with_suffix(".yml"),
    ]
    for c in candidates:
        if c.exists():
            return c
    for c in base_yml.parent.iterdir():
        if is_yaml(c) and "spacetime" in c.stem.lower():
            if c.stem.lower().startswith(base_yml.stem.lower()):
                return c
    return None

def pick_yaml_for_tech(base_yml: Path, tech: str) -> Path:
    if tech.lower() == "spacetime":
        y = find_spacetime_variant(base_yml)
        if y:
            logging.info("Using SpaceTime variant: %s", y.name)
            return y
        logging.info("[fallback] Using generic %s for SpaceTime", base_yml.name)
    return base_yml

def run_one(yml_to_load: Path, out_base: Path, tech: str) -> None:
    cfg = load_cfg(yml_to_load)
    out_txt = generate(cfg, tech)
    out_path = out_base.with_name(f"{out_base.stem}_{tech}.txt")
    write_txt(out_path, out_txt)
    logging.info("ok %-20s [%s] -> %s", out_base.name, tech, out_path.name)

# ----------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Generates queries for each technology from YAML.")
    ap.add_argument("--tech", nargs="+", choices=list(GENERATOR_CLASS),
                    default=list(GENERATOR_CLASS),
                    help="Technologies to generate (default: sedona postgis spacetime)")
    ap.add_argument("-v", "--verbose", action="count", default=0, help="Verbosity (-v, -vv)")
    return ap.parse_args()

def main() -> None:
    args = parse_args()
    lvl = logging.WARNING - 10 * args.verbose if args.verbose else logging.INFO
    logging.basicConfig(level=max(logging.DEBUG, lvl), format="%(levelname)s: %(message)s")

    tst_dir = TEST_DIR
    if not tst_dir.exists() or not tst_dir.is_dir():
        print(f"Error: folder not found: {tst_dir}")
        sys.exit(1)

    bases = list_base_yaml_files(tst_dir)
    if not bases:
        logging.warning("No YAML found in %s", tst_dir)
        sys.exit(0)

    for base in bases:
        for tech in args.tech:
            yml_to_load = pick_yaml_for_tech(base, tech)
            run_one(yml_to_load, base, tech)

    logging.info("Done. %d files processed.", len(bases))

if __name__ == "__main__":
    main()