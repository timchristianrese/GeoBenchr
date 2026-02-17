import argparse
import re
import sys
import yaml
from pathlib import Path

from queryConfigGenerator import QueryConfig
from postGISGenerator import PostGISGenerator
from mongoDBGenerator import MongoDBGenerator
from sedonaGenerator import SedonaGenerator
from spaceTimeGenerator import SpaceTimeGenerator
from CQLGenerator import CQLGenerator

def sanitize_name(name: str, default: str = "unnamed") -> str:
    if not name or not str(name).strip():
        return default
    
    s = str(name).strip()
    s = re.sub(r"[^\w\-\.]+", "_", s, flags=re.UNICODE)
    s = re.sub(r"_+", "_", s)
    s = s.strip("._-")
    return s or default

def ensure_unique(path: Path) -> Path:
    if not path.exists():
        return path
    
    base = path.stem
    suffix = path.suffix
    parent = path.parent
    i = 1
    while True:
        cand = parent / f"{base}-{i}{suffix}"
        if not cand.exists():
            return cand
        i += 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Generate SQL/NoSQL queries from a YAML configuration file"
    )
    parser.add_argument(
        '-c', '--cql', action='store_true',
        help="Generate CQL queries"
    )
    parser.add_argument(
        '-p', '--postgis', action='store_true',
        help="Generate PostGIS SQL queries"
    )
    parser.add_argument(
        '-m', '--mongo', action='store_true',
        help="Generate MongoDB aggregation pipelines"
    )
    parser.add_argument(
        '-s', '--sedona', action='store_true',
        help="Generate Sedona code"
    )
    parser.add_argument(
        '-st', '--spacetime', action='store_true',
        help="Generate SpaceTime code"
    )
    parser.add_argument(
        '-r', '--recursive', action='store_true',
        help="When CONFIG is a directory, recurse into subdirectories"
    )
    parser.add_argument(
        '-o', '--outdir', default='queries',
        help="Base output directory for generated queries (default: 'queries')"
    )
    parser.add_argument(
        'config', metavar='CONFIG.yaml',
        help="Path to the YAML file containing query definitions"
    )
    args = parser.parse_args()

    if not (args.postgis or args.mongo or args.sedona or args.spacetime):
        parser.error("You must specify at least one of -p/--postgis, -m/--mongo or -s/--sedona")


    def process_one_yaml(config_path: Path) -> None:
        try:
            with open(config_path, encoding="utf-8") as f:
                queries_conf = yaml.safe_load(f) or []
        except Exception as e:
            print(f"[ERROR] Failed to read YAML {config_path}: {e}", file=sys.stderr)
            return
    
        if not isinstance(queries_conf, list):
            print("YAML must contain a list of query definitions.", file=sys.stderr)
            sys.exit(1)

        root_name = sanitize_name(config_path.stem)
        base_out_dir = Path(args.outdir)
        base_out_dir.mkdir(parents=True, exist_ok=True)
        root_dir = ensure_unique(base_out_dir / root_name)
        root_dir.mkdir(parents=True, exist_ok=True)

        index_lines = []

        for idx, raw_q in enumerate(queries_conf, start=1):
            try:
                cfg = QueryConfig.from_dict(raw_q)
            except Exception as e:
                print(f"[ERROR] {config_path} query #{idx}: {e}", file=sys.stderr)
                continue

            q_name_raw = getattr(cfg, "name", None) or raw_q.get('name') or f"query-{idx}"
            q_dir_name = sanitize_name(q_name_raw, default=f"query-{idx}")
            q_dir = ensure_unique(root_dir / q_dir_name)
            q_dir.mkdir(parents=True, exist_ok=True)

            index_lines.append(f"- {q_dir_name}/")

            if args.postgis:
                try:
                    pg = PostGISGenerator(cfg)
                    sql_path = q_dir / "postgis.sql"
                    with open(sql_path, "w", encoding="utf8") as out:
                        out.write(pg.generate())
                        out.write("\n")
                    index_lines.append(f"  • PostGIS: {sql_path.name}")
                except Exception as e:
                    print(f"[ERROR] PostGIS generation failed for {q_dir_name}: {e}", file=sys.stderr)

            if args.spacetime:
                try:
                    pg = SpaceTimeGenerator(cfg)
                    st_path = q_dir / "spacetime.sql"
                    with open(st_path, "w", encoding="utf8") as out:
                        out.write(pg.generate())
                        out.write("\n")
                    index_lines.append(f"  • SpaceTime: {st_path.name}")
                except Exception as e:
                    print(f"[ERROR] PostGIS generation failed for {q_dir_name}: {e}", file=sys.stderr)

            if args.mongo:
                try:
                    mg = MongoDBGenerator(cfg)
                    mongo_path = q_dir / "mongodb.py"
                    with open(mongo_path, "w", encoding="utf-8") as out:
                        out.write(mg.generate())
                        out.write("\n")
                    index_lines.append(f"  • MongoDB: {mongo_path.name}")
                except Exception as e:
                    print(f"[ERROR] Mongo generation failed for {q_dir_name}: {e}", file=sys.stderr)

            if args.sedona:
                try:
                    sg = SedonaGenerator(cfg)
                    sedona_path = q_dir / "sedona_rdd.py"
                    with open(sedona_path, "w", encoding="utf-8") as out:
                        out.write(sg.generate())
                        out.write("\n")
                    index_lines.append(f"  • Sedona: {sedona_path.name}")
                except Exception as e:
                    print(f"[ERROR] Sedona generation failed for {q_dir_name}: {e}", file=sys.stderr)   
            if args.cql:
                try:
                    cg = CQLGenerator(cfg)
                    cql_path = q_dir / "cql.txt"
                    with open(cql_path, "w", encoding="utf-8") as out:
                        out.write(cg.generate())
                        out.write("\n")
                    index_lines.append(f"  • CQL: {cql_path.name}")
                except Exception as e:
                    print(f"[ERROR] CQL generation failed for {q_dir_name}: {e}", file=sys.stderr)

            
        if index_lines:
            with open(root_dir / "INDEX.txt", "w", encoding="utf-8") as idxf:
                idxf.write("\n".join(index_lines))
                idxf.write("\n")

        print(f"[OK] Output generated: {root_dir.resolve()}")

    config_path = Path(args.config)

    if config_path.is_file():
        process_one_yaml(config_path)
    elif config_path.is_dir():
        pattern = "**/*.y*ml" if args.recursive else "*.y*ml"
        yaml_files = sorted(config_path.glob(pattern)) if not args.recursive else sorted(config_path.rglob("*.y*ml"))
        if not yaml_files:
            print(f"[WARN] No YAML files found in directory: {config_path}", file=sys.stderr)
            sys.exit(0)
        for yf in yaml_files:
            process_one_yaml(yf)
    else:
        print(f"Config path not found: {config_path}", file=sys.stderr)
        sys.exit(1)