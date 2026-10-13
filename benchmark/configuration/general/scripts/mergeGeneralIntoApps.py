"""
Instantiates the application-agnostic query templates in ../config/combinedBenchConf.yaml once
per real application (ais/aviation/cycling/geolife) -- using that application's own table name and
spatial/time bounds files, exactly what DATASET_CONFIG and the generateParameterVariations.py
machinery in this same directory were already built to do -- and merges the result into each
application's own existing query files, instead of what actually happened before: the whole
"general" folder was generated exactly once, silently defaulting to geolife (config['benchmark']
has no "dataset" key, and prepare_query_tasks() falls back to "geolife" when it's missing), then
shipped as if it were its own separate selectable application. There's no dataset called "general"
in real life -- these are meant to run against every dataset, not instead of them.

Reuses generateParameterVariations.py's own functions (DATASET_CONFIG, prepare_query_tasks, the
per-dialect post-processing regexes) unchanged rather than re-deriving them, since a lot of
non-obvious correctness already went into that file (see its own FIX comments) -- this script
only adds the "which app, and where does the result go" layer around it.

Only merges into the 6 dialect files DialectRegistry.java (benchmark_client's actual dialect ->
query-file mapping) and BenchmarkClient.java both know how to run: postgisSQL_queries.yaml,
clickhouse_queries.yaml, duckdb_queries.yaml, sedonaSQL_queries.yaml, mobilitydb_queries.yaml,
spaceTimeSQL_queries.yaml. Deliberately skips tsdb_queries.yaml and the *_partitioned/*_unprocess
variants some app folders also have -- nothing in the actively-used code path reads those.
"""
import os
import random
import sys

import yaml

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import generateParameterVariations as gpv  # noqa: E402
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "scripts"))
import sedona_crs  # noqa: E402

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))  # .../benchmark/configuration/general/scripts
GENERAL_APP_DIR = os.path.dirname(SCRIPT_DIR)  # .../benchmark/configuration/general
CONFIGURATION_DIR = os.path.dirname(GENERAL_APP_DIR)  # .../benchmark/configuration
BENCHMARK_DIR = os.path.dirname(CONFIGURATION_DIR)  # .../benchmark
RESOURCES_DIR = os.path.join(BENCHMARK_DIR, "client", "benchmark_client", "src", "main", "resources")
RESOURCES_DIR = os.path.normpath(RESOURCES_DIR)

# dialect key -> (query-list variable name from generate_for_app(), resource filename)
DIALECT_FILES = [
    ("postgis", "postgisSQL_queries.yaml"),
    ("clickhouse", "clickhouse_queries.yaml"),
    ("duckdb", "duckdb_queries.yaml"),
    ("sedona", "sedonaSQL_queries.yaml"),
    ("mobilitydb", "mobilitydb_queries.yaml"),
    ("spacetime", "spaceTimeSQL_queries.yaml"),
]


class LiteralString(str):
    pass


def literal_str_representer(dumper, data):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


yaml.add_representer(LiteralString, literal_str_representer)


def generate_for_app(app_key: str, config: dict, rnd: random.Random):
    """Runs the existing query-generation + dialect-specific post-processing for one application,
    returning {dialect_key: [entries]}. Mirrors generateParameterVariations.py's __main__ block,
    minus the parts (mobilitydb grid-partition variants, tsdb renaming, file writing) that either
    don't apply here or are handled by the caller instead."""
    dataset_conf = gpv.DATASET_CONFIG[app_key]
    table_name = dataset_conf["table"]

    (
        mobilitydb_queries,
        postgis_queries,
        sedona_queries,
        spacetime_queries,
        clickhouse_queries,
        duckdb_queries,
    ) = gpv.prepare_query_tasks(config, rnd)

    # Same shuffle-by-permutation the original script applies, so query order is randomized the
    # same way across dialects (a given index still refers to "the same logical query" everywhere)
    # -- EXCEPT this can no longer assume every dialect list is the same length: mobilitydb_queries
    # can now be shorter than the other five (see prepare_query_tasks's "needs_trajectory" check
    # in generateParameterVariations.py -- queries that need a MobilityDB trajectory table are
    # skipped entirely for mobilitydb when running against a dataset with no such table, e.g.
    # geolife). A single shared `indices` sized to len(mobilitydb_queries) and applied to every
    # list -- the previous version of this code -- silently truncated the other five lists down
    # to mobilitydb's shorter length instead (confirmed live: geolife's postgis/clickhouse/
    # duckdb/spacetime lists were cut from 300 entries to 100, discarding 200 real ones, and
    # already got merged into the real resource files once before this was caught and reverted).
    # The other five dialects are still always mutually equal in length (nothing skips per-dialect
    # for them), so they can still share one permutation; mobilitydb gets its own, sized to its
    # own (possibly shorter) length.
    other_len = len(postgis_queries)
    assert other_len == len(sedona_queries) == len(spacetime_queries) == len(clickhouse_queries) == len(duckdb_queries), (
        "postgis/sedona/spacetime/clickhouse/duckdb are expected to always stay the same length "
        "as each other (only mobilitydb ever skips entries per-dataset) -- if this fires, that "
        "assumption no longer holds and the shuffle below needs rethinking, not silencing."
    )
    other_indices = list(range(other_len))
    rnd.shuffle(other_indices)
    postgis_queries = [postgis_queries[i] for i in other_indices]
    sedona_queries = [sedona_queries[i] for i in other_indices]
    spacetime_queries = [spacetime_queries[i] for i in other_indices]
    clickhouse_queries = [clickhouse_queries[i] for i in other_indices]
    duckdb_queries = [duckdb_queries[i] for i in other_indices]

    # (2026-10-13) MobilityDB takes the same permutation as the other dialects whenever its list is as
    # long as theirs (every scenario but geolife). It used to get a permutation of its own, so it ran
    # the same parameter sets in a different order, and its two warm-up repetitions were different ones.
    if len(mobilitydb_queries) == other_len:
        mob_indices = other_indices
    else:
        mob_indices = list(range(len(mobilitydb_queries)))
        rnd.shuffle(mob_indices)
    mobilitydb_queries = [mobilitydb_queries[i] for i in mob_indices]
    # (2026-10-09) MobilityDB is a trajectory system and geolife has no trajectories: it runs no geolife
    # queries at all, including these general point-table ones. Cleared after the shuffle so the random
    # sequence -- and every other dialect's parameters -- stays the same.
    if app_key == "geolife":
        mobilitydb_queries = []

    # sedonaSQL-specific post-processing (verbatim from generateParameterVariations.py's __main__)
    def fix_sedona(entries):
        import re

        for e in entries:
            sql = e.sql
            sql = re.sub(r"TIMESTAMP\s*\(\s*'([^']+)'\s*\)", r"TIMESTAMP '\1'", sql)
            sql = re.sub(
                r"\(ft\.start_time,\s*fe\.end_time\)\s*OVERLAPS\s*\[\s*'([^']+)'\s*,\s*'([^']+)'\s*\]",
                r"(ft.start_time <= TIMESTAMP '\2' AND fe.end_time >= TIMESTAMP '\1')",
                sql,
            )
            sql = sql.replace("DATE(st.start_ts)", "CAST(st.start_ts AS DATE)")
            sql = sedona_crs.fix_text(sql)  # explicit CRS strings for ST_Transform (scripts/sedona_crs.py)
            e.sql = sql
        return entries

    sedona_queries = fix_sedona(sedona_queries)

    # spaceTimeSQL-specific post-processing. The original generateParameterVariations.py __main__
    # applies these as text substitutions over a yaml.dump() of the *whole* entry list (including
    # each entry's `params`, which are plain query parameter values, not SQL) and reparses the
    # result with yaml.safe_load(). All the substitutions here are themselves plain regexes over
    # SQL syntax (ST_ToGeom/ST_SetSRID/ST_Point/TIMESTAMP/DATE literals) -- they were never actually
    # dependent on YAML structure. Round-tripping the whole entry list through dump+reload just to
    # run text regexes broke here: a `params` value produced by generate_fixed_time_span() (e.g.
    # "'2024-06-08 10:34:51' AND '2024-06-08 10:35:04'") contains literal single-quote characters,
    # which yaml.dump() correctly escapes, but the blanket .replace("''", "'") step (meant to undo
    # *YAML's own* quote-doubling around the whole scalar) also collapses those escaped quotes
    # *inside* the string, producing invalid YAML that yaml.safe_load() then chokes on.
    # FIX: apply the substitutions directly to each entry's `sql` string instead of round-tripping
    # the entire entry (including `params`) through YAML text -- same result for `sql` (the only
    # field any of these regexes actually target), no risk of `params` corrupting the parse.
    def fix_spacetime(entries):
        import re

        st_point_pattern = re.compile(
            r"""ST_ToGeom\(\s*
                ST_SetSRID\(\s*
                ST_Point\(\s*([^)]+?)\s*\)\s*
                ,\s*3857\s*\)\s*
                ::\s*gpoint\s*\)
            """,
            re.X | re.IGNORECASE,
        )
        coord_pattern = re.compile(
            r"ST_Point\(\s*([+-]?\d+(?:\.\d+)?),\s*([+-]?\d+(?:\.\d+)?)\s*\)::gpoint"
        )
        from pyproj import Transformer

        transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)

        def convert_coords(match):
            x, y = float(match.group(1)), float(match.group(2))
            lon, lat = transformer.transform(x, y)
            return f"ST_GeogPoint({lon:.12f}, {lat:.12f})"

        out = []
        for e in entries:
            d = dict(e.__dict__)
            sql = d.get("sql") or ""
            sql = gpv.adjust_timestamp_format(sql)
            sql = sql.replace("SRID=3857;", "")
            sql = st_point_pattern.sub(r"ST_ToGeom(ST_Point(\1)::gpoint)", sql)
            sql = coord_pattern.sub(convert_coords, sql)
            sql = sql.replace(", 3857)", "").replace("ST_SetSRID(", "").replace(
                "ST_ToGeom(ST_Point(", "ST_ToGeom(ST_GeogPoint("
            )
            d["sql"] = sql
            out.append(d)
        return out

    spacetime_dicts = fix_spacetime(spacetime_queries)

    def as_dicts(entries):
        return [e.__dict__ for e in entries]

    return {
        "postgis": as_dicts(postgis_queries),
        "clickhouse": as_dicts(clickhouse_queries),
        "duckdb": as_dicts(duckdb_queries),
        "sedona": as_dicts(sedona_queries),
        "mobilitydb": as_dicts(mobilitydb_queries),
        "spacetime": spacetime_dicts,
    }


def literalize(entries):
    """Force block-style ('|') rendering for every entry's multi-line sql, whether it was just
    generated or loaded back from an existing file -- otherwise the two would render
    inconsistently (existing entries as plain escaped strings, new ones as block scalars) in the
    same merged file. Purely cosmetic -- benchmark_client's YAML parser accepts either -- but
    keeps the generated files readable."""
    out = []
    for e in entries:
        e = dict(e)
        sql = e.get("sql")
        if isinstance(sql, str) and "\n" in sql:
            e["sql"] = LiteralString(sql)
        out.append(e)
    return out


def merge_into_file(path: str, new_entries: list):
    existing = []
    if os.path.exists(path):
        with open(path) as f:
            existing = yaml.safe_load(f) or []
    # Skip entries with blank/whitespace-only SQL (e.g. spacetime queries this project doesn't
    # implement for a given template) -- nothing to run, no point shipping a no-op query.
    new_entries = [e for e in new_entries if (e.get("sql") or "").strip()]

    # FIX: generation is deterministic (fixed random_seed), so re-running this script produces the
    # exact same new_entries for a given app+dialect every time. Without this check, re-running
    # after a partial failure (e.g. this exact scenario: the run crashed partway through on an
    # unrelated pre-existing YAML bug in a later app's file, after already successfully merging
    # several earlier apps/dialects) would append the same general queries a second time into
    # whichever files already got merged. Detect "already merged" by checking whether the tail of
    # `existing` already equals `new_entries` (compared by name+sql, the two fields that make a
    # query unique here) and skip appending if so -- makes this script safe to interrupt and rerun.
    if new_entries and len(existing) >= len(new_entries):
        tail = existing[len(existing) - len(new_entries):]
        already_merged = all(
            t.get("name") == n.get("name") and t.get("sql") == n.get("sql")
            for t, n in zip(tail, new_entries)
        )
        if already_merged:
            return len(existing) - len(new_entries), 0

    merged = literalize(existing) + literalize(new_entries)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(merged, f, sort_keys=False, allow_unicode=True)
    return len(existing), len(new_entries)


def main():
    config = gpv.load_config(os.path.join(SCRIPT_DIR, "..", "config", "combinedBenchConf.yaml"))
    if not config:
        print("Failed to load general combinedBenchConf.yaml")
        sys.exit(1)

    main_seed = config["benchmark"]["random_seed"]

    for app_key in gpv.DATASET_CONFIG:
        # Point the shared get_time_bounds()/get_spatial_bounds() (which read a bare relative
        # filename from the current working directory) at this app's own bounds files -- they
        # live in this same scripts/ directory (spatial_bounds_<app>.txt / time_bounds_<app>.txt).
        os.chdir(SCRIPT_DIR)
        config["benchmark"]["dataset"] = app_key
        rnd = random.Random(main_seed)

        generated = generate_for_app(app_key, config, rnd)

        print(f"=== {app_key} (table={gpv.DATASET_CONFIG[app_key]['table']}) ===")
        for dialect_key, filename in DIALECT_FILES:
            new_entries = generated[dialect_key]

            resource_path = os.path.join(RESOURCES_DIR, app_key, filename)
            n_existing, n_new = merge_into_file(resource_path, new_entries)

            staging_path = os.path.join(CONFIGURATION_DIR, app_key, "queries", filename)
            merge_into_file(staging_path, new_entries)

            print(f"  {filename}: {n_existing} existing + {n_new} general = {n_existing + n_new}")


if __name__ == "__main__":
    main()
