# Query configuration

This folder defines the queries `benchmark_client` runs against each SUT, for each real dataset
("scenario"): `ais`, `aviation`, `cycling`, `geolife`. A fifth folder, `general/`, holds
dataset-agnostic query templates that get instantiated once per real scenario and folded into
that scenario's own query files -- see [General queries](#general-queries-general) below. There
is no dataset called "general"; it is not a scenario you can select on its own.

## Layout

Each scenario folder (`ais/`, `aviation/`, `cycling/`, `geolife/`, and `general/`) has the same
shape:
```
<scenario>/
  config/
    combinedBenchConf.yaml   # query definitions -- the file you actually edit
    combineYaml.py           # one-time historical bootstrap (see below); not part of normal use
  scripts/
    generateParameterVariations.py   # reads combinedBenchConf.yaml, writes queries/*.yaml
    spatial_bounds*.txt, time_bounds*.txt   # cached data extent, used to keep generated
                                             # parameters inside the real dataset's footprint
  queries/
    <dialect>_queries.yaml   # generated output, staged here...
  data/                      # (ais only) small CSV reference datasets some queries join against
```
...and generation also copies the same files into
`benchmark/client/benchmark_client/src/main/resources/<scenario>/`, which is what
`benchmark_client` actually reads at runtime (via `QueryDispatcher.loadQueries`, a classpath
resource lookup) -- the `queries/` copies are for inspection/version control, not directly read
by the client.

Only six of the generated files per scenario are ever loaded by the client (see
`DialectRegistry.java` / `BenchmarkClient.java`):
`postgisSQL_queries.yaml`, `clickhouse_queries.yaml`, `duckdb_queries.yaml`,
`sedonaSQL_queries.yaml`, `mobilitydb_queries.yaml`, `spaceTimeSQL_queries.yaml`. Some scenarios
also produce `tsdb_queries.yaml`, `*_unprocess.yaml`, and `*_partitioned.yaml` variants (older/
experimental dialects and intermediate pipeline stages) -- these are generated for reference but
nothing in the active code path reads them.

`combineYaml.py` merges three separate legacy per-dialect files
(`mobilityDBBenchConf.yaml`/`postgisSQLBenchConf.yaml`/`sedonaSQLBenchConf.yaml`) into a single
`combinedBenchConf.yaml`. Those source files no longer exist in most scenarios -- treat
`combinedBenchConf.yaml` as the hand-maintained source of truth going forward, not something you
regenerate from `combineYaml.py`.

## combinedBenchConf.yaml

```yaml
benchmark:
  sut: MobilityDB      # informational label only -- not read by benchmark_client
  mixed: false          # see "Repetition and randomization" below
  nodes: ['']           # informational only (client's real thread/node config is the
  threads: 16            #  NUM_THREADS env var, not this file)
  test: false           # informational only
  random_seed: 1500     # makes generation reproducible -- same seed -> same generated queries

queryConfigs:
  - name: SpatialBBox
    use: true            # false skips this query entirely (no rows generated for it)
    description: "..."   # free text, not used by generation or the client
    type: spatial         # free text, carried through to the output file's "type" field
    repetition: 10        # how many randomized parameter draws to generate for this query
    parameters:
      - region
    mobilitydb: |
      ...SQL with :region placeholders...
    postgis: |
      ...
    sedona: |
      ...
    clickhouse: |
      ...
    duckdb: |
      ...
    spacetime: |
      ...   # may be blank -- see below
```

Each `queryConfigs` entry defines one *logical* query, once per dialect, sharing the same
`:placeholder` parameter names across all six SQL blocks so the same randomly-generated value
(same region, same time period, etc.) is substituted into every dialect's version -- letting you
compare the same logical query's plan/execution across systems. A dialect block left blank (`""`)
means that query genuinely isn't implemented for that dialect (not every query template makes
sense in every dialect); it is simply skipped when writing that dialect's output file.

### Editing queries

- Toggle `use: true`/`false` to include/exclude a query from generation entirely.
- Edit the SQL directly in the relevant dialect block. Keep placeholder names consistent across
  all six blocks for a given `queryConfigs` entry -- `return_param_values()` (in
  `generateParameterVariations.py`) substitutes each `:placeholder` into every block using the
  *same* randomly-generated value per repetition, so if a dialect's SQL references a placeholder
  no other dialect's block uses, only that dialect gets it substituted (harmless), but if you
  rename a placeholder in one block without the others, the others simply won't get it replaced.
- Available placeholders (the exact set `return_param_values()` knows how to generate a value
  for): `region`, `region1`, `second_region`, `region_medium`, `region_medium2` (random bounding
  boxes, `region_medium*` covering 5x the area of the others), `radius`/`distance` (a distance
  scaled to the dataset's spatial extent), `period` (a random fixed-length time span, already
  rendered as `'start' AND 'end'` for direct use in a `BETWEEN` clause -- don't wrap it in
  another layer of quotes), `point` (a random point), `k` (a random integer 1-10, e.g. for
  `LIMIT`). Anything else in `parameters:` resolves to an empty string.
- To add a brand-new query, add a new `queryConfigs` entry with a unique `name`, fill in whichever
  dialect blocks you want implemented (leave the rest `""`), list its `parameters`, and set
  `use`/`repetition`.

### Repetition and randomization

`repetition: N` controls how many independently-randomized parameter draws are generated for that
one query template -- each draw becomes one full entry (one per dialect) in the output files. The
total entry count per dialect file is the sum of `repetition` over every `use: true` queryConfig
(e.g. 6 queryConfigs x `repetition: 10` = 60 entries per dialect). Raise `repetition` for a query
to test it under more parameter variety; set `use: false` (rather than `repetition: 0`) to disable
it outright.

`random_seed` seeds a single `random.Random` instance used for the entire run, so regenerating
with the same config produces byte-identical output -- useful for reproducing a specific benchmark
run, or for diffing the effect of a config change. `mixed: true` additionally reshuffles the
Sedona query order independently, before the generator's own final shuffle (which reorders all six
dialects' lists together, by the same permutation, so a given index still refers to the same
logical query across dialects); leave `mixed: false` unless you specifically want Sedona's query
order decorrelated from the other dialects, since combining both shuffles can break that
same-index correspondence for Sedona specifically.

### Spatial/time bounds

`spatial_bounds*.txt` (`min_lon,min_lat,max_lon,max_lat`) and `time_bounds*.txt`
(`lower_timestamp,upper_timestamp`) in each scenario's `scripts/` folder cap where/when generated
parameters (regions, points, periods) fall, so they land inside the real dataset's actual extent
rather than empty space. These are checked-in cache files, not computed live from a database
connection -- if a scenario's real data footprint changes, regenerate these by hand (or point them
at the new extent) before regenerating queries, or generated queries may target areas/periods with
little or no real data.

Each file holds the minimum and maximum of that scenario's own point table (longitude/latitude, and the
timestamp as stored, i.e. the wall-clock values the queries compare against). The general queries read
`general/scripts/spatial_bounds_<scenario>.txt` / `time_bounds_<scenario>.txt`, the scenario-specific ones
`<scenario>/scripts/spatial_bounds.txt` / `time_bounds.txt`; both must hold the same extent.

| Scenario | Longitude | Latitude | Time |
|---|---|---|---|
| AIS | 1.341 – 18.775 | 52.370 – 60.490 | 2024-01-01 00:00:00 – 2025-07-07 23:59:59 |
| Aviation | 5.500 – 9.500 | 50.300 – 52.600 | 2024-01-01 00:00:01 – 2025-01-01 02:54:20 |
| Cycling | 13.121 – 13.573 | 52.386 – 52.595 | 2026-06-01 09:22:34 – 2026-06-04 20:43:53 |
| Geolife | 116.165 – 116.628 | 39.784 – 40.038 | 2019-06-30 22:00:00 – 2019-07-31 21:59:59 |

For a benchmark run, the query files are built on a copy of this folder with
[`scripts/realbench/build_jar.sh`](scripts/realbench/build_jar.sh), which sets the number of repetitions.

### Running the generator

From inside a scenario's `scripts/` folder (paths in the script are relative to it):
```
cd ais/scripts
python3 generateParameterVariations.py
```
This reads `../config/combinedBenchConf.yaml`, writes each dialect's queries to
`../queries/<dialect>_queries.yaml`, and copies the client-consumed subset into
`benchmark/client/benchmark_client/src/main/resources/<scenario>/`. Repeat per scenario you want
to regenerate -- there's no single command that regenerates all four real scenarios at once (each
scenario's `generateParameterVariations.py` is its own independent copy with that scenario's table
name and bounds files hardcoded in).

The generated query file schema (what `benchmark_client`'s `QueryDispatcher`/`QueryConfig`
actually read) is a flat YAML list of:
```yaml
- name: SpatialBBox
  type: spatial
  sql: |
    SELECT COUNT(*) ... WHERE ST_Intersects(geom, ST_MakeEnvelope(...));
  params:
    - ST_MakeEnvelope(...)
```
`sql` is fully parameter-substituted already -- there's no runtime placeholder binding, the client
just executes the literal SQL string. `params` is generation-time metadata (the values that went
into producing `sql`) carried through for readability/debugging; it is not read by the client at
query execution time.

## General queries (`general/`)

`general/config/combinedBenchConf.yaml` defines a set of query templates the same way scenario
configs do, except its SQL blocks are dataset-agnostic: they reference `:dataset` for the table
name instead of hardcoding one (e.g. `FROM :dataset WHERE ST_Intersects(geom, :region)`), and
`repetition: 10` across its `queryConfigs` currently produces 60 total entries per dialect.
`general/scripts/generateParameterVariations.py` (a shared, superset implementation of the
per-scenario script) knows the real table name and bounds files for every real scenario via
`DATASET_CONFIG`:
```python
DATASET_CONFIG = {
    "ais":      {"table": "crossing_points", "spatial_bounds_file": "spatial_bounds_ais.txt",      ...},
    "aviation": {"table": "flight_points",    "spatial_bounds_file": "spatial_bounds_aviation.txt", ...},
    "geolife":  {"table": "geolife_points",   "spatial_bounds_file": "spatial_bounds_geolife.txt",  ...},
    "cycling":  {"table": "ride_points",      "spatial_bounds_file": "spatial_bounds_cycling.txt",  ...},
}
```

**These general templates are never generated or shipped as their own standalone "general"
application.** `general/scripts/mergeGeneralIntoApps.py` is what actually ties them in: for each
real scenario, it instantiates the general templates using that scenario's own table name and
bounds (so `:dataset` becomes e.g. `geolife_points`, and generated regions/periods fall inside
that scenario's real data extent, exactly like a scenario's own generator would), then *appends*
the resulting entries onto that scenario's own existing query files -- both the `queries/`
staging copy and the `resources/<scenario>/` copy the client reads -- rather than writing a
separate file anywhere. The effect: every general query template runs against every real dataset,
alongside that dataset's own dedicated queries, in the same query files the client already loads.

Run it (no arguments -- it always processes all four real scenarios in one pass):
```
cd general/scripts
python3 mergeGeneralIntoApps.py
```
It's safe to rerun: since generation is deterministic (fixed `random_seed`), it detects when a
scenario/dialect file already has the general entries appended (by checking whether the tail of
the file already matches what it's about to generate) and skips re-appending rather than
duplicating them. It generates entries for all six dialects but filters out any with blank SQL before merging, the
same way blank dialect blocks are always skipped -- since every general template currently leaves
its `mobilitydb`/`spacetime` blocks blank, in practice this means no general queries currently end
up in `mobilitydb_queries.yaml`/`spaceTimeSQL_queries.yaml`, only in the other four. It only
merges into the six client-consumed dialect files described above -- `tsdb_queries.yaml` and the
`*_partitioned`/`*_unprocess` variants are left alone.

If you add or edit a general query template, rerunning `mergeGeneralIntoApps.py` will append the
*new* set of general entries again (the old ones aren't removed automatically, since they're now
indistinguishable from a scenario's own hand-edited queries once merged) -- regenerate each
scenario's own queries from scratch first (its own `generateParameterVariations.py`, which
overwrites rather than appends) if you want a clean slate before re-merging general queries in.
