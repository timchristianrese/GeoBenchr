"""Writes the README of one benchmark run folder (e.g. read-only/1-thread/) from the CSVs build_results.py and
empty_results.py produced in it. Everything in it is derived from those files.
    python3 make_run_readme.py read-only/1-thread 1
"""
import csv
import os
import statistics
import sys
from collections import Counter, defaultdict

RUN, THREADS = sys.argv[1], int(sys.argv[2])
MAIN_README = os.path.relpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "README.md"), RUN)
SYSTEMS = ["PostGIS", "MobilityDB", "ClickHouse", "DuckDB", "SpaceTime", "Sedona"]
SCEN = ["ais", "aviation", "cycling", "geolife"]
SCEN_NAME = {"ais": "AIS", "aviation": "Aviation", "cycling": "Cycling", "geolife": "Geolife"}
NODES = ["nst-1", "nst-2", "nst-3", "nst-4", "nst-5"]


def rows(name):
    p = os.path.join(RUN, name)
    return list(csv.DictReader(open(p))) if os.path.exists(p) else []


def ms(v):
    if v in (None, ""):
        return "–"
    v = float(v)
    return f"{v / 1000:.2f} s" if v >= 1000 else f"{v:.0f} ms"


def num(v, f="{:.2f}"):
    return "–" if v in (None, "", "None") else f.format(float(v))


scores = {r["system"]: r for r in rows("system_scores.csv")}
summary = rows("summary.csv")
winners = rows("winners.csv")
pairs = rows("pairwise_total_time.csv")
dropped = rows("dropped_parameter_sets.csv")
empty = rows("empty_results.csv")
hw = rows("hardware_by_phase.csv")
execs = rows("executions.csv")
measured = [r for r in execs if r["warmup"] != "True"]
present = [s for s in SYSTEMS if s in scores]
WRITE_TYPES = {"insert", "update", "update_single", "delete", "delete_single", "bulk_insert"}
writes = [r for r in summary if r["query_type"] in WRITE_TYPES and r["planned"] not in ("0", "")]
n_reads = sum(1 for r in rows("winners.csv") if r["query_type"] not in WRITE_TYPES)
dates = sorted(r["logged_at"][:10] for r in execs if r.get("logged_at"))

share = None
if writes:
    nr = sum(int(r["succeeded"] or 0) for r in summary if r["query_type"] not in WRITE_TYPES and r["system"] == "PostGIS")
    nw = sum(int(r["succeeded"] or 0) for r in writes if r["system"] == "PostGIS")
    share = round(100 * nr / (nr + nw)) if nr + nw else None
title = f"Reads and writes {share}/{100 - share}" if share else "Read-only"
out = [f"# {title}, {THREADS} client thread{'s' if THREADS > 1 else ''}", ""]
out += [(f"Run of {dates[0]} to {dates[-1]}. " if dates else "") +
        f"Every system alone on the five-node cluster, one system after the other; {THREADS} client thread{'s' if THREADS > 1 else ''} "
        f"taking the queries from one shared queue{' (so ' + str(THREADS) + ' queries run at the same time)' if THREADS > 1 else ''}. "
        f"Each of the {n_reads} read queries runs with **128 measured parameter sets** (the same sets in every client-thread run) "

        f"after {THREADS} warm-up repetition{'s' if THREADS > 1 else ''}. "
        + (f"Write queries (inserts, updates, bulk inserts of 10 rows) are mixed in: {100 - share} % of the measured "
           "executions; everything they added is removed after each system (`purge_writes.sh`). Systems without write "
           "support are not part of this workload. " if writes else "") +
        "Timeout 600 s per query. The configuration of every "
        "system is described in the [main README](" + MAIN_README + "#configuration). All numbers below come from the CSV "
        "files in this folder (`build_results.py`, `empty_results.py`); [`tables.md`](tables.md) has every table in full.", ""]

# --- summary
out += ["## Summary", "",
        "| System | Queries won | Ranked on | Geo-mean slowdown vs fastest | Median slowdown | Measured executions | Failed |",
        "|---|---:|---:|---:|---:|---:|---:|"]
st = Counter((r["system"], r["status"]) for r in measured)
for s in sorted(present, key=lambda s: (-int(scores[s]["wins"]), float(scores[s]["geomean_slowdown_vs_best"] or 1e9))):
    r = scores[s]
    n = sum(v for (sy, _), v in st.items() if sy == s)
    failed = n - st[(s, "success")]
    out.append(f"| {s} | {r['wins']} | {r['queries_ranked']} of {r['queries_with_query']} | {num(r['geomean_slowdown_vs_best'])}× | "
               f"{num(r['median_slowdown_vs_best'])}× | {n:,} | {failed} |")
out += ["", ("Wins and slowdowns count the read queries only (comparable with the read-only runs); the writes are "
        "below (*Writes*). " if writes else "") + "*Slowdown*: a system's mean runtime on a query divided by the fastest ranked system's, over the queries the "
        "system is ranked on. A system is ranked on a query when every measured repetition succeeded and its results match "
        "PostGIS (pre-run equivalence check; accepted: nearest-neighbour ties, numbers within 1 %, an empty answer as no row "
        "or one row of zeros).", ""]

# --- wins by scenario / type
wins = Counter((w["scenario"], w["winner"]) for w in winners if w["winner"] and w["query_type"] not in WRITE_TYPES)
wt = Counter((w["query_type"], w["winner"]) for w in winners if w["winner"])
cols = [s for s in present if any(w["winner"] == s for w in winners)]
out += ["**Wins by scenario and by query type:**", "", "| | " + " | ".join(cols) + " |", "|---|" + "---:|" * len(cols)]
for sc in SCEN:
    out.append(f"| {SCEN_NAME[sc]} | " + " | ".join(str(wins[(sc, s)] or "–") for s in cols) + " |")
for t in ("spatial", "temporal", "spatiotemporal"):
    out.append(f"| {t} | " + " | ".join(str(wt[(t, s)] or "–") for s in cols) + " |")
out.append("")

# --- pairwise
out += ["## Total runtime, pair by pair", "",
        "Sum of the measured repetitions' runtimes over only the queries both systems completed.", "",
        "| System A | System B | Common queries | A | B | A / B |", "|---|---|---:|---:|---:|---:|"]
for p in pairs:
    out.append(f"| {p['system_a']} | {p['system_b']} | {p['common_queries']} | {float(p['total_a_s']) / 60:.1f} min | "
               f"{float(p['total_b_s']) / 60:.1f} min | {p['ratio_a_over_b']} |")
out.append("")

# --- failures
fail = Counter((r["system"], r["scenario"], r["query"], r["status"]) for r in measured if r["status"] != "success")
out += ["## Failures", ""]
if dropped:
    out += ["Parameter sets left out for every system:", "", "| Scenario | Query | Repetition | Reason |", "|---|---|---:|---|"]
    out += [f"| {d['scenario']} | {d['query']} | {d['repetition']} | {d['reason']} |" for d in dropped]
    out.append("")
if fail:
    out += ["Measured executions that did not succeed (including those in the parameter sets above):", "",
            "| System | Scenario | Query | Status | Count |", "|---|---|---|---|---:|"]
    out += [f"| {k[0]} | {k[1]} | {k[2]} | {k[3]} | {v} |" for k, v in sorted(fail.items())]
else:
    out.append("Every measured execution succeeded.")
out.append("")

# --- not ranked
nr = [r for r in summary if r["planned"] not in ("0", "") and r["eligible"] != "True" and r["system"] in present]
if nr:
    out += ["## Not ranked", "", "| Scenario | Query | System | Reason |", "|---|---|---|---|"]
    for r in nr:
        why = r["equivalence"] if r["complete"] == "True" else f"{r['succeeded']}/{r['planned']} succeeded"
        out.append(f"| {r['scenario']} | {r['query']} | {r['system']} | {why[:90]} |")
    out.append("")

# --- writes
if writes:
    wsys = [x for x in present if any(r["system"] == x for r in writes)]
    out += ["## Writes", "",
            "Mean (95th percentile) runtime of each write query, over its measured executions; **bold**: fastest. "
            "An update moves points that an insert of the same run added; ClickHouse writes an update as a re-insert "
            "plus a lightweight delete, MobilityDB writes into its trajectories.", "",
            "| Scenario | Write query | " + " | ".join(wsys) + " |", "|---|---|" + "---:|" * len(wsys)]
    byq = defaultdict(dict)
    for r in writes:
        byq[(r["scenario"], r["query"])][r["system"]] = r
    for (sc, q), d in sorted(byq.items(), key=lambda kv: (SCEN.index(kv[0][0]), kv[0][1])):
        best = min((float(r["mean_ms"]) for r in d.values() if r["mean_ms"]), default=None)
        cells = []
        for x in wsys:
            r = d.get(x)
            if not r or not r["mean_ms"]:
                cells.append("–"); continue
            b = "**" if float(r["mean_ms"]) == best else ""
            cells.append(f"{b}{ms(r['mean_ms'])}{b} ({ms(r['p95_ms'])})")
        out.append(f"| {SCEN_NAME[sc]} | {q} | " + " | ".join(cells) + " |")
    out += ["", "| System | Write queries won | Geo-mean slowdown vs fastest |", "|---|---:|---:|"]
    for x in wsys:
        r = scores[x]
        out.append(f"| {x} | {r.get('write_wins', '–')} | {num(r.get('write_geomean_slowdown_vs_best'))}× |")
    out.append("")

# --- latency per scenario: box plot, then one table per scenario
out += ["## Latency per scenario", "",
        "One section per scenario. The box plot shows every system's runtimes over the measured repetitions that "
        "succeeded (box: 25th to 75th percentile, line: median, whiskers: 1.5 × the box height, dots: outliers; log "
        "scale; hatched: not ranked). The table below it gives mean, median, 95th and 99th percentile and standard "
        "deviation over those repetitions (n); the variance (standard deviation squared, ms²) is in `summary.csv` "
        "(`var_ms2`). **Bold**: fastest ranked system on the query (by mean).", ""]
win_of = {(w["scenario"], w["query"]): w["winner"] for w in winners}
for sc in SCEN:
    rs = [r for r in summary if r["scenario"] == sc and r["succeeded"] not in ("0", "")]
    if not rs:
        continue
    out += [f"### {SCEN_NAME[sc]}", ""]
    if os.path.exists(os.path.join(RUN, "plots", f"boxplot_{sc}.png")):
        out += [f"![{SCEN_NAME[sc]}: runtime per query and system](plots/boxplot_{sc}.png)", ""]
    out += ["| Query | System | n | Mean | Median | p95 | p99 | Std dev |", "|---|---|---:|---:|---:|---:|---:|---:|"]
    last = None
    for r in sorted(rs, key=lambda r: (r["query"], SYSTEMS.index(r["system"]))):
        b = "**" if win_of.get((sc, r["query"])) == r["system"] else ""
        q = f"`{r['query']}`" if r["query"] != last else ""
        last = r["query"]
        out.append(f"| {q} | {b}{r['system']}{b} | {r['succeeded']} | {b}{ms(r['mean_ms'])}{b} | {ms(r['median_ms'])} | "
                   f"{ms(r['p95_ms'])} | {ms(r['p99_ms'])} | {ms(r['std_ms'])} |")
    out.append("")

# --- empty results
if empty:
    by = defaultdict(dict)
    for r in empty:
        by[(r["scenario"], r["query"])][r["system"]] = r
    out += ["## Empty results", "",
            "Share of the measured parameter sets for which a query returned no row (from the row counts the client "
            "prints; SpaceTime's client does not print them). Aggregates that always return one row (a count, an "
            "average, ...) answer \"no data\" with a row of zeros, which row counts cannot show; they are not listed.", "",
            "| Scenario | Query | Empty | Rows (median / max) |", "|---|---|---:|---|"]
    for (sc, q), d in sorted(by.items()):
        r = d.get("PostGIS") or next(iter(d.values()))
        if int(r["empty"]):
            same = len({x["empty"] for x in d.values()}) == 1
            out.append(f"| {sc} | {q} | {r['empty_pct']} % ({r['empty']}/{r['executions']}){'' if same else ' (differs by system)'} | "
                       f"{r['median_rows']} / {r['max_rows']} |")
    out += ["", "Every system that prints row counts returned no row on exactly the same parameter sets.", ""]

# --- hardware
if hw:
    agg = defaultdict(lambda: defaultdict(list))
    for r in hw:
        m = float(r["minutes"] or 0)
        agg[r["system"]]["minutes"].append(m)
        for n in NODES:
            for k in ("cpu_busy_mean_pct", "mem_used_mean_gb", "disk_read_mean_mbs"):
                v = r.get(f"{n}_{k}")
                if v not in (None, ""):
                    agg[r["system"]][k].append((float(v), m))

    def wmean(xs):
        w = sum(b for _, b in xs)
        return sum(a * b for a, b in xs) / w if w else 0

    out += ["## Hardware", "", "Means over the five nodes and over the time the system ran (5 s samples; per scenario and "
            "node in `hardware_by_phase.csv`).", "",
            "| System | Run time | CPU busy | Memory used | Disk read |", "|---|---:|---:|---:|---:|"]
    for s in present:
        d = agg.get(s)
        if not d:
            continue
        mins = sum(d["minutes"])
        out.append(f"| {s} | {mins / 60:.1f} h | {wmean(d['cpu_busy_mean_pct']):.0f} % | {wmean(d['mem_used_mean_gb']):.1f} GB | "
                   f"{wmean(d['disk_read_mean_mbs']):.0f} MB/s |")
    out.append("")

# --- caveats
out += ["## Caveats", "",
        ] + ([f"- **SpaceTime's Geolife data** differs slightly from the other systems'; its Geolife queries whose results "
              f"differ from PostGIS ({', '.join(sorted({r['query'] for r in rows('equivalence.csv') if 'kept ranked' in r['verdict']}))}) "
              "are ranked on request (`equivalence.csv`)."]
             if any("kept ranked" in r["verdict"] for r in rows("equivalence.csv")) else
             ["- **SpaceTime's Geolife queries and cycling `kNN`** are ranked; their comparison with PostGIS is repeated "
              "after the runs."] if any("re-check after the runs" in r["verdict"] for r in rows("equivalence.csv")) else []) + [
        "- **Client host:** the benchmark client runs on a separate host with one CPU core and 1 GB of RAM. Results are "
        "read in batches of 100,000 rows; with several client threads, the client can become a limit for queries with "
        "large results.",
        "- **Sedona** skips GeoParquet files by their bounding box only when a query compares with a constant geometry; "
        "joins with a whole reference table (harbours, islands, districts, counties) read the whole table.",
        ("- **Parameters** are drawn where the data is (points, regions, time windows and places from a sample of the "
         "loaded data; see benchmark/configuration/README.md), each measured parameter set of a query distinct; every "
         "system answers the same parameter sets."
         if dates and dates[0] >= "2026-10-22" else
         "- **Parameters** are drawn uniformly over each scenario's area and time span, so some queries often find no "
         "data (see *Empty results*); every system answers the same parameter sets."), ""]

out += ["## Files", "", "| File | One row per |", "|---|---|",
        "| `executions.csv` | query execution (scenario, system, query, repetition, warm-up, status, duration, parameters, SQL) |",
        "| `summary.csv` | (scenario, query, system): runs, failures, mean/median/p95/p99/min/max/std dev/variance, equivalence, rank |",
        "| `winners.csv` | (scenario, query): fastest ranked system, runner-up, margin |",
        "| `system_scores.csv` | system: wins, completed and ranked queries, slowdowns |",
        "| `pairwise_total_time.csv` | pair of systems: total runtime over the queries both completed |",
        "| `empty_results.csv` | (scenario, query, system): measured executions without a row |",
        "| `equivalence.csv` | (scenario, query, system): result compared with PostGIS |",
        "| `hardware_by_phase.csv` | (scenario, system): CPU, memory, disk and network per node |",
        "| `dropped_parameter_sets.csv` | parameter sets left out for every system |",
        "| `tables.md`, `plots/` | the tables and figures generated from the above (`plots/boxplot_<scenario>.png`: the box plots) |",
        "| `raw/` | client logs, query files, hardware samples, the equivalence check |", ""]
open(os.path.join(RUN, "README.md"), "w").write("\n".join(out))
print("wrote", os.path.join(RUN, "README.md"), len(out), "lines")
