"""Turn a GeoBenchr benchmark run into CSV tables and plots.

    python3 build_results.py            # reads results/raw/, writes results/*.csv and results/plots/

Inputs (all under results/raw/, copied there from the servers):
  logs/<System>query_results_<id>.log   one line per execution, written by benchmark_client's QueryLogger:
        [thread] [System] [scenario] SUCCESS: 'query': 123ms [rep=3 sql=1a2b3c4d at=2026-...]
        [thread] [System] [scenario] ERROR: 'query': <message> [rep=... ]
        [thread] [System] [scenario] TIMEOUT: 'query': exceeded 600000ms cutoff [rep=... ]
  queries/<scenario>/<dialect>_queries.yaml   the exact queries the run executed (from the jar)
  equivalence/compare.json      cross-system comparison against PostGIS (compare_all.py)
  equivalence/h3_pairs.jsonl    ClickHouse H3 vs scan-only queries, all repetitions

Outputs (results/):
  executions.csv     one row per query execution: scenario, system, query, repetition, status,
                     duration, the query's parameters and SQL
  summary.csv        one row per (scenario, query, system): runs, failures, mean/median/min/max/std,
                     equivalence verdict, rank, slowdown vs the fastest system
  winners.csv        one row per (scenario, query): fastest system, runner-up, margin
  dropped_parameter_sets.csv  parameter sets left out for every system: broken parameters, or every
                     system timed out (also left out: repetitions 1..WARMUP_REPS, warm-up)
  system_scores.csv  per system: wins, queries completed, geometric-mean slowdown vs the fastest
  equivalence.csv    per (scenario, query, system): verdict against PostGIS
  h3_vs_scan.csv     per ClickHouse query and repetition: H3 vs scan-only result (count + hash)
  hardware_by_phase.csv  per (scenario, system) phase: CPU/memory/disk per node, CPU per process group
                     (from raw/hardware/, written by scripts/hw_sampler.py on nst-1..5)
  plots/             one PNG per query, one overview per scenario, wins, slowdown heatmap
"""
import csv
import glob
import json
import math
import os
import re
import statistics
from collections import defaultdict

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.environ.get("RESULTS_DIR", os.path.join(HERE, "results"))
RAW = os.path.join(RES, "raw")
PLOTS = os.path.join(RES, "plots")

SYSTEMS = ["PostGIS", "MobilityDB", "ClickHouse", "DuckDB", "SpaceTime", "Sedona"]
SCENARIOS = ["ais", "aviation", "cycling", "geolife"]
DIALECT_FILE = {"PostGIS": "postgisSQL", "MobilityDB": "mobilitydb", "ClickHouse": "clickhouse",
                "DuckDB": "duckdb", "SpaceTime": "spaceTimeSQL", "Sedona": "sedonaSQL"}
COMPARE_KEY = {"MobilityDB": "mobilitydb", "ClickHouse": "clickhouse", "DuckDB": "duckdb", "SpaceTime": "spacetime",
               "Sedona": "sedona"}
COLORS = {"PostGIS": "#336791", "MobilityDB": "#5b9bd5", "ClickHouse": "#f2b705", "DuckDB": "#7f7f7f",
          "SpaceTime": "#c0392b", "Sedona": "#27ae60"}

# (scenario, system) pairs outside the benchmark: MobilityDB is a trajectory system and runs no geolife
# queries (it used to run the two general point-table queries there; those executions are ignored).
EXCLUDED = {("geolife", "MobilityDB")}

# Repetitions 1..WARMUP_REPS of every query are warm-up: kept in executions.csv (warmup=True), left out of
# every statistic. 2 for the 52-repetition run of 2026-10-09/11 (results/); 0 for the superseded
# 5-repetition run of 2026-10-07/08 (superseded/run_5rep_2026-10-08/).
WARMUP_REPS = int(os.environ.get("WARMUP_REPS", "2"))

LINE = re.compile(r"^\[[^\]]*\] \[(\w+)\] \[(\w+)\] (SUCCESS|ERROR|TIMEOUT): '([^']+)':\s?(.*)$")
TAG = re.compile(r"\s\[(?:rep=(\d+) sql=([0-9a-f]{8}) )?at=([^\]]+)\]$")


def java_hash(s):
    """Java's String.hashCode() as unsigned hex -- matches the client's sql=<hash> tag."""
    b = s.encode("utf-16-be")
    h = 0
    for i in range(0, len(b), 2):
        h = (31 * h + ((b[i] << 8) | b[i + 1])) & 0xFFFFFFFF
    return "%08x" % h


def broken_params(params):
    """A parameter with an unescaped quote (the harbour "'t Waar" before the generator fix of 2026-10-09)
    makes the query a syntax error on every system; such parameter sets are left out for all systems."""
    def strings(p):
        if isinstance(p, list):
            for x in p:
                yield from strings(x)
        elif isinstance(p, str):
            yield p
    return any(v.count("'") % 2 == 1 for v in strings(params or []))


INVALID = set()   # (scenario, query, rep) with broken parameters, filled by load_queries


def all_timed_out(execs):
    """Parameter sets on which every system that ran them hit the timeout (at least two systems): no
    system answers them within the limit, so they rank nobody and are left out for all systems, like
    broken parameters. Example: countActiveCrossingsNearIsland on islands 43/44 without a time filter."""
    status = defaultdict(list)
    for r in execs:
        if not r["warmup"]:
            status[(r["scenario"], r["query"], r["repetition"])].append(r["status"])
    return {k for k, st in status.items() if len(st) >= 2 and all(x == "timeout" for x in st)}


def load_queries():
    """(scenario, system, query, rep) -> {sql, params, type, hash}; reps count non-empty entries in order."""
    out = {}
    for sc in SCENARIOS:
        for system, f in DIALECT_FILE.items():
            path = os.path.join(RAW, "queries", sc, f"{f}_queries.yaml")
            if not os.path.exists(path):
                continue
            reps = defaultdict(int)
            for e in yaml.safe_load(open(path)) or []:
                if not (e.get("sql") or "").strip() or (sc, system) in EXCLUDED:
                    continue
                reps[e["name"]] += 1
                if broken_params(e.get("params")):
                    INVALID.add((sc, e["name"], reps[e["name"]]))
                out[(sc, system, e["name"], reps[e["name"]])] = {
                    "sql": e["sql"], "params": e.get("params"), "type": e.get("type"), "hash": java_hash(e["sql"])}
    return out


def load_executions(queries):
    rows = []
    for f in sorted(glob.glob(os.path.join(RAW, "logs", "**", "*query_results_*.log"), recursive=True)):
        seen = defaultdict(int)
        for line in open(f, errors="replace"):
            m = LINE.match(line.rstrip("\n"))
            if not m:
                continue
            system, sc, status, q, rest = m.groups()
            if (sc, system) in EXCLUDED:
                continue
            t = TAG.search(rest)
            rep, sqlhash, at = (t.group(1), t.group(2), t.group(3)) if t else (None, None, None)
            body = rest[:t.start()] if t else rest
            if rep is None:              # untagged (older client): repetitions in log order
                seen[(system, sc, q)] += 1
                rep = seen[(system, sc, q)]
            rep = int(rep)
            meta = queries.get((sc, system, q, rep), {})
            duration = None
            if status == "SUCCESS":
                ms = re.match(r"(\d+)ms", body)
                duration = int(ms.group(1)) if ms else None
            rows.append({
                "scenario": sc, "system": system, "query": q, "query_type": meta.get("type"),
                "repetition": rep, "warmup": rep <= WARMUP_REPS, "status": status.lower(), "duration_ms": duration,
                "error": body.strip() if status != "SUCCESS" else "",
                "sql_hash": sqlhash or "", "sql_hash_matches": (sqlhash == meta.get("hash")) if sqlhash else "",
                "logged_at": at or "", "log_file": os.path.relpath(f, RAW),
                "params": json.dumps(meta.get("params"), ensure_ascii=False, default=str),
                "sql": " ".join((meta.get("sql") or "").split()),
            })
    # a query that timed out and finished later logs twice: the timeout is what counts. Logs under
    # raw/logs/rerun_*/ replace the original executions of the same (scenario, system, query, rep):
    # they re-ran queries that were fixed after the main run (see README).
    final = {}
    for r in sorted(rows, key=lambda r: r["log_file"].startswith(("logs/rerun_", "logs" + os.sep + "rerun_"))):
        k = (r["scenario"], r["system"], r["query"], r["repetition"])
        rerun = "rerun_" in r["log_file"]
        r["rerun"] = rerun
        if k not in final or rerun or final[k]["status"] != "timeout":
            final[k] = r
    return sorted(final.values(), key=lambda r: (SCENARIOS.index(r["scenario"]) if r["scenario"] in SCENARIOS else 9,
                                                  r["query"], SYSTEMS.index(r["system"]) if r["system"] in SYSTEMS else 9,
                                                  r["repetition"]))


def load_equivalence():
    path = os.path.join(RAW, "equivalence", "compare.json")
    if not os.path.exists(path):
        return {}
    out = {}
    for r in json.load(open(path)):
        sc, q = r["key"].split("/", 1)
        out[(sc, q, "PostGIS")] = "reference"
        for system, k in COMPARE_KEY.items():
            out[(sc, q, system)] = "n/a" if (sc, system) in EXCLUDED else r.get(k, "")
    return out


def acceptable(verdict):
    """Identical, or only minor differences (within 1 %, kNN format/ties); not checked counts as acceptable."""
    return verdict in (None, "", "reference") or verdict.startswith(("SAME", "CLOSE", "KNN", "n/a"))


def summarize(execs, equiv, queries):
    planned = defaultdict(int)
    for (sc, system, q, rep) in queries:
        if rep > WARMUP_REPS and (sc, q, rep) not in INVALID:
            planned[(sc, q, system)] += 1
    by = defaultdict(list)
    for r in execs:
        by[(r["scenario"], r["query"], r["system"])].append(r)
    keys = sorted(set(by) | set(planned), key=lambda k: (SCENARIOS.index(k[0]) if k[0] in SCENARIOS else 9, k[1],
                                                         SYSTEMS.index(k[2]) if k[2] in SYSTEMS else 9))
    summary = []
    for k in keys:
        rs = by.get(k, [])
        ok = [r["duration_ms"] for r in rs if r["status"] == "success" and r["duration_ms"] is not None]
        v = equiv.get(k)
        n_plan = planned.get(k, 0)
        row = {
            "scenario": k[0], "query": k[1], "system": k[2],
            "query_type": next((r["query_type"] for r in rs if r["query_type"]), ""),
            "planned": n_plan, "succeeded": len(ok),
            "errors": sum(r["status"] == "error" for r in rs), "timeouts": sum(r["status"] == "timeout" for r in rs),
            "mean_ms": round(statistics.mean(ok), 1) if ok else None,
            "median_ms": statistics.median(ok) if ok else None,
            "min_ms": min(ok) if ok else None, "max_ms": max(ok) if ok else None,
            "std_ms": round(statistics.stdev(ok), 1) if len(ok) > 1 else None,
            "cv": round(statistics.stdev(ok) / statistics.mean(ok), 3) if len(ok) > 1 and statistics.mean(ok) > 0 else None,
            "equivalence": v if v is not None else ("no query" if n_plan == 0 else "not checked"),
        }
        row["complete"] = n_plan > 0 and len(ok) == n_plan
        row["eligible"] = row["complete"] and acceptable(v)
        summary.append(row)
    # rank + slowdown among eligible systems per query
    groups = defaultdict(list)
    for r in summary:
        groups[(r["scenario"], r["query"])].append(r)
    winners = []
    for (sc, q), rs in groups.items():
        el = sorted([r for r in rs if r["eligible"]], key=lambda r: r["mean_ms"])
        best = el[0]["mean_ms"] if el else None
        for i, r in enumerate(el):
            r["rank"] = i + 1
            r["slowdown_vs_best"] = round(r["mean_ms"] / best, 2) if best else None
        for r in rs:
            r.setdefault("rank", None)
            r.setdefault("slowdown_vs_best", None)
        w = {"scenario": sc, "query": q, "query_type": next((r["query_type"] for r in rs if r["query_type"]), ""),
             "winner": el[0]["system"] if el else "", "winner_mean_ms": best,
             "runner_up": el[1]["system"] if len(el) > 1 else "",
             "runner_up_mean_ms": el[1]["mean_ms"] if len(el) > 1 else None,
             "margin_x": round(el[1]["mean_ms"] / best, 2) if len(el) > 1 and best else None,
             "ranking": " > ".join(f"{r['system']} ({fmt_ms(r['mean_ms'])})" for r in el),
             "not_ranked": "; ".join(f"{r['system']}: {why(r)}" for r in rs if not r["eligible"])}
        winners.append(w)
    winners.sort(key=lambda w: (SCENARIOS.index(w["scenario"]) if w["scenario"] in SCENARIOS else 9, w["query"]))
    return summary, winners


def why(r):
    if r["planned"] == 0:
        return "no query"
    if r["succeeded"] == 0 and r["errors"] == 0 and r["timeouts"] == 0:
        return "not run"
    if not r["complete"]:
        return f"{r['succeeded']}/{r['planned']} ok ({r['errors']} errors, {r['timeouts']} timeouts)"
    return f"result differs: {r['equivalence'][:60]}"


def fmt_ms(ms):
    if ms is None:
        return "-"
    return f"{ms / 1000:.2f} s" if ms >= 1000 else f"{ms:.0f} ms"


def system_scores(summary, winners):
    out = []
    for s in SYSTEMS:
        rs = [r for r in summary if r["system"] == s]
        if not rs:
            continue
        sd = [r["slowdown_vs_best"] for r in rs if r["slowdown_vs_best"]]
        out.append({
            "system": s,
            "wins": sum(w["winner"] == s for w in winners),
            "queries_with_query": sum(r["planned"] > 0 for r in rs),
            "queries_complete": sum(r["complete"] for r in rs),
            "queries_ranked": sum(r["eligible"] for r in rs),
            "geomean_slowdown_vs_best": round(math.exp(statistics.mean(math.log(x) for x in sd)), 2) if sd else None,
            "median_slowdown_vs_best": round(statistics.median(sd), 2) if sd else None,
            "total_runtime_s": round(sum((r["mean_ms"] or 0) * (r["succeeded"] or 0) for r in rs) / 1000, 1),
        })
    return out


def h3_rows():
    path = os.path.join(RAW, "equivalence", "h3_pairs.jsonl")
    if not os.path.exists(path):
        return []
    rows = []
    for line in open(path):
        r = json.loads(line)
        sc, q = r["key"].split("/", 1)
        h, s = r["h3"], r["scan"]
        rows.append({"scenario": sc, "query": q, "repetition": r["rep"],
                     "identical": h["rc"] == 0 and s["rc"] == 0 and h["out"] == s["out"],
                     "h3_rows": h["out"].split("\t")[0] if h["rc"] == 0 else "", "scan_rows": s["out"].split("\t")[0] if s["rc"] == 0 else "",
                     "h3_secs": h["secs"], "scan_secs": s["secs"],
                     "error": (h["err"] if h["rc"] else "") or (s["err"] if s["rc"] else "")})
    return rows


def write_csv(name, rows):
    if not rows:
        return
    # rows may differ in their columns (e.g. hardware phases without a DuckDB process): union, in order
    fields = list(dict.fromkeys(k for r in rows for k in r))
    with open(os.path.join(RES, name), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def plots(execs, summary, winners, scores):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np

    os.makedirs(os.path.join(PLOTS, "queries"), exist_ok=True)
    by = defaultdict(list)
    for r in execs:
        if r["status"] == "success" and r["duration_ms"] is not None:
            by[(r["scenario"], r["query"], r["system"])].append(max(r["duration_ms"], 1) / 1000)
    sumk = {(r["scenario"], r["query"], r["system"]): r for r in summary}
    win = {(w["scenario"], w["query"]): w["winner"] for w in winners}

    def draw(ax, sc, q, small=False):
        present = [s for s in SYSTEMS if (sc, q, s) in sumk]
        for i, s in enumerate(present):
            r = sumk[(sc, q, s)]
            vals = by.get((sc, q, s), [])
            if vals:
                ax.bar(i, statistics.mean(vals), color=COLORS[s], alpha=0.85,
                       edgecolor="black" if win.get((sc, q)) == s else "none", linewidth=2)
                ax.scatter([i + (j - (len(vals) - 1) / 2) * 0.08 for j in range(len(vals))], vals,
                           s=10 if small else 18, color="black", zorder=3)
            not_run = r["succeeded"] == 0 and r["errors"] == 0 and r["timeouts"] == 0
            note = "" if r["eligible"] else ("n/a" if r["planned"] == 0 else "not run" if not_run else
                                             "≠" if r["complete"] else f"{r['succeeded']}/{r['planned']}")
            if note:
                ax.text(i, ax.get_ylim()[0] if vals else 1e-3, note, ha="center", va="bottom", fontsize=7 if small else 9, color="#a00")
        ax.set_yscale("log")
        ax.set_xticks(range(len(present)))
        ax.set_xticklabels(present, rotation=45 if small else 30, ha="right", fontsize=7 if small else 9)
        ax.set_ylabel("seconds (log)", fontsize=7 if small else 9)
        ax.set_title(f"{sc}/{q}" if not small else q, fontsize=8 if small else 11)
        ax.grid(axis="y", which="both", alpha=0.3)

    for (sc, q) in sorted({(r["scenario"], r["query"]) for r in summary}):
        fig, ax = plt.subplots(figsize=(7, 4.2))
        draw(ax, sc, q)
        ax.set_ylim(bottom=1e-3)
        fig.text(0.01, 0.01, "bar = mean, dots = repetitions, black outline = fastest ranked system; "
                 "≠ result differs from PostGIS, n/a no query, k/n repetitions succeeded", fontsize=6)
        fig.tight_layout()
        fig.savefig(os.path.join(PLOTS, "queries", f"{sc}__{q}.png"), dpi=130)
        plt.close(fig)

    for sc in SCENARIOS:
        qs = sorted({r["query"] for r in summary if r["scenario"] == sc})
        if not qs:
            continue
        cols = 4
        rows_n = math.ceil(len(qs) / cols)
        fig, axes = plt.subplots(rows_n, cols, figsize=(16, 3.6 * rows_n))
        for ax, q in zip(np.ravel(axes), qs):
            draw(ax, sc, q, small=True)
            ax.set_ylim(bottom=1e-3)
        for ax in np.ravel(axes)[len(qs):]:
            ax.axis("off")
        fig.suptitle(f"{sc}: mean runtime per query (log scale), dots = repetitions", fontsize=13)
        fig.tight_layout()
        fig.savefig(os.path.join(PLOTS, f"overview_{sc}.png"), dpi=110)
        plt.close(fig)

    if scores:
        fig, ax = plt.subplots(figsize=(7, 4))
        ax.bar([s["system"] for s in scores], [s["wins"] for s in scores], color=[COLORS[s["system"]] for s in scores])
        ax.set_ylabel("queries won")
        ax.set_title("Fastest system per query (ranked systems only)")
        fig.tight_layout()
        fig.savefig(os.path.join(PLOTS, "wins.png"), dpi=130)
        plt.close(fig)

    qs = sorted({(r["scenario"], r["query"]) for r in summary})
    present = [s for s in SYSTEMS if any(r["system"] == s for r in summary)]
    m = np.full((len(qs), len(present)), np.nan)
    for i, k in enumerate(qs):
        for j, s in enumerate(present):
            r = sumk.get((k[0], k[1], s))
            if r and r["slowdown_vs_best"]:
                m[i, j] = math.log10(r["slowdown_vs_best"])
    fig, ax = plt.subplots(figsize=(8, 0.32 * len(qs) + 1.5))
    im = ax.imshow(m, aspect="auto", cmap="RdYlGn_r", vmin=0, vmax=3)
    ax.set_xticks(range(len(present)))
    ax.set_xticklabels(present, rotation=30, ha="right")
    ax.set_yticks(range(len(qs)))
    ax.set_yticklabels([f"{a}/{b}" for a, b in qs], fontsize=7)
    for i in range(len(qs)):
        for j in range(len(present)):
            if not np.isnan(m[i, j]):
                ax.text(j, i, f"{10 ** m[i, j]:.0f}×" if m[i, j] > 0.05 else "1×", ha="center", va="center", fontsize=6)
    cb = fig.colorbar(im, ax=ax)
    cb.set_label("slowdown vs fastest (log10); blank = not ranked")
    ax.set_title("Slowdown vs the fastest ranked system per query")
    fig.tight_layout()
    fig.savefig(os.path.join(PLOTS, "slowdown_heatmap.png"), dpi=130)
    plt.close(fig)


def markdown_tables(summary, winners, scores):
    """results/tables.md: the tables the write-up uses, regenerated with the CSVs."""
    present = [s for s in SYSTEMS if any(r["system"] == s and r["planned"] for r in summary)]
    sumk = {(r["scenario"], r["query"], r["system"]): r for r in summary}
    out = ["# Result tables (generated by build_results.py)\n", "## Per system\n",
           "| System | Queries won | Queries with a query | Completed (all reps) | Ranked | Geo-mean slowdown vs fastest | Median slowdown |",
           "|---|---:|---:|---:|---:|---:|---:|"]
    for s in scores:
        out.append(f"| {s['system']} | {s['wins']} | {s['queries_with_query']} | {s['queries_complete']} | {s['queries_ranked']} | "
                   f"{s['geomean_slowdown_vs_best'] or '-'}× | {s['median_slowdown_vs_best'] or '-'}× |")
    for sc in SCENARIOS:
        ws = [w for w in winners if w["scenario"] == sc]
        if not ws:
            continue
        out += [f"\n## {sc}: mean runtime per query\n",
                "Mean over the repetitions (each with different parameters). **Bold** = fastest ranked system; "
                "≈ = result within 1 % / kNN ties of PostGIS; ≠ = major difference (not ranked); "
                "k/n = only k of n repetitions succeeded (not ranked); n/a = no query for this system.\n",
                "| Query | Type | " + " | ".join(present) + " | Fastest | Margin |", "|---|---|" + "---:|" * len(present) + "---|---:|"]
        for w in ws:
            cells = []
            for s in present:
                r = sumk.get((sc, w["query"], s))
                if not r or r["planned"] == 0:
                    cells.append("n/a")
                    continue
                if r["succeeded"] == 0:
                    cells.append("not run" if not (r["errors"] or r["timeouts"]) else f"0/{r['planned']}")
                    continue
                v = fmt_ms(r["mean_ms"])
                eq = r["equivalence"] or ""
                if not r["complete"]:
                    v += f" ({r['succeeded']}/{r['planned']})"
                elif not acceptable(eq):
                    v += " ≠"
                elif eq.startswith(("CLOSE", "KNN")):
                    v += " ≈"
                cells.append(f"**{v}**" if s == w["winner"] else v)
            out.append(f"| {w['query']} | {w['query_type']} | " + " | ".join(cells) +
                       f" | {w['winner'] or '-'} | {str(w['margin_x']) + '×' if w['margin_x'] else '-'} |")
    open(os.path.join(RES, "tables.md"), "w").write("\n".join(out) + "\n")


def parse_ts(x):
    from datetime import datetime
    return datetime.fromisoformat(x.replace("Z", "+00:00")[:32]).timestamp()


def hardware(execs):
    """Per (scenario, system) phase: node CPU / memory / disk and per-process-group CPU while it ran.

    raw/hardware/hw_<host>.csv and proc_<host>.csv come from scripts/hw_sampler.py (5 s samples, UTC).
    A phase runs from the first query's start (log time minus duration) to the last query's log time."""
    hwdir = os.path.join(RAW, "hardware")
    if not os.path.isdir(hwdir):
        return [], {}
    hw, proc = [], []
    for f in sorted(glob.glob(os.path.join(hwdir, "hw_*.csv"))):
        hw += list(csv.DictReader(open(f)))
    for f in sorted(glob.glob(os.path.join(hwdir, "proc_*.csv"))):
        proc += list(csv.DictReader(open(f)))
    for r in hw:
        r["t"] = parse_ts(r["ts"])
    for r in proc:
        r["t"] = parse_ts(r["ts"])
    phases = {}
    for r in execs:
        if not r["logged_at"]:
            continue
        end = parse_ts(r["logged_at"])
        start = end - (r["duration_ms"] or 0) / 1000
        k = (r["scenario"], r["system"] + (" (re-run)" if r.get("rerun") else ""))
        lo, hi = phases.get(k, (start, end))
        phases[k] = (min(lo, start), max(hi, end))
    rows = []
    for (sc, system), (lo, hi) in sorted(phases.items(), key=lambda kv: kv[1][0]):
        row = {"scenario": sc, "system": system, "start_utc": datetime_str(lo), "end_utc": datetime_str(hi),
               "minutes": round((hi - lo) / 60, 1)}
        for host in sorted({r["host"] for r in hw}):
            xs = [r for r in hw if r["host"] == host and lo <= r["t"] <= hi]
            if not xs:
                continue
            # busy = user + system; iowait (waiting for disk, often background I/O) is reported separately
            busy = [float(r["cpu_user_pct"]) + float(r["cpu_system_pct"]) for r in xs]
            row[f"{host}_cpu_busy_mean_pct"] = round(statistics.mean(busy), 1)
            row[f"{host}_cpu_busy_max_pct"] = round(max(busy), 1)
            row[f"{host}_iowait_mean_pct"] = round(statistics.mean(float(r["cpu_iowait_pct"]) for r in xs), 1)
            row[f"{host}_mem_used_mean_gb"] = round(statistics.mean(int(r["mem_used_mb"]) for r in xs) / 1024, 1)
            row[f"{host}_disk_read_mean_mbs"] = round(statistics.mean(float(r["disk_read_mbs"]) for r in xs), 1)
        for g in sorted({r["group"] for r in proc}):
            xs = [r for r in proc if r["group"] == g and lo <= r["t"] <= hi]
            if xs:
                per_ts = defaultdict(float)
                for r in xs:
                    per_ts[r["ts"]] += float(r["cpu_pct_of_one_core"])
                row[f"cluster_{g}_cores_mean"] = round(statistics.mean(per_ts.values()) / 100, 2)
                row[f"cluster_{g}_cores_max"] = round(max(per_ts.values()) / 100, 2)
        rows.append(row)
    return rows, {"hw": hw, "phases": phases}


def datetime_str(t):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def hardware_plot(data):
    if not data or not data["hw"]:
        return
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from datetime import datetime, timezone
    hosts = sorted({r["host"] for r in data["hw"]})
    # only the benchmark's own phases (+10 min): the samplers may have run longer, e.g. through an
    # earlier attempt that was set aside
    lo_all = min(lo for lo, hi in data["phases"].values()) - 600 if data["phases"] else float("-inf")
    hi_all = max(hi for lo, hi in data["phases"].values()) + 600 if data["phases"] else float("inf")
    fig, axes = plt.subplots(2, 1, figsize=(16, 8), sharex=True)
    for host in hosts:
        xs = sorted((r for r in data["hw"] if r["host"] == host and lo_all <= r["t"] <= hi_all), key=lambda r: r["t"])
        t = [datetime.fromtimestamp(r["t"], timezone.utc) for r in xs]
        axes[0].plot(t, [float(r["cpu_user_pct"]) + float(r["cpu_system_pct"]) for r in xs], lw=0.6, label=host)
        axes[1].plot(t, [int(r["mem_used_mb"]) / 1024 for r in xs], lw=0.8, label=host)
    for (sc, system), (lo, hi) in data["phases"].items():
        for ax in axes:
            ax.axvspan(datetime.fromtimestamp(lo, timezone.utc), datetime.fromtimestamp(hi, timezone.utc),
                       color=COLORS.get(system.split(" ")[0], "#ccc"), alpha=0.12)
        axes[0].text(datetime.fromtimestamp(lo, timezone.utc), 102, f"{sc[:3]}/{system[:6]}", fontsize=6, rotation=60)
    axes[0].set_ylabel("CPU busy % (user+system)")
    axes[0].set_ylim(0, 130)
    axes[1].set_ylabel("memory used (GB)")
    axes[0].legend(loc="upper right", fontsize=8)
    axes[0].set_title("Node CPU and memory during the benchmark (shaded: system phases, colored by system)")
    fig.tight_layout()
    fig.savefig(os.path.join(PLOTS, "hardware_timeline.png"), dpi=110)
    plt.close(fig)


def main():
    os.makedirs(RES, exist_ok=True)
    queries = load_queries()
    execs = load_executions(queries)
    equiv = load_equivalence()
    dropped = [{"scenario": k[0], "query": k[1], "repetition": k[2], "reason": "broken parameters (unescaped quote)"}
               for k in sorted(INVALID)]
    timed_out = all_timed_out(execs) - INVALID
    dropped += [{"scenario": k[0], "query": k[1], "repetition": k[2], "reason": "every system timed out"}
                for k in sorted(timed_out)]
    INVALID.update(timed_out)
    measured = [r for r in execs if not r["warmup"] and (r["scenario"], r["query"], r["repetition"]) not in INVALID]
    for d in dropped:
        print(f"left out for every system: {d['scenario']}/{d['query']} rep {d['repetition']} ({d['reason']})")
    write_csv("dropped_parameter_sets.csv", dropped)
    summary, winners = summarize(measured, equiv, queries)
    scores = system_scores(summary, winners)
    write_csv("executions.csv", execs)
    write_csv("summary.csv", summary)
    write_csv("winners.csv", winners)
    write_csv("system_scores.csv", scores)
    write_csv("equivalence.csv", [{"scenario": k[0], "query": k[1], "system": k[2], "verdict": v,
                                   "acceptable": acceptable(v)} for k, v in sorted(equiv.items())])
    write_csv("h3_vs_scan.csv", h3_rows())
    markdown_tables(summary, winners, scores)
    plots(measured, summary, winners, scores)
    hw_rows, hw_data = hardware(execs)
    write_csv("hardware_by_phase.csv", hw_rows)
    hardware_plot(hw_data)
    bad = sum(1 for r in execs if r["sql_hash_matches"] is False)
    print(f"{len(execs)} executions ({len(execs) - len(measured)} warm-up), {len(summary)} (scenario, query, system) cells, {len(winners)} queries; "
          f"{bad} executions whose SQL hash doesn't match the query files")
    for s in scores:
        print(f"  {s['system']:11} wins {s['wins']:3}  ranked {s['queries_ranked']:3}  geomean slowdown {s['geomean_slowdown_vs_best']}")


if __name__ == "__main__":
    main()
