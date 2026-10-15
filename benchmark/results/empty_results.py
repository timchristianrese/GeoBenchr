"""How often each query returned no rows, per system (2026-10-15).

Reads the client output of a run (raw/logs/**/<scenario>_stdout.log): with one client thread every execution prints
"INFO: Starting '<query>'", then "Rows returned[/affected]: <n>", then its SUCCESS line with the repetition number,
strictly in that order. Writes results/empty_results.csv, one row per (scenario, query, system):
  executions          measured repetitions with a row count (warm-up excluded, WARMUP_REPS as for build_results.py)
  empty               of those, how many returned no row at all
  always_one_row      every execution returned exactly one row -- an aggregate (count, average, ...), whose "no data"
                      answer is a row of zeros, which row counts cannot show; see the equivalence check for those
  min_rows / median_rows / max_rows
Only valid for single-thread runs (with several threads the output lines of different queries interleave).
    RESULTS_DIR=<results> WARMUP_REPS=1 python3 empty_results.py
"""
import csv
import glob
import os
import re
import statistics
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
RES = os.environ.get("RESULTS_DIR", os.path.join(HERE, "results"))
WARMUP_REPS = int(os.environ.get("WARMUP_REPS", "1"))
START = re.compile(r"^\[[^\]]*\] \[(\w+)\] \[(\w+)\] INFO: Starting '([^']+)'")
ROWS = re.compile(r"^Rows returned(?:/affected)?: (\d+)")
DONE = re.compile(r"^\[[^\]]*\] \[(\w+)\] \[(\w+)\] SUCCESS: '([^']+)'.*\[rep=(\d+)")

counts = defaultdict(dict)          # (scenario, query, system) -> {rep: rows}
for path in glob.glob(os.path.join(RES, "raw", "logs", "**", "*_stdout.log"), recursive=True):
    pending = None
    for line in open(path, errors="replace"):
        m = START.match(line)
        if m:
            pending = None
            continue
        m = ROWS.match(line)
        if m:
            pending = int(m.group(1))
            continue
        m = DONE.match(line)
        if m and pending is not None:
            system, sc, q, rep = m.group(1), m.group(2), m.group(3), int(m.group(4))
            counts[(sc, q, system)][rep] = pending
            pending = None

rows = []
for (sc, q, system), by_rep in sorted(counts.items()):
    vals = [n for rep, n in by_rep.items() if rep > WARMUP_REPS]
    if not vals:
        continue
    rows.append({"scenario": sc, "query": q, "system": system, "executions": len(vals),
                 "empty": sum(n == 0 for n in vals), "empty_pct": round(100 * sum(n == 0 for n in vals) / len(vals), 1),
                 "always_one_row": all(n == 1 for n in vals),
                 "min_rows": min(vals), "median_rows": statistics.median(vals), "max_rows": max(vals)})
with open(os.path.join(RES, "empty_results.csv"), "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["scenario"])
    w.writeheader()
    w.writerows(rows)
print(f"{len(rows)} (scenario, query, system) cells; with empty results:")
for r in rows:
    if r["empty"]:
        print(f"  {r['scenario']:8} {r['query']:52} {r['system']:10} {r['empty']:4}/{r['executions']} empty ({r['empty_pct']} %)")
