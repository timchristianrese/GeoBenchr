"""Turns a pre-run equivalence check into the verdicts build_results.py ranks with (2026-10-14).

Input: one directory with the results of every query on several parameter sets, compared with PostGIS:
  values.jsonl   {"key": "<scenario>/<query>#<rep>", "dialect", "rc", "rows" (<= 2001), "err"}  PostGIS, MobilityDB,
                 ClickHouse, DuckDB (eq_20261001/run_values.py)
  counts.jsonl   the same with rows = [[row count]], for results above 2000 rows
  st.jsonl       SpaceTime: {"key", "rc", "count", "rows", "err"}
  sedona.jsonl   Sedona (benchmark client with RESULTS_OUT): key "<scenario>/<query>", in the order of REPS
Output: compare.json, one row per query: {"key": "<scenario>/<query>", "postgis": ..., "<dialect>": verdict}.

Verdict per (query, system) over all parameter sets, worst first:
  DIFF / COUNT DIFF / ERROR   a real difference (not ranked)
  CLOSE (max x %)             every number within 1 % of PostGIS
  KNN tie                     nearest-neighbour results with the same row count: equal-distance ties broken
                              differently, or for ClosestRestaurantToPoint the same venue at the same distance
  SAME (shape)                PostGIS returns no row where the other returns one row of zeros or an empty
                              average (the same empty answer)
  SAME / SAME COUNT           identical
    equivalence_verdicts.py RESULT_DIR REPS OUT.json        e.g. ... eqfull/res 2,12,22,32,42 compare.json
"""
import json
import os
import re
import sys

D, REPS, OUT = sys.argv[1], [int(x) for x in sys.argv[2].split(",")], sys.argv[3]
SYSTEMS = ["mobilitydb", "clickhouse", "duckdb", "spacetime", "sedona"]
VENUE = {"0": "Apartment", "1": "Restaurant", "2": "Pub", "3": "Workplace"}
TOL = 0.01


def jl(name):
    p = os.path.join(D, name)
    return [json.loads(l) for l in open(p) if l.strip().startswith("{")] if os.path.exists(p) else []


vals = {(r["key"], r["dialect"]): r for r in jl("values.jsonl")}
cnts = {(r["key"], r["dialect"]): r for r in jl("counts.jsonl")}
st = {r["key"]: r for r in jl("st.jsonl")}
sed, seen = {}, {}
for r in jl("sedona.jsonl"):
    i = seen[r["key"]] = seen.get(r["key"], -1) + 1
    if i < len(REPS):
        sed[f"{r['key']}#{REPS[i]}"] = r


def norm(c):
    c = (c or "").strip()
    if c.lower() in ("t", "true"):
        return "true"
    if c.lower() in ("f", "false"):
        return "false"
    if c.lower() in ("nan", "", "none", "null"):
        return ""
    m = re.fullmatch(r"(\d{4}-\d\d-\d\d)[ T](\d\d:\d\d:\d\d)(\.\d+)?([+-]\d\d(:?\d\d)?|Z)?", c)
    if m:
        return f"{m.group(1)} {m.group(2)}"
    try:
        return "%.6g" % float(c)
    except ValueError:
        return c


def canon(rows):
    return sorted(tuple(norm(x) for x in r) for r in rows)


def num(c):
    try:
        return float(c)
    except (TypeError, ValueError):
        return None


def close(a, b):
    worst = 0.0
    for ra, rb in zip(a, b):
        if len(ra) != len(rb):
            return None
        for x, y in zip(ra, rb):
            if x == y:
                continue
            fx, fy = num(x), num(y)
            if fx is None or fy is None:
                return None
            d = abs(fx - fy) / max(abs(fx), abs(fy), 1)
            if d > TOL:
                return None
            worst = max(worst, d)
    return worst


def result(key, d):
    """(row count, rows if <= 2000, error)"""
    if d in ("spacetime", "sedona"):
        r = (st if d == "spacetime" else sed).get(key)
        if not r:
            return None, None, "not run"
        if r["rc"] != 0:
            return None, None, (r.get("err") or "")[-160:]
        rows = r.get("rows")
        if d == "spacetime" and key.startswith("geolife/CountPOIsByTypeInRegion") and rows:
            rows = [[VENUE.get(x[0], x[0])] + x[1:] for x in rows]
        return r["count"], rows if r["count"] <= 2000 else None, None
    v, c = vals.get((key, d)), cnts.get((key, d))
    if not v:
        return None, None, "not run"
    if v["rc"] != 0:
        return None, None, v["err"][-160:]
    if v["rows"] is not None and len(v["rows"]) <= 2000:
        return len(v["rows"]), v["rows"], None
    if not c:
        return None, None, "count not run"
    if c["rc"] != 0:
        return None, None, c["err"][-160:]
    return int(c["rows"][0][0]), None, None


def empty_answer(rows):
    """One row whose numbers are all 0 or empty: the same answer as no row at all."""
    return len(rows) == 1 and all(norm(x) in ("0", "") or num(x) is None for x in rows[0]) and any(norm(x) in ("0", "") for x in rows[0])


def verdict(key, d):
    query = key.split("/", 1)[1].split("#")[0]
    pn, prow, perr = result(key, "postgis")
    n, rows, err = result(key, d)
    if err == "not run":
        return None
    if err:
        return "ERROR " + err.replace("\n", " ")[:100]
    if perr:
        return "ERROR postgis " + perr[:80]
    if pn > 2000 or n > 2000:
        if n == pn:
            return "SAME COUNT"
        return f"CLOSE COUNT {n} vs {pn}" if abs(n - pn) / max(n, pn) <= TOL else f"COUNT DIFF {n} vs {pn}"
    a, b = canon(prow), canon(rows)
    if a == b:
        return "SAME"
    if n == pn and close(a, b) is not None:
        return f"CLOSE ({close(a, b):.3%})"
    if pn == 0 and empty_answer(rows) or n == 0 and empty_answer(prow):
        return "SAME (shape)"
    if query == "kNN" and n == pn:
        return "KNN tie"
    if query == "ClosestRestaurantToPoint" and n == pn == 1:
        # same venue (venue_id, venue_type) at the same distance: a different visit to it is an equal answer
        if prow[0][2:4] == rows[0][2:4]:
            return "KNN tie"
    if query == "flightClosestToPoint" and n == pn == 1 and prow[0][0] == rows[0][0] and abs(num(prow[0][1]) - num(rows[0][1])) <= 0.15:
        return "CLOSE (same flight, distance within 0.15 m)"
    return f"DIFF {n} vs {pn} rows, e.g. postgis={a[:1]} {d}={b[:1]}"


RANK = ["ERROR", "COUNT DIFF", "DIFF", "CLOSE", "KNN", "SAME (shape)", "SAME COUNT", "SAME"]


def severity(v):
    return next(i for i, p in enumerate(RANK) if v.startswith(p))


keys = sorted({k for k, _ in vals} | set(st) | set(sed))
by_query = {}
for key in keys:
    by_query.setdefault(key.split("#")[0], []).append(key)
out = []
for q, ks in sorted(by_query.items()):
    row = {"key": q, "postgis": "reference"}
    for d in SYSTEMS:
        vs = [v for v in (verdict(k, d) for k in ks) if v]
        if not vs:
            continue
        worst = min(vs, key=severity)
        if worst.startswith("CLOSE ("):
            pcts = [float(m.group(1)) for v in vs for m in [re.match(r"CLOSE \(([\d.]+)%\)", v)] if m]
            worst = f"CLOSE (max {max(pcts):.3f} %)" if pcts else worst
        n_worst = sum(severity(v) == severity(worst) for v in vs)
        row[d] = worst + (f" [{n_worst}/{len(vs)} parameter sets]" if worst.startswith(("DIFF", "COUNT DIFF", "ERROR")) else "")
    out.append(row)
json.dump(out, open(OUT, "w"), indent=1)
bad = [(r["key"], d, r[d]) for r in out for d in SYSTEMS if d in r and r[d].startswith(("DIFF", "COUNT DIFF", "ERROR"))]
print(f"{len(out)} queries; {len(bad)} (query, system) pairs with a real difference:")
for b in bad:
    print("  ", b[0], b[1], b[2][:150])
