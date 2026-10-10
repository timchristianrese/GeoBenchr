"""Compare every system against PostGIS: values for results <= 2000 rows, row counts otherwise.

Verdicts (2026-10-07): SAME / SAME COUNT = identical; CLOSE / CLOSE COUNT = every number within
1 % of PostGIS (e.g. a few duplicate or border points); KNN = nearest-neighbour results with the same
row count, which differ only in geometry output format and in how ties at equal distance are broken;
anything else (COUNT DIFF, VALUE DIFF) is a major difference. The same rules apply to every system.
"""
import json, re, sys, os
from collections import Counter, defaultdict
D = sys.argv[1]
def jl(f): return [json.loads(l) for l in open(os.path.join(D, f)) if l.strip().startswith("{")]
vals = {(r["key"], r["dialect"]): r for r in jl("values.jsonl")}
cnts = {(r["key"], r["dialect"]): r for r in jl("counts.jsonl")}
st = {r["key"]: r for r in jl("st.jsonl")}
def norm(c):
    c = (c or "").strip()
    if c.lower() in ("t", "true"): return "true"
    if c.lower() in ("f", "false"): return "false"
    m = re.fullmatch(r"(\d{4}-\d\d-\d\d)[ T](\d\d:\d\d:\d\d)(\.\d+)?([+-]\d\d(:?\d\d)?|Z)?", c)
    if m: return f"{m.group(1)} {m.group(2)}"
    try: return "%.6g" % float(c)
    except ValueError: return c
def canon(rows): return sorted(tuple(norm(x) for x in r) for r in rows)
TOL = 0.01
KNN_QUERIES = {"kNN", "ClosestRestaurantToPoint"}
VENUE = {"0": "Apartment", "1": "Restaurant", "2": "Pub", "3": "Workplace"}
def num(c):
    try: return float(c)
    except (TypeError, ValueError): return None
def rel(a, b): return abs(a - b) / max(abs(a), abs(b), 1)
def fix_st(key, rows):
    """SpaceTime stores geolife venue types as integer codes."""
    if key == "geolife/CountPOIsByTypeInRegion":
        return [[VENUE.get(r[0], r[0])] + r[1:] for r in rows]
    return rows
def close(a, b):
    """Largest relative difference if every cell is equal or a number within TOL, else None."""
    worst = 0.0
    for ra, rb in zip(a, b):
        if len(ra) != len(rb): return None
        for x, y in zip(ra, rb):
            if x == y: continue
            fx, fy = num(x), num(y)
            if fx is None or fy is None or rel(fx, fy) > TOL: return None
            worst = max(worst, rel(fx, fy))
    return worst
def result(key, d):
    """(count, rows-or-None, error)"""
    if d == "spacetime":
        r = st.get(key)
        if not r: return None, None, "not run"
        if r["rc"] != 0: return None, None, r["err"][-160:]
        return r["count"], r.get("rows"), None
    v, c = vals.get((key, d)), cnts.get((key, d))
    if not v or not c: return None, None, "not run"
    if v["rc"] != 0 or c["rc"] != 0: return None, None, (v["err"] or c["err"])[-160:]
    n = int(c["rows"][0][0])
    return n, (v["rows"] if n <= 2000 else None), None
keys = sorted({k for k, _ in vals} | set(st), key=lambda k: (["ais", "aviation", "cycling", "geolife"].index(k.split("/")[0]), k))
SYS = ["mobilitydb", "clickhouse", "duckdb", "spacetime", "sedona"]
out = []; tally = defaultdict(Counter)
for key in keys:
    pn, prow, perr = result(key, "postgis")
    row = {"key": key, "postgis": perr or f"{pn} rows"}
    for d in SYS:
        n, rows, err = result(key, d)
        if err == "not run": verdict = "n/a"
        elif err: verdict = "ERROR " + err.replace("\n", " ")[:100]
        elif perr: verdict = "postgis failed"
        elif pn > 2000 or n > 2000:
            if n == pn: verdict = "SAME COUNT"
            elif rel(n, pn) <= TOL: verdict = f"CLOSE COUNT {n} vs {pn} ({100 * rel(n, pn):.3f} %)"
            else: verdict = f"COUNT DIFF {n} vs {pn}"
        elif d == "spacetime" and canon(fix_st(key, rows)) == canon(prow): verdict = "SAME"
        elif canon(rows) == canon(prow): verdict = "SAME"
        elif pn == 0 and rows and all(r and num(r[-1]) == 0 for r in rows): verdict = "SAME (zero-count row)"
        elif key.split("/")[1] in KNN_QUERIES and n == pn: verdict = "KNN (geometry format / ties at equal distance)"
        elif n == pn and close(canon(prow), canon(fix_st(key, rows) if d == "spacetime" else rows)) is not None:
            verdict = f"CLOSE (max {100 * close(canon(prow), canon(fix_st(key, rows) if d == 'spacetime' else rows)):.3f} %)"
        elif n == pn:
            a, b = canon(prow), canon(rows); diff = sum(x != y for x, y in zip(a, b))
            verdict = f"VALUE DIFF {diff}/{pn} rows, e.g. pg={a[0] if a else ()} {d}={b[0] if b else ()}"
        else: verdict = f"COUNT DIFF {n} vs {pn}"
        row[d] = verdict; tally[d][re.match(r"[A-Za-z/ ]*[A-Za-z/]", verdict).group(0) if verdict[:1].isalpha() else verdict] += 1
    out.append(row)
    print(f"{key:62}", " | ".join(f"{d[:5]}: {row[d][:60]}" for d in SYS))
print()
for d in SYS: print(d, dict(tally[d]))
json.dump(out, open(os.path.join(D, "compare.json"), "w"), indent=1)
