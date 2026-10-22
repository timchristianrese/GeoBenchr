"""Client jars for every workload and number of client threads, cut from one base jar (2026-10-22).

The base jar (build_jar.sh with WRITES=1, GEN_WARMUP=<most threads>) holds every read and write query
128 + GEN_WARMUP times in one seeded order, each repetition with parameters of its own (generateParameterVariations.py;
a query with fewer distinct parameter sets has all of them plus GEN_WARMUP repeats). The jar for a workload and n
client threads keeps per query:
  - measured: the last distinct parameter sets in file order -- reads up to 128; writes as many as the workload's
    share of the scenario's measured reads asks for (ro: none, 75: a third of them, 90: a ninth), spread evenly
    over the write queries -- the same sets in every jar of the workload, and the same reads in every workload;
  - warm-up: n further repetitions of the query, placed first among its repetitions (build_results.py with
    WARMUP_REPS=n).
Everything stays in the base jar's order otherwise, so reads and writes are mixed the same way in every jar.
    python3 make_workload_jars.py BASE.jar OUT_DIR WORKLOADS THREADS
    e.g. make_workload_jars.py benchmark_client-base136.jar ~/jars ro,75,90 1,2,4,8
         -> OUT_DIR/benchmark_client-<workload>-t<n>.jar
"""
import io
import sys
import zipfile

import yaml

BASE, OUT, WORKLOADS, THREADS = sys.argv[1], sys.argv[2], sys.argv[3].split(","), [int(x) for x in sys.argv[4].split(",")]
READS = 128
WRITE_SHARE = {"ro": 0, "75": 1 / 3, "90": 1 / 9}
WRITE_TYPES = {"insert", "update", "update_single", "delete", "delete_single", "bulk_insert"}


class Lit(str):
    pass


yaml.add_representer(Lit, lambda d, s: d.represent_scalar("tag:yaml.org,2002:str", s, style="|"))


def key(e):
    return repr(e.get("params"))


def measured_positions(entries, name, m):
    """Positions of the last m distinct parameter sets of query `name`."""
    seen, out = set(), []
    for i in range(len(entries) - 1, -1, -1):
        e = entries[i]
        if e["name"] == name and key(e) not in seen and len(out) < m:
            seen.add(key(e))
            out.append(i)
    return sorted(out)


def cut(entries, targets, n):
    """Keep per query its measured repetitions (targets[name]) and n warm-ups, the warm-ups first."""
    keep = {}
    for name, m in targets.items():
        meas = measured_positions(entries, name, m)
        if not meas:
            continue
        rest = [i for i in range(len(entries)) if entries[i]["name"] == name and i not in set(meas)]
        warm = rest[-n:] if n else []
        if len(warm) < n:
            sys.exit(f"{name}: only {len(warm)} warm-up repetitions left, {n} needed")
        slots = sorted(meas + warm)
        for slot, i in zip(slots, warm + meas):   # warm-ups take the first slots
            keep[slot] = i
    out = []
    for slot in sorted(keep):
        e = dict(entries[keep[slot]])
        if isinstance(e.get("sql"), str) and "\n" in e["sql"]:
            e["sql"] = Lit(e["sql"])
        out.append(e)
    return out


base = zipfile.ZipFile(BASE)
SCENARIOS = ("ais", "aviation", "cycling", "geolife")   # general/ holds templates, not run
files = {n: yaml.safe_load(base.read(n)) or [] for n in base.namelist()
         if n.count("/") == 1 and n.endswith("_queries.yaml") and n.split("/")[0] in SCENARIOS}
# per scenario: the measured reads (from PostGIS's file: every query, distinct parameter sets) and the write queries
plan = {}
for path, entries in files.items():
    if not path.endswith("postgisSQL_queries.yaml"):
        continue
    sc = path.split("/")[0]
    reads = {e["name"] for e in entries if e.get("type") not in WRITE_TYPES}
    writes = sorted({e["name"] for e in entries if e.get("type") in WRITE_TYPES})
    n_reads = {q: min(READS, len({key(e) for e in entries if e["name"] == q})) for q in reads}
    plan[sc] = (n_reads, writes)

for wl in WORKLOADS:
    for n in THREADS:
        target_jar = f"{OUT}/benchmark_client-{wl}-t{n}.jar"
        summary = []
        with zipfile.ZipFile(target_jar, "w", zipfile.ZIP_DEFLATED) as z:
            for item in base.infolist():
                if item.filename not in files:
                    # a copy of the entry's ZipInfo: writestr() changes the one it is given (offsets)
                    info = zipfile.ZipInfo(item.filename, item.date_time)
                    info.compress_type, info.external_attr = item.compress_type, item.external_attr
                    z.writestr(info, base.read(item.filename))
                    continue
                sc = item.filename.split("/")[0]
                n_reads, writes = plan[sc]
                total_writes = round(sum(n_reads.values()) * WRITE_SHARE[wl])
                targets = dict(n_reads)
                for k, w in enumerate(writes):
                    targets[w] = total_writes // len(writes) + (1 if k < total_writes % len(writes) else 0)
                out = cut(files[item.filename], targets, n)
                buf = io.StringIO()
                yaml.dump(out, buf, sort_keys=False, allow_unicode=True)
                z.writestr(item.filename, buf.getvalue())
                if item.filename.endswith("postgisSQL_queries.yaml"):
                    nw = sum(1 for e in out if e.get("type") in WRITE_TYPES)
                    summary.append(f"{sc}: {len(out) - nw} reads + {nw} writes")
        print(f"{target_jar}: " + "; ".join(summary), flush=True)
