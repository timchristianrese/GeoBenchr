"""Prepares a copy of the query configuration for a benchmark run: every query gets REPS repetitions,
the write queries are switched off, and with "scan" ClickHouse's scan-only version is rendered in place
of the H3 one. Edits the combinedBenchConf.yaml files under CONFIG_DIR in place -- run it on a copy
(build_jar.sh does).
    python3 set_reps.py CONFIG_DIR REPS h3|scan"""
import re
import sys

B, reps, swap = sys.argv[1], sys.argv[2], sys.argv[3] == "scan"
W = ("insert", "update", "update_single", "delete", "delete_single", "bulk_insert")
for sc in ["ais", "aviation", "cycling", "geolife", "general"]:
    p = f"{B}/{sc}/config/combinedBenchConf.yaml"
    out = [re.sub(r"^  repetition: \d+", f"  repetition: {reps}", l) for l in open(p).read().split("\n")]
    i = 0
    while i < len(out):
        if out[i].startswith("- name: "):
            j = i + 1; use_j = None; typ = None
            while j < len(out) and not out[j].startswith("- name: "):
                if out[j].startswith("  use: "):
                    use_j = j
                m = re.match(r"^  type: (\w+)", out[j]); typ = m.group(1) if m else typ
                j += 1
            if typ in W and use_j is not None:
                out[use_j] = "  use: false"
            if swap:
                ks = [k for k in range(i, j) if out[k] in ("  clickhouse: |", "  clickhouse_scan: |")]
                if len(ks) == 2:
                    for k in ks:
                        out[k] = "  clickhouse_scan: |" if out[k] == "  clickhouse: |" else "  clickhouse: |"
            i = j
        else:
            i += 1
    open(p, "w").write("\n".join(out))
