"""Replace DuckDB's and Sedona's equivalence results with the new ones, then compare everything with PostGIS."""
import json, os, subprocess, sys
D = os.path.dirname(os.path.abspath(__file__))
def jl(p): return [json.loads(l) for l in open(p) if l.strip()] if os.path.exists(p) else []
vals = [r for r in jl(f"{D}/values.jsonl") if r["dialect"] not in ("duckdb", "sedona")]
cnts = [r for r in jl(f"{D}/counts.jsonl") if r["dialect"] not in ("duckdb", "sedona")]
vals += jl(f"{D}/duck_values.jsonl"); cnts += jl(f"{D}/duck_counts.jsonl")
for r in jl(f"{D}/sedona_equivalence.jsonl"):
    vals.append({"key": r["key"], "dialect": "sedona", "rc": r["rc"], "rows": r.get("rows") if r["rc"] == 0 else None, "err": r["err"], "secs": 0})
    cnts.append({"key": r["key"], "dialect": "sedona", "rc": r["rc"], "rows": [[str(r.get("count", 0))]] if r["rc"] == 0 else None, "err": r["err"], "secs": 0})
os.makedirs(f"{D}/final", exist_ok=True)
open(f"{D}/final/values.jsonl", "w").write("".join(json.dumps(r) + "\n" for r in vals))
open(f"{D}/final/counts.jsonl", "w").write("".join(json.dumps(r) + "\n" for r in cnts))
open(f"{D}/final/st.jsonl", "w").write(open(f"{D}/st.jsonl").read())
out = subprocess.run([sys.executable, f"{D}/compare_all.py", f"{D}/final"], capture_output=True, text=True)
open(f"{D}/final/compare.txt", "w").write(out.stdout + out.stderr)
print(out.stdout[-1500:])
