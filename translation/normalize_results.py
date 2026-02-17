#!/usr/bin/env python3

import argparse, csv, json, ast, io, re
from typing import Any, List, Dict

def _round_float(x: float) -> float:
    return round(x, 6)

def _cast_scalar(v: str):
    if v is None:
        return None
    s = v.strip()
    if s == "":
        return None
    
    if s.lower() == "true": return True
    if s.lower() == "false": return False

    try:
        return int(s)
    except Exception:
        try:
            return _round_float(float(s))
        except Exception:
            return v
        
def _canon(x: Any):
    if isinstance(x, dict):
        return {k: _canon(v) for k, v in sorted(x.items())}
    if isinstance(x, list):
        return [_canon(v) for v in x]
    if isinstance(x, float):
        return _round_float(x)
    return x

def _drop_field(rows: List[Dict], to_drop: List[str]) -> List[Dict]:
    if not to_drop:
        return rows
    drop = set([f for f in to_drop if f])
    out = []
    for r in rows:
        if isinstance(r, dict):
            out.append({k: v for k, v in r.items() if k not in drop})
        else:
            out.append(r)
    return out

# CSV -> JSON
def csv_to_json(path_in: str) -> List[Dict[str, Any]]:
    with open(path_in, "r", encoding="utf-8") as f:
        data = f.read()
    if not data.strip():
        return []
    if data.startswith("\ufeff"):
        data = data.lstrip("\ufeff")

    def _normalize_row(d: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in d.items():
            if v is None:
                continue
            out[k] = v
        return out
                

    f2 = io.StringIO(data)
    rdr = csv.reader(f2)
    
    try:
        raw_header = next(rdr)
    except StopIteration:
        return []
    
    header_clean: List[str] = []
    counts: Dict[str, int] = {}

    for i, h in enumerate(raw_header):
        base = (h or "").strip() or f"col{i+1}"
        n = counts.get(base, 0) + 1
        counts[base] = n
        header_clean.append(base if n == 1 else f"{base}_{n}")
    
    rows: List[Dict[str, Any]] = []
    for r in rdr:
        values = [r[i] if i < len(r) else None for i in range(len(header_clean))]
        casted = {header_clean[i]: _cast_scalar(values[i]) for i in range(len(header_clean))}
        rows.append(_normalize_row(casted))

    return rows

# MONGO RAW -> JSON
OBJID   = re.compile(r"ObjectId\((['\"])([0-9a-fA-F]{24})\1\)")
DT      = re.compile(
    r"datetime\.datetime\(\s*"
    r"(\d{4})\s*,\s*(\d{1,2})\s*,\s*(\d{1,2})\s*,\s*"
    r"(\d{1,2})\s*,\s*(\d{1,2})"
    r"(?:\s*,\s*(\d{1,2}))?"
    r"(?:\s*,[^)]*)?\)"
)

def _clean_bson(line: str) -> str:
    line = OBJID.sub(r"'\2'", line)
    line = DT.sub(_dt_call_to_literal, line)
    return line

def _dt_call_to_literal(m: re.Match) -> str:
    y, mo, d, h, mi, se = m.groups()
    se = se or "0"
    return f"'{int(y):04d}-{int(mo):02d}-{int(d):02d} {int(h):02d}:{int(mi):02d}:{int(se):02d}'"

def mongo_raw_to_json(path_in: str) -> List[Dict[str, Any]]:
    with open(path_in, "r", encoding="utf-8") as f:
        raw = f.read()

    s = raw.strip()
    if not s:
        return []
    
    rows: List[Dict[str, Any]] = []
    for line in raw.splitlines():
        line = line.strip()
        if not line:
            continue

        obj = ast.literal_eval(_clean_bson(line))
        if isinstance(obj, dict):
            obj = {k: v for k, v in obj.items() if v is not None}
            rows.append(obj)
            continue

    return rows

    

def main():
    ap = argparse.ArgumentParser(description="Normalize CSV or Mongo RAW to JSON.")
    sub = ap.add_subparsers(dest="mode", required=True)

    ap_csv = sub.add_parser("csv", help="Parse a CSV (with header) to JSON.")
    ap_csv.add_argument("--in", dest="fin", required=True, help="Input CSV file")
    ap_csv.add_argument("--out", dest="fout", required=True, help="Output JSON file")

    ap_mg = sub.add_parser("mongo", help="Parse Mongo raw output to JSON.")
    ap_mg.add_argument("--in", dest="fin", required=True, help="Input raw file")
    ap_mg.add_argument("--out", dest="fout", required=True, help="Output JSON file")

    args= ap.parse_args()

    if args.mode == "csv":
        rows = csv_to_json(args.fin)
    else:
        rows = mongo_raw_to_json(args.fin)
    rows = _drop_field(rows, ["id", "_id"])

    with open(args.fout, "w", encoding="utf-8") as out:
        json.dump(rows, out, ensure_ascii=False, sort_keys=True)

if __name__ == "__main__":
    main()