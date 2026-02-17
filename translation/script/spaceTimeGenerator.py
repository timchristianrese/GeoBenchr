from typing import Dict, List, Optional
import re

from queryConfigGenerator import (
    QueryConfig,
    Filter,
    TimeFilter,
    SpatialFilter,
    Geometry,
    SchemaField,
)

def _split_source_alias(raw: str):
    parts = raw.split()
    return parts[0], (parts[1] if len(parts) > 1 else None)

class SpaceTimeGenerator:
    _SPATIAL_FUNC_MAP: Dict[str, str] = {
        "intersects": "ST_Intersects",
        "contains": "ST_Contains",
        "within": "ST_Within",
        "touches": "ST_Touches",
        "covers": "ST_Covers",
        "coveredby": "ST_CoveredBy",
        "equals": "ST_Equals",
        "overlaps": "ST_Overlaps",
        "crosses": "ST_Crosses",
        "disjoint": "ST_Disjoint",
        "dwithin": "ST_DWithin",
    }

    _OPERATOR_MAP: Dict[str, str] = {
        "eq": "=", "neq": "!=", "gt": ">", "lt": "<", "gte": ">=", "lte": "<=",
        "=": "=", "!=": "!=", ">": ">", "<": "<", ">=": ">=", "<=": "<=",
    }

    def __init__(self, cfg: QueryConfig):
        self.cfg = cfg
        self._schemas: Dict[str, List[SchemaField]] = {}
        self._index_schemas()

    def _index_schemas(self) -> None:
        src, alias = _split_source_alias(self.cfg.source.source)
        main_alias = alias or src.split(".")[-1]
        schema_main = getattr(self.cfg.source.source_data, "schema", []) or []
        norm_main = []
        for f in schema_main:
            if isinstance(f, SchemaField):
                norm_main.append(f)
            elif isinstance(f, dict):
                norm_main.append(SchemaField(
                    name=f["name"],
                    type=f.get("type", "string"),
                    format=f.get("format")
                ))
        self._schemas[main_alias] = norm_main

        # joins
        for j in self.cfg.joins:
            jsrc, jalias = _split_source_alias(j.source)
            al = jalias or jsrc.split(".")[-1]
            raw_sd = j.source_data or {}
            schema_list = raw_sd.get("schema", [])
            norm: List[SchemaField] = []
            for f in schema_list:
                if isinstance(f, SchemaField):
                    norm.append(f)
                else:
                    norm.append(SchemaField(name=f["name"], type=f.get("type", "string"), format=f.get("format")))
            self._schemas[al] = norm

    def _field_format(self, alias: Optional[str], col: str) -> Optional[str]:
        if alias:
            for f in self._schemas.get(alias, []):
                if f.name == col:
                    return f.format
            return None
        for fields in self._schemas.values():
            for f in fields:
                if f.name == col and f.format:
                    return f.format
        return None
    
    def _is_point_like(self, raw) -> bool:
        if not isinstance(raw, str):
            return False
        s = raw.strip().upper()
        if s.startswith("ST_POINT(") or s.startswith("POINT("):
            return True
        m = re.match(r"^([A-Z_]\w*)(?:\.([A-Z_]\w*))?$", raw, flags=re.I)
        if m:
            alias = m.group(1) if m.group(2) else None
            col   = m.group(2) or m.group(1)
            fmt = self._field_format(alias, col)
            if fmt and fmt.lower().endswith("gpoint"):
                return True
        return False

    def _is_polygon_like(self, raw) -> bool:
        if not isinstance(raw, str):
            return False
        s = raw.strip().upper()
        if s.startswith("POLYGON(") or s.startswith("MULTIPOLYGON("):
            return True
        if "::GPOLYGON" in s or "::GMPOLYGON" in s:
            return True
        m = re.match(r"^([A-Z_]\w*)(?:\.([A-Z_]\w*))?$", raw, flags=re.I)
        if m:
            alias = m.group(1) if m.group(2) else None
            col   = m.group(2) or m.group(1)
            fmt = self._field_format(alias, col)
            if not fmt:
                return False
            tgt = fmt.split(":", 1)[-1].lower()  # ex: "wkt:gpolygon"
            return tgt in ("gpolygon", "gmpolygon")
        return False


    # ------------------------------------------------------------------
    # Geometries
    # ------------------------------------------------------------------
    def _spacetime_geom_type(self, geom: Geometry) -> str:
        t = geom.type.lower()
        g = {
            "point": "gpoint",
            "polygon": "gpolygon",
            "multipolygon": "gmpolygon",
            "linestring": "gline",
            "multilinestring": "gmline",
            "multipoint": "gmpoint",
        }.get(t)
        if not g:
            raise ValueError(f"SPACETIME: Unknown geometry type for SpaceTime: {geom.type}")
        return g

    def _wkt_from_coords(self, geom: Geometry) -> str:
        t = geom.type.lower()

        def _close_ring(coords, eps=1e-9):
            if not coords:
                return coords
            x0, y0 = coords[0]
            x1, y1 = coords[-1]
            if abs(x0 - x1) > eps or abs(y0 - y1) > eps:
                coords = list(coords) + [coords[0]]
            return coords

        if t == "point":
            x, y = geom.coordinates
            return f"POINT({x} {y})"
        if t == "polygon":
            coords = _close_ring(list(geom.coordinates))
            pts = ", ".join(f"{x} {y}" for x, y in coords)
            return f"POLYGON(({pts}))"
        if t == "linestring":
            pts = ", ".join(f"{x} {y}" for x, y in geom.coordinates)
            return f"LINESTRING({pts})"
        if t == "multipolygon":
            polys = []
            for poly in geom.coordinates:
                ring = _close_ring(list(poly))
                pts = ", ".join(f"{x} {y}" for x, y in ring)
                polys.append(f"(({pts}))")
            return f"MULTIPOLYGON({', '.join(polys)})"
        raise ValueError(f"SPACETIME: Unknown geometry type: {geom.type}")

    def _geom_to_sql(self, geom_name: str) -> str:
        geom = self.cfg.geometries.get(geom_name)
        if not geom:
            raise ValueError(f"SPACETIME: Unknown geometry: {geom_name}")
        wkt = self._wkt_from_coords(geom)
        st_type = self._spacetime_geom_type(geom)
        return f"ST_ToGeom('{wkt}'::{st_type})"

    def _resolve_geom_arg(self, arg) -> str:
        if isinstance(arg, str) and arg.startswith(":geometries."):
            name = arg.split(".", 1)[1]
            return self._geom_to_sql(name)

        if isinstance(arg, str) and re.match(r"^(POINT|LINESTRING|POLYGON|MULTI\w+)\s*\(", arg.strip(), re.I):
            head = arg.split("(", 1)[0].strip().upper()
            gtype = {
                "POINT": "gpoint",
                "LINESTRING": "gline",
                "POLYGON": "gpolygon",
                "MULTIPOLYGON": "gmpolygon",
                "MULTILINESTRING": "gmline",
                "MULTIPOINT": "gmpoint",
            }.get(head, "gpolygon")
            return f"ST_ToGeom('{arg}'::{gtype})"

        if isinstance(arg, str) and re.match(r"^[A-Za-z_]\w*(\.[A-Za-z_]\w*)?$", arg):
            if "." in arg:
                alias, col = arg.split(".", 1)
            else:
                alias, col = None, arg
            fmt = self._field_format(alias, col)
            if fmt and fmt.lower().startswith("wkt:"):
                gtype = fmt.split(":", 1)[1]
                return f"ST_ToGeom({arg}::{gtype})"
            return arg

        return str(arg)

    # ------------------------------------------------------------------
    # Valeurs / formats
    # ------------------------------------------------------------------
    def _format_value(self, val, typ):
        if isinstance(val, str):
            if val.startswith(":"):
                return val
            if len(val) >= 2 and val[0] == val[-1] and val[0] in ("'", '"'):
                return val
            if re.match(r"^[A-Za-z_]\w*(\.[A-Za-z_]\w*)?$", val):
                return val
            if (typ or "").lower() in ("str", "string"):
                return f"'{val}'"
        if isinstance(val, bool):
            return str(val).lower()
        return str(val)

    # ------------------------------------------------------------------
    # Simple / temporal / spatial filters
    # ------------------------------------------------------------------

    def _distance_to_meters(self, raw, lat_hint: float | None = None) -> float:
        if isinstance(raw, dict):
            v = float(raw.get("value"))
            units = (raw.get("units") or "deg").lower()
            if units in ("m", "meter", "meters"):
                return v
            return v * 111_320.0
        return float(raw) * 111_320.0

    def _lat_hint_from_args(self, a, b) -> float | None:
        if isinstance(a, str) and a.startswith(":geometries."):
            name = a.split(".", 1)[1]
            g = self.cfg.geometries.get(name)
            if g and g.type.lower() == "point":
                return float(g.coordinates[1])
        if isinstance(b, str) and b.startswith(":geometries."):
            name = b.split(".", 1)[1]
            g = self.cfg.geometries.get(name)
            if g and g.type.lower() == "point":
                return float(g.coordinates[1])
        for s in (a, b):
            if isinstance(s, str) and s.strip().upper().startswith("POINT("):
                try:
                    nums = s[s.find("(")+1:s.find(")")].split()
                    return float(nums[1])
                except Exception:
                    pass
        return None

    def _render_spatial_filter(self, sf: SpatialFilter) -> str:
        t = sf.type.lower()

        if t == "dwithin":
            if len(sf.args) != 3:
                raise ValueError("SPACETIME: dwithin requires 3 arguments")
            left_raw, right_raw, dist_raw = sf.args[0], sf.args[1], sf.args[2]
            left_sql  = self._resolve_geom_arg(left_raw)
            right_sql = self._resolve_geom_arg(right_raw)
            lat_hint  = self._lat_hint_from_args(left_raw, right_raw)
            dist_m    = self._distance_to_meters(dist_raw, lat_hint)
            return f"ST_DWithin({left_sql}, {right_sql}, {dist_m})"

        if sf.type == "&&":
            if len(sf.args) != 2:
                raise ValueError("SPACETIME: '&&' requires exactly two arguments")
            a0 = self._resolve_geom_arg(sf.args[0])
            a1 = self._resolve_geom_arg(sf.args[1])
            return f"ST_Intersects(ST_Envelope({a0}), ST_Envelope({a1}))"

        if t == "intersects":
            if len(sf.args) == 2:
                a1, b1 = sf.args[0], sf.args[1]
                # point <@ polygon
                if self._is_point_like(a1) and self._is_polygon_like(b1):
                    point_sql = self._resolve_geom_arg(a1)
                    poly_sql  = self._resolve_geom_arg(b1)
                    return f"{point_sql} <@ {poly_sql}"
                if self._is_point_like(b1) and self._is_polygon_like(a1):
                    point_sql = self._resolve_geom_arg(b1)
                    poly_sql  = self._resolve_geom_arg(a1)
                    return f"{point_sql} <@ {poly_sql}"

        if t == "contains":
            if len(sf.args) != 2:
                raise ValueError("SPACETIME: contains requires 2 arguments")
            a1, b1 = sf.args
            if self._is_point_like(a1) and self._is_polygon_like(b1):
                a1, b1 = b1, a1
            left_sql  = self._resolve_geom_arg(a1)
            right_sql = self._resolve_geom_arg(b1)
            return f"ST_Contains({left_sql}, {right_sql})"

        args = [self._resolve_geom_arg(x) for x in sf.args]
        func = self._SPATIAL_FUNC_MAP.get(t)
        if not func:
            raise ValueError(f"SPACETIME: Unknown spatial function: {sf.type}")
        if len(args) != 2:
            raise ValueError(f"SPACETIME: {sf.type} requires 2 arguments")
        return f"{func}({args[0]}, {args[1]})"


    def _time_field_kind(self, field: str) -> str:
        if "." in field:
            alias, col = field.split(".", 1)
        else:
            alias, col = None, field
        fmt = self._field_format(alias, col)
        if fmt in ("epoch", "epochrange"):
            return fmt
        return "unknown"

    def _epoch_lit(self, v) -> str:
        s = str(v).strip()
        if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
            s = s[1:-1]
        return f"EPOCH '{s}'"

    def _epochrange_lit(self, s, e) -> str:
        ss = str(s).strip().strip("'").strip('"')
        ee = str(e).strip().strip("'").strip('"')
        return f"EPOCHRANGE '[{ss}, {ee}]'"

    def _st_timefmt(self, s: str) -> str:
        s = str(s).strip().strip("'").strip('"')
        m = re.search(r'([+-]\d{2}:\d{2}|Z)$', s)
        tz = m.group(1) if m else None
        s = s.replace("T", " ")
        s = re.sub(r'\.\d{1,6}Z?$', '', s)

        if tz and tz != 'Z':
            raise ValueError("SPACETIME: timestamp only in UTC (it mean need 'Z' at the end).")
        return s.rstrip('Z')


    def _render_time_filter(self, tf: TimeFilter) -> str:
        s, e = tf.start, tf.end
        if s and e:
            ss, ee = self._st_timefmt(s), self._st_timefmt(e)
            return f"{tf.field} <@ EPOCHRANGE '[{ss}, {ee}]'"
        if s and not e:
            return f"{tf.field} >= EPOCH '{self._st_timefmt(s)}'"
        if e and not s:
            return f"{tf.field} <= EPOCH '{self._st_timefmt(e)}'"
        raise ValueError("SPACETIME: Invalid TimeFilter")
    
    def _render_simple_filter(self, f: Filter) -> str:
        op = f.operator.strip().upper()

        if op in ("IS NULL", "IS NOT NULL"):
            return f"{f.field} {op}"

        if op in ("IN", "NOT IN"):
            vals = f.value
            if not isinstance(vals, (list, tuple)):
                raise ValueError(f"SPACETIME: {op} requires a list value, got {type(vals)}")
            inner = ", ".join(self._format_value(v, f.type) for v in vals)
            return f"({f.field} {'NOT IN' if op == 'NOT IN' else 'IN'} ({inner}))"

        mapped = self._OPERATOR_MAP.get(op, op)
        rhs = self._format_value(f.value, f.type)
        return f"({f.field} {mapped} {rhs})"



    def _render_filter_node(self, node) -> str:
        if isinstance(node, Filter):
            return self._render_simple_filter(node)
        if isinstance(node, TimeFilter):
            return self._render_time_filter(node)
        if isinstance(node, SpatialFilter):
            return self._render_spatial_filter(node)

        if isinstance(node, dict):
            if "time_filter" in node:
                filters = node["time_filter"]
                if not isinstance(filters, list):
                    filters = [filters]
                return "(" + " AND ".join(self._render_time_filter(f) for f in filters) + ")"

            if "spatial_filter" in node:
                filters = node["spatial_filter"]
                if not isinstance(filters, list):
                    filters = [filters]
                return "(" + " AND ".join(self._render_spatial_filter(f) for f in filters) + ")"

            if "filter" in node:
                return self._render_filter_node(node["filter"])

        if isinstance(node, list):
            if not node:
                return "1=1"
            if len(node) == 1:
                return self._render_filter_node(node[0])

            op, *children = node
            if op == "and":
                return "(" + " AND ".join(self._render_filter_node(ch) for ch in children) + ")"
            if op == "or":
                return "(" + " OR ".join(self._render_filter_node(ch) for ch in children) + ")"
            if op == "not":
                if len(children) != 1:
                    raise ValueError("SPACETIME: NOT must have one child")
                return f"NOT ({self._render_filter_node(children[0])})"

            return "(" + " AND ".join(self._render_filter_node(ch) for ch in [op] + children) + ")"

        raise ValueError(f"SPACETIME: Invalid filter node: {node}")

    # ------------------------------------------------------------------
    # WHERE / ORDER BY / full SQL
    # ------------------------------------------------------------------
    def _render_filters(self) -> str:
        if not self.cfg.filters:
            return ""
        return self._render_filter_node(self.cfg.filters)

    def _render_orderby(self) -> str:
        if not self.cfg.order_by:
            return ""
        parts = []
        for o in self.cfg.order_by:
            parts.append(f"{o.field} {'DESC' if o.direction == 'desc' else 'ASC'}")
        return ", ".join(parts)

    def _rewrite_agg_aliases_in_expr(self, expr: str, aggs=None) -> str:
        out = expr
        the_aggs = self.cfg.aggs if aggs is None else aggs
        for agg in the_aggs:
            if agg.alias:
                pat = re.compile(rf"\b{re.escape(agg.alias)}\b")
                repl = f"{agg.function}({agg.field})"
                out = pat.sub(repl, out)
        return out



    def _split_distinct_fields(self, field: str) -> list[str]:
        s = field.strip()
        if s.upper().startswith("DISTINCT "):
            s = s[len("DISTINCT "):].strip()
        return [c.strip() for c in s.split(",") if c.strip()]

    def _render_select_like(self, obj) -> str:
        aggs = list(getattr(obj, "aggs", []))
        if (
            len(aggs) == 1
            and aggs[0].function.upper() == "COUNT"
            and str(aggs[0].field).strip().upper().startswith("DISTINCT ")
            and not getattr(obj, "group_by", [])
        ):
            alias = aggs[0].alias or "n"
            distinct_cols = self._split_distinct_fields(aggs[0].field)

            parts_in = []
            parts_in.append("SELECT DISTINCT " + ", ".join(distinct_cols))
            parts_in.append(f"FROM {obj.source.source}")

            # JOINS
            for j in getattr(obj, "joins", []):
                on_sql = self._render_filter_node(j.on_clause)
                parts_in.append(f"{j.type.upper()} JOIN {j.source} ON {on_sql}")

            # WHERE
            if getattr(obj, "filters", None):
                where_clause = self._render_filter_node(obj.filters)
                if where_clause:
                    parts_in.append(f"WHERE {where_clause}")

            inner_sql = "\n".join(parts_in)

            outer_sql = f"SELECT COUNT(*) AS {alias}\nFROM (\n{inner_sql}\n) _dedup"
            return outer_sql

        parts = []
        if getattr(obj, "select", None):
            parts.extend(obj.select)
        for agg in getattr(obj, "aggs", []):
            e = f"{agg.function}({agg.field})"
            if agg.alias:
                e += f" AS {agg.alias}"
            parts.append(e)
        select_clause = ", ".join(parts) if parts else "*"

        sql = f"SELECT {select_clause}\nFROM {obj.source.source}"

        for j in getattr(obj, "joins", []):
            on_sql = self._render_filter_node(j.on_clause)
            sql += f"\n{j.type.upper()} JOIN {j.source} ON {on_sql}"

        if getattr(obj, "filters", None):
            where_clause = self._render_filter_node(obj.filters)
            if where_clause:
                sql += f"\nWHERE {where_clause}"

        if getattr(obj, "group_by", []):
            sql += f"\nGROUP BY {', '.join(obj.group_by)}"
        if getattr(obj, "having", None):
            sql += f"\nHAVING {self._render_filter_node(obj.having)}"
        if getattr(obj, "order_by", []):
            ob = [f"{o.field} {'DESC' if o.direction=='desc' else 'ASC'}" for o in obj.order_by]
            sql += f"\nORDER BY {', '.join(ob)}"
        if getattr(obj, "limit", None) is not None:
            sql += f"\nLIMIT {obj.limit}"
        if getattr(obj, "offset", None) is not None:
            sql += f"\nOFFSET {obj.offset}"
        return sql

    def _render_with(self) -> str:
        with_list = getattr(self.cfg, "with_", []) or []
        if not with_list:
            return ""
        parts = []
        for d in with_list:
            sel = self._render_select_like(d)
            parts.append(f"{d.name} AS (\n{sel}\n)")
        return "WITH " + ",\n".join(parts) + "\n"

    def generate(self) -> str:
        with_sql = self._render_with()
        main_sql = self._render_select_like(self.cfg)
        return (with_sql + main_sql).rstrip(";") + ";"

