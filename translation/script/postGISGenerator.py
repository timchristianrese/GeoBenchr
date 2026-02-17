from queryConfigGenerator import QueryConfig, Filter, TimeFilter, SpatialFilter, Geometry
from typing import Dict
import re

class PostGISGenerator:
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
        "eq": "=",
        "neq": "!=",
        "gt": ">",
        "lt": "<",
        "gte": ">=",
        "lte": "<=",
        "=": "=",
        "!=": "!=",
        ">": ">",
        "<": "<",
        ">=": ">=",
        "<=": "<=",
    }

    def __init__(self, cfg: QueryConfig):
        self.cfg = cfg

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

        if t == "polygon":
            coords = _close_ring(list(geom.coordinates))
            pts = ", ".join(f"{x} {y}" for x, y in coords)
            return f"POLYGON(({pts}))"
        if t == "point":
            x, y = geom.coordinates
            return f"POINT({x} {y})"
        raise ValueError(f"POSTGIS: Unknown type geometry: {geom.type}")

    def _geom_to_sql(self, geom_name: str) -> str:
        geom = self.cfg.geometries.get(geom_name)
        if not geom:
            raise ValueError(f"POSTGIS: Unknown geometry: {geom_name}")
        wkt = self._wkt_from_coords(geom)
        return f"ST_GeomFromText('{wkt}', {geom.srid})"
    
    def _render_joins(self, joins=None) -> str:
        lines = []
        the_joins = self.cfg.joins if joins is None else joins
        for j in the_joins:
            if not j.on_clause:
                continue
            on_sql = self._render_filter_node(j.on_clause)
            lines.append(f"{j.type.upper()} JOIN {j.source} ON {on_sql}")
        return "\n".join(lines)

    def _format_value(self, val, typ):
        if isinstance(val, str):
            s = val.strip()
            if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
                inner = s[1:-1].replace("'", "''")
                return f"'{inner}'"

            import re
            if re.match(r"^[A-Za-z_]\w*(\.[A-Za-z_]\w*)?$", s):
                if (typ or "").lower() in ("str", "string"):
                    return "'" + s.replace("'", "''") + "'"
                return s

            return "'" + s.replace("'", "''") + "'"

        if isinstance(val, bool):
            return str(val).lower()
        return str(val)

    def _render_simple_filter(self, f: Filter) -> str:
        op = f.operator.upper()
        if op in ('IS NULL', 'IS NOT NULL'):
            return f"{f.field} {op}"

        if op in ('IN', 'NOT IN'):
            vals = f.value
            if not isinstance(vals, list):
                raise ValueError("POSTGIS: For IN/NOT IN, value must be a list")
            inner = ", ".join(self._format_value(v, f.type) for v in vals)
            neg = "NOT " if op == "NOT IN" else ""
            return f"{f.field} {neg}IN ({inner})"

        rhs = self._format_value(f.value, f.type)
        mapped = self._OPERATOR_MAP.get(f.operator.lower(), f.operator.upper())
        return f"{f.field} {mapped} {rhs}"

    def _q_ts(self, v: str) -> str:
        if v is None: return None
        s = str(v).strip()
        if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
            return s
        return f"'{s}'"


    def _render_time_filter(self, tf: TimeFilter) -> str:
        s = self._q_ts(tf.start) if tf.start is not None else None
        e = self._q_ts(tf.end)   if tf.end   is not None else None
        if s and e:
            return f"{tf.field} BETWEEN {s} AND {e}"
        if s and not e:
            return f"{tf.field} >= {s}"
        if e and not s:
            return f"{tf.field} <= {e}"
        raise ValueError("POSTGIS: Error with time filter")

        
    def _parse_distance(self, raw):
        if isinstance(raw, dict):
            v = float(raw.get("value"))
            units = (raw.get("units") or "deg").lower()
            if units not in ("m", "meter", "meters", "deg", "degree", "degrees"):
                units = "deg"
            if units.startswith("m"):
                return v, "m"
            return v, "deg"
        return float(raw), "deg"

    def _srid_hint(self):
        try:
            sd = self.cfg.source.source_data
            if sd and getattr(sd, "geom", None) and sd.geom.srid:
                return int(sd.geom.srid)
        except Exception:
            pass
        return 4326
        
    def _render_spatial_filter(self, sf: SpatialFilter) -> str:
        sql_args = []
        for arg in sf.args:
            if isinstance(arg, str) and arg.startswith(":geometries."):
                name = arg.split(".",1)[1]
                sql_args.append(self._geom_to_sql(name))
            else:
                s = str(arg)
                # NEW: si st point, on met un srid
                if s.strip().upper().startswith("ST_POINT(") and "ST_SETSRID" not in s.upper():
                    s = f"ST_SetSRID({s}, 4326)"
                sql_args.append(s)

        if sf.type == "&&":
            return f"{sql_args[0]} && {sql_args[1]}"
        
        func_name = self._SPATIAL_FUNC_MAP.get(sf.type.lower())
        if func_name is None:
            raise ValueError(f"POSTGIS: Unknown spatial filter: {sf.type}")

        if sf.type.lower() == "dwithin":
            if len(sql_args) != 3:
                raise ValueError("POSTGIS: DWithin requires 3 args")
            left, right, dist_raw = sf.args[0], sf.args[1], sf.args[2]
            L, R = sql_args[0], sql_args[1]
            val, units = self._parse_distance(dist_raw)
            srid = self._srid_hint()

            if units == "m":
                if srid == 4326:
                    return f"ST_DWithin(({L})::geography, ({R})::geography, {val})"
                else:
                    return f"ST_DWithin({L}, {R}, {val})"
            else:
                return f"ST_DWithin({L}, {R}, {val})"
        # ---------------------------------------------------------------

        return f"{func_name}({', '.join(sql_args)})"
        
    def _render_filter_node(self, node: list) -> str:
        if isinstance(node, Filter):
            return self._render_simple_filter(node)

        if isinstance(node, TimeFilter):
            return self._render_time_filter(node)

        if isinstance(node, SpatialFilter):
            return self._render_spatial_filter(node)

        if isinstance(node, list) and node:
            op, *children = node
            if isinstance(op, (Filter, TimeFilter, SpatialFilter)) and len(children) == 0:
                return self._render_filter_node(op)
            if op == "and":
                parts = [self._render_filter_node(ch) for ch in children]
                return "(" + " AND ".join(parts) + ")"
            if op == "or":
                parts = [self._render_filter_node(ch) for ch in children]
                return "(" + " OR ".join(parts) + ")"
            if op == "not":
                if len(children) != 1:
                    raise ValueError(f"POSTGIS: NOT requires 1 child, got : {children!r}")
                inner = self._render_filter_node(children[0])
                return f"NOT ({inner})"
            parts = [self._render_filter_node(ch) for ch in children]
            return "(" + " AND ".join(parts) + ")"

        raise ValueError(f"POSTGIS: Invalid filter node type: {node!r}")

    def _render_filters(self, filters=None) -> str:
        f = self.cfg.filters if filters is None else filters
        if not f:
            return ""
        return self._render_filter_node(f)

    def _render_orderby(self, order_by=None) -> str:
        f = self.cfg.order_by if order_by is None else order_by
        if not f:
            return ""
        parts = []
        for o in f:
            direction = "DESC" if o.direction == 'desc' else "ASC"
            parts.append(f"{o.field} {direction}")
        return ", ".join(parts)
        
    def _rewrite_agg_aliases_in_expr(self, expr: str, aggs=None) -> str:
        out = expr
        the_aggs = self.cfg.aggs if aggs is None else aggs
        for agg in the_aggs:
            if agg.alias:
                pattern = re.compile(rf"\b{re.escape(agg.alias)}\b")
                repl = f"{agg.function}({agg.field})"
                out = pattern.sub(repl, out)
        return out

    def _render_select_like(self, obj) -> str:
        parts = []
        if getattr(obj, "select", None):
            parts.extend(obj.select)
        for agg in getattr(obj, "aggs", []):
            expr = f"{agg.function}({agg.field})"
            if agg.alias:
                expr += f" AS {agg.alias}"
            parts.append(expr)
        select_clause = ", ".join(parts) or "*"

        sql = f"SELECT {select_clause}\nFROM {obj.source.source}"

        # JOINS
        join_clause = self._render_joins(getattr(obj, "joins", []))
        if join_clause:
            sql += "\n" + join_clause

        # WHERE
        where_clause = self._render_filters(getattr(obj, "filters", None))
        if where_clause:
            sql += f"\nWHERE {where_clause}"

        # GROUP BY
        if getattr(obj, "group_by", []):
            gb = ", ".join(obj.group_by)
            sql += f"\nGROUP BY {gb}"

        # HAVING 
        if getattr(obj, "having", None):
            having_clause = self._render_filter_node(obj.having)
            having_clause = self._rewrite_agg_aliases_in_expr(having_clause, getattr(obj, "aggs", []))
            sql += f"\nHAVING {having_clause}"

        # ORDER BY / LIMIT / OFFSET 
        orderby_clause = self._render_orderby(getattr(obj, "order_by", []))
        if orderby_clause:
            sql += f"\nORDER BY {orderby_clause}"

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
