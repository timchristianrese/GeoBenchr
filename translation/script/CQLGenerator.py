from typing import Dict, Any
import re
from queryConfigGenerator import QueryConfig, Filter, TimeFilter, SpatialFilter, Geometry

class CQLGenerator:
    _SPATIAL_FUNC_MAP: Dict[str, str] = {
        "intersects": "S_INTERSECTS",
        "contains": "S_CONTAINS",
        "within": "S_WITHIN",
        "touches": "S_TOUCHES",
        "equals": "S_EQUALS",
        "overlaps": "S_OVERLAPS",
        "crosses": "S_CROSSES",
        "disjoint": "S_DISJOINT",
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
        t = (geom.type or "").lower()

        def as_ring(points):
            ring = [[float(x), float(y)] for x, y in points]
            if ring and (ring[0][0] != ring[-1][0] or ring[0][1] != ring[-1][1]):
                ring.append(ring[0])
            return ring
        
        if t == "polygon":
            coords = as_ring(geom.coordinates)
            pts = ', '.join(f"{x} {y}" for x, y in coords)
            wkt = f"POLYGON(({pts}))"
        elif t == "point":
            x, y = geom.coordinates
            wkt = f"POINT({x} {y})"
        else:
            raise ValueError(f"CQL: Unknown geometry type: {geom.type}")

    def _geom_literal(self, ref: str) -> str:
        if isinstance(ref, str) and ref.startswith(":geometries."):
            name = ref.split(".", 1)[1]
            g = self.cfg.geometries.get(name)
            if not g:
                raise ValueError(f"CQL: Unknown geometry: {name}")
            return self._wkt_from_coords(g)
        return str(ref)

    def _format_value(self, val: Any, t: str) -> str:
        if isinstance(val, str):
            s = val.strip()
            if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
                inner = s[1:-1].replace("'", "''")
                return f"'{inner}'"
            
            if re.match(r"^[A-Za-z_]\w*(\.[A-Za-z_]\w*)?$", s):
                if t == "str":
                    return "'" + s.replace("'", "''") + "'"
                return s
            
            return "'" + s.replace("'", "''") + "'"
        
        if isinstance(val, bool):
            return "true" if val else "false"
        
        return str(val)

    def _quote_ts(self, v: str) -> str:
        if v is None:
            return None
        s = str(v).strip()
        if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
            return s
        return f"'{s}'"

    def _render_simple_filter(self, f: Filter) -> str:
        op = f.operator.upper()

        if op in ("IS NULL", "IS NOT NULL"):
            return f"{f.field} {op}"

        if op in ("IN", "NOT IN"):
            vals = f.value
            if not isinstance(vals, list):
                raise ValueError("CQL: For IN/NOT IN, value must be a list")
            inner = ", ".join(self._format_value(v, f.type) for v in vals)
            return f"{f.field} {op} ({inner})"
        
        mapped = self._OPERATOR_MAP.get(f.operator.lower(), "=")
        rhs = self._format_value(f.value, f.type)
        return f"{f.field} {mapped} {rhs}"
    
    def _render_time_filter(self, tf: TimeFilter) -> str:
        s = self._quote_ts(tf.start)
        e = self._quote_ts(tf.end)
        if s and e:
            return f"{tf.field} DURING {s}/{e}"
        if s and not e:
            return f"{tf.field} AFTER {s}"
        if e and not s:
            return f"{tf.field} BEFORE {e}"
        raise ValueError("CQL: Error with time filter")

    def _render_spatial_filter(self, sf: SpatialFilter) -> str:
        ftype = (sf.type or "").lower()
        func = self._SPATIAL_FUNC_MAP.get(ftype)
        if func is None:
            raise ValueError(f"CQL: Unknown spatial filter: {sf.type}")
        
        args = []
        for a in sf.args:
            if isinstance(a, str) and a.startswith(":geometries."):
                args.append(self._geom_literal(a))
            else:
                args.append(str(a))

        return f"{func}({', '.join(args)})"

    def _render_filter_node(self, node) -> str:
        if isinstance(node, Filter):
            return self._render_simple_filter(node)
        if isinstance(node, TimeFilter):
            return self._render_time_filter(node)
        if isinstance(node, SpatialFilter):
            return self._render_spatial_filter(node)
        
        if isinstance(node, list) and node:
            op, *children = node
            if isinstance(op, (Filter, TimeFilter, SpatialFilter)) and not children:
                return self._render_filter_node(op)
            
            if op == "and":
                parts = [self._render_filter_node(ch) for ch in children]
                return "(" + " AND ".join(parts) + ")"
            if op == "or":
                parts = [self._render_filter_node(ch) for ch in children]
                return "(" + " OR ".join(parts) + ")"
            if op == "not":
                if len(children) != 1:
                    raise ValueError(f"CQL: NOT requires 1 child, got: {children!r}")
                inner = self._render_filter_node(children[0])
                return f"NOT ({inner})"
            
            parts = [self._render_filter_node(ch) for ch in children]
            return "(" + " AND ".join(parts) + ")"
        
        raise ValueError(f"CQL: Invalide filter node type: {node!r}")

    def _render_filters(self) -> str:
        f = self.cfg.filters
        if not f:
            return "INCLUDE"
        return self._render_filter_node(f)

    def generate(self) -> str:
        return self._render_filters()