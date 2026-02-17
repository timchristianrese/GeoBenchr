from typing import Any, Dict, List
from queryConfigGenerator import (
    QueryConfig,
    Filter,
    TimeFilter,
    SpatialFilter,
)
import re
import datetime

class MongoDBGenerator:

    _SPATIAL_FUNC_MAP: Dict[str, str] = {
        "intersects": "$geoIntersects",
        "within": "$geoWithin",
        "coveredby": "$geoWithin",   # alias within
        # "contains" / "covers" -> approximation via $geoIntersects
        "contains": "$geoIntersects",
        "covers": "$geoIntersects",
        # others (touches/overlaps/crosses/equals) -> approx intersects
        "equals": "$geoIntersects",
        "overlaps": "$geoIntersects",
        "touches": "$geoIntersects",
        "crosses": "$geoIntersects",
        "disjoint": "$geoIntersects"  # handled via $nor below
    }

    _OPERATOR_MAP: Dict[str, str] = {
        "=": "$eq", "eq": "$eq",
        "!=": "$ne", "neq": "$ne",
        ">": "$gt", "gt": "$gt",
        "<": "$lt", "lt": "$lt",
        ">=": "$gte", "gte": "$gte",
        "<=": "$lte", "lte": "$lte",
    }

    def __init__(self, cfg: QueryConfig):
        self.cfg = cfg
        self._warnings: List[str] = []

    def _find_derived(self, src: str):
        parts = src.split()
        name = parts[0]
        alias = parts[1] if len(parts) >= 2 else parts[0]
        for d in getattr(self.cfg, "with_", []) or []:
            if d.name == name:
                return d, alias
        return None, alias
    
    def _cte_select_to_set_stage(self, select_list, main_alias: str):
        _AS_RE = re.compile(r"^([A-Za-z0-9_]+)\.([A-Za-z0-9_]+)\s+AS\s+([A-Za-z0-9_]+)$", re.I)
        sets = {}
        for s in (select_list or []):
            m = _AS_RE.match(s.strip())
            if not m:
                continue
            a, name, out = m.groups()
            if a == main_alias:
                sets[out] = "$_id" if name == "id" else f"${name}"
            else:
                base = "_id" if name == "id" else name
                sets[out] = f"${a}.{base}"
        return {"$set": sets} if sets else {}

    def _base_collection_name(self, src: str) -> str:
        # "orders o" -> "orders"
        return src.split()[0]
    
    def _join_alias(self, src: str) -> str:
        parts = src.split()
        if len(parts) >= 2:
            return parts[1]
        return parts[0]
    
    def _escape_var(self, path: str) -> str:
        # user.id -> user_id
        return path.replace(".", "_")
    
    def _strip_alias(self, f: str) -> str:
        return f.split(".", 1)[-1] if "." in f else f
    
    def _main_alias(self) -> str:
        parts = self.cfg.source.source.split()
        return parts[1] if len(parts) >= 2 else parts[0]
    
    def _field_alias_and_name(self, path: str):
        if "." in path:
            a, name = path.split(".", 1)
            return a, ("_id" if name == "id" else name)
        return None, ("_id" if path == "id" else path)
    
    def _resolve_main_field(self, f: str) -> str:
        alias = self._main_alias()
        if "." in f:
            a, name = f.split(".", 1)
            if a == alias:
                return "_id" if name == "id" else name
        return "_id" if f == "id" else f

    def _out_key_for_select(self, alias: str, name: str, main: str) -> str:
        return name if alias == main else f"{alias}_{name}"

    def _order_key_after_project(self, f: str) -> str:
        if self.cfg.group_by and f in self.cfg.group_by:
            return f.split(".", 1)[-1]
        
        if "." not in f:
            return "id" if f == "id" else f
        main = self._main_alias()
        alias, name = f.split(".", 1)
        return name if alias == main else f"{alias}_{name}"
    
    def _is_field_path(self, x: Any) -> bool:
        if not isinstance(x, str):
            return False
        if x.startswith(":"):
            return False
        if x in {"id", "geom", "geometry"}:
            return True
        return "." in x
    
    def _parse_distinct_field(self, raw: str) -> tuple[bool, str]:
        if not isinstance(raw, str):
            return (False, raw)
        s = raw.strip()
        if s.upper().startswith("DISTINCT "):
            return (True, s[9:].strip())
        return (False, s)
    
    def _collect_distinct_select(self):
        distincts = []
        main = self._main_alias()
        for s in (self.cfg.select or []):
            is_distinct, raw = self._parse_distinct_field(s)
            if not is_distinct:
                continue
            if "." in raw:
                a, name = raw.split(".", 1)
            else:
                a, name = main, raw
            resolved = self._resolve_field_for_group(raw)
            out_key = name if a == main else f"{a}_{name}"
            distincts.append({"raw": raw, "resolved": resolved, "out_key": out_key})
        return distincts    

    def _is_geom_literal(self, x: Any) -> bool:
        if isinstance(x, dict) and "type" in x and "coordinates" in x:
            return True
        return isinstance(x, str) and x.startswith(":geometries.")

    def _resolve_yaml_geometry(self, ref: str) -> Dict[str, Any]:
        if not isinstance(ref, str) or not ref.startswith(":geometries."):
            raise ValueError(f"MONGODB: invalid geometry reference {ref!r}")
        
        name = ref.split(".", 1)[-1]
        geoms = getattr(self.cfg, "geometries", None)
        if not geoms:
            raise ValueError(f"MONGODB: geometry reference {ref} but no geometries in config.")
        
        if isinstance(geoms, dict):
            g = geoms.get(name)
        else:
            g = next((gg for gg in geoms if getattr(gg, "name", None) == name), None)

        if g is None:
            raise ValueError(f"MONGODB: geometry {name} not found in config.")

        gtype = str(getattr(g, "type", "")).lower()
        coords = getattr(g, "coordinates", None)
        if coords is None:
            raise ValueError(f"MONGODB: geometry {name} has no 'coordinates'")
        
        def as_ring(points):
            ring = [[float(x), float(y)] for x, y in points]
            if ring and ring[0] != ring[-1]:
                ring.append(ring[0])
            return ring
        
        if gtype == "point":
            try:
                lon, lat = coords
            except Exception as e:
                raise ValueError(f"MONGODB: point '{name}' expects [lon, lat], got {coords!r}") from e
            return {"type": "Point", "coordinates": [float(lon), float(lat)]}
        
        elif gtype == "polygon":
            is_list_of_rings = (
                isinstance(coords, (list, tuple))
                and coords
                and isinstance(coords[0], (list, tuple))
                and coords[0]
                and isinstance(coords[0][0], (list, tuple))
            )
            if is_list_of_rings:
                rings = [as_ring(r) for r in coords]
            else:
                rings = [as_ring(coords)]
            return {"type": "Polygon", "coordinates": rings}
        
        else:
            raise ValueError(f"MONGODB: unsupported geometry type '{gtype}' for {name}")
                    
    def _as_geojson(self, x: Any) -> Dict[str, Any]:
        if isinstance(x, dict) and "type" in x and "coordinates" in x:
            return x
        if isinstance(x, str) and x.startswith(":geometries."):
            return self._resolve_yaml_geometry(x)
        raise ValueError(f"MONGODB: expected a literal geometry or :geometries.<name>, got {x!r}")


    def _parse_iso_to_dt(self, value: Any) -> Any:
        if isinstance(value, datetime.datetime):
            return value if value.tzinfo is not None else value.replace(tzinfo=datetime.timezone.utc)
        if isinstance(value, str):
            s = value.strip()

            if (s.startswith("'") and s.endswith("'")) or (s.startswith('"') and s.endswith('"')):
                s = s[1:-1].strip()

            if s.endswith('Z'):
                s = s[:-1] + '+00:00'
            try:
                dt = datetime.datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=datetime.timezone.utc)
                return dt
            except ValueError:
                return value
            
        return value

    def _format_like(self, field: str, value: str, case_insensitive: bool = True):
        pattern = ''.join('.*' if c == '%' else '.' if c == '_' else re.escape(c) for c in value)
        doc = {"$regex": f"^{pattern}$"}
        if case_insensitive:
            doc["$options"] = "i"
        return {field: doc}

    def _format_filter_value(self, f: Filter) -> Dict[str, Any]:
        mongo_op = self._OPERATOR_MAP.get(f.operator.lower())
        if f.type == "int" and mongo_op:
            return {f.field: {mongo_op: int(f.value)}}
        elif f.type == "str" and mongo_op:
            return {f.field: {mongo_op: str(f.value)}}
        elif f.type == "var" and mongo_op:
            return {"$expr": {mongo_op: ["$"+str(f.field), "$"+str(f.value)]}}
        else:
            if not f.type:
                raise ValueError(f"MONGODB: Filter type unknown: {f.type}")
            elif not mongo_op:
                if f.type == "str":
                    return {f.field: str(f.value)}
                if f.type == "int":
                    return {f.field: int(f.value)}
                if f.type == "var":
                    return {"$expr": {"$eq": ["$"+str(f.field), "$"+str(f.value)]}}

    def _format_simple_filter(self, f: Filter) -> Dict[str, Any]:
        op = f.operator.upper()
        
        if op == "IS NULL":
            return {f.field: None}
        if op == "IS NOT NULL":
            return {f.field: {"$ne": None}}
        if op == "IN":
            if not isinstance(f.value, list):
                raise ValueError(f"MONGODB: For IN, value must be a list, got {type(f.value)}")
            return {f.field: {"$in": f.value}}
        if op == "LIKE":
            return self._format_like(f.field, str(f.value))
        
        return self._format_filter_value(f)
    
    def _format_time_filter(self, tf: TimeFilter) -> Dict[str, Any]:
        fld = tf.field
        start = self._parse_iso_to_dt(tf.start)
        end = self._parse_iso_to_dt(tf.end)

        if start is not None and end is not None and start != end:
            return {fld: {"$gte": start, "$lte": end}}
        if start is None and end is not None:
            return {fld: {"$lte": tf.end}}
        if start is not None and end is None:
            return {fld: {"$gte": start}}
        return {fld: start}

    def _format_time_filter_expr_for_join(self, tf: TimeFilter) -> Dict[str, Any]:
        start = self._parse_iso_to_dt(tf.start)
        end = self._parse_iso_to_dt(tf.end)
        parts = []
        if start is not None:
            parts.append({"$gte": [ f"${tf.field}", start ]})
        if end is not None:
            parts.append({"$lte": [ f"${tf.field}", end ]})
        if not parts:
            return {}
        return { "$expr": { "$and": parts } }

    def _format_spatial_filter(self, sf: SpatialFilter, context: str = "main", join_alias: str | None = None) -> Dict[str, Any]:
        fn = str(sf.type).lower()
        args = getattr(sf, "args", None)
        mongo_op = self._SPATIAL_FUNC_MAP.get(fn)

        if not mongo_op:
            raise ValueError(f"MONGODB: Unsupported spatial function {sf.type}")
    
        if not isinstance(args, (list, tuple)) or len(args) < 2:
            raise ValueError("MONGODB: SpatialFilter expects args with at least two geometry operands.")

        left, right = args[0], args[1]

        left_is_field = self._is_field_path(left)
        right_is_field = self._is_field_path(right)
        left_is_geom = self._is_geom_literal(left)
        right_is_geom = self._is_geom_literal(right)

        if (left_is_field and right_is_field):
            raise ValueError("MONGODB: spatial join between two fields is not supported in $lookup/$match; one side must be a literal geometry.")

        if not ((left_is_field and right_is_geom)or (right_is_field and left_is_geom)):
            raise ValueError("MONGODB: spatial filter must compare a geometry field to a literal geometry (or :geometries.<name>).")
        
        if left_is_field and right_is_geom:
            field_path = left
            geom = self._as_geojson(right)
        else:
            field_path = right
            geom = self._as_geojson(left)

        if context == "join":
            fa, local_name = self._field_alias_and_name(str(field_path))
            if fa and join_alias and fa != join_alias:
                raise ValueError(f"MONGODB: spatial_filter in join must target alias '{join_alias}', got '{fa}'.")
            field_key = local_name
        else:
            field_key = self._resolve_main_field(str(field_path))

        if fn == "disjoint":
            return {"$nor": [{ field_key: { mongo_op: { "$geometry": geom } } }]}

        if fn in ("within", "coveredby"):
            return { field_key: { mongo_op: { "$geometry": geom } } }

        if fn in ("intersects", "contains", "covers", "equals", "overlaps", "touches", "crosses"):
            if fn in ("contains", "covers", "equals", "overlaps", "touches", "crosses"):
                self._warnings.append(f"MONGODB: spatial function '{fn}' approximated with $geoIntersects.")
            return { field_key: { mongo_op: { "$geometry": geom } } }

        raise ValueError(f"MONGODB: Unsupported spatial function '{fn}'.")

    def _format_join_filter(self, f: Filter, let_vars, join_alias: str, outer_aliases: set) -> Dict[str, Any]:
        mongo_op = self._OPERATOR_MAP.get(f.operator.lower(), "$eq")

        left_alias, left_name = self._field_alias_and_name(str(f.field))
        right_alias, right_name = self._field_alias_and_name(str(f.value))
        main_alias = self._main_alias()

        def outer_expr(a: str, name: str) -> str:
            if a == main_alias:
                resolved = self._resolve_main_field(f"{a}.{name if name != '_id' else 'id'}")
                return f"${resolved}"
            return f"${a}.{name}"

        if left_alias == join_alias and right_alias in outer_aliases:
            var_name = self._escape_var(str(f.value))
            let_vars[var_name] = outer_expr(right_alias, right_name)
            return {"$expr": {mongo_op: [f"${left_name}", f"$${var_name}"]}}
        
        if right_alias == join_alias and left_alias in outer_aliases:
            var_name = self._escape_var(str(f.field))
            let_vars[var_name] = outer_expr(left_alias, left_name)
            return {"$expr": {mongo_op: [f"$${var_name}", f"${right_name}"]}}
        
        raise ValueError(
            "MONGODB: JOIN var filter must compare the joined alias to the main alias "
            f"(found {left_alias} and {right_alias}, main={main_alias}, join={join_alias})."
        )

    def _parse_filter_node(self, node: Any, context="main", let_vars=None, join_alias: str | None = None, outer_aliases: set | None = None) -> Dict[str, Any]:
        if isinstance(node, Filter):
            if context == "join" and (not node.type) and self._is_field_path(node.field) and self._is_field_path(node.value):
                node = Filter(field= node.field, operator=node.operator, value=node.value, type="var")
            if context == "join":
                if node.type == "var":
                    return self._format_join_filter(node, let_vars, join_alias or "", outer_aliases or set())

                fa, name = self._field_alias_and_name(str(node.field))
                if fa and join_alias and fa != join_alias:
                    raise ValueError(f"MONGODB: join clause cannot address alias '{fa}' (current join is '{join_alias}').")
                
                n2 = Filter(field=name, operator=node.operator, type=node.type, value=node.value)
                return self._format_simple_filter(n2)
            
            field = self._resolve_main_field(node.field)
            n2 = Filter(field=field, operator=node.operator, type=node.type, value=node.value)
            return self._format_simple_filter(n2)
        
        if isinstance(node, TimeFilter):
            if context == "join":
                fa, name = self._field_alias_and_name(str(node.field))
                if fa and join_alias and fa != join_alias:
                    raise ValueError(f"MONGODB: time_filter in join must target alias '{join_alias}'.")
                tf2 = TimeFilter(field=name, start=node.start, end=node.end)
                return self._format_time_filter_expr_for_join(tf2)
            
            fld = self._resolve_main_field(node.field)
            tf2 = TimeFilter(field=fld, start=node.start, end=node.end)
            return self._format_time_filter(tf2)
        
        if isinstance(node, SpatialFilter):
            if context == "join":
                return self._format_spatial_filter(node, context="join", join_alias=join_alias)
            return self._format_spatial_filter(node, context="main")
        
        if isinstance(node, list) and node:
            f, *children = node
            if isinstance(f, (Filter, TimeFilter, SpatialFilter)) and len(children) == 0:
                return self._parse_filter_node(f, context=context, let_vars=let_vars, join_alias=join_alias, outer_aliases=outer_aliases)
            if f == "and":
                return {"$and": [self._parse_filter_node(c, context, let_vars, join_alias, outer_aliases) for c in children]}
            if f == "or":
                return {"$or": [self._parse_filter_node(c, context, let_vars, join_alias, outer_aliases) for c in children]}
            if f == "not":
                if len(children) != 1:
                    raise ValueError(f"MONGODB: NOT requires 1 child, got {children!r}")
                return {"$nor": [self._parse_filter_node(children[0], context, let_vars, join_alias, outer_aliases)]}
            return {"$and": [self._parse_filter_node(c, context, let_vars, join_alias, outer_aliases) for c in children]}
        raise ValueError(f"MONGODB: Invalid filter node: {node!r}")
    
    def _build_match(self, f) -> Dict[str, Any]:
        if not f:
            return {}

        return {"$match": self._parse_filter_node(f, context="main")}
    
    def _build_joins(self) -> List[Dict[str, Any]]:
        stages: List[Dict[str, Any]] = []
        available_aliases = { self._main_alias() }

        for j in self.cfg.joins:
            # CTE ?
            derived, join_alias = self._find_derived(j.source)
            if derived is not None:
                with_list = [d for d in (self.cfg.with_ or []) if d.name != derived.name]
                derived_cfg = QueryConfig(
                    name=None,
                    source=derived.source,
                    select=derived.select,
                    joins=derived.joins,
                    aggs=derived.aggs,
                    filters=derived.filters,
                    having=derived.having,
                    group_by=derived.group_by,
                    order_by=[], limit=None, offset=None,
                    geometries=self.cfg.geometries,
                    with_=with_list,
                )
                subgen = MongoDBGenerator(derived_cfg)
                from_coll = subgen._base_collection_name(derived_cfg.source.source)
                inner = subgen._compile_pipeline_stages()
                set_stage = subgen._cte_select_to_set_stage(derived.select, subgen._main_alias())
                if set_stage:
                    inner = inner + [set_stage]

                let_vars: Dict[str, str] = {}
                outer_aliases = set(available_aliases)
                match_doc = {}
                if j.on_clause:
                    match_doc = self._parse_filter_node(
                        j.on_clause, context="join",
                        let_vars=let_vars, join_alias=join_alias, outer_aliases=outer_aliases
                    )

                lookup_stage: Dict[str, Any] = {
                    "$lookup": {
                        "from": from_coll,
                        "as": join_alias,
                        "let": {k: v for k, v in let_vars.items()} if let_vars else {},
                        "pipeline": inner + ([{"$match": match_doc}] if match_doc else []),
                    }
                }

            else:
                from_coll = self._base_collection_name(j.source)
                join_alias = self._join_alias(j.source)

                let_vars: Dict[str, str] = {}
                outer_aliases = set(available_aliases)
                if j.on_clause:
                    match_doc = self._parse_filter_node(
                        j.on_clause, context="join",
                        let_vars=let_vars, join_alias=join_alias, outer_aliases=outer_aliases
                    )
                    lookup_stage = {
                        "$lookup": {
                            "from": from_coll,
                            "as": join_alias,
                            "let": {k: v for k, v in let_vars.items()} if let_vars else {},
                            "pipeline": [{"$match": match_doc}] if match_doc else [],
                        }
                    }
                else:
                    lookup_stage = {
                        "$lookup": {
                            "from": from_coll,
                            "as": join_alias,
                            "pipeline": [],
                        }
                    }

            stages.append(lookup_stage)

            jt = (j.type or "left").lower()
            stages.append({
                "$unwind": {
                    "path": f"${join_alias}",
                    "preserveNullAndEmptyArrays": False if jt.startswith("inner") else True
                }
            })

            available_aliases.add(join_alias)

        return stages
    
    def _resolve_field_for_group(self, f: str) -> str:
        if "." in f:
            a, name = f.split(".", 1)
            if a == self._main_alias():
                return "_id" if name == "id" else name
            else:
                return f"{a}.{name if name != 'id' else '_id'}"
        return "_id" if f == "id" else f

    def _build_aggregations(self) -> Dict[str, Any]:
        if not self.cfg.aggs:
            return {}
        
        if self.cfg.group_by:
            gid = {}
            for f in self.cfg.group_by:
                key = f.replace(".", "_")
                gid[key] = f"${self._resolve_field_for_group(f)}"
        else:
            gid = None

        group_doc: Dict[str, Any] = {"_id": gid}
        self._needs_distinct_size = set()

        for agg in self.cfg.aggs:
            name = agg.alias or agg.function.lower()
            fn = agg.function.lower()

            is_distinct, raw_field = self._parse_distinct_field(agg.field) if getattr(agg, "field", None) else (False, None)

            fld = None

            if raw_field and raw_field != "*":
                resolved = self._resolve_field_for_group(raw_field)
                fld = f"${resolved}"
                
            if fn == "count":
                if is_distinct:
                    if not raw_field or raw_field == "*":
                        raise ValueError("MONGODB: COUNT DISTINCT requires a concrete field, not '*'.")
                    group_doc[name] = {"$addToSet": fld}
                    self._needs_distinct_size.add(name)
                else:
                    if self.cfg.joins:
                        group_doc[name] = {
                            "$sum": {
                                "$cond": [
                                    {"$ifNull": [f"${self._join_alias(self.cfg.joins[0].source)}._id", False]},
                                    1, 0
                                ]
                            }
                        }
                    else:
                        group_doc[name] = {"$sum": 1}

            elif fn == "sum":
                group_doc[name] = {"$sum": fld}
            elif fn == "avg":
                group_doc[name] = {"$avg": fld}
            elif fn == "min":
                group_doc[name] = {"$min": fld}
            elif fn == "max":
                group_doc[name] = {"$max": fld}
            else:
                raise ValueError(f"MONGODB: Unsupported aggregation: {agg.function}")
        
        return {"$group": group_doc}


    def _compile_pipeline_stages(self) -> List[Dict[str, Any]]:
        pipeline: List[Dict[str, Any]] = []

        join_stages = self._build_joins()
        if join_stages:
            pipeline.extend(join_stages)

        match_doc = self._build_match(self.cfg.filters)
        if match_doc:
            pipeline.append(match_doc)

        distinct_fields = self._collect_distinct_select()
        use_distinct = bool(distinct_fields) and not (self.cfg.aggs or self.cfg.group_by or self.cfg.having)
        if use_distinct:
            if len(distinct_fields) == 1:
                rf = distinct_fields[0]["resolved"]
                pipeline.append({"$group": {"_id": f"${rf}"}})
            else:
                gid = {d["out_key"]: f"${d['resolved']}" for d in distinct_fields}
                pipeline.append({"$group": {"_id": gid}})

        aggs = self._build_aggregations()
        if aggs:
            pipeline.append(aggs)

        if self.cfg.having:
            having_doc = self._build_match(self.cfg.having)
            if having_doc:
                pipeline.append(having_doc)

        # PROJECTION
        proj: Dict[str, Any] = {}
        if self.cfg.group_by:
            for f in self.cfg.group_by:
                alias = f.replace(".", "_")
                out_key = f.split(".", 1)[-1]
                proj[out_key] = f"{'$'}_id.{alias}"
            for agg in self.cfg.aggs:
                name = agg.alias or agg.function.lower()
                if getattr(self, "_needs_distinct_size", None) and name in self._needs_distinct_size:
                    proj[name] = {"$size": {"$setDifference": [f"${name}", [None]]}}
                else:
                    proj[name] = f"${name}"
            proj["_id"] = 0
        else:
            main = self._main_alias()
            selected_main_id = False

            if 'use_distinct' in locals() and use_distinct:
                if len(distinct_fields) == 1:
                    out_key = distinct_fields[0]["out_key"]
                    proj[out_key] = "$_id"
                else:
                    for d in distinct_fields:
                        proj[d["out_key"]] = f"$_id.{d['out_key']}"
                proj["_id"] = 0
            else:
                for f in (self.cfg.select or []):
                    if "." in f:
                        alias, name = f.split(".", 1)
                        if alias == main:
                            if name == "id":
                                proj["id"] = "$_id"; selected_main_id = True
                            else:
                                proj[name] = 1
                        else:
                            out_key = self._out_key_for_select(alias, name, main)
                            fieldname = "_id" if name == "id" else name
                            proj[out_key] = f"${alias}.{fieldname}"
                    else:
                        if f == "id":
                            proj["id"] = "$_id"; selected_main_id = True
                        else:
                            proj[f] = 1

                if not proj and getattr(self, "_needs_distinct_size", None):
                    for agg in self.cfg.aggs:
                        name = agg.alias or agg.function.lower()
                        if name in self._needs_distinct_size:
                            proj[name] = {"$size": {"$setDifference": [f"${name}", [None]]}}

                if not selected_main_id and "_id" not in proj:
                    proj["_id"] = 0

        if proj:
            pipeline.append({"$project": proj})

        # ORDER / SKIP / LIMIT
        if self.cfg.order_by:
            sort_doc = { self._order_key_after_project(o.field): (1 if o.direction.lower() == "asc" else -1)
                        for o in self.cfg.order_by }
            pipeline.append({"$sort": sort_doc})
        if self.cfg.offset:
            pipeline.append({"$skip": self.cfg.offset})
        if self.cfg.limit:
            pipeline.append({"$limit": self.cfg.limit})

        return pipeline
                        

    def generate(self) -> str:
        mongodb = (
            "import datetime\n"
            "from pymongo import MongoClient\n"
            "client = MongoClient(\"mongodb://localhost:27017/\")\n"
            "db = client['my_base']\n"
        )

        pipeline: List[Dict[str, Any]] = []

        # Source = CTE ?
        d0, _ = self._find_derived(self.cfg.source.source)
        if d0 is not None:
            with_list = [d for d in (self.cfg.with_ or []) if d.name != d0.name]
            derived_cfg = QueryConfig(
                name=None,
                source=d0.source,
                select=d0.select,
                joins=d0.joins,
                aggs=d0.aggs,
                filters=d0.filters,
                having=d0.having,
                group_by=d0.group_by,
                order_by=[], limit=None, offset=None,
                geometries=self.cfg.geometries,
                with_=with_list,
            )
            subgen0 = MongoDBGenerator(derived_cfg)
            collection_name = subgen0._base_collection_name(derived_cfg.source.source)
            pipeline.extend(subgen0._compile_pipeline_stages())
            set_stage0 = subgen0._cte_select_to_set_stage(d0.select, subgen0._main_alias())
            if set_stage0:
                pipeline.append(set_stage0)
        else:
            collection_name = self.cfg.source.source.split()[0]

        mongodb += f"collection = db['{collection_name}']\n\n"

        pipeline.extend(self._compile_pipeline_stages())
        mongodb += f"pipeline = {pipeline}\n\n"

        mongodb += (
            "cursor = collection.aggregate(pipeline)\n"
            "for doc in cursor:\n"
            "    print(doc)\n"
        )
        return mongodb