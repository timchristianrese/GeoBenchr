from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple, Union

from queryConfigGenerator import (
    Aggregation,
    CsvOptions,
    Filter,
    Geometry,
    Join,
    QueryConfig,
    SchemaField,
    SourceData,
    SpatialFilter,
    TimeFilter,
)

_HEADER_TMPL = """\
# --- bootstrap Spark + Sedona ----------------------------------------------
from pyspark.sql import SparkSession
import pyspark

# automatically determine the correct Spark / Scala pair
spark_mm  = '.'.join(pyspark.__version__.split('.')[:2])        # "3.4", "3.5", …
scala_ver = '2.13' if float(spark_mm) >= 3.5 else '2.12'

WAREHOUSE_DIR = "/home/arthur/Benchmark-Tests/cycling spark-warehouse" # hardcoded for now but should be parameterizable

sedona_version = "1.7.2"
sedona_pkg   = f"org.apache.sedona:sedona-spark-{spark_mm}_{scala_ver}:{sedona_version}"
geotools_pkg = f"org.datasyslab:geotools-wrapper:{sedona_version}-28.5"

try:
    spark
except NameError:
    spark = (
        SparkSession.builder
        .appName("sedona_auto")
        .config("spark.jars.packages", f"{sedona_pkg},{geotools_pkg}")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrator",
                "org.apache.sedona.core.serde.SedonaKryoRegistrator")
        .config("spark.sql.warehouse.dir", WAREHOUSE_DIR)
        .config("spark.sql.catalogImplementation", "hive")
        .enableHiveSupport()    
        .getOrCreate()
    )

try:
    from sedona.spark import SedonaContext
    SedonaContext.create(spark)
except Exception:
    from sedona.register import SedonaRegistrator
    SedonaRegistrator.registerAll(spark)
# ---------------------------------------------------------------------------
"""


# ---------------------------------------------------------------------------
# Helpers (generator-side, not emitted)
# ---------------------------------------------------------------------------

def _split_source_alias(raw: str) -> Tuple[str, str | None]:
    """
    Split a raw source string:

        "schema.table t"  ->  ("schema.table", "t")
        "my_table"        ->  ("my_table", None)

    Only the first whitespace is considered a separator.
    """
    parts = raw.split()
    return parts[0], parts[1] if len(parts) > 1 else None

class SedonaGenerator:
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

    _GEOM_REF_PREFIXES = (":geometries.", "geometry.")

    def __init__(self, cfg: QueryConfig):
        self.cfg = cfg
        self.output: List[str] = []
        self.sd = cfg.source.source_data

        self.source_type: str = self.sd.type.lower()
        self.source_path: str = self.sd.path or cfg.source.source
        self.geometry_column = self.sd.geometry_column

        # Default geometry column: index 0 for WKT files, otherwise "geom"
        if self.geometry_column is None:
            self.geometry_column = 0 if self.source_type == "wkt" else "geom"


    def _add(self, line: str) -> None:
        self.output.append(line)

    def _emit(self, *lines: str) -> None:
        self.output.extend(lines)


    @staticmethod
    def _strip_alias(col: str) -> str:
        return col.split(".", 1)[-1]

    def _quote_identifier(self, name: str) -> str:
        return f"`{name}`" if "." in name else name

    def _col_expr(self, name: str) -> str:
        col_name = name
        if not self.cfg.joins:
            _, inline_alias = _split_source_alias(self.cfg.source.source)
            if inline_alias and col_name.startswith(inline_alias + "."):
                col_name = col_name[len(inline_alias) + 1:]
        if "." in col_name:
            return f'F.col("`{col_name}`")'
        return f'F.col("{col_name}")'

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate(self) -> str:
        """
        Compile the QueryConfig into a Sedona-ready Python script.

        Steps:
          1) Header: bootstrap Spark + Sedona
          2) Load main source into df
          3) Register temp views for source/alias (if no joins)
          4) Apply either joins OR DataFrame-level filters
          5) Projection / aggregation
          6) Post-processing: ORDER BY, LIMIT, OFFSET
          7) Expose DataFrame under 'result' (+ show)
        """
        self._generate_header()
        self._add("# Auto-generated Sedona query — DO NOT EDIT BY HAND\n")
        self._render_load_main()

        src_name, inline_alias = _split_source_alias(self.cfg.source.source)
        base_name = src_name.split(".")[-1]
        if not self.cfg.joins:
            if inline_alias:
                self._add(f'df.createOrReplaceTempView("{base_name}")')
                if inline_alias != base_name:
                    self._add(f'df.createOrReplaceTempView("{inline_alias}")')
            else:
                self._add(f'df.createOrReplaceTempView("{base_name}")')

        if self.cfg.joins:
            self._render_joins()
        else:
            self._render_filters_df()

        if self.cfg.group_by or self.cfg.aggs:
            self._render_groupby_agg()
        elif not self.cfg.joins:
            self._render_select()

        self._render_post_processing()
        self._emit("\n# Final result variable is: result", "result.show()")
        return "\n".join(self.output)

    # ------------------------------------------------------------------
    # Loading helpers
    # ------------------------------------------------------------------

    def _generate_header(self) -> None:
        for line in _HEADER_TMPL.splitlines():
            self._add(line)

    def _render_load_main(self) -> None:
        self._add("df = None  # Placeholder to avoid NameError if joins exist")
        self._render_load_source(
            self.source_type, self.source_path, self.geometry_column, "df"
        )

    def _render_load_source(
        self,
        source_type: str,
        path: str,
        geom_col: Union[str, int, None],
        target: str,
        source_data: dict | None = None,
    ) -> None:
        """
        Emit code to read a source into a Spark DataFrame:
        - table: spark.read.table(path)
        - csv:   spark.read.csv(options...)
        """
        source_type = source_type.lower()

        if source_data is None:
            csv_opts_raw = self.sd.csv_options.__dict__ if self.sd.csv_options else {}
            schema_raw = [f.__dict__ for f in self.sd.schema]
        else:
            csv_opts_raw = source_data.get("csv_options", {})
            schema_raw = source_data.get("schema", [])

        # --- TABLE Spark ----------------------------------------------------
        if source_type == "table":
            src_only, _ = _split_source_alias(path)
            self._add(f'{target} = spark.read.table("{src_only}")')
            return

        # --- CSV ------------------------------------------------------------
        if source_type == "csv":
            delimiter = csv_opts_raw.get("delimiter", ";")
            header = str(csv_opts_raw.get("header", False)).lower()
            encoding = csv_opts_raw.get("encoding", "UTF-8")
            quote = csv_opts_raw.get("quote", '"').replace('"', '\\"')
            escape = csv_opts_raw.get("escape", "\\").replace("\\", "\\\\")
            infer = str(csv_opts_raw.get("infer_schema", False)).lower()

            self._emit(
                "reader = (spark.read",
                f'    .option("delimiter", "{delimiter}")',
                f'    .option("header", "{header}")',
                f'    .option("encoding", "{encoding}")',
                f'    .option("quote", "{quote}")',
                f'    .option("escape", "{escape}")',
                f'    .option("inferSchema", "{infer}")',
                f'    .csv("{path}") )',
            )

            if schema_raw and header == "false":
                colnames = ", ".join(f'"{c["name"]}"' for c in schema_raw)
                self._add(f"df_tmp = reader.toDF({colnames})")
                self._add("from pyspark.sql import functions as F")
                for c in schema_raw:
                    if c.get("type") and c.get("type") != "string":
                        self._add(
                            f'df_tmp = df_tmp.withColumn("{c["name"]}", '
                            f'F.col("{c["name"]}").cast("{c["type"]}"))'
                        )
            else:
                self._add("df_tmp = reader")

            def _csv_colname(idx_or_name: Union[str, int]) -> str:
                if isinstance(idx_or_name, int):
                    if schema_raw and header == "false":
                        try:
                            return schema_raw[idx_or_name]["name"]
                        except Exception:
                            return f"_c{idx_or_name}"
                    return f"_c{idx_or_name}"
                return str(idx_or_name)

            if geom_col is not None:
                geom_colname = _csv_colname(geom_col)
                self._emit(
                    "from pyspark.sql import functions as F",
                    "from sedona.sql.types import GeometryType",
                    'df_tmp = df_tmp.withColumn("geom", '
                    f'F.expr("ST_GeomFromWKT({geom_colname})").cast(GeometryType()))',
                )

            for c in schema_raw:
                name = c["name"]
                if geom_col is not None and name == _csv_colname(geom_col):
                    continue
                lname = name.lower()
                if "wkt" in lname or "traj" in lname:
                    if lname.startswith("wkt_"):
                        new_name = name[4:]
                    elif lname.endswith("_wkt"):
                        new_name = name[:-4]
                    else:
                        new_name = "traj" if lname == "wkt_traj" else name
                    if new_name == "geom":
                        continue
                    self._add(
                        f'df_tmp = df_tmp.withColumn("{new_name}", '
                        f'F.expr("ST_GeomFromWKT({name})").cast(GeometryType()))'
                    )

            self._add(f"{target} = df_tmp")
            return

        raise ValueError(f"SEDONA: Unsupported source_type: {source_type}")

    # ------------------------------------------------------------------
    # SELECT / GROUP-BY / HAVING
    # ------------------------------------------------------------------

    def _render_select(self) -> None:
        if not self.cfg.select:
            return

        exprs = list(self.cfg.select)

        if not self.cfg.joins:
            _, inline_alias = _split_source_alias(self.cfg.source.source)
            if inline_alias:
                pattern = re.compile(rf"\b{re.escape(inline_alias)}\.")
                exprs = [pattern.sub("", e) for e in exprs]

        cols = ", ".join(repr(e) for e in exprs)
        self._add(f"df = df.selectExpr({cols})")

    def _render_groupby_agg(self) -> None:
        self._add("from pyspark.sql import functions as F")

        src_name, inline_alias = _split_source_alias(self.cfg.source.source)
        if self.cfg.joins:
            gb_names = list(self.cfg.group_by)
        else:
            gb_names = []
            for c in self.cfg.group_by:
                if inline_alias and c.startswith(inline_alias + "."):
                    gb_names.append(c[len(inline_alias) + 1 :])
                else:
                    gb_names.append(c)

        if gb_names:
            checks = [f"{self._quote_identifier(c)} IS NOT NULL" for c in gb_names]
            non_null = " AND ".join(checks)
            self._add(f'df = df.filter("{non_null}")')

        agg_exprs: List[str] = []
        for agg in self.cfg.aggs:
            func = agg.function.upper()
            if func == "COUNT" and agg.field.upper().startswith("DISTINCT "):
                colname = agg.field[len("DISTINCT ") :].strip()
                if (
                    not self.cfg.joins
                    and inline_alias
                    and colname.startswith(inline_alias + ".")
                ):
                    colname = colname[len(inline_alias) + 1 :]
                expr = f"F.countDistinct({self._col_expr(colname)})"
            else:
                field = "*" if agg.field == "*" else agg.field
                expr = f'F.{func.lower()}("*")' if field == "*" else f"F.{func.lower()}({self._col_expr(field)})"
            if agg.alias:
                expr += f'.alias("{agg.alias}")'
            agg_exprs.append(expr)

        agg_expr = ", ".join(agg_exprs) if agg_exprs else "*"

        if gb_names:
            group_cols = ", ".join(self._col_expr(c) for c in gb_names)
            self._add(f"df = df.groupBy({group_cols}).agg({agg_expr})")
        else:
            self._add(f"df = df.agg({agg_expr})")

        if self.cfg.having:
            having_sql = self._compile_filter_expr(self.cfg.having)
            self._add(f'df = df.filter("{having_sql}")')

    # ------------------------------------------------------------------
    # Post-processing
    # ------------------------------------------------------------------

    def _render_post_processing(self) -> None:
        if self.cfg.order_by:
            self._add("from pyspark.sql import functions as F")
            order_exprs = [
                f'{self._col_expr(o.field)}.{"desc()" if o.direction.lower() == "desc" else "asc()"}'
                for o in self.cfg.order_by
            ]
            self._add(f"df = df.orderBy({', '.join(order_exprs)})")

        if self.cfg.limit is not None:
            if not self.cfg.order_by:
                self._emit(
                    "from pyspark.sql import functions as F",
                    "df = df.withColumn('__idx', F.monotonically_increasing_id())",
                    "df = df.orderBy('__idx')",
                )
            self._add(f"df = df.limit({self.cfg.limit})")
            if not self.cfg.order_by:
                self._add("df = df.drop('__idx')")

        if self.cfg.offset is not None:
            self._emit(
                "from pyspark.sql import Window, functions as F",
                "w = Window.orderBy(F.monotonically_increasing_id())",
                f'df = df.withColumn("__rn", F.row_number().over(w))'
                f'.filter(F.col("__rn") > {self.cfg.offset}).drop("__rn")',
            )
        self._add("result = df")

    # ------------------------------------------------------------------
    # WHERE (no-join path)
    # ------------------------------------------------------------------

    def _render_filters_df(self) -> None:
        if not self.cfg.filters:
            return

        raw = self._compile_filter_expr(self.cfg.filters)

        if not self.cfg.joins:
            _, inline_alias = _split_source_alias(self.cfg.source.source)
            if inline_alias:
                raw = re.sub(rf"\b{re.escape(inline_alias)}\.", "", raw)

        safe = raw.replace('"', r"\"")
        self._add(f'df = df.filter("{safe}")')

    # ------------------------------------------------------------------
    # JOINS
    # ------------------------------------------------------------------

    @staticmethod
    def _rewrite_alias(s: str, old: str, new: str) -> str:
        return re.sub(fr"\b{re.escape(old)}\.", f"{new}.", s)

    def _render_joins(self) -> None:
        src_name, inline_alias = _split_source_alias(self.cfg.source.source)
        base_name = src_name.split(".")[-1]
        main_alias = inline_alias or base_name

        if inline_alias:
            self._add(f'df.createOrReplaceTempView("{inline_alias}")')
            if base_name != inline_alias:
                self._add(f'df.createOrReplaceTempView("{base_name}")')
        else:
            self._add(f'df.createOrReplaceTempView("{base_name}")')

        for join in self.cfg.joins:
            jsrc, jinline_alias = _split_source_alias(join.source)
            base_r = jsrc.split(".")[-1]
            alias = jinline_alias or base_r

            jtype = (join.source_data or {}).get("type", "table").lower()
            jpath = (join.source_data or {}).get("path", jsrc)
            jgeom_col = (join.source_data or {}).get("geometry_column")
            if jgeom_col is None and getattr(join, "geometries", None):
                jgeom_col = next(iter(join.geometries.keys()))

            self._render_load_source(
                jtype, jpath, jgeom_col, f"df_{alias}", join.source_data or {}
            )

            self._add(f'df_{alias}.createOrReplaceTempView("{alias}")')
            if base_r != alias:
                self._add(f'df_{alias}.createOrReplaceTempView("{base_r}")')

            join_sql = {
                "inner": "JOIN",
                "left": "LEFT JOIN",
                "right": "RIGHT JOIN",
                "full": "FULL JOIN",
                "spatial": "JOIN",
            }.get(join.type.lower(), "JOIN")

            needed: List[str] = []
            if self.cfg.select:
                needed.extend(self.cfg.select)
            for g in self.cfg.group_by:
                if g not in needed:
                    needed.append(g)
            for agg in self.cfg.aggs:
                fld = agg.field
                if fld == "*":
                    continue
                if fld.upper().startswith("DISTINCT "):
                    fld = fld[len("DISTINCT ") :].strip()
                if fld not in needed:
                    needed.append(fld)

            raw_on = self._compile_filter_expr(join.on_clause)
            if base_name != main_alias:
                raw_on = self._rewrite_alias(raw_on, base_name, main_alias)
            if base_r != alias:
                raw_on = self._rewrite_alias(raw_on, base_r, alias)

            select_parts: List[str] = []
            for col in needed:
                c = col
                if base_name != main_alias:
                    c = self._rewrite_alias(c, base_name, main_alias)
                if base_r != alias:
                    c = self._rewrite_alias(c, base_r, alias)

                if re.search(r"\bas\b", c, flags=re.IGNORECASE):
                    select_parts.append(c)
                elif "." in c:
                    select_parts.append(f"{c} AS `{c}`")
                else:
                    select_parts.append(c)

            raw_select = ", ".join(select_parts) if select_parts else "*"

            filter_sql = ""
            if self.cfg.filters:
                filter_sql = self._compile_filter_expr(self.cfg.filters)
                for old, new in ((base_name, main_alias), (base_r, alias)):
                    filter_sql = self._rewrite_alias(filter_sql, old, new)

            self._emit(
                'df = spark.sql("""',
                f"    SELECT {raw_select}",
                f"    FROM {main_alias}",
                f"    {join_sql} {alias}",
                f"    ON {raw_on}",
            )
            if filter_sql:
                self._add(f"    WHERE {filter_sql}")
            self._add('""")')

    # ------------------------------------------------------------------
    # Filter compilers
    # ------------------------------------------------------------------

    def _compile_filter_expr(self, expr: Any) -> str:
        if isinstance(expr, list):
            if not expr:
                return "(1=1)"
            first = expr[0]
            if len(expr) == 1 and isinstance(first, (Filter, TimeFilter, SpatialFilter, dict)):
                return self._compile_filter_expr(first)
            if not isinstance(first, str):
                return "(" + " AND ".join(self._compile_filter_expr(e) for e in expr) + ")"
            op = first.lower()
            if op == "not":
                return f"(NOT {self._compile_filter_expr(expr[1])})"
            joiner = f" {op.upper()} "
            return f"({joiner.join(self._compile_filter_expr(e) for e in expr[1:])})"

        if isinstance(expr, Filter):
            return self._compile_basic_filter(expr)

        if isinstance(expr, TimeFilter):
            def _q(v: Any) -> str | None:
                if v is None:
                    return None
                s = str(v).strip()
                if len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"'):
                    return s
                return f"'{s}'"

            start = _q(expr.start)
            end = _q(expr.end)
            if start and end:
                return f"({expr.field} BETWEEN {start} AND {end})"
            if start:
                return f"({expr.field} >= {start})"
            if end:
                return f"({expr.field} <= {end})"
            return "(1=1)"

        if isinstance(expr, SpatialFilter):
            return self._compile_spatial_filter(expr)

        if isinstance(expr, dict):
            return self._compile_basic_filter(
                Filter(expr["field"], expr["operator"], expr.get("value"), expr.get("type"))
            )

        raise TypeError(expr)

    def _compile_basic_filter(self, flt: Filter) -> str:
        op = flt.operator.strip().lower()

        if op in {"is null", "is not null"}:
            return f"({flt.field} {'IS NOT NULL' if op == 'is not null' else 'IS NULL'})"

        if op in {"in", "not in"}:
            vals = ", ".join(
                f"'{v}'" if isinstance(v, str) and not v.startswith("'") else str(v)
                for v in flt.value
            )
            return f"({flt.field} {'NOT IN' if op == 'not in' else 'IN'} ({vals}))"

        op_sql = self._OPERATOR_MAP.get(op, op.upper())
        val = flt.value

        if isinstance(val, str) and re.match(r"^[a-zA-Z_]\w*\.[a-zA-Z_]\w*$", val):
            rhs = val
        elif isinstance(val, str):
            iso_like = re.match(r"^\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?Z?)?$", val)
            if iso_like:
                rhs = f"'{val}'"
            elif val and (val[0] == val[-1]) and val[0] in ("'", '"'):
                rhs = val
            else:
                raise ValueError(f"SEDONA: String literal {val!r} is not quoted.")
        elif isinstance(val, bool):
            rhs = str(val).lower()
        else:
            rhs = val

        return f"({flt.field} {op_sql} {rhs})"
    
    def _parse_distance(self, raw):
        if isinstance(raw, dict):
            v = float(raw.get("value"))
            units = (raw.get("units") or "deg").lower()
            if units not in ("m", "meter", "meters", "deg", "degree", "degrees"):
                units = "deg"
            return (v, "m") if units.startswith("m") else (v, "deg")
        return float(raw), "deg"

    def _srid_hint(self) -> int:
        try:
            sd = self.cfg.source.source_data
            if sd and getattr(sd, "geom", None) and sd.geom.srid:
                return int(sd.geom.srid)
        except Exception:
            pass
        return 4326


    def _compile_spatial_filter(self, sf: SpatialFilter) -> str:
        t = sf.type.lower()

        if t == "dwithin":
            if len(sf.args) != 3:
                raise ValueError("SEDONA: dwithin requires exactly three arguments")

            left_sql  = self._resolve_geom_arg(sf.args[0])
            right_sql = self._resolve_geom_arg(sf.args[1])
            val, units = self._parse_distance(sf.args[2])
            srid = self._srid_hint()

            if units == "m":
                if srid == 4326:
                    return (
                        f"(ST_Distance("
                        f"ST_Transform({left_sql}, 'EPSG:4326', 'EPSG:3857'), "
                        f"ST_Transform({right_sql}, 'EPSG:4326', 'EPSG:3857')"
                        f") <= {float(val)})"
                    )
                else:
                    return f"(ST_Distance({left_sql}, {right_sql}) <= {float(val)})"
            else:
                return f"(ST_Distance({left_sql}, {right_sql}) <= {float(val)})"

        if sf.type == "&&":
            if len(sf.args) != 2:
                raise ValueError("SEDONA: bbox operator '&&' requires exactly two arguments")
            left_sql, right_sql = map(self._resolve_geom_arg, sf.args)
            return f"ST_Intersects(ST_Envelope({left_sql}), ST_Envelope({right_sql}))"

        func = self._SPATIAL_FUNC_MAP.get(t, sf.type)
        if len(sf.args) != 2:
            raise ValueError("SEDONA: Spatial filter requires exactly two arguments")
        left, right = sf.args

        const_right = (
            isinstance(right, str)
            and (
                right.startswith(self._GEOM_REF_PREFIXES)
                or right.upper().startswith(("POINT", "POLYGON", "LINESTRING"))
            )
        )
        if (
            t == "contains"
            and isinstance(left, str)
            and left.lower() in {"geom", self.geometry_column}
            and const_right
        ):
            left, right = right, left

        left_sql = self._resolve_geom_arg(left)
        right_sql = self._resolve_geom_arg(right)
        return f"{func}({left_sql}, {right_sql})"



    # ------------------------------------------------------------------
    # Geometry helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _geometry_to_wkt(geom: Geometry) -> str:
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
            if isinstance(geom.coordinates[0], (list, tuple)):
                coords = _close_ring(list(geom.coordinates))
                pts = [f"{x} {y}" for x, y in coords]
            else:
                raw = [p.strip("() ") for p in geom.coordinates]
                pts = raw
            return f"POLYGON(({', '.join(pts)}))"

        if t == "point":
            if isinstance(geom.coordinates, (list, tuple)):
                x, y = geom.coordinates
            else:
                x, y = geom.coordinates.strip("() ").split()
            return f"POINT({x} {y})"

        if t == "linestring":
            if isinstance(geom.coordinates[0], (list, tuple)):
                pts = [f"{x} {y}" for x, y in geom.coordinates]
            else:
                pts = [p.strip("() ") for p in geom.coordinates]
            return f"LINESTRING({', '.join(pts)})"

        raise ValueError(geom.type)

    def _resolve_geom_arg(self, arg: Any) -> str:
        if isinstance(arg, str) and arg.startswith(self._GEOM_REF_PREFIXES):
            name = arg.split(".", 1)[1]
            geom = self.cfg.geometries[name]
            return f"ST_GeomFromText('{self._geometry_to_wkt(geom)}', {geom.srid})"

        if isinstance(arg, str) and arg.upper().startswith(
            ("POINT", "POLYGON", "LINESTRING", "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION",)
        ):
            return f"ST_GeomFromText('{arg}', 4326)"

        if isinstance(arg, str):
            m = re.match(r"^([a-zA-Z_]\w*\.)?([a-zA-Z_]\w*)$", arg)
            if m:
                alias_part, colname = m.group(1), m.group(2)
                alias = alias_part[:-1] if alias_part else None

                def _schema_for(alias_name: str):
                    src_name, inline_alias = _split_source_alias(self.cfg.source.source)
                    base_alias = inline_alias or src_name.split(".")[-1]
                    if alias_name in (None, base_alias, inline_alias):
                        return self.sd, self.source_type
                    for j in self.cfg.joins:
                        jsrc, jinline = _split_source_alias(j.source)
                        ja = jinline or jsrc.split(".")[-1]
                        if alias_name in (ja, jinline):
                            sd = j.source_data or {}
                            return sd, (sd.get("type", "table") if isinstance(sd, dict) else "table")
                    return None, None

                sd_raw, _stype = _schema_for(alias)
                if sd_raw and isinstance(sd_raw, (SourceData, dict)):
                    schema_list = (
                        [f.__dict__ for f in sd_raw.schema]
                        if isinstance(sd_raw, SourceData)
                        else sd_raw.get("schema", [])
                    )
                    for fld in schema_list:
                        if fld.get("name") == colname and fld.get("type", "string") == "string":
                            if colname != "geom" and ("wkt" in colname.lower() or "traj" in colname.lower()):
                                return f"ST_GeomFromWKT({arg})"

        return str(arg)