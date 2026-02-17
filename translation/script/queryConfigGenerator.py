from dataclasses import dataclass, field
from typing import List, Any, Optional, Dict, Union

@dataclass
class GeomInfo:
    column: str
    kind: str
    storage: str = "wkt"
    srid: int = 4326

@dataclass
class CsvOptions:
    delimiter: str = ","
    header: bool   = True
    encoding: str  = "UTF-8"
    quote: str     = '"'
    escape: str    = "\\"
    infer_schema: bool = False # if false, then use schema

@dataclass
class SchemaField:
    name: str
    type: str = "string"
    format: Optional[str] = None

@dataclass
class SourceData:
    type: str = "table"
    path: Optional[str] = None
    geometry_column: Optional[str] = None
    csv_options: Optional[CsvOptions] = None
    schema: List[SchemaField] = field(default_factory=list)
    geom: Optional[GeomInfo] = None

@dataclass
class Source:
    source: str
    source_data: SourceData

@dataclass
class Geometry:
    name: str
    type: str
    srid: int
    coordinates: Any

@dataclass
class SpatialFilter:
    type: str
    args: List[Any]

@dataclass
class Filter:
    field: str
    operator: str
    value: Any
    type: str

@dataclass
class TimeFilter:
    field: str
    start: str
    end: str

@dataclass
class Aggregation:
    function: str
    field: str
    alias: Optional[str]

@dataclass
class Join:
    type: str
    source: str
    source_data: Dict
    on_clause: Union[List[Union[Filter, TimeFilter]], Dict[str, Any]]

@dataclass
class OrderBy:
    field: str
    direction: str

@dataclass
class Derived:
    name: str
    source: Source
    select: List[str] = field(default_factory=list)
    joins: List[Join] = field(default_factory=list)
    aggs:  List[Aggregation] = field(default_factory=list)
    filters: Any = field(default_factory=list)
    group_by: List[str] = field(default_factory=list)
    having: Any = None

@dataclass
class QueryConfig:
    name: Optional[str]
    source: Source
    select: List[str] = field(default_factory=list)
    geometries: Dict[str, Geometry] = field(default_factory=dict)
    joins: List[Join] = field(default_factory=list)
    with_: List[Derived] = field(default_factory=list)
    aggs: List[Aggregation] = field(default_factory=list)
    filters: Union[List[Union[Filter, TimeFilter, SpatialFilter]], Dict[str, Any]] = field(default_factory=list)
    having: Union[List[Union[Filter, TimeFilter]], Dict[str, Any], None] = None
    group_by: List[str] = field(default_factory=list)
    order_by: List[OrderBy] = field(default_factory=list)
    limit: Optional[int] = None
    offset: Optional[int] = None

    @staticmethod
    def _parse_filters_spec(raw: Any) -> List[Any]:
        def parse_node(node: Any) -> Any:
            if isinstance(node, (Filter, TimeFilter, SpatialFilter)):
                return node
            
            if isinstance(node, dict):
                if "and" in node:
                    return ["and"] + [parse_node(sub) for sub in node["and"]]

                if "or" in node:
                    return ["or"] + [parse_node(sub) for sub in node["or"]]

                if "not" in node:
                    return ["not"] + [parse_node(node["not"])]

                if "time_filter" in node:
                    tfs = []
                    for tf in node["time_filter"]:
                        start = tf.get("start") if "start" in tf else None
                        end   = tf.get("end")   if "end"   in tf else None
                        tfs.append(TimeFilter(field=tf["field"], start=start, end=end))
                    return tfs[0] if len(tfs) == 1 else ["and"] + tfs

                if "spatial_filter" in node:
                    sfs = []
                    for sf in node["spatial_filter"]:
                        sfs.append(SpatialFilter(type=sf["type"], args=sf.get("args", [])))
                    return sfs[0] if len(sfs) == 1 else ["and"] + sfs

                return Filter(
                    field=node["field"],
                    operator=node["operator"],
                    value=node.get("value"),
                    type=node.get("type")
                )

        if isinstance(raw, list):
            if len(raw) > 1:
                return ["and"] + [parse_node(e) for e in raw]
            else:
                return [parse_node(e) for e in raw]

        if isinstance(raw, dict):
            root = parse_node(raw)
            return root if isinstance(root, list) else ["and", root]

        return []

    @staticmethod
    def _parse_csv_options(raw: Dict[str, Any]) -> CsvOptions:
        return CsvOptions(
            delimiter    = raw.get("delimiter", ","),
            header       = raw.get("header", True),
            encoding     = raw.get("encoding", "UTF-8"),
            quote        = raw.get("quote", '"'),
            escape       = raw.get("escape", "\\"),
            infer_schema = raw.get("infer_schema", False),
        )

    @staticmethod
    def _parse_schema(raw_list: List[Dict[str, Any]]) -> List[SchemaField]:
        return [
            SchemaField(
                name   = f["name"],
                type   = f.get("type", "string"),
                format = f.get("format"),
            )
            for f in raw_list
        ]   

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "QueryConfig":
        name = d.get("name")

        raw_sd   = d.get("source_data", {"type": "table"})
        sd_type  = raw_sd.get("type", "table")

        geom_info: Optional[GeomInfo] = None
        if sd_type == "table" and "geom" in raw_sd and raw_sd["geom"]:
            graw = raw_sd["geom"]
            geom_info = GeomInfo(
                column  = graw["column"],
                kind    = graw["kind"],
                storage = graw.get("storage", "wkt"),
                srid    = graw.get("srid", 4326),
            )

        source_data = SourceData(
            type            = sd_type,
            path            = raw_sd.get("path"),
            geometry_column = raw_sd.get("geometry_column"),
            csv_options     = QueryConfig._parse_csv_options(raw_sd.get("csv_options", {}))
                            if sd_type == "csv" else None,
            schema          = QueryConfig._parse_schema(raw_sd.get("schema", [])),
                            #if sd_type == "csv" else [],
            geom            = geom_info,
        )

        source = Source(source=d["source"], source_data=source_data)

        select = d.get("select", [])

        aggs: List[Aggregation] = [
            Aggregation(
                function=a["function"].upper(),
                field=a["field"],
                alias=a.get("alias")
            )
            for a in d.get("aggregations", [])
        ]

        geometries: Dict[str, Geometry] = {}
        for g in d.get("geometries", []):
            geom = Geometry(
                name        = g["name"],
                type        = g["type"],
                srid        = g.get("srid", 4326),
                coordinates = g["coordinates"]
            )
            geometries[geom.name] = geom

        joins: List[Join] = []
        for j in d.get("joins", []):
            raw_on = j.get("clause", [])
            on_clause = QueryConfig._parse_filters_spec(raw_on)
            joins.append(Join(
                type=j["type"],
                source=j["source"],
                source_data=j.get("source_data"),
                on_clause=on_clause
            ))

        raw_filters = d.get("filter")
        filters_spec = QueryConfig._parse_filters_spec(raw_filters)

        raw_having = d.get("having")
        having_spec = None
        if raw_having is not None:
            having_spec = QueryConfig._parse_filters_spec(raw_having)

        group_by = d.get("group_by", [])

        order_by = []
        for o in d.get("order_by", []):
            order_by.append(OrderBy(field=o["field"], direction=o["direction"]))

        limit = d.get("limit")
        offset = d.get("offset")

        with_list: List[Derived] = []
        for w in d.get("with", []):
            w_dict = {
                "name": w.get("name"),
                "source": w["source"],
                "source_data": w.get("source_data", {"type": "table"}),
                "select": w.get("select", []),
                "joins":  w.get("joins", []),
                "aggregations": w.get("aggregations", []),
                "filter": w.get("filter"),
                "group_by": w.get("group_by", []),
                "having": w.get("having"),
            }
            tmp = QueryConfig.from_dict(w_dict)

            with_list.append(Derived(
                name   = w["name"],
                source = tmp.source,
                select = tmp.select,
                joins  = tmp.joins,
                aggs   = tmp.aggs,
                filters= tmp.filters,
                having = tmp.having,
                group_by = tmp.group_by,
            ))

        return QueryConfig(
            name=name,
            source=source,
            select=select,
            joins=joins,
            aggs=aggs,
            geometries=geometries,
            filters=filters_spec,
            having=having_spec,
            group_by=group_by,
            order_by=order_by,
            limit=limit,
            offset=offset,
            with_=with_list,
        )