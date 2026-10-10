package benchmark_client;
import org.apache.sedona.spark.SedonaContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class SedonaExecutor implements QueryExecutor {

    private static SparkSession sedona;
    private static boolean initialized = false;

    // FIX: the constructor took a masterUrl parameter but never actually used it -- it hardcoded
    // .master("local[*]"), so every query ran against a throwaway local Spark instance instead of
    // the SUT cluster, and separately tried to load a fixed set of CSVs from hardcoded local paths
    // (e.g. "/home/tim/data/aviation/point_NRW*") that don't exist here or correspond to any
    // dataset this project loads. Actually using masterUrl (submit to the real YARN cluster, per
    // roles/spark's spark-defaults.conf) and dropping the unrelated dataset loading makes this
    // executor connect like every other one here -- open a session, run the SQL it's given.
    //
    // resourceManagerHost: submitting to YARN needs to resolve the ResourceManager's address
    // (and, to stage job resources, the HDFS NameNode's) somehow. spark-submit gets this for free
    // by adding $HADOOP_CONF_DIR's yarn-site.xml/core-site.xml to its launch classpath -- but nothing
    // does that for a plain "java -cp ..." process (how this project otherwise runs, e.g. via
    // run_manager.sh/run_worker.sh's `mvn exec:java`), so without it Hadoop's Configuration silently
    // falls back to unreachable defaults and YarnClient.getNewApplication() retries forever (found
    // via a thread dump, stuck retrying that exact RPC). roles/hadoop's core-site.xml.j2/
    // yarn-site.xml.j2 each only ever set one property to the master's hostname
    // (yarn.resourcemanager.hostname / fs.defaultFS, both deriving every other address from it at
    // their default ports) -- setting those two directly means this needs no XML files or
    // classpath setup on whatever machine runs it, just the same hostname every other dialect here
    // already takes as SUT_HOST.
    public SedonaExecutor(String masterUrl, String resourceManagerHost) {
        synchronized (SedonaExecutor.class) {
            if (!initialized) {
                SparkSession.Builder builder = SedonaContext.builder()
                        .master(masterUrl)
                        .appName("benchmark_client-sedona")
                        // SMOKETEST FIX: an explicit "spark.serializer" -> KryoSerializer override
                        // here (removed) was the actual root cause of two distinct crashes hit
                        // during the smoke test -- a broadcast-serialization
                        // InaccessibleObjectException on java.nio.ByteBuffer, and (deeper into the
                        // query) an UnsupportedOperationException trying to Kryo-serialize
                        // Parquet's own PlainValuesDictionary$PlainLongDictionary. Both reproduced
                        // identically whether launched via plain "java -cp" or "spark-submit", since
                        // this line always wins over spark-defaults.conf regardless (loadSparkDefaultsConf()
                        // runs after this). The exact same queries run fine via plain spark-sql,
                        // which never overrides the serializer -- letting Spark use its default here
                        // instead (matching spark-sql) resolved both crashes. If a future change
                        // needs Kryo specifically, this needs additional --add-opens JVM flags for
                        // Java 17+ (java.base/java.nio, java.util, etc.) AND still needs testing
                        // against Parquet's internal dictionary classes, not just re-adding this line.
                        // FIX (attempt): explicit schemas alone didn't avoid the distributed
                        // schema-merge crash (see registerOne's FIX comment) -- Parquet's
                        // ParquetFileFormat still calls mergeSchemasInParallel regardless of a
                        // user-supplied schema in this Spark version. That distributed path is
                        // itself gated by this threshold (default 32): above it, file listing/
                        // schema merge runs as a Spark job across executors (the path that's
                        // crashing); below it, everything happens locally on the driver, no task
                        // closures involved at all. Our point tables have 124-256 part files,
                        // comfortably over the default -- raising this past our largest file count
                        // forces the local-only path unconditionally.
                        .config("spark.sql.sources.parallelPartitionDiscovery.threshold", "10000");
                if ("yarn".equals(masterUrl)) {
                    builder = builder
                            .config("spark.hadoop.yarn.resourcemanager.hostname", resourceManagerHost)
                            .config("spark.hadoop.fs.defaultFS", "hdfs://" + resourceManagerHost + ":9000");
                }
                // FIX: spark-defaults.conf (dynamic allocation, executor sizing computed from real
                // cluster capacity by roles/common, the shuffle service, Sedona's SQL extension,
                // ...) is only ever read by the spark-submit launcher script -- nothing reads it
                // for a plain "java -cp ..." process, which is how this project actually runs
                // (mvn exec:java, or direct java, never spark-submit). Loading it explicitly here
                // means this executor picks up the same tuning a real spark-submit invocation
                // would, rather than silently ignoring all of it.
                builder = loadSparkDefaultsConf(builder);
                if ("yarn".equals(masterUrl)) {
                    // spark-defaults.conf sets deployMode=cluster (the driver runs inside a YARN
                    // container) for actual spark-submit jobs -- but this executor IS the driver,
                    // running embedded in whatever process constructed it, so it must stay client
                    // mode regardless of what the file says. Applied after loading the file so it
                    // always wins.
                    builder = builder.config("spark.submit.deployMode", "client");
                }
                // (2026-10-08) SPARK_CONF="key=value;key=value" overrides spark-defaults.conf for this
                // run (applied last), e.g. executor counts for a benchmark run without editing the
                // cluster-wide defaults.
                String extra = System.getenv().getOrDefault("SPARK_CONF", "").trim();
                if (!extra.isEmpty()) {
                    for (String kv : extra.split(";")) {
                        int eq = kv.indexOf('=');
                        if (eq > 0) {
                            builder = builder.config(kv.substring(0, eq).trim(), kv.substring(eq + 1).trim());
                            System.out.println("SPARK_CONF: " + kv.trim());
                        }
                    }
                }
                SparkSession config = builder.getOrCreate();
                sedona = SedonaContext.create(config);
                registerTables(sedona);
                // untimed warm-up, like the other systems' session warm-up (JdbcSessions)
                sedona.sql("SELECT 1").collectAsList();
                initialized = true;
            }
        }
    }

    // FIX: nothing here ever registered the tables GeoBenchr's query set references by bare name
    // (e.g. "flight_points", "counties") -- every Sedona query failed outright with "table or view
    // not found", since Spark's in-memory catalog (spark.sql.catalogImplementation=in-memory, no
    // Hive metastore) starts empty every session. Originally these were read live via JDBC from
    // the same Postgres instance every other engine reads (confirmed live: every unfiltered query
    // took well over a minute regardless of what the query itself was doing, since every row
    // crossed the JDBC wire on every single request); each table now has its own one-time Parquet
    // export on HDFS (see server/multinode/ansible/roles/sedona's "Export GeoBenchr tables to
    // Parquet" task) that this just reads and registers as a session-scoped temp view -- cheap
    // (a Parquet read, not a live database scan) and needs to happen once per session, matching
    // the no-persistent-metastore design rather than fighting it.
    private static final String[] TABLES = {
        "flight_points", "geolife_points", "crossing_points", "ride_points",
        "counties", "cities", "berlin_districts", "universities", "harbours", "islands"
    };

    // Generic, engine-portable trip-summary tables (one row per trip, start/end point + scalar
    // stats, built by benchmark/configuration/scripts/summary_*.sql) -- distinct from the
    // MobilityDB-native tgeompoint "flights"/"rides"/"crossings" tables, which only MobilityDB
    // understands and which this Sedona/Parquet path never touches. Two geometry columns each
    // (start_location/end_location), unlike TABLES above's single "geom".
    private static final String[] SUMMARY_TABLES = {
        "flights_summary", "rides_summary", "crossings_summary"
    };
    private static final String[] SUMMARY_GEOM_COLUMNS = {"start_location", "end_location"};

    // FIX: reading these Parquet exports with no explicit schema made Spark run its own
    // distributed schema-inference pass (ParquetFileFormat.inferSchema ->
    // SchemaMergeUtils.mergeSchemasInParallel, which parallelizes across the executors that will
    // eventually do the real read) *before* any query of ours ever runs -- confirmed live,
    // 100% reproducible, every single query on every scenario: the driver-serialized task closure
    // for that inference step fails deserializing on the executor with
    // "IllegalStateException: unread block data" the instant the driver runs on a machine outside
    // the nst-1..5 cluster (nst-inserter) rather than on a cluster member -- happens even when the
    // task lands on nst-1's own (newly added) NodeManager, so it's about the DRIVER's process/
    // environment serializing the closure, not which node executes it. Ruled out real jar-version
    // skew first (byte-identical spark-core/hadoop-client-api/hadoop-client-runtime jars between
    // nst-1 and nst-inserter, confirmed via md5sum) -- root cause not fully pinned down (network
    // path between nst-inserter and the cluster is the leading remaining theory), but supplying an
    // explicit schema makes Spark skip inference entirely (DataSource.getOrInferFileFormatSchema
    // short-circuits on userSpecifiedSchema before ever reaching inferSchema), sidestepping the
    // crash regardless of its exact cause. Schemas below were read directly off one real exported
    // Parquet file per table (DuckDB's read_parquet DESCRIBE), not guessed -- table shape is
    // whatever `SELECT *` against the live Postgres table produced, minus the transient
    // partition_col.
    private static org.apache.spark.sql.types.StructType schemaFor(String table) {
        org.apache.spark.sql.types.DataType LONG = org.apache.spark.sql.types.DataTypes.LongType;
        org.apache.spark.sql.types.DataType TS = org.apache.spark.sql.types.DataTypes.TimestampType;
        org.apache.spark.sql.types.DataType BIN = org.apache.spark.sql.types.DataTypes.BinaryType;
        org.apache.spark.sql.types.DataType DBL = org.apache.spark.sql.types.DataTypes.DoubleType;
        org.apache.spark.sql.types.DataType STR = org.apache.spark.sql.types.DataTypes.StringType;
        switch (table) {
            case "flight_points":
                return org.apache.spark.sql.types.DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                    org.apache.spark.sql.types.DataTypes.createStructField("id", LONG, false),
                    org.apache.spark.sql.types.DataTypes.createStructField("timestamp", TS, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("geom", BIN, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("altitude", LONG, true),
                });
            case "ride_points":
                return org.apache.spark.sql.types.DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                    org.apache.spark.sql.types.DataTypes.createStructField("id", LONG, false),
                    org.apache.spark.sql.types.DataTypes.createStructField("rider_id", LONG, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("timestamp", TS, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("geom", BIN, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("line_geom", STR, true),
                });
            case "geolife_points":
                return org.apache.spark.sql.types.DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                    org.apache.spark.sql.types.DataTypes.createStructField("id", LONG, false),
                    org.apache.spark.sql.types.DataTypes.createStructField("timestamp", TS, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("venue_id", LONG, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("venue_type", STR, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("lat", DBL, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("lon", DBL, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("geom", BIN, true),
                });
            case "crossing_points":
                // SMOKETEST FIX: no explicit schema for this one (falls through to the
                // default/null case -> registerOne() lets Spark infer it instead). Confirmed via
                // direct Parquet metadata inspection (DuckDB's parquet_schema()) that this file's
                // "id" column has converted_type=UINT_64 (matching the raw AIS source's
                // `ubigint`) -- unlike every other table's "id" here (plain signed bigint at the
                // source), so forcing LONG (signed int64) on this one specific column mismatches
                // its on-disk unsigned annotation. That mismatch is what crashed every
                // crossing_points query with "UnsupportedOperationException:
                // PlainValuesDictionary$PlainLongDictionary" -- confirmed by ruling out the
                // serializer (separate fix above) and the launch method (identical crash via
                // spark-submit) first. Letting Spark infer the schema itself (as it does
                // correctly via plain spark-sql, which has no explicit-schema override at all)
                // should let it pick a type that actually matches the file's UINT_64 annotation.
                // If mergeSchemasInParallel crashes come back for just this table as a result
                // (see the FIX comment on parallelPartitionDiscovery.threshold above), that's the
                // next thing to try -- not reintroducing this explicit LONG schema.
                return null;
            case "flights_summary":
                return org.apache.spark.sql.types.DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                    org.apache.spark.sql.types.DataTypes.createStructField("id", LONG, false),
                    org.apache.spark.sql.types.DataTypes.createStructField("start_time", TS, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("end_time", TS, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("num_points", LONG, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("start_location", BIN, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("end_location", BIN, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("min_altitude", LONG, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("max_altitude", LONG, true),
                });
            case "rides_summary":
                return org.apache.spark.sql.types.DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                    org.apache.spark.sql.types.DataTypes.createStructField("id", LONG, false),
                    org.apache.spark.sql.types.DataTypes.createStructField("rider_id", LONG, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("start_time", TS, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("end_time", TS, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("num_points", LONG, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("start_location", BIN, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("end_location", BIN, true),
                });
            case "crossings_summary":
                return org.apache.spark.sql.types.DataTypes.createStructType(new org.apache.spark.sql.types.StructField[]{
                    org.apache.spark.sql.types.DataTypes.createStructField("id", LONG, false),
                    org.apache.spark.sql.types.DataTypes.createStructField("start_time", TS, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("end_time", TS, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("num_points", LONG, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("start_location", BIN, true),
                    org.apache.spark.sql.types.DataTypes.createStructField("end_location", BIN, true),
                });
            default:
                return null;
        }
    }

    private static void registerTables(SparkSession sedona) {
        for (String table : TABLES) {
            registerOne(sedona, table, new String[]{"geom"});
        }
        for (String table : SUMMARY_TABLES) {
            registerOne(sedona, table, SUMMARY_GEOM_COLUMNS);
        }
    }

    // (2026-10-08) SEDONA_GEOPARQUET="flight_points,ride_points,..." reads those tables from their
    // spatially sorted GeoParquet copies (hdfs:///geobenchr/<table>.geoparquet, built by
    // configuration/scripts/sedona_geoparquet/build_geoparquet.sh) instead of the plain-Parquet WKB
    // export: real geometries plus a bounding-box covering column, so Sedona can skip files and row
    // groups that miss a spatial filter. The covering column is hidden from the view.
    private static final java.util.Set<String> GEOPARQUET = new java.util.HashSet<>(java.util.Arrays.asList(
            System.getenv().getOrDefault("SEDONA_GEOPARQUET", "").replace(" ", "").split(",")));

    private static void registerOne(SparkSession sedona, String table, String[] geomColumns) {
        if (GEOPARQUET.contains(table)) {
            Dataset<Row> gp = sedona.read().format("geoparquet").load("hdfs:///geobenchr/" + table + ".geoparquet");
            if (java.util.Arrays.asList(gp.columns()).contains("geom_bbox")) {
                gp = gp.drop("geom_bbox");
            }
            gp.createOrReplaceTempView(table);
            System.out.println("Sedona: " + table + " from GeoParquet");
            return;
        }
        org.apache.spark.sql.types.StructType schema = schemaFor(table);
        org.apache.spark.sql.DataFrameReader reader = sedona.read();
        if (schema != null) {
            reader = reader.schema(schema);
        }
        Dataset<Row> df = reader.parquet("hdfs:///geobenchr/" + table + ".parquet");
        // geom is stored as plain WKB bytes (see the export script's own comment for why:
        // sidesteps any GeoParquet-specific write/read version mismatch between the Sedona
        // build that exported it and the one reading it back here) -- turn it back into a
        // real Sedona Geometry before queries can use ST_Distance/ST_Intersects/etc. on it.
        for (String col : geomColumns) {
            df = df.withColumn(col, org.apache.spark.sql.functions.expr("ST_GeomFromWKB(" + col + ")"));
        }
        df.createOrReplaceTempView(table);
    }

    private static SparkSession.Builder loadSparkDefaultsConf(SparkSession.Builder builder) {
        String sparkHome = System.getenv().getOrDefault("SPARK_HOME", "/opt/spark");
        File confFile = new File(sparkHome, "conf/spark-defaults.conf");
        if (!confFile.isFile()) {
            return builder;
        }
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(confFile)) {
            props.load(in);
        } catch (IOException e) {
            System.err.println("Could not read " + confFile + ": " + e.getMessage());
            return builder;
        }
        for (String key : props.stringPropertyNames()) {
            builder = builder.config(key, props.getProperty(key));
        }
        return builder;
    }

    @Override
    public void execute(String sql, String queryName, String dbType, String application, String runID) {
        if (sedona == null) {
            System.err.println("Sedona context not initialized.");
            return;
        }

        QueryLogger.logStart("Sedona", application, queryName, runID);
        try {
            // FIX (2026-10-07): the timer used to stop right after sedona.sql(sql), which only parses
            // and plans -- Spark executes lazily, so the query itself ran afterwards, untimed, inside
            // result.count(). Now timed like every other system: until every result row has reached
            // the client (collected to the driver; count() alone would also let Spark prune columns).
            long start = System.currentTimeMillis();
            Dataset<Row> result = sedona.sql(sql);
            // (2026-10-08) geometry columns go to the client as WKB, like PostGIS sends them: collecting
            // Sedona's geometry type itself fails on the driver ("EXPRESSION_DECODING_FAILED ...
            // GeometryUDT.deserialize"), which broke every query returning a geometry (kNN, ...).
            for (org.apache.spark.sql.types.StructField f : result.schema().fields()) {
                if (f.dataType() instanceof org.apache.spark.sql.sedona_sql.UDT.GeometryUDT) {
                    result = result.withColumn(f.name(), org.apache.spark.sql.functions.expr("ST_AsBinary(`" + f.name() + "`)"));
                }
            }
            java.util.List<Row> rows = result.collectAsList();
            long count = rows.size();
            long duration = System.currentTimeMillis() - start;
            System.out.println("Rows returned: " + count);
            writeResult(application, queryName, rows, null);

            QueryLogger.logSuccess("Sedona", application, queryName, runID, duration);
        } catch (Exception e) {
            QueryLogger.logError("Sedona", application, queryName, runID, e);
            writeResult(application, queryName, null, e);
        }
    }

    /** Cancels the running Spark jobs; collectAsList() then fails and execute() returns. */
    @Override
    public void cancel() {
        if (sedona != null) {
            sedona.sparkContext().cancelAllJobs();
        }
    }

    // (2026-10-08) With RESULTS_OUT=<file>, every query's result is appended there as one JSON line
    // ({"key": "<scenario>/<query>", "dialect": "sedona", "rc", "count", "rows" (first 2001), "err"}),
    // the format of the cross-system equivalence check (configuration/scripts/pipeline/compare_all.py).
    // Off by default; never part of a timing.
    private static final com.fasterxml.jackson.databind.ObjectMapper JSON = new com.fasterxml.jackson.databind.ObjectMapper();

    private static synchronized void writeResult(String application, String queryName, java.util.List<Row> rows, Exception error) {
        String out = System.getenv("RESULTS_OUT");
        if (out == null || out.isEmpty()) {
            return;
        }
        java.util.Map<String, Object> rec = new java.util.LinkedHashMap<>();
        rec.put("key", application + "/" + queryName);
        rec.put("dialect", "sedona");
        rec.put("rc", error == null ? 0 : 1);
        rec.put("err", error == null ? "" : String.valueOf(error.getMessage()));
        if (rows != null) {
            rec.put("count", rows.size());
            java.util.List<java.util.List<String>> values = new java.util.ArrayList<>();
            for (Row r : rows.subList(0, Math.min(rows.size(), 2001))) {
                java.util.List<String> cells = new java.util.ArrayList<>();
                for (int i = 0; i < r.length(); i++) {
                    Object v = r.isNullAt(i) ? null : r.get(i);
                    // timestamps in UTC (the session zone of the benchmark runs), not the JVM's default zone
                    cells.add(v == null ? "" : v instanceof java.sql.Timestamp
                            ? ((java.sql.Timestamp) v).toInstant().toString().replace('T', ' ').replace("Z", "")
                            : String.valueOf(v));
                }
                values.add(cells);
            }
            rec.put("rows", values);
        }
        try (java.io.FileWriter fw = new java.io.FileWriter(out, true)) {
            fw.write(JSON.writeValueAsString(rec) + "\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
