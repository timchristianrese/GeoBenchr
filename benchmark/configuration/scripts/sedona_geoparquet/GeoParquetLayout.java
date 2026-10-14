// Spatially partitioned GeoParquet for Sedona (Track C, 2026-10-13), run inside the benchmark client's own
// Sedona session (same jars, Spark settings and table views as SedonaExecutor).
//
// Why this layout: Sedona 1.8.1 prunes GeoParquet for a spatial filter ONLY at the file level. Its optimizer
// rule SpatialFilterPushDownForGeoParquet turns ST_Intersects/ST_Contains/ST_Within/ST_DWithin/
// ST_Distance(..) <= d etc. against a geometry *literal* (constant-folded, so ST_Transform(ST_MakeEnvelope(..))
// is fine) into a GeoParquetSpatialFilter, and GeoParquetFileFormat evaluates it against the bbox in each
// file's "geo" footer metadata; a file whose bbox misses is not read. There is no row-group-level spatial
// pruning, and the bbox covering column ('geoparquet.covering.*') is only written, never used to prune.
// The earlier builds (SORT BY inside each output file, no shuffle) kept every file's rows from one input
// split, i.e. a time range over the whole area: every file's bbox was the whole area, nothing was skipped.
//
// Layout (one rule for every table):
//   1. B time slabs of equal row counts (approx. percentiles of the timestamp),
//   2. inside each slab, K = 64 spatial cells: range partitioning on (slab, Z-order key of the 3857 point),
//      so each output file holds one slab x one compact spatial cell (tight file bbox),
//   3. each file sorted by timestamp and written with small row groups (4 MB), so time filters still
//      skip row groups through the timestamp min/max statistics (timestamps as INT64 TIMESTAMP_MICROS;
//      the old plain exports of ride/crossing/geolife had INT96 timestamps without statistics, so Spark
//      could not skip anything by time there).
//   N = B*K files, N = ceil(plain size / 128 MB) rounded up to a multiple of K.
// Measured on ride_points (1.18e9 rows, 40 executors x 1 core x 2 GB, warm, 52-repetition cycling queries,
// identical results), plain Parquet -> K=16/8 MB row groups -> K=64/4 MB row groups (chosen):
//   SpatialBBox            16.5 s / 17.2 GB read  ->  2.8 s / 1.1 GB    ->  1.2-1.4 s / 0.35-0.5 GB
//   kNN                    16.5 s / 27.0 GB       ->  1.7-2.8 s / 1-2.5 GB -> 0.7-1.6 s / 0.24-1.0 GB
//   spatioTemporalRange    10.0 s / 27.0 GB       ->  0.75 s / 0.1 GB   ->  0.4 s / 0.06-0.08 GB
//   timeInterval            3.1 s /  9.9 GB       ->  0.5 s / 0.06-0.09 GB -> 0.4 s / 0.07-0.11 GB
//   countRideInDistrict    25 s   / 19.5 GB (full scan, spatial join: no pushdown) -> 14 s -> 14 s
//   size: plain 72.3 GB -> 52.0 GB (K=16) -> 39.8 GB (K=64)
// crossing_points (6.0e9 rows), plain -> K=64, 1 run each: SpatialBBox 42-43 s -> 5.3-6.8 s, kNN 39-46 s -> 4.4-5.2 s,
//   spatioTemporalRange 17 s -> 1.5 s, timeAggregation 7.5-8.2 s -> 1.9-2.0 s, HarbourActivityInPeriod 11.7 -> 2.3 s,
//   but crossingsAroundHarbour (full-scan spatial join) 38 s -> 75 s: the plain files' id statistics let the
//   id range filter skip 36 % of the rows, and the GeoParquet reader is not vectorized.
// Built 2026-10-13/14 (build_geoparquet_layout.sh): ride 39.8 GB / 640 files, flight 67.8 GB / 448, geolife
//   34.8 GB / 640, crossing 150.3 GB / 1472. The plain exports these were built from were deleted afterwards;
//   'build' reads hdfs:///geobenchr/<table>.parquet, so a rebuild needs a plain export first.
//
//   java ... GeoParquetLayout build <table> <out hdfs path> [K] [fileMB] [rowGroupMB]
//   java ... GeoParquetLayout bench <query file: name<TAB>sql per line> <reps> <label=path|plain> ...
//   java ... GeoParquetLayout count <table> <path>      row count of the GeoParquet copy vs the plain table
//   java ... GeoParquetLayout verify <path>             every footer: TIMESTAMP_MICROS, min/max statistics, geo bbox
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import static org.apache.spark.sql.functions.*;

public class GeoParquetLayout {
    static final AtomicLong tasks = new AtomicLong(), bytes = new AtomicLong(), recs = new AtomicLong(), dataTasks = new AtomicLong();

    public static void main(String[] args) throws Exception {
        new benchmark_client.SedonaExecutor(System.getenv().getOrDefault("SPARK_MASTER", "yarn"),
                                            System.getenv().getOrDefault("SUT_HOST", "nst-1"));
        SparkSession spark = SparkSession.active();
        spark.sparkContext().addSparkListener(new SparkListener() {
            @Override public void onTaskEnd(SparkListenerTaskEnd e) {
                if (e.taskMetrics() == null) return;
                long b = e.taskMetrics().inputMetrics().bytesRead();
                tasks.incrementAndGet();
                bytes.addAndGet(b);
                recs.addAndGet(e.taskMetrics().inputMetrics().recordsRead());
                if (b > (1 << 20)) dataTasks.incrementAndGet();
            }
        });
        switch (args[0]) {
            case "build": build(spark, args); break;
            case "bench": bench(spark, args); break;
            case "count": count(spark, args); break;
            case "verify": verify(spark, args); break;
            default: throw new IllegalArgumentException(args[0]);
        }
        spark.stop();
    }

    static long dirBytes(SparkSession spark, String path) throws Exception {
        org.apache.hadoop.fs.Path p = new org.apache.hadoop.fs.Path(path);
        return p.getFileSystem(spark.sparkContext().hadoopConfiguration()).getContentSummary(p).getLength();
    }

    static void build(SparkSession spark, String[] a) throws Exception {
        String table = a[1], out = a[2];
        int k = a.length > 3 ? Integer.parseInt(a[3]) : 16;
        long fileMB = a.length > 4 ? Long.parseLong(a[4]) : 128;
        long rgMB = a.length > 5 ? Long.parseLong(a[5]) : 8;
        // PLAIN_BYTES: size of the plain export the file count is derived from, for a rebuild after the plain
        // export was removed (keeps the same number of files as the first build)
        String pb = System.getenv("PLAIN_BYTES");
        long plain = pb != null ? Long.parseLong(pb) : dirBytes(spark, "hdfs:///geobenchr/" + table + ".parquet");
        int n = (int) Math.ceil(plain / (fileMB * 1048576.0));
        int b = Math.max(1, (n + k - 1) / k);
        n = b * k;
        long t0 = System.currentTimeMillis();
        Dataset<Row> df = spark.table(table);
        // ROW_FILTER: rows to keep, e.g. "id NOT BETWEEN 1 AND 20" (2026-10-14: 20 test rides removed from ride_points)
        String rowFilter = System.getenv("ROW_FILTER");
        if (rowFilter != null && !rowFilter.isEmpty()) df = df.where(rowFilter);
        // time slab boundaries: B-1 approximate percentiles of the timestamp (equal row counts per slab)
        Column ts = col("timestamp").cast("long");
        Dataset<Row> withTs = df.withColumn("_ts", ts);
        String slabExpr = "0";
        List<Object> cuts = new java.util.ArrayList<>();
        if (b > 1) {
            StringBuilder fr = new StringBuilder();
            for (int i = 1; i < b; i++) fr.append(i > 1 ? "," : "").append((double) i / b);
            Row q = withTs.selectExpr("approx_percentile(_ts, array(" + fr + "), 10000)").first();
            cuts = q.getList(0);
            slabExpr = "size(filter(array(" + cuts.toString().replaceAll("[\\[\\]]", "") + "), c -> c <= _ts))";
            System.out.println("slab cuts: " + cuts);
        }
        // Z-order (geohash) key of the EPSG:3857 point, scaled linearly into lon/lat range (no reprojection)
        String zExpr = "ST_GeoHash(ST_Point(ST_X(geom) / 20037508.342789244 * 180.0,"
                + " ST_Y(geom) / 20037508.342789244 * 90.0), 12)";
        Dataset<Row> keyed = withTs.withColumn("_slab", expr(slabExpr)).withColumn("_z", expr(zExpr));
        // Large tables are written in chunks of consecutive slabs (one job each, appended to the same
        // directory), which bounds the shuffle's local disk use; every file is still one slab x one cell.
        long chunkGB = Long.parseLong(System.getenv().getOrDefault("CHUNK_GB", "60"));
        int chunks = (int) Math.min(b, Math.max(1, Math.ceil(plain / (chunkGB * 1073741824.0))));
        System.out.println("build " + table + ": plain " + plain / 1048576 + " MB, " + n + " files = " + b + " slabs x " + k
                + " cells, row groups " + rgMB + " MB, " + chunks + " chunk(s)");
        for (int c = 0; c < chunks; c++) {
            int s0 = (int) ((long) b * c / chunks), s1 = (int) ((long) b * (c + 1) / chunks);
            Dataset<Row> part = keyed;
            if (chunks > 1) {
                // slab range, plus the same bounds on the raw timestamp so a table with timestamp statistics
                // only reads the chunk's row groups
                String tsRange = (s0 > 0 ? "timestamp >= timestamp_seconds(" + cuts.get(s0 - 1) + ")" : "true")
                        + " AND " + (s1 < b ? "timestamp < timestamp_seconds(" + cuts.get(s1 - 1) + " + 1)" : "true");
                part = keyed.where("(timestamp IS NULL OR (" + tsRange + ")) AND _slab >= " + s0 + " AND _slab < " + s1);
            }
            Dataset<Row> laid = part.repartitionByRange((s1 - s0) * k, col("_slab"), col("_z"))
                    .sortWithinPartitions(col("timestamp"))
                    .drop("_ts", "_slab", "_z");
            laid.write().mode(c == 0 ? "overwrite" : "append").format("geoparquet")
                    .option("parquet.block.size", String.valueOf(rgMB * 1048576))
                    .save(out);
            System.out.println("chunk " + (c + 1) + "/" + chunks + " (slabs " + s0 + ".." + (s1 - 1) + ") written after "
                    + (System.currentTimeMillis() - t0) / 1000 + " s");
        }
        System.out.println("done in " + (System.currentTimeMillis() - t0) / 1000 + " s, " + dirBytes(spark, out) / 1048576 + " MB");
    }

    static void count(SparkSession spark, String[] a) throws Exception {
        long gp = spark.read().format("geoparquet").load(a[2]).count();
        long pl = spark.read().parquet("hdfs:///geobenchr/" + a[1] + ".parquet").count();
        System.out.println("COUNT " + a[1] + " geoparquet=" + gp + " plain=" + pl + (gp == pl ? " OK" : " MISMATCH"));
    }

    // every file: timestamp stored as INT64 TIMESTAMP(MICROS), min/max statistics in every row group, a "geo" bbox
    static void verify(SparkSession spark, String[] a) throws Exception {
        org.apache.hadoop.conf.Configuration conf = spark.sparkContext().hadoopConfiguration();
        org.apache.hadoop.fs.Path dir = new org.apache.hadoop.fs.Path(a[1]);
        org.apache.hadoop.fs.FileSystem fs = dir.getFileSystem(conf);
        int files = 0, rgs = 0, bad = 0;
        long rows = 0;
        String tsType = null;
        for (org.apache.hadoop.fs.FileStatus st : fs.listStatus(dir)) {
            if (!st.getPath().getName().endsWith(".parquet")) continue;
            try (org.apache.parquet.hadoop.ParquetFileReader r = org.apache.parquet.hadoop.ParquetFileReader.open(
                    org.apache.parquet.hadoop.util.HadoopInputFile.fromStatus(st, conf))) {
                org.apache.parquet.hadoop.metadata.ParquetMetadata m = r.getFooter();
                files++;
                String t = m.getFileMetaData().getSchema().getType("timestamp").toString();
                if (tsType == null) tsType = t;
                boolean ok = t.contains("int64") && t.contains("MICROS") && m.getFileMetaData().getKeyValueMetaData().containsKey("geo");
                for (org.apache.parquet.hadoop.metadata.BlockMetaData bm : m.getBlocks()) {
                    rgs++;
                    rows += bm.getRowCount();
                    for (org.apache.parquet.hadoop.metadata.ColumnChunkMetaData cc : bm.getColumns()) {
                        if (cc.getPath().toDotString().equals("timestamp") && !cc.getStatistics().hasNonNullValue()
                                && cc.getStatistics().getNumNulls() < bm.getRowCount()) ok = false;
                    }
                }
                if (!ok) { bad++; System.out.println("BAD " + st.getPath()); }
            }
        }
        System.out.println("VERIFY " + a[1] + ": " + files + " files, " + rgs + " row groups, " + rows + " rows (footers), timestamp '"
                + tsType + "', files without ts statistics/geo metadata: " + bad);
    }

    static void register(SparkSession spark, String table, String path) {
        Dataset<Row> gp = spark.read().format("geoparquet").load(path);
        if (java.util.Arrays.asList(gp.columns()).contains("geom_bbox")) gp = gp.drop("geom_bbox");
        gp.createOrReplaceTempView(table);
    }

    static void bench(SparkSession spark, String[] a) throws Exception {
        List<String> lines = Files.readAllLines(Paths.get(a[1]));
        int reps = Integer.parseInt(a[2]);
        String table = System.getenv().getOrDefault("BENCH_TABLE", "ride_points");
        Dataset<Row> plainView = spark.table(table);
        for (int v = 3; v < a.length; v++) {
            String label = a[v].substring(0, a[v].indexOf('=')), path = a[v].substring(a[v].indexOf('=') + 1);
            if (path.equals("plain")) plainView.createOrReplaceTempView(table); else register(spark, table, path);
            for (String line : lines) {
                if (line.isBlank() || line.startsWith("#")) continue;
                String name = line.substring(0, line.indexOf('\t')), sql = line.substring(line.indexOf('\t') + 1);
                if (v == 3 || !path.equals("plain")) {
                    String plan = spark.sql(sql).queryExecution().executedPlan().toString();
                    for (String pl : plan.split("\n")) if (pl.contains("FileScan") || pl.contains("Scan ")) System.out.println("PLAN " + label + " " + name + ": " + pl.trim().replaceAll("Location: .*?,", ""));
                }
                for (int r = 1; r <= reps; r++) {
                    spark.sparkContext().listenerBus().waitUntilEmpty();
                    tasks.set(0); bytes.set(0); recs.set(0); dataTasks.set(0);
                    long t0 = System.nanoTime();
                    List<Row> res = spark.sql(sql).collectAsList();
                    double s = (System.nanoTime() - t0) / 1e9;
                    spark.sparkContext().listenerBus().waitUntilEmpty();
                    String first = res.isEmpty() ? "" : res.get(0).toString();
                    System.out.printf("RESULT\t%s\t%s\t%d\t%.2f\t%d\t%d\t%d\t%d\t%d\t%s%n", label, name, r, s, tasks.get(), dataTasks.get(),
                            bytes.get() / 1048576, recs.get(), res.size(), first.length() > 60 ? first.substring(0, 60) : first);
                }
            }
        }
    }
}
