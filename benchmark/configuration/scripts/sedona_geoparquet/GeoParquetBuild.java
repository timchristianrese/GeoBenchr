// Runs one Spark SQL statement in the benchmark client's own Sedona session (same jars, Spark settings and
// registered table views as SedonaExecutor), e.g. to write GeoParquet. The cluster's spark-sql shell cannot
// initialise this Sedona build ("NoClassDefFoundError: FoldableUnevaluable"), the client's session can.
//   java -cp <this dir>:<benchmark jar>:<deps>/* GeoParquetBuild "<statement>"
public class GeoParquetBuild {
    public static void main(String[] args) throws Exception {
        new benchmark_client.SedonaExecutor(System.getenv().getOrDefault("SPARK_MASTER", "yarn"),
                                            System.getenv().getOrDefault("SUT_HOST", "nst-1"));
        org.apache.spark.sql.SparkSession spark = org.apache.spark.sql.SparkSession.active();
        long t0 = System.currentTimeMillis();
        spark.sql(args[0]).collectAsList();
        System.out.println("done in " + (System.currentTimeMillis() - t0) / 1000 + " s");
        spark.stop();
    }
}
