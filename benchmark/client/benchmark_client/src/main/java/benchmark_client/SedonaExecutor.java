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
                        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
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
                SparkSession config = builder.getOrCreate();
                sedona = SedonaContext.create(config);
                initialized = true;
            }
        }
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
            long start = System.currentTimeMillis();
            Dataset<Row> result = sedona.sql(sql);
            long duration = System.currentTimeMillis() - start;

            long count = result.count();
            System.out.println("Rows returned: " + count);

            QueryLogger.logSuccess("Sedona", application, queryName, runID, duration);
        } catch (Exception e) {
            QueryLogger.logError("Sedona", application, queryName, runID, e);
        }
    }
}
