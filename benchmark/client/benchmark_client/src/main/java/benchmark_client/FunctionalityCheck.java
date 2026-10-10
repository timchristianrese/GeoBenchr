package benchmark_client;

/**
 * Mini functionality smoke test -- one lightweight query per Executor, independent of the full
 * YAML-driven benchmark pipeline (repetition counts, parameter generation, etc.). Answers "does
 * this Executor actually connect and execute" rather than measuring performance. Run with:
 *
 *   java --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
 *        --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED \
 *        --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
 *        --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
 *        --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
 *        --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED \
 *        --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
 *        -cp benchmark_client-1.0-SNAPSHOT.jar:deps/* benchmark_client.FunctionalityCheck
 *
 * Reads the same SUT_HOST/SUT_DB_PASSWORD/SPACETIME_* env vars as BenchmarkClient.java.
 *
 * FIX: without those --add-opens flags, SedonaExecutor's constructor builds and registers tables
 * fine, but the first actual Spark job (even just Parquet schema inference) throws
 * InaccessibleObjectException from Kryo reflectively touching java.nio.ByteBuffer's private
 * fields, and the executor thread dies before ever contacting YARN -- so no application ever
 * shows up in `yarn application -list` ("Sedona doesn't launch a YARN job"). pom.xml's
 * exec-maven-plugin config already carries this exact flag set for `mvn exec:java` runs; a plain
 * `java -cp` invocation (e.g. from a deps/ dir copied straight onto a cluster node, without the
 * Maven project) needs it spelled out explicitly since nothing else supplies it. Confirmed live
 * on nst-1: identical `java -cp ...` runs against the real 5-node cluster failed with that
 * InaccessibleObjectException and created no YARN application both with and without
 * /opt/hadoop/etc/hadoop on the classpath; adding these flags made `yarn application -list`
 * show a real `benchmark_client-sedona` application that reached SUCCEEDED.
 */
public class FunctionalityCheck {
    public static void main(String[] args) {
        String sutHost = System.getenv().getOrDefault("SUT_HOST", "geobenchr-sut-5");
        String sutDbPassword = System.getenv().getOrDefault("SUT_DB_PASSWORD", "test");
        String runID = "check";

        // SpaceTime
        check("SpaceTime", () -> {
            String spaceTimePython = System.getenv().getOrDefault(
                    "SPACETIME_PYTHON", System.getProperty("user.home") + "/bench/.venv/bin/python");
            String spaceTimeScript = System.getenv().getOrDefault(
                    "SPACETIME_RUNNER_SCRIPT", System.getProperty("user.home") + "/bench/spacetime_query_runner.py");
            int spaceTimePort = Integer.parseInt(System.getenv().getOrDefault("SPACETIME_PORT", "31340"));
            SpaceTimeExecutor executor = new SpaceTimeExecutor(sutHost, spaceTimePort, spaceTimePython, spaceTimeScript);
            executor.execute("SELECT COUNT(*) FROM rides", "functionalityCheck", "SpaceTime", "check", runID);
        });

        PostgreSQLExecutor postgresqlExecutor = new PostgreSQLExecutor(
                "jdbc:postgresql://" + sutHost + ":5433/", "postgres", sutDbPassword, "check");

        // PostGIS
        check("PostGIS", () ->
                postgresqlExecutor.execute("SELECT 1", "functionalityCheck", "PostGIS", "check", runID));

        // MobilityDB
        check("MobilityDB", () ->
                postgresqlExecutor.execute("SELECT COUNT(*) FROM flights", "functionalityCheck", "MobilityDB", "check", runID));

        // DuckDB
        check("DuckDB", () -> {
            DuckDBExecutor executor = new DuckDBExecutor("jdbc:quack://" + sutHost + ":9494/", "my-secret-token");
            executor.execute("SELECT 1", "functionalityCheck", "DuckDB", "check", runID);
        });

        // ClickHouse
        check("ClickHouse", () -> {
            ClickHouseExecutor executor = new ClickHouseExecutor("jdbc:clickhouse:http://" + sutHost + ":8123/", "default", "");
            executor.execute("SELECT 1", "functionalityCheck", "ClickHouse", "check", runID);
        });

        // Sedona
        check("Sedona", () -> {
            String sparkMasterUrl = System.getenv().getOrDefault("SPARK_MASTER", "yarn");
            SedonaExecutor executor = new SedonaExecutor(sparkMasterUrl, sutHost);
            executor.execute("SELECT COUNT(*) FROM flight_points LIMIT 1", "functionalityCheck", "Sedona", "check", runID);
        });

        System.out.println("Functionality check complete.");
    }

    private interface CheckedRunnable {
        void run() throws Exception;
    }

    private static void check(String name, CheckedRunnable action) {
        System.out.println("=== " + name + " ===");
        try {
            action.run();
        } catch (Exception e) {
            System.out.println("[" + name + "] uncaught exception: " + e);
            e.printStackTrace();
        }
    }
}
