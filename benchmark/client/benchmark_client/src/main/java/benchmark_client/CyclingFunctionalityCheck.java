package benchmark_client;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * Parallel functionality smoke test against the cycling dataset (ride_points) -- runs a small
 * number of REAL queries from each engine's generated query file (not hardcoded probes), one
 * engine per thread, all started together. Answers "does each engine actually execute its own
 * real query set against the current data" rather than measuring performance.
 *
 *   java --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
 *        --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED \
 *        --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
 *        --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
 *        --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
 *        --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED \
 *        --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
 *        -cp benchmark_client-1.0-SNAPSHOT.jar:deps/* benchmark_client.CyclingFunctionalityCheck
 *
 * See FunctionalityCheck.java's matching FIX comment for why the --add-opens flags are required
 * here specifically (without them, Sedona's YARN job never launches -- confirmed live on nst-1).
 */
public class CyclingFunctionalityCheck {
    private static final int QUERIES_PER_ENGINE = 2;
    private static final String RUN_ID = "cyclecheck";

    public static void main(String[] args) throws InterruptedException {
        String sutHost = System.getenv().getOrDefault("SUT_HOST", "geobenchr-sut-5");
        String sutDbPassword = System.getenv().getOrDefault("SUT_DB_PASSWORD", "test");
        String spaceTimePython = System.getenv().getOrDefault(
                "SPACETIME_PYTHON", System.getProperty("user.home") + "/bench_venv/bin/python");
        String spaceTimeScript = System.getenv().getOrDefault(
                "SPACETIME_RUNNER_SCRIPT", System.getProperty("user.home") + "/bench/spacetime_query_runner.py");
        int spaceTimePort = Integer.parseInt(System.getenv().getOrDefault("SPACETIME_PORT", "31340"));
        String sparkMasterUrl = System.getenv().getOrDefault("SPARK_MASTER", "yarn");

        // FIX: constructing every Executor up front in main() before starting any thread meant a
        // single slow-or-failing constructor (Sedona's -- builds a whole SparkSession) blocked or
        // crashed the entire run before the other 5 engines got a chance to start at all
        // (confirmed live: an uncaught exception from `new SedonaExecutor(...)` took down main()
        // with zero output ever written, even for engines that would have succeeded in seconds).
        // Each engine now constructs its own executor inside its own thread, so one engine's
        // failure is contained to that engine.
        Runnable[] jobs = new Runnable[] {
            () -> runEngine("PostGIS",
                    () -> new PostgreSQLExecutor("jdbc:postgresql://" + sutHost + ":5433/", "postgres", sutDbPassword, "check"),
                    "cycling/postgisSQL_queries.yaml"),
            () -> runEngine("MobilityDB",
                    () -> new PostgreSQLExecutor("jdbc:postgresql://" + sutHost + ":5433/", "postgres", sutDbPassword, "check"),
                    "cycling/mobilitydb_queries.yaml"),
            () -> runEngine("DuckDB",
                    () -> new DuckDBExecutor("jdbc:quack://" + sutHost + ":9494/", "my-secret-token"),
                    "cycling/duckdb_queries.yaml"),
            () -> runEngine("ClickHouse",
                    () -> new ClickHouseExecutor("jdbc:clickhouse:http://" + sutHost + ":8123/", "default", ""),
                    "cycling/clickhouse_queries.yaml"),
            () -> runEngine("SpaceTime",
                    () -> new SpaceTimeExecutor(sutHost, spaceTimePort, spaceTimePython, spaceTimeScript),
                    "cycling/spaceTimeSQL_queries.yaml"),
            () -> runEngine("Sedona",
                    () -> new SedonaExecutor(sparkMasterUrl, sutHost),
                    "cycling/sedonaSQL_queries.yaml"),
        };

        CountDownLatch latch = new CountDownLatch(jobs.length);
        for (Runnable job : jobs) {
            Thread t = new Thread(() -> {
                try {
                    job.run();
                } finally {
                    latch.countDown();
                }
            });
            t.start();
        }
        latch.await();
        System.out.println("=== Cycling functionality check complete ===");
    }

    private static final java.util.Set<String> WRITE_TYPES = java.util.Set.of(
            "insert", "bulk_insert", "update_single", "update_mass", "delete_single", "delete_mass");

    @FunctionalInterface
    private interface ExecutorSupplier {
        QueryExecutor get() throws Exception;
    }

    private static void runEngine(String label, ExecutorSupplier supplier, String yamlPath) {
        System.out.println("--- " + label + ": loading " + yamlPath + " ---");
        System.out.flush();
        try {
            QueryExecutor executor = supplier.get();
            List<QueryConfig> all = QueryDispatcher.loadQueries(yamlPath);
            List<QueryConfig> picked = all.stream()
                    .filter(q -> !WRITE_TYPES.contains(q.getType()))
                    .limit(QUERIES_PER_ENGINE)
                    .collect(Collectors.toList());
            if (picked.isEmpty()) {
                System.out.println("[" + label + "] no non-write queries found in " + yamlPath);
                return;
            }
            for (QueryConfig q : picked) {
                executor.execute(q.getSql(), q.getName(), label, "cycling-check", RUN_ID);
                System.out.flush();
            }
        } catch (Exception e) {
            System.out.println("[" + label + "] uncaught exception: " + e);
            e.printStackTrace();
            System.out.flush();
        }
    }
}
