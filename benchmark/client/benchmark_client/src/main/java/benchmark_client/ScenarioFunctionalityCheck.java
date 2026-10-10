package benchmark_client;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * Parallel functionality smoke test against a given scenario's dataset -- runs a small number of
 * REAL queries from each engine's generated query file (not hardcoded probes), one engine per
 * thread, all started together. Answers "does each engine actually execute its own real query set
 * against the current data" rather than measuring performance. Generalization of
 * CyclingFunctionalityCheck.java (same pattern) parameterized by scenario name so it works for
 * general/aviation/cycling/ais/geolife without duplicating a class per scenario.
 *
 *   java --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
 *        --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED \
 *        --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
 *        --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
 *        --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
 *        --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED \
 *        --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
 *        -cp benchmark_client-1.0-SNAPSHOT.jar:deps/* benchmark_client.ScenarioFunctionalityCheck <scenario>
 *
 * See FunctionalityCheck.java's matching FIX comment for why the --add-opens flags are required
 * here specifically (without them, Sedona's YARN job never launches -- confirmed live on nst-1).
 */
public class ScenarioFunctionalityCheck {
    private static final int QUERIES_PER_ENGINE = 1;

    public static void main(String[] args) throws InterruptedException {
        String scenario = args.length > 0 ? args[0] : "cycling";
        String runId = scenario + "check";
        // Optional 2nd arg: comma-separated engine labels to run (default: all 6). Lets a single
        // engine (e.g. Sedona) be retested without waiting on a slow one (e.g. MobilityDB) again.
        java.util.Set<String> only = args.length > 1
                ? java.util.Arrays.stream(args[1].split(",")).collect(Collectors.toSet())
                : null;

        String sutHost = System.getenv().getOrDefault("SUT_HOST", "geobenchr-sut-5");
        String sutDbPassword = System.getenv().getOrDefault("SUT_DB_PASSWORD", "test");
        String spaceTimePython = System.getenv().getOrDefault(
                "SPACETIME_PYTHON", System.getProperty("user.home") + "/bench_venv/bin/python");
        String spaceTimeScript = System.getenv().getOrDefault(
                "SPACETIME_RUNNER_SCRIPT", System.getProperty("user.home") + "/bench/spacetime_query_runner.py");
        int spaceTimePort = Integer.parseInt(System.getenv().getOrDefault("SPACETIME_PORT", "31340"));
        String sparkMasterUrl = System.getenv().getOrDefault("SPARK_MASTER", "yarn");

        java.util.LinkedHashMap<String, Runnable> allJobs = new java.util.LinkedHashMap<>();
        allJobs.put("PostGIS", () -> runEngine("PostGIS",
                    () -> new PostgreSQLExecutor("jdbc:postgresql://" + sutHost + ":5433/", "postgres", sutDbPassword, "check"),
                    scenario + "/postgisSQL_queries.yaml", runId));
        allJobs.put("MobilityDB", () -> runEngine("MobilityDB",
                    () -> new PostgreSQLExecutor("jdbc:postgresql://" + sutHost + ":5433/", "postgres", sutDbPassword, "check"),
                    scenario + "/mobilitydb_queries.yaml", runId));
        allJobs.put("DuckDB", () -> runEngine("DuckDB",
                    () -> new DuckDBExecutor("jdbc:quack://" + sutHost + ":9494/", "my-secret-token"),
                    scenario + "/duckdb_queries.yaml", runId));
        allJobs.put("ClickHouse", () -> runEngine("ClickHouse",
                    () -> new ClickHouseExecutor("jdbc:clickhouse:http://" + sutHost + ":8123/", "default", ""),
                    scenario + "/clickhouse_queries.yaml", runId));
        allJobs.put("SpaceTime", () -> runEngine("SpaceTime",
                    () -> new SpaceTimeExecutor(sutHost, spaceTimePort, spaceTimePython, spaceTimeScript),
                    scenario + "/spaceTimeSQL_queries.yaml", runId));
        allJobs.put("Sedona", () -> runEngine("Sedona",
                    () -> new SedonaExecutor(sparkMasterUrl, sutHost),
                    scenario + "/sedonaSQL_queries.yaml", runId));

        List<Runnable> jobs = allJobs.entrySet().stream()
                .filter(e -> only == null || only.contains(e.getKey()))
                .map(java.util.Map.Entry::getValue)
                .collect(Collectors.toList());

        CountDownLatch latch = new CountDownLatch(jobs.size());
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
        System.out.println("=== " + scenario + " functionality check complete ===");
    }

    private static final java.util.Set<String> WRITE_TYPES = java.util.Set.of(
            "insert", "bulk_insert", "update_single", "update_mass", "delete_single", "delete_mass");

    @FunctionalInterface
    private interface ExecutorSupplier {
        QueryExecutor get() throws Exception;
    }

    private static void runEngine(String label, ExecutorSupplier supplier, String yamlPath, String runId) {
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
                executor.execute(q.getSql(), q.getName(), label, runId + "-check", runId);
                System.out.flush();
            }
        } catch (Exception e) {
            System.out.println("[" + label + "] uncaught exception: " + e);
            e.printStackTrace();
            System.out.flush();
        }
    }
}
