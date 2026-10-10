package benchmark_client.distributed;

import benchmark_client.ClickHouseExecutor;
import benchmark_client.DuckDBExecutor;
import benchmark_client.PostgreSQLExecutor;
import benchmark_client.QueryExecutor;
import benchmark_client.SedonaExecutor;
import benchmark_client.SpaceTimeExecutor;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shared dialect -> (query yaml filename, QueryExecutor) mapping used by the manager/worker
 * processes. Connection defaults mirror the hardcoded values in BenchmarkClient.java (the
 * single-machine entry point, left untouched); the hostname can be overridden with the SUT_HOST
 * env var so a distributed run can target a different cluster without editing source, matching
 * the existing NUM_THREADS env-var convention.
 */
public class DialectRegistry {

    public static final String[] DEFAULT_DIALECTS = {
        "SpaceTime", "PostGIS", "MobilityDB", "DuckDB", "ClickHouse", "Sedona"
    };

    private static final String SUT_HOST = System.getenv().getOrDefault("SUT_HOST", "geobenchr-sut-5");
    // See BenchmarkClient.java's matching FIX comment: "test" is right for the single-box
    // "geobenchr-sut-5" setup, wrong for this partner cluster (real password: "postgres").
    private static final String SUT_DB_PASSWORD = System.getenv().getOrDefault("SUT_DB_PASSWORD", "test");

    // See BenchmarkClient.java's matching FIX comment: SpaceTime needs no auth, its real port is
    // 31340 (not 31339), and its only client is the Python-only `pymsql` library shelled out to
    // via scripts/spacetime_query_runner.py -- not the nonexistent CLI binary this used to assume.
    private static final int SPACETIME_PORT = Integer.parseInt(System.getenv().getOrDefault("SPACETIME_PORT", "31340"));
    private static final String SPACETIME_PYTHON = System.getenv().getOrDefault(
            "SPACETIME_PYTHON", System.getProperty("user.home") + "/bench/.venv/bin/python");
    private static final String SPACETIME_RUNNER_SCRIPT = System.getenv().getOrDefault(
            "SPACETIME_RUNNER_SCRIPT", System.getProperty("user.home") + "/bench/spacetime_query_runner.py");

    // Unlike the JDBC dialects above (a plain host:port), submitting to Sedona's cluster means
    // acting as a YARN client, matching roles/spark's spark-defaults.conf (spark.master yarn).
    // SedonaExecutor resolves the ResourceManager/NameNode addresses from SUT_HOST directly (see
    // its own comment for why -- HADOOP_CONF_DIR alone isn't enough here). Override SPARK_MASTER
    // for e.g. local[*] testing off-cluster.
    private static final String SPARK_MASTER = System.getenv().getOrDefault("SPARK_MASTER", "yarn");

    private static final Map<String, String> YAML_FILENAMES = new LinkedHashMap<>();
    static {
        YAML_FILENAMES.put("SpaceTime", "spaceTimeSQL_queries.yaml");
        YAML_FILENAMES.put("PostGIS", "postgisSQL_queries.yaml");
        YAML_FILENAMES.put("MobilityDB", "mobilitydb_queries.yaml");
        YAML_FILENAMES.put("DuckDB", "duckdb_queries.yaml");
        YAML_FILENAMES.put("ClickHouse", "clickhouse_queries.yaml");
        YAML_FILENAMES.put("Sedona", "sedonaSQL_queries.yaml");
    }

    private DialectRegistry() {}

    public static Map<String, String> yamlFilenames() {
        return Collections.unmodifiableMap(YAML_FILENAMES);
    }

    public static boolean isKnownDialect(String dialect) {
        return YAML_FILENAMES.containsKey(dialect);
    }

    public static String yamlFilenameFor(String dialect) {
        String filename = YAML_FILENAMES.get(dialect);
        if (filename == null) {
            throw new IllegalArgumentException("Unknown dialect \"" + dialect + "\" (known: " + YAML_FILENAMES.keySet() + ")");
        }
        return filename;
    }

    /** application is only threaded through for parity with PostgreSQLExecutor's constructor signature. */
    public static QueryExecutor createExecutor(String dialect, String application) {
        switch (dialect) {
            case "SpaceTime":
                return new SpaceTimeExecutor(SUT_HOST, SPACETIME_PORT, SPACETIME_PYTHON, SPACETIME_RUNNER_SCRIPT);
            case "PostGIS":
            case "MobilityDB":
                return new PostgreSQLExecutor("jdbc:postgresql://" + SUT_HOST + ":5433/", "postgres", SUT_DB_PASSWORD, application);
            case "DuckDB":
                return new DuckDBExecutor("jdbc:quack://" + SUT_HOST + ":9494/", "my-secret-token");
            case "ClickHouse":
                return new ClickHouseExecutor("jdbc:clickhouse:http://" + SUT_HOST + ":8123/", "default", "");
            case "Sedona":
                return new SedonaExecutor(SPARK_MASTER, SUT_HOST);
            default:
                throw new IllegalArgumentException("Unknown dialect \"" + dialect + "\" (known: " + YAML_FILENAMES.keySet() + ")");
        }
    }
}
