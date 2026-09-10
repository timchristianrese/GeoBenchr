package benchmark_client;

import java.util.List;

public class BenchmarkClient {
    public static void main(String[] args) throws Exception {
        //File encoding to handle german characters in data
        System.setProperty("file.encoding", "UTF-8");
        int numThreads = Integer.parseInt(System.getenv().getOrDefault("NUM_THREADS", "1"));
        // FIX: every connection below hardcoded the literal string "geobenchr-sut-5" -- pointing
        // this at a different cluster meant editing 5 separate string literals in this file.
        // DialectRegistry.java (the distributed manager/worker entry point) already reads this
        // same SUT_HOST env var for exactly this reason; doing the same here means both entry
        // points vary by the same single env var, no file edits needed either way.
        String sutHost = System.getenv().getOrDefault("SUT_HOST", "geobenchr-sut-5");
        //generate random 5 letter ID for benchmark run
        String runID = java.util.UUID.randomUUID().toString().substring(0, 5);
        
        BenchmarkMarkerLogger benchLogger = new BenchmarkMarkerLogger(runID);
        //Select application
        String application = args.length > 0 ? args[0] : "aviation";
        System.out.println("Starting benchmark for application: " + application + " with " + numThreads + " threads.");

        //Initialize variables
        String yamlPath;
        List<QueryConfig> queries;
        BenchmarkRunner runner;


        //SpaceTime
        yamlPath = application + "/spaceTimeSQL_queries.yaml";
        SpaceTimeExecutor spaceTimeExecutor = new SpaceTimeExecutor(
                sutHost + ":31339",
        "mireo","root","@2e9R]3]=O"
        );
        queries = QueryDispatcher.loadQueries(yamlPath);
        System.out.println("Setup complete. Starting benchmark...");
        runner = new BenchmarkRunner(spaceTimeExecutor, queries, numThreads, "SpaceTime", application, runID);
        benchLogger.logText("SpaceTime Start");
        runner.run();
        benchLogger.logText("SpaceTime End");
    
        

        PostgreSQLExecutor postgresqlExecutor = new PostgreSQLExecutor(
              "jdbc:postgresql://" + sutHost + ":5433/", "postgres", "test", application
        );
        //PostGIS
        yamlPath = application + "/" + "postgisSQL_queries.yaml";
        queries = QueryDispatcher.loadQueries(yamlPath);
        runner = new BenchmarkRunner(postgresqlExecutor, queries, numThreads, "PostGIS", application, runID);
        benchLogger.logText("PostGIS Start");
        runner.run();
        benchLogger.logText("PostGIS End");

        //MobilityDB
        yamlPath = application + "/" + "mobilitydb_queries.yaml";
        queries = QueryDispatcher.loadQueries(yamlPath);
        runner = new BenchmarkRunner(postgresqlExecutor, queries, numThreads, "MobilityDB", application, runID);
        benchLogger.logText("MobilityDB Start");
        runner.run();
        benchLogger.logText("MobilityDB End");

        // //MobilityDB Time Partitioned
        // yamlPath = application + "/" + "mobilitydb_queries_time_partitioned.yaml";
        // queries = QueryDispatcher.loadQueries(yamlPath);
        // runner = new BenchmarkRunner(postgresqlExecutor, queries, numThreads, "MobilityDB_Time_Partitioned", application, runID);
        // runner.run();

        // //MobilityDB Space Partitioned
        // yamlPath = application + "/" + "mobilitydb_queries_space_partitioned.yaml";
        // queries = QueryDispatcher.loadQueries(yamlPath);
        // runner = new BenchmarkRunner(postgresqlExecutor, queries, numThreads, "MobilityDB_Space_Partitioned", application, runID);
        // runner.run();

        //TimescaleDB
        // yamlPath = application + "/" + "tsdb_queries.yaml";
        // queries = QueryDispatcher.loadQueries(yamlPath);
        // runner = new BenchmarkRunner(postgresqlExecutor, queries, numThreads, "TimescaleDB", application, runID);
        // benchLogger.logText("TimescaleDB Start");
        // runner.run();
        // benchLogger.logText("TimescaleDB End");

        
        //DuckDB
        //Need to start the DuckDB Quack server with the following command:#
        //-- in any DuckDB session
        // INSTALL quack;
        // LOAD quack;
        // CALL quack_serve('quack:127.0.0.1:9494', token=>'my-secret-token');
        yamlPath = application + "/" + "duckdb_queries.yaml";
        // FIX: URL must use the "jdbc:quack:" scheme -- that's the prefix the quack-jdbc driver
        // (com.gizmodata.quack.jdbc.sql.QuackDriver) registers and accepts for the remote
        // quack_serve() endpoint above. The standard org.duckdb.DuckDBDriver (also on the
        // classpath, for local/embedded DuckDB files) claims any "jdbc:duckdb:" URL first, so the
        // previous "jdbc:duckdb://host:9494/" URL was silently routed to the wrong driver.
        DuckDBExecutor duckDBExecutor = new DuckDBExecutor(
                "jdbc:quack://" + sutHost + ":9494/", "my-secret-token"
        );
        queries = QueryDispatcher.loadQueries(yamlPath);
        runner = new BenchmarkRunner(duckDBExecutor, queries, numThreads, "DuckDB", application, runID);
        benchLogger.logText("DuckDB Start");
        runner.run();
        benchLogger.logText("DuckDB End");

        //ClickHouse
        yamlPath = application + "/" + "clickhouse_queries.yaml";
        ClickHouseExecutor clickHouseExecutor = new ClickHouseExecutor(
                "jdbc:clickhouse:http://" + sutHost + ":8123/", "default", ""
        );
        queries = QueryDispatcher.loadQueries(yamlPath);
        runner = new BenchmarkRunner(clickHouseExecutor, queries, numThreads, "ClickHouse", application, runID);
        benchLogger.logText("ClickHouse Start");
        runner.run();
        benchLogger.logText("ClickHouse End");

        //Sedona (Not SedonaDB)
        // FIX: was commented out with two bugs that would've kept it broken even if enabled --
        // "spark://geobenchr-sut-5:7077" assumes a standalone Spark master, but roles/spark never
        // starts one (no start-master.sh anywhere in that role); the cluster is YARN-only, matching
        // spark-defaults.conf's "spark.master yarn". And the BenchmarkRunner call was missing the
        // trailing runID argument every other call here passes.
        yamlPath = application + "/" + "sedonaSQL_queries.yaml";
        String sparkMasterUrl = System.getenv().getOrDefault("SPARK_MASTER", "yarn");
        SedonaExecutor sedonaExecutor = new SedonaExecutor(sparkMasterUrl, sutHost);
        queries = QueryDispatcher.loadQueries(yamlPath);
        runner = new BenchmarkRunner(sedonaExecutor, queries, numThreads, "Sedona", application, runID);
        benchLogger.logText("Sedona Start");
        runner.run();
        benchLogger.logText("Sedona End");
        
        //GeoMesa
        // //yamlPath = application + "/" + "geomesaSQL_queries.yaml";
        // GeoMesaExecutor geoMesaExecutor = new GeoMesaExecutor(
        //         "test","server-peter-lan","root","test","example","flight_points",
        // );
        // queries = QueryDispatcher.loadQueries(yamlPath);
        // runner = new BenchmarkRunner(geoMesaExecutor, queries, numThreads, "GeoMesa", application, runID);
        // runner.run();
        
        //QueryExecutor executor = new MockExecutor();

        benchLogger.moveLogFile();
        System.out.println("Benchmark run " + runID + " completed.");
        
    }
}
