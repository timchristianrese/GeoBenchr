package benchmark_client;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class GeoMesaExecutor implements QueryExecutor {
    private final String zookeeper;
    private final String user;
    private final String password;
    private final String catalog;
    private final String featureName;
    private final String accumuloInstance;


    public GeoMesaExecutor(String accumuloInstance, String zookeeper, String user, String password, String catalog, String featureName) {
        this.accumuloInstance = accumuloInstance;
        this.zookeeper = zookeeper;
        this.user = user;
        this.password = password;
        this.catalog = catalog;
        this.featureName = featureName;
    }

    @Override
    public void execute(String sql, String queryName, String dbType, String application, String runID) {
        QueryLogger.logStart("GeoMesa", application, queryName, runID);

        ProcessBuilder pb = new ProcessBuilder(
            "/opt/geomesa-accumulo/bin/geomesa-accumulo",
            "export",
            "-i", accumuloInstance,
            "-z", zookeeper,
            "-u", user,
            "-p", password,
            "-c", catalog,
            "-f", featureName,
            "-q", "INCLUDE", 
            "-m", "10"
        );

        long start = System.currentTimeMillis();

        try {
            Process process = pb.start();

            // capture stdout
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    // optionally process/parse result here
                    // System.out.println(line);
                }
            }

            // capture stderr
            try (BufferedReader errorReader = new BufferedReader(
                    new InputStreamReader(process.getErrorStream()))) {
                String line;
                while ((line = errorReader.readLine()) != null) {
                    System.err.println(line);
                }
            }

            int exitCode = process.waitFor();

            long duration = System.currentTimeMillis() - start;

            if (exitCode == 0) {
                QueryLogger.logSuccess("GeoMesa", application, queryName, runID, duration);
            } else {
                QueryLogger.logError("GeoMesa", application, queryName, runID,
                        new RuntimeException("Non-zero exit code: " + exitCode));
            }

        } catch (Exception e) {
            QueryLogger.logError("GeoMesa", application, queryName, runID, e);
        }
    }
}
