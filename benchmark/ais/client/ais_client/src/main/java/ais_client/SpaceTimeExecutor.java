package ais_client;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class SpaceTimeExecutor implements QueryExecutor {
    private final String host;
    private final String org;
    private final String user;
    private final String password;

    public SpaceTimeExecutor(String host, String org, String user, String password) {
        this.host = host;
        this.org = org;
        this.user = user;
        this.password = password;
    }

    @Override
    public void execute(String sql, String queryName) {
        QueryLogger.logStart("SpaceTime", queryName);

        ProcessBuilder pb = new ProcessBuilder(
            "/opt/msql-server/msql-client",
            "-H", host,
            "-o", org,
            "-u", user,
            "-p", password,
            "-v",
            "-q", sql
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
                QueryLogger.logSuccess("SpaceTime", queryName, duration);
            } else {
                QueryLogger.logError("SpaceTime", queryName,
                        new RuntimeException("Non-zero exit code: " + exitCode));
            }

        } catch (Exception e) {
            QueryLogger.logError("SpaceTime", queryName, e);
        }
    }
}
