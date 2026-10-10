package benchmark_client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;


public class DuckDBExecutor implements QueryExecutor {
    // The quack driver gives up on any HTTP request after 60 s by default ("request timed out"),
    // far below BenchmarkRunner's 600 s per-query limit, so slower DuckDB queries failed where every
    // other system was allowed to finish. Raised just above the runner's limit so that one applies.
    static final String REQUEST_TIMEOUT_SECONDS =
            String.valueOf(BenchmarkRunner.DEFAULT_TIMEOUT_SECONDS + 60);

    private final JdbcSessions sessions;
    // (2026-10-09) quack-jdbc's Statement.cancel() returns but the query keeps running on the server
    // (checked: a cancelled query went on at 100 % CPU), and DuckDB has no statement timeout. The
    // only way to stop a query is to restart the server: DUCKDB_CANCEL_CMD is a shell command that
    // does that and returns once the server answers again (realbench/svc.sh restart-duckdb).
    private final String cancelCommand = System.getenv("DUCKDB_CANCEL_CMD");

    public DuckDBExecutor(String url, String token) {
        Properties props = new Properties();
        props.setProperty("token", token);
        props.setProperty("requestTimeout", REQUEST_TIMEOUT_SECONDS);
        // (2026-10-07) sessions are reused across queries, opened outside the measurement -- see JdbcSessions
        this.sessions = new JdbcSessions(() -> DriverManager.getConnection(url, props));
    }

    @Override
    public void execute(String sql, String queryName, String dbType, String application, String runID) {
        QueryLogger.logStart(dbType, application, queryName, runID);
        Connection conn;
        try {
            conn = sessions.borrow();
        } catch (Exception e) {
            QueryLogger.logError(dbType, application, queryName, runID, e);
            return;
        }
        long start = System.currentTimeMillis();
        boolean ok = false;
        PreparedStatement registered = null;
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            sessions.started(stmt);
            registered = stmt;

            // read queries return a ResultSet; write queries (insert/update/delete) don't -- branch
            // on execute()'s return value instead of assuming executeQuery(), which throws for writes.
            boolean hasResultSet = stmt.execute();
            int count = 0;
            if (hasResultSet) {
                try (ResultSet rs = stmt.getResultSet()) {
                    int columnCount = rs.getMetaData().getColumnCount();
                    while (rs.next()) {
                        count++;
                        for (int i = 1; i <= columnCount; i++) {
                            System.out.print(rs.getString(i) + " ");
                        }
                        System.out.println();
                    }
                }
            } else {
                count = stmt.getUpdateCount();
            }

            System.out.println("Rows returned/affected: " + count);

            long duration = System.currentTimeMillis() - start;
            QueryLogger.logSuccess(dbType, application, queryName, runID, duration);
            ok = true;

        } catch (Exception e) {
            QueryLogger.logError(dbType, application, queryName, runID, e);
        } finally {
            if (registered != null) {
                sessions.finished(registered);
            }
            if (ok) {
                sessions.giveBack(conn);
            } else {
                sessions.discard(conn);
            }
        }
    }

    @Override
    public void cancel() {
        sessions.cancelRunning();
        if (cancelCommand == null || cancelCommand.isBlank()) {
            System.err.println("DUCKDB_CANCEL_CMD not set: the timed-out query keeps running on the server");
            return;
        }
        try {
            Process p = new ProcessBuilder("/bin/sh", "-c", cancelCommand).inheritIO().start();
            if (!p.waitFor(10, java.util.concurrent.TimeUnit.MINUTES)) {
                p.destroyForcibly();
                System.err.println("DUCKDB_CANCEL_CMD did not finish within 10 minutes");
            } else {
                System.err.println("DUCKDB_CANCEL_CMD exited with " + p.exitValue());
            }
        } catch (Exception e) {
            System.err.println("DUCKDB_CANCEL_CMD failed: " + e);
        }
    }
}
