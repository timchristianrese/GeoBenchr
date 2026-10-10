package benchmark_client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class ClickHouseExecutor implements QueryExecutor {
    private final JdbcSessions sessions;

    public ClickHouseExecutor(String url, String user, String password) {
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        // (2026-10-09) server-side backstop for the runner's timeout, which cancels the query itself
        props.setProperty("clickhouse_setting_max_execution_time", String.valueOf(BenchmarkRunner.DEFAULT_TIMEOUT_SECONDS + 30));
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
        try {
            // FIX: some ClickHouse write queries (e.g. updateflightPointMass/Single and its
            // ride/crossing/geolife equivalents) genuinely need two statements -- ClickHouse forbids UPDATE on
            // any column a MATERIALIZED key column (h3) derives from, so "moving" a point has to
            // be a DELETE followed by a re-INSERT at the new location, and ClickHouse has no
            // single-statement construct for that. This used to hand the whole `sql` string
            // (semicolon and all) to one PreparedStatement, which the ClickHouse JDBC driver
            // just runs as one statement -- anything after the first ";" was silently never
            // executed. Splitting on ";" and running each statement in turn over the same
            // connection covers both the old single-statement queries (split produces exactly
            // one non-empty part) and these new two-statement ones.
            int count = 0;
            for (String statement : sql.split(";")) {
                String trimmed = statement.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                try (PreparedStatement stmt = conn.prepareStatement(trimmed)) {
                    sessions.started(stmt);
                    try {
                        boolean hasResultSet = stmt.execute();
                        if (hasResultSet) {
                            try (ResultSet rs = stmt.getResultSet()) {
                                while (rs.next()) {
                                    count++;
                                }
                            }
                        } else {
                            count = stmt.getUpdateCount();
                        }
                    } finally {
                        sessions.finished(stmt);
                    }
                }
            }

            System.out.println("Rows returned/affected: " + count);

            long duration = System.currentTimeMillis() - start;
            QueryLogger.logSuccess(dbType, application, queryName, runID, duration);
            ok = true;
        } catch (Exception e) {
            QueryLogger.logError(dbType, application, queryName, runID, e);
        } finally {
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
    }
}
