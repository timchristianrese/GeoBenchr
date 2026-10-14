package benchmark_client;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import benchmark_client.QueryExecutor;

public class PostgreSQLExecutor implements QueryExecutor {
    private final String sessionTimeZone;
    private final JdbcSessions sessions;

    // FIX (2026-10-01): pgjdbc sets the session TimeZone from the client JVM's default (verified:
    // a JDBC `options=-c TimeZone=...` property does NOT override it), so every timestamp/timestamptz
    // comparison silently depended on whichever host ran the benchmark. Pinned explicitly instead.
    // Europe/Berlin is the server default the data was loaded and verified against. (Queries that
    // need another zone say so explicitly with AT TIME ZONE: Citus evaluates most expressions on the
    // workers, which never see a coordinator-side SET TIME ZONE.)
    public static final String DEFAULT_SESSION_TIME_ZONE = "Europe/Berlin";
    // (2026-10-14) the cursor fetches 100,000 rows per round trip (was 10,000). Not pgjdbc's adaptiveFetch: it
    // sizes batches by the rows' size on the wire, while a row held in the client takes several times that (Java
    // object overhead), so with 8 client threads its batches of over a million small rows ran the client out of
    // memory. 100,000 rows of a few columns are a few MB per connection; a 5-million-row result needs 50 round trips.
    static final int FETCH_SIZE = 100000;

    public PostgreSQLExecutor(String url, String user, String password, String application) {
        this(url, user, password, application, DEFAULT_SESSION_TIME_ZONE);
    }

    public PostgreSQLExecutor(String url, String user, String password, String application, String sessionTimeZone) {
        this.sessionTimeZone = sessionTimeZone;
        // (2026-10-07) sessions are reused across queries; the time zone is set once per session,
        // outside the measurement -- see JdbcSessions
        this.sessions = new JdbcSessions(() -> {
            java.util.Properties props = new java.util.Properties();
            props.setProperty("user", user);
            if (password != null) props.setProperty("password", password);
            Connection conn = DriverManager.getConnection(url, props);
            try (Statement tz = conn.createStatement()) {
                tz.execute("SET TIME ZONE '" + this.sessionTimeZone + "'");
                // (2026-10-09) server-side backstop for the runner's timeout, which cancels the query itself
                tz.execute("SET statement_timeout = '" + (BenchmarkRunner.DEFAULT_TIMEOUT_SECONDS + 30) + "s'");
            }
            return conn;
        });
    }

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
            // (2026-10-08) stream the result in batches through a cursor (pgjdbc only does that with
            // autocommit off and a fetch size): by default pgjdbc holds the entire result in client
            // memory, and the multi-million-row results of e.g. CountRidesFullyInDistrictRadius ran
            // the 1 GB client box out of heap. Every row is still read.
            conn.setAutoCommit(false);
            stmt.setFetchSize(FETCH_SIZE);
            // read queries return a ResultSet; write queries (insert/update/delete) don't -- branch
            // on execute()'s return value instead of assuming executeQuery(), which throws for writes.
            boolean hasResultSet = stmt.execute();
            int count = 0;
            if (hasResultSet) {
                try (ResultSet rs = stmt.getResultSet()) {
                    while (rs.next()) {
                        count++;
                    }
                }
            } else {
                count = stmt.getUpdateCount();
            }

            conn.commit();
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
    }

    @Override
    public void cancel(Thread worker) {
        sessions.cancelRunning(worker);
    }
}
