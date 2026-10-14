package benchmark_client;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Reused JDBC sessions for one executor (2026-10-07). Every executor used to open a new connection
 * per query inside the timed section, which charged each system its own connection setup cost
 * (~6 ms ClickHouse, ~15 ms DuckDB, ~25 ms PostgreSQL, ~105 ms for the SpaceTime bridge process).
 * Now each benchmark thread borrows an open session, runs its query, and hands the session back;
 * opening a session (plus one untimed warm-up statement) happens outside the measurement. A session
 * whose query failed is closed instead of reused, so a broken connection never carries over.
 */
final class JdbcSessions {
    interface Opener {
        Connection open() throws SQLException;
    }

    private static final String WARM_UP_SQL = "SELECT 1";

    private final Opener opener;
    private final ConcurrentLinkedQueue<Connection> idle = new ConcurrentLinkedQueue<>();
    private final java.util.Map<Statement, Thread> running = new ConcurrentHashMap<>();

    JdbcSessions(Opener opener) {
        this.opener = opener;
    }

    Connection borrow() throws SQLException {
        Connection conn;
        while ((conn = idle.poll()) != null) {
            if (!conn.isClosed()) {
                return conn;
            }
        }
        conn = opener.open();
        try (Statement warmUp = conn.createStatement()) {
            if (warmUp.execute(WARM_UP_SQL)) {
                try (ResultSet rs = warmUp.getResultSet()) {
                    while (rs.next()) {
                        // drain
                    }
                }
            }
        } catch (SQLException e) {
            discard(conn);
            throw e;
        }
        return conn;
    }

    /** Registers a statement as running, so cancelRunning() can stop it; unregister with finished(). */
    void started(Statement stmt) {
        running.put(stmt, Thread.currentThread());
    }

    void finished(Statement stmt) {
        running.remove(stmt);
    }

    /** Statement.cancel() on every running statement (the driver stops the query on the server). */
    void cancelRunning() {
        cancelRunning(null);
    }

    /** Statement.cancel() on the statements the given thread is running (all threads if null). */
    void cancelRunning(Thread worker) {
        for (java.util.Map.Entry<Statement, Thread> e : running.entrySet()) {
            if (worker != null && e.getValue() != worker) {
                continue;
            }
            try {
                e.getKey().cancel();
            } catch (SQLException ex) {
                System.err.println("Statement.cancel failed: " + ex.getMessage());
            }
        }
    }

    void giveBack(Connection conn) {
        idle.add(conn);
    }

    void discard(Connection conn) {
        try {
            conn.close();
        } catch (SQLException ignored) {
            // already broken
        }
    }
}
