package benchmark_client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

// FIX: this used to shell out to a nonexistent "/opt/msql-server/msql-client" binary with
// "-o org -u user -p password" auth -- none of that matches the real partner cluster (see
// docs/spacetime-cluster-access.md): SpaceTime (MSQL) needs no auth at all, and the only client
// that exists is `pymsql`, a Python-only async library (no JDBC driver, confirmed live -- the
// partner's own tooling is Python too, see tu-berlin-bench in the same doc). Rather than
// reimplementing pymsql's binary wire protocol + Arrow IPC decoding in Java, this talks to
// scripts/spacetime_query_runner.py (deploy alongside pymsql's venv -- see that script's docstring
// and docs/spacetime-cluster-access.md).
//
// (2026-10-07) Persistent sessions: the runner used to be started once per query, so every
// SpaceTime timing included a Python start, the pymsql import and a new connection (~105 ms, vs
// ~3 ms for a query on an open session). Now each benchmark thread borrows a long-lived worker
// (spacetime_query_runner.py --serve, one pymsql connection each), started and warmed up with an
// untimed SELECT 1 outside the measurement -- the same treatment the JDBC executors get from
// JdbcSessions. The timing covers sending the query and receiving the worker's reply.
public class SpaceTimeExecutor implements QueryExecutor {
    private static final String WARM_UP_SQL = "SELECT 1";

    private final String host;
    private final int port;
    private final String pythonBin;
    private final String scriptPath;
    private final ConcurrentLinkedQueue<Worker> idle = new ConcurrentLinkedQueue<>();
    private final List<Worker> all = new CopyOnWriteArrayList<>();

    public SpaceTimeExecutor(String host, int port, String pythonBin, String scriptPath) {
        this.host = host;
        this.port = port;
        this.pythonBin = pythonBin;
        this.scriptPath = scriptPath;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> all.forEach(Worker::stop)));
    }

    /** One spacetime_query_runner.py --serve process holding one pymsql connection. */
    private static final class Worker {
        private final Process process;
        private final OutputStream stdin;
        private final BufferedReader stdout;
        private final StringBuffer stderrTail = new StringBuffer();

        Worker(Process process) {
            this.process = process;
            this.stdin = process.getOutputStream();
            this.stdout = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
            Thread drain = new Thread(() -> drainStderr(process.getErrorStream()), "spacetime-worker-stderr");
            drain.setDaemon(true);
            drain.start();
        }

        private void drainStderr(InputStream err) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(err, StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    stderrTail.append(line).append('\n');
                    if (stderrTail.length() > 4000) {
                        stderrTail.delete(0, stderrTail.length() - 2000);
                    }
                }
            } catch (IOException ignored) {
                // process gone
            }
        }

        /** Sends one statement and returns the reply line ("OK <rows>" or "ERR <message>"). */
        String call(String sql) throws IOException {
            byte[] body = sql.getBytes(StandardCharsets.UTF_8);
            stdin.write((body.length + "\n").getBytes(StandardCharsets.UTF_8));
            stdin.write(body);
            stdin.flush();
            String reply = stdout.readLine();
            if (reply == null) {
                throw new IOException("spacetime_query_runner.py exited"
                        + (stderrTail.length() > 0 ? ": " + stderrTail.toString().trim() : ""));
            }
            return reply;
        }

        void stop() {
            process.destroy();
        }
    }

    private Worker borrow() throws IOException {
        Worker worker;
        while ((worker = idle.poll()) != null) {
            if (worker.process.isAlive()) {
                return worker;
            }
            all.remove(worker);
        }
        ProcessBuilder pb = new ProcessBuilder(
            pythonBin,
            scriptPath,
            "--host", host,
            "--port", String.valueOf(port),
            "--serve"
        );
        worker = new Worker(pb.start());
        all.add(worker);
        String reply = worker.call(WARM_UP_SQL);
        if (!reply.startsWith("OK")) {
            discard(worker);
            throw new IOException("SpaceTime warm-up failed: " + reply);
        }
        return worker;
    }

    /** Kills every worker that is running a query (its pymsql connection closes with it). */
    @Override
    public void cancel() {
        for (Worker worker : all) {
            if (!idle.contains(worker)) {
                worker.process.destroyForcibly();
            }
        }
    }

    private void discard(Worker worker) {
        all.remove(worker);
        worker.stop();
    }

    @Override
    public void execute(String sql, String queryName, String dbType, String application, String runID) {
        QueryLogger.logStart("SpaceTime", application, queryName, runID);
        Worker worker;
        try {
            worker = borrow();
        } catch (Exception e) {
            QueryLogger.logError("SpaceTime", application, queryName, runID, e);
            return;
        }

        long start = System.currentTimeMillis();
        try {
            String reply = worker.call(sql.trim());
            long duration = System.currentTimeMillis() - start;
            // the worker stays usable after an ERR reply (it reconnects itself), so it goes back either way
            idle.add(worker);
            if (reply.startsWith("OK")) {
                QueryLogger.logSuccess("SpaceTime", application, queryName, runID, duration);
            } else {
                QueryLogger.logError("SpaceTime", application, queryName, runID,
                        new RuntimeException("spacetime_query_runner.py: " + reply));
            }
        } catch (Exception e) {
            discard(worker);
            QueryLogger.logError("SpaceTime", application, queryName, runID, e);
        }
    }
}
