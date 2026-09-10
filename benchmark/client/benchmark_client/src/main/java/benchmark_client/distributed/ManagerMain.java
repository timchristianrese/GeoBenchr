package benchmark_client.distributed;

import benchmark_client.QueryConfig;
import benchmark_client.QueryDispatcher;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Distributed benchmark manager: builds the full task queue for one benchmark run (one entry per
 * query execution, across whichever dialects were requested) and hands tasks out to WorkerMain
 * processes -- possibly on other machines -- as they ask for them ("dynamic pull-based"
 * distribution, see /Users/gov/.claude/plans/steady-herding-swan.md). No authentication; this is
 * meant for a trusted internal network, consistent with the plaintext SUT credentials already
 * used throughout this project.
 */
public class ManagerMain {

    private static class TaskEnvelope {
        final long id;
        final String dialect;
        final QueryConfig query;

        TaskEnvelope(long id, String dialect, QueryConfig query) {
            this.id = id;
            this.dialect = dialect;
            this.query = query;
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: ManagerMain <application> [dialect1,dialect2,...] [port]");
            System.err.println("  application: scenario name, e.g. aviation/ais/cycling/geolife/general");
            System.err.println("  dialects: comma-separated, default = " + String.join(",", DialectRegistry.DEFAULT_DIALECTS));
            System.err.println("  port: default 8787");
            System.exit(1);
        }
        String application = args[0];
        String[] dialects = (args.length > 1 && !args[1].isEmpty())
                ? args[1].split(",")
                : DialectRegistry.DEFAULT_DIALECTS;
        int port = args.length > 2 ? Integer.parseInt(args[2]) : 8787;
        String runID = UUID.randomUUID().toString().substring(0, 5);

        // --- Build the task queue up front: one entry per query execution, per dialect. ---
        Map<String, ConcurrentLinkedQueue<TaskEnvelope>> queues = new LinkedHashMap<>();
        Map<String, Integer> totalPerDialect = new LinkedHashMap<>();
        Map<String, AtomicInteger> completedPerDialect = new LinkedHashMap<>();
        Map<String, AtomicInteger> dispensedPerDialect = new LinkedHashMap<>();
        ConcurrentHashMap<Long, String> inFlight = new ConcurrentHashMap<>();
        AtomicInteger idCounter = new AtomicInteger(0);
        AtomicInteger completedTotal = new AtomicInteger(0);
        int totalTasks = 0;

        for (String dialect : dialects) {
            if (!DialectRegistry.isKnownDialect(dialect)) {
                System.err.println("Skipping unknown dialect \"" + dialect + "\" (known: " + DialectRegistry.yamlFilenames().keySet() + ")");
                continue;
            }
            String yamlPath = application + "/" + DialectRegistry.yamlFilenameFor(dialect);
            List<QueryConfig> queries = QueryDispatcher.loadQueries(yamlPath);
            ConcurrentLinkedQueue<TaskEnvelope> queue = new ConcurrentLinkedQueue<>();
            for (QueryConfig q : queries) {
                queue.add(new TaskEnvelope(idCounter.incrementAndGet(), dialect, q));
            }
            queues.put(dialect, queue);
            totalPerDialect.put(dialect, queries.size());
            completedPerDialect.put(dialect, new AtomicInteger(0));
            dispensedPerDialect.put(dialect, new AtomicInteger(0));
            totalTasks += queries.size();
            System.out.println("Loaded " + queries.size() + " " + dialect + " tasks from " + yamlPath);
        }

        if (totalTasks == 0) {
            System.err.println("No tasks loaded for application \"" + application + "\" -- nothing to do.");
            return;
        }

        final int totalTasksFinal = totalTasks;

        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        // HttpServer.stop() does not shut down an executor supplied via setExecutor() -- per the
        // JDK docs the caller owns its lifecycle. Without explicitly shutting it down below, this
        // fixed thread pool's non-daemon threads keep the JVM alive forever after "shutting down
        // manager" prints (confirmed by testing: the process never actually exited).
        ExecutorService httpExecutor = Executors.newFixedThreadPool(64);
        server.setExecutor(httpExecutor);

        server.createContext("/runid", exchange -> respondText(exchange, 200, runID));

        server.createContext("/task", exchange -> {
            if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                respondText(exchange, 405, "GET only");
                return;
            }
            List<String> candidates = requestedDialects(exchange, queues.keySet());
            TaskEnvelope task = null;
            String pickedDialect = null;
            for (String dialect : candidates) {
                ConcurrentLinkedQueue<TaskEnvelope> queue = queues.get(dialect);
                if (queue == null) continue;
                TaskEnvelope polled = queue.poll();
                if (polled != null) {
                    task = polled;
                    pickedDialect = dialect;
                    break;
                }
            }
            if (task == null) {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
                return;
            }
            dispensedPerDialect.get(pickedDialect).incrementAndGet();
            inFlight.put(task.id, task.dialect);

            Map<String, Object> body = new LinkedHashMap<>();
            body.put("id", task.id);
            body.put("dialect", task.dialect);
            body.put("application", application);
            body.put("runID", runID);
            body.put("name", task.query.getName());
            body.put("type", task.query.getType());
            body.put("sql", task.query.getSql());
            body.put("params", task.query.getParams());
            // A fresh Yaml() per request, not a shared instance -- Yaml is not thread-safe and
            // this handler runs concurrently across the server's thread pool (confirmed by
            // testing: a shared instance here corrupted responses under concurrent /task load).
            respondText(exchange, 200, new Yaml().dump(body));
        });

        server.createContext("/complete", exchange -> {
            if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                respondText(exchange, 405, "POST only");
                return;
            }
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).trim();
            try {
                long id = Long.parseLong(body);
                String dialect = inFlight.remove(id);
                if (dialect != null) {
                    completedPerDialect.get(dialect).incrementAndGet();
                    completedTotal.incrementAndGet();
                    respondText(exchange, 200, "ok");
                } else {
                    respondText(exchange, 200, "unknown-id");
                }
            } catch (NumberFormatException e) {
                respondText(exchange, 400, "bad task id");
            }
        });

        server.createContext("/status", exchange -> respondText(exchange, 200,
                statusText(application, runID, totalTasksFinal, completedTotal.get(),
                        totalPerDialect, dispensedPerDialect, completedPerDialect)));

        server.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            server.stop(0);
            httpExecutor.shutdownNow();
        }));

        System.out.println("Manager listening on port " + port + " for application \"" + application
                + "\", runID " + runID + ", " + totalTasksFinal + " total tasks across dialects "
                + queues.keySet() + ".");
        System.out.println("Workers should be started with: managerHost:" + port);

        // Print progress and block until every dispensed task has been marked complete.
        try {
            while (completedTotal.get() < totalTasksFinal) {
                Thread.sleep(5000);
                System.out.println(statusText(application, runID, totalTasksFinal, completedTotal.get(),
                        totalPerDialect, dispensedPerDialect, completedPerDialect));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        System.out.println("All " + totalTasksFinal + " tasks completed for runID " + runID + ". Shutting down manager.");
        server.stop(1);
        httpExecutor.shutdownNow();
    }

    private static List<String> requestedDialects(HttpExchange exchange, Iterable<String> known) {
        Map<String, String> params = parseQueryParams(exchange.getRequestURI().getRawQuery());
        String filter = params.get("dialects");
        List<String> result = new ArrayList<>();
        if (filter == null || filter.isEmpty()) {
            for (String d : known) result.add(d);
            return result;
        }
        for (String d : filter.split(",")) {
            result.add(d.trim());
        }
        return result;
    }

    private static Map<String, String> parseQueryParams(String rawQuery) {
        Map<String, String> params = new LinkedHashMap<>();
        if (rawQuery == null || rawQuery.isEmpty()) return params;
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            if (eq < 0) continue;
            String key = URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8);
            String value = URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            params.put(key, value);
        }
        return params;
    }

    private static String statusText(String application, String runID, int total, int completed,
                                      Map<String, Integer> totalPerDialect,
                                      Map<String, AtomicInteger> dispensedPerDialect,
                                      Map<String, AtomicInteger> completedPerDialect) {
        StringBuilder sb = new StringBuilder();
        sb.append("application=").append(application).append(" runID=").append(runID)
                .append(" completed=").append(completed).append("/").append(total).append("\n");
        for (String dialect : totalPerDialect.keySet()) {
            sb.append("  ").append(dialect)
                    .append(": total=").append(totalPerDialect.get(dialect))
                    .append(" dispensed=").append(dispensedPerDialect.get(dialect).get())
                    .append(" completed=").append(completedPerDialect.get(dialect).get())
                    .append("\n");
        }
        return sb.toString();
    }

    private static void respondText(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
