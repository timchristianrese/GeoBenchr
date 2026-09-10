package benchmark_client.distributed;

import benchmark_client.BenchmarkMarkerLogger;
import benchmark_client.QueryExecutor;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Distributed benchmark worker: repeatedly asks a ManagerMain for the next task and executes it
 * with the same QueryExecutor implementations (and QueryLogger output) the single-machine
 * BenchmarkClient uses -- see /Users/gov/.claude/plans/steady-herding-swan.md. Run one of these
 * per machine (or several, pointed at the same manager) to spread load across workers.
 */
public class WorkerMain {

    private static final int MAX_CONSECUTIVE_FAILURES = 5;

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: WorkerMain <managerHost:port> [numThreads] [dialect1,dialect2,...]");
            System.exit(1);
        }
        String managerAddress = args[0];
        String baseUrl = managerAddress.startsWith("http://") || managerAddress.startsWith("https://")
                ? managerAddress
                : "http://" + managerAddress;

        int numThreads = args.length > 1
                ? Integer.parseInt(args[1])
                : Integer.parseInt(System.getenv().getOrDefault("NUM_THREADS", "1"));
        String dialectFilter = args.length > 2 ? args[2] : null;

        // HTTP_1_1 explicitly: com.sun.net.httpserver.HttpServer (the manager) only speaks
        // HTTP/1.1, and leaving the client on its default HTTP/2-with-downgrade negotiation
        // caused occasional silently-dropped /task responses under concurrent load in testing
        // (task marked dispensed on the manager but never reaching a worker thread, with no
        // exception on either side to retry against).
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        String runID = get(client, baseUrl + "/runid").body().trim();
        BenchmarkMarkerLogger benchLogger = new BenchmarkMarkerLogger(runID);
        benchLogger.logText("Worker Start (manager=" + managerAddress + ", threads=" + numThreads
                + ", dialects=" + (dialectFilter == null ? "all" : dialectFilter) + ")");

        System.out.println("Worker connected to manager " + baseUrl + ", runID " + runID
                + ", " + numThreads + " thread(s), dialects=" + (dialectFilter == null ? "all" : dialectFilter));

        Map<String, QueryExecutor> executorCache = new ConcurrentHashMap<>();
        AtomicInteger executedCount = new AtomicInteger(0);
        String taskUrl = baseUrl + "/task" + (dialectFilter != null ? "?dialects=" + dialectFilter : "");

        Thread[] threads = new Thread[numThreads];
        for (int t = 0; t < numThreads; t++) {
            threads[t] = new Thread(() -> workerLoop(client, taskUrl, baseUrl, runID, executorCache, executedCount));
            threads[t].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        benchLogger.logText("Worker End (executed " + executedCount.get() + " tasks)");
        benchLogger.moveLogFile();
        System.out.println("Worker finished: executed " + executedCount.get() + " tasks. Local logs use runID " + runID + ".");
    }

    private static void workerLoop(HttpClient client, String taskUrl, String baseUrl, String runID,
                                    Map<String, QueryExecutor> executorCache, AtomicInteger executedCount) {
        // One Yaml instance per thread, never shared -- Yaml is not thread-safe, and a shared
        // instance here caused sporadic parse corruption (ClassCastException/YAMLException/etc.)
        // under concurrent polling in testing.
        Yaml yaml = new Yaml();
        int consecutiveFailures = 0;
        while (true) {
            HttpResponse<String> response;
            try {
                response = get(client, taskUrl);
            } catch (Exception e) {
                consecutiveFailures++;
                System.err.println("[" + Thread.currentThread().getName() + "] Failed to reach manager: " + e.getMessage());
                if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
                    System.err.println("[" + Thread.currentThread().getName() + "] Giving up after " + consecutiveFailures + " consecutive failures.");
                    return;
                }
                sleepQuietly(1000);
                continue;
            }

            if (response.statusCode() == 204) {
                return; // no more tasks
            }
            if (response.statusCode() != 200) {
                consecutiveFailures++;
                System.err.println("[" + Thread.currentThread().getName() + "] Unexpected manager response: " + response.statusCode());
                if (consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) return;
                sleepQuietly(1000);
                continue;
            }
            consecutiveFailures = 0;

            @SuppressWarnings("unchecked")
            Map<String, Object> task = yaml.load(response.body());
            long id = ((Number) task.get("id")).longValue();
            String dialect = (String) task.get("dialect");
            String application = (String) task.get("application");
            String name = (String) task.get("name");
            String sql = (String) task.get("sql");

            QueryExecutor executor = executorCache.computeIfAbsent(dialect, d -> DialectRegistry.createExecutor(d, application));
            executor.execute(sql, name, dialect, application, runID);
            executedCount.incrementAndGet();

            try {
                post(client, baseUrl + "/complete", Long.toString(id));
            } catch (Exception e) {
                System.err.println("[" + Thread.currentThread().getName() + "] Failed to report completion of task " + id + ": " + e.getMessage());
            }
        }
    }

    private static HttpResponse<String> get(HttpClient client, String url) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url)).GET().timeout(Duration.ofSeconds(30)).build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> post(HttpClient client, String url, String body) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .timeout(Duration.ofSeconds(30))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
