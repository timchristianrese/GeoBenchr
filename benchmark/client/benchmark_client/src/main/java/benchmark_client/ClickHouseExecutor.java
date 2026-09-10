package benchmark_client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

public class ClickHouseExecutor implements QueryExecutor {
    private final String url;
    private final String user;
    private final String password;

    public ClickHouseExecutor(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    @Override
    public void execute(String sql, String queryName, String dbType, String application, String runID) {
        QueryLogger.logStart(dbType, application, queryName, runID);
        long start = System.currentTimeMillis();
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);
        try (Connection conn = DriverManager.getConnection(url, props);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

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

            System.out.println("Rows returned/affected: " + count);

            long duration = System.currentTimeMillis() - start;
            QueryLogger.logSuccess(dbType, application, queryName, runID, duration);

        } catch (Exception e) {
            QueryLogger.logError(dbType, application, queryName, runID, e);
        }
    }
}
