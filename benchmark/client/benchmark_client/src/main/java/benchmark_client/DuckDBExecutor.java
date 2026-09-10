package benchmark_client;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;


public class DuckDBExecutor implements QueryExecutor {
    private final String url;
    private final String token;

    public DuckDBExecutor(String url, String token) {
        this.url = url;
        this.token = token;
    }

    @Override
    public void execute(String sql, String queryName, String dbType, String application, String runID) {
        QueryLogger.logStart(dbType, application, queryName, runID);
        long start = System.currentTimeMillis();
        Properties props = new Properties();
        props.setProperty("token", token);
        try (Connection conn = DriverManager.getConnection(url, props);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

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

        } catch (Exception e) {
            QueryLogger.logError(dbType, application, queryName, runID, e);
        }
    }
}