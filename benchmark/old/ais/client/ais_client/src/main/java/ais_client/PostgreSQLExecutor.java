package ais_client;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class PostgreSQLExecutor implements QueryExecutor {
    private final String url;
    private final String user;
    private final String password;

    public PostgreSQLExecutor(String url, String user, String password) {
        this.url = url;
        this.user = user;
        this.password = password;
    }

    // public void executeQuery(String sql) {

    //     try (Connection conn = DriverManager.getConnection(url, user, password);
    //          PreparedStatement stmt = conn.prepareStatement(sql);
    //          ResultSet rs = stmt.executeQuery()) {
    //         while (rs.next()) {
    //             // optionally process result
    //         }
    //     } catch (Exception e) {
    //         e.printStackTrace();
    //     }
    // }

    @Override
    public void execute(String sql, String queryName) {
        QueryLogger.logStart("PostgreSQL", queryName);
        try (Connection conn = DriverManager.getConnection(url, user, password);
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery()) {

            long start = System.currentTimeMillis();

            int count = 0;
            while (rs.next()) {
                count++;
            }
            
            long duration = System.currentTimeMillis() - start;
            // Optional: Print row count
            // System.out.println("Rows returned: " + count);
            QueryLogger.logSuccess("PostgreSQL", queryName, runID, duration);

        } catch (Exception e) {
            QueryLogger.logError("PostgreSQL", queryName, runID, e);
        }
    }
}