package benchmark_client;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import benchmark_client.QueryExecutor;

public class PostgreSQLExecutor implements QueryExecutor {
    private final String url;
    private final String user;
    private final String password;

    public PostgreSQLExecutor(String url, String user, String password, String application ) {
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

    public void execute(String sql, String queryName, String dbType, String application, String runID) {
        QueryLogger.logStart(dbType, application, queryName, runID);
        long start = System.currentTimeMillis();
        try (Connection conn = DriverManager.getConnection(url, user, password);
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery()) {

            
            //print the first row and last row
            
            int count = 0;
            while (rs.next()) {
                count++;
                //print the entire row, all columns, all rows
            }  

            // Optional: Print row count and last row
            System.out.println("Rows returned: " + count);
            long duration = System.currentTimeMillis() - start;
            // Optional: Print row count
            // System.out.println("Rows returned: " + count);
            QueryLogger.logSuccess(dbType, application, queryName, runID, duration);

        } catch (Exception e) {
            QueryLogger.logError(dbType, application, queryName, runID, e);
        }
    }
}