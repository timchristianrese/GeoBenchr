package aviation_client;
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

    public void execute(String sql, String queryName) {
        QueryLogger.logStart("PostgreSQL", queryName);
        try (Connection conn = DriverManager.getConnection(url, user, password);
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery()) {

            long start = System.currentTimeMillis();
            
            //print the first row and last row
            
            int count = 0;
            while (rs.next()) {
                count++;
                //print the entire row, all columns, all rows
                System.out.println("Row " + count + ": ");
                for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                    System.out.print(rs.getMetaData().getColumnName(i) + ": " + rs.getObject(i) + ", ");
                }
                System.out.println();
            }  

            // Optional: Print row count and last row
            System.out.println("Rows returned: " + count);
            long duration = System.currentTimeMillis() - start;
            // Optional: Print row count
            // System.out.println("Rows returned: " + count);
            QueryLogger.logSuccess("PostgreSQL", queryName, duration);

        } catch (Exception e) {
            QueryLogger.logError("PostgreSQL", queryName, e);
        }
    }
}