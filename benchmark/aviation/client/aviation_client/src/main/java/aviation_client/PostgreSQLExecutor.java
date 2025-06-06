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

    public void executeQuery(String sql) {
        try (Connection conn = DriverManager.getConnection(url, user, password);
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                // optionally process result
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(String sql, String queryName) {
        try (Connection conn = DriverManager.getConnection(url, user, password);
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                // Optionally process rows
            }
        } catch (Exception e) {
            System.err.printf("Error executing query %s: %s%n", queryName, e.getMessage());
        }
    }
}
