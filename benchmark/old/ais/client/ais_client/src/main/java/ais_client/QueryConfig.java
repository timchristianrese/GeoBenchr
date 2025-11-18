package ais_client;
import java.util.List;

public class QueryConfig {
    private String name;
    private String type;
    private String sql;
    private List<Object> params;

    // Empty constructor needed for SnakeYAML
    public QueryConfig() {}

    public QueryConfig(String name, String type, String sql, List<Object> params) {
        this.name = name;
        this.type = type;
        this.sql = sql;
        this.params = params;
    }

    // Getters and setters...

    public String getName() { return name; }
    public String getType() { return type; }
    public String getSql() { return sql; }
    public List<Object> getParams() { return params; }

    public void setName(String name) { this.name = name; }
    public void setType(String type) { this.type = type; }
    public void setSql(String sql) { this.sql = sql; }
    public void setParams(List<Object> params) { this.params = params; }
}
