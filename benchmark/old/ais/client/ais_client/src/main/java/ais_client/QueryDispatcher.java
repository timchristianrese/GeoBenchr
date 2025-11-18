package ais_client;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public class QueryDispatcher {

    public static List<QueryConfig> loadQueries(String yamlPath) {
    InputStream inputStream = QueryDispatcher.class
            .getClassLoader()
            .getResourceAsStream(yamlPath);

    if (inputStream == null) {
        throw new RuntimeException("YAML file not found in resources");
    }

    Yaml yaml = new Yaml();
    List<Map<String, Object>> rawList = yaml.load(inputStream);

    List<QueryConfig> result = new ArrayList<>();

    for (Map<String, Object> entry : rawList) {
        String name = (String) entry.get("name");
        String type = (String) entry.get("type");
        String sql = (String) entry.get("sql");
        List<Object> params = (List<Object>) entry.get("params");

        QueryConfig qc = new QueryConfig(name, type, sql, params);
        result.add(qc);
    }

    return result;
}
}
