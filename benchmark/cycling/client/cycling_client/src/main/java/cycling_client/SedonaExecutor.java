package cycling_client;
import org.apache.sedona.spark.SedonaContext;
import org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class SedonaExecutor implements QueryExecutor {
    
    private static SparkSession sedona;
    private static boolean initialized = false;

    public SedonaExecutor(String masterUrl) {
        synchronized (SedonaExecutor.class) {
        if (!initialized) {
            // Initialize Sedona context
            SparkSession config = SedonaContext.builder()
            .master("local[*]") // Optional: Remove or replace if running on Spark cluster
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryo.registrator", SedonaVizKryoRegistrator.class.getName()) // org.apache.sedona.viz.core.Serde.SedonaVizKryoRegistrator
            .getOrCreate();
        // Activate Sedona SQL extensions
            sedona = SedonaContext.create(config);
            String resourcePath = System.getenv().getOrDefault("RESOURCE_PATH", "/home/tim/data/cycling/resources/");
            String dataPath =     System.getenv().getOrDefault("DATA_PATH", "/home/tim/data/cycling/");
            
            System.out.println("Initializing SedonaExecutor with master URL: " + masterUrl);
            System.out.println("Using resource path: " + resourcePath);
            System.out.println("Using data path: " + dataPath);
            System.out.println("Loading datasets from: " + dataPath + "*.csv");
            // Load and cache the dataset once
            //Dataset<Row> geoDf = sedona.read().format("csv").option("delimiter", ";").option("header", "false").load(dataPath + "*.csv");

            //Test loading a single file
            Dataset<Row> geoDf = sedona.read().format("csv").option("delimiter", ";").option("header", "false").load(dataPath + "point_NRW_LOW_1123.csv");


            geoDf.createOrReplaceTempView("cycling_points");
            geoDf = sedona.sql("SELECT _c0 as flightid, _c1 as airplanetype, _c2 as origin, _c3 as destination, ST_GeomFromWKT(_c4) AS geom, _c5 as timestamp, _c6 as altitude FROM flight_points;");
            geoDf.createOrReplaceTempView("flight_points");
            geoDf.cache().count();
            geoDf.show(5); // Show first 5 rows for debugging
            geoDf.printSchema(); // Print schema for debugging
            
            




            Dataset<Row> universitiesDf = sedona.read().format("csv").option("delimiter", ";").option("header", "true").load(resourcePath+"universities.csv");
            universitiesDf.createOrReplaceTempView("universities");
            //convert column 1 (latitude) and column 2 (longitude) to a geometry column
            universitiesDf = sedona.sql("SELECT name, ST_GeomFromWKT(polygon)      AS geom FROM universities;");
            universitiesDf.createOrReplaceTempView("universities");
            universitiesDf.cache().count();
            universitiesDf.printSchema();

            Dataset<Row> districtsDf = sedona.read().format("csv").option("delimiter", ";").option("header", "true").load(resourcePath+"districts.csv");
            districtsDf.createOrReplaceTempView("districts");
            //convert column 1 (latitude) and column 2 (longitude) to a geometry column
            districtsDf = sedona.sql("SELECT name, ST_GeomFromWKT(polygon) AS geom FROM districts;");
            districtsDf.createOrReplaceTempView("districts");
            districtsDf.cache().count();
            districtsDf.printSchema();
            

    

            districtsDf.createOrReplaceTempView("districts");

            initialized = true;
            System.out.println("Finished initializing SedonaExecutor with master URL: " + masterUrl);
            }
    
        }
    }


    @Override
    public void execute(String sql, String queryName) {
        if (sedona == null) {
            System.err.println("Sedona context not initialized.");
            return;
        }

        QueryLogger.logStart("Sedona", queryName);
        try {
            long start = System.currentTimeMillis();
            Dataset<Row> result = sedona.sql(sql);
            long duration = System.currentTimeMillis() - start;

            // Optional: Uncomment if you want row count
            // long count = result.count();
            // System.out.println("Rows returned: " + count);

            QueryLogger.logSuccess("Sedona", queryName, duration);
        } catch (Exception e) {
            QueryLogger.logError("Sedona", queryName, e);
        }
    }
}
