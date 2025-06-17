package aviation_client;
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
            String resourcePath = System.getenv().getOrDefault("RESOURCE_PATH", "/home/tim/data/aviation/resources/");
            String dataPath =     System.getenv().getOrDefault("DATA_PATH", "/home/tim/data/aviation/");
            
            System.out.println("Initializing SedonaExecutor with master URL: " + masterUrl);
            System.out.println("Using resource path: " + resourcePath);
            System.out.println("Using data path: " + dataPath);
            System.out.println("Loading datasets from: " + dataPath + "*.csv");
            // Load and cache the dataset once
            //Dataset<Row> geoDf = sedona.read().format("csv").option("delimiter", ";").option("header", "false").load(dataPath + "*.csv");

            //Test loading a single file
            Dataset<Row> geoDf = sedona.read().format("csv").option("delimiter", ";").option("header", "false").load(dataPath + "point_NRW_LOW_1123.csv");


            geoDf.createOrReplaceTempView("flight_points");
            geoDf = sedona.sql("SELECT _c0 as flightid, _c1 as airplanetype, _c2 as origin, _c3 as destination, ST_GeomFromWKT(_c4) AS geom, _c5 as timestamp, _c6 as altitude FROM flight_points;");
            geoDf.createOrReplaceTempView("flight_points");
            geoDf.cache().count();
            geoDf.show(5); // Show first 5 rows for debugging
            geoDf.printSchema(); // Print schema for debugging
            
            

            Dataset<Row> airportsDf = sedona.read()
            .format("csv")
            .option("header", "true")
            .load(resourcePath+"airports.csv");


            Dataset<Row> countiesDf = sedona.read().format("csv").option("delimiter", ";").option("header", "true").load(resourcePath+"counties-wkt.csv");
            countiesDf.createOrReplaceTempView("counties");
            countiesDf = sedona.sql("SELECT name, ST_GeomFromWKT(polygon) AS geom FROM counties;");
            countiesDf.createOrReplaceTempView("counties");
            countiesDf.cache().count();
            countiesDf.printSchema();
            // Rename polygon column to geom
            // change geom column to geometry type
            
            Dataset<Row> districtsDf = sedona.read().format("csv").option("delimiter", ";").option("header", "true").load(resourcePath+"districts-wkt.csv");
            districtsDf.createOrReplaceTempView("districts");
            districtsDf = sedona.sql("SELECT name, ST_GeomFromWKT(polygon) AS geom FROM districts;");
            districtsDf.createOrReplaceTempView("districts");
            districtsDf.cache().count();
            districtsDf.printSchema();
             //rename polygon column to geom
            


            Dataset<Row> municipalitiesDf = sedona.read().format("csv").option("delimiter", ";").option("header", "true").load(resourcePath+"municipalities-wkt.csv");
            municipalitiesDf.createOrReplaceTempView("municipalities");
            municipalitiesDf = sedona.sql("SELECT name, ST_GeomFromWKT(polygon) AS geom FROM municipalities;");
            municipalitiesDf.createOrReplaceTempView("municipalities");
            municipalitiesDf.cache().count();
            municipalitiesDf.printSchema();
            
            //rename polygon column to geom
            Dataset<Row> citiesDf = sedona.read().format("csv").option("header", "true").load(resourcePath+"cities.csv");
            citiesDf.createOrReplaceTempView("cities");
            citiesDf = sedona.sql("SELECT area, latitude, longitude, district, name, population,  ST_GeomFromWKT(geom) AS geom FROM cities;");
            citiesDf.createOrReplaceTempView("cities");
            citiesDf.cache().count();
            citiesDf.printSchema();
            // Register the dataframes as temporary view
    
            geoDf.createOrReplaceTempView("flight_points");
            citiesDf.createOrReplaceTempView("cities");
            countiesDf.createOrReplaceTempView("counties");
            districtsDf.createOrReplaceTempView("districts");
            municipalitiesDf.createOrReplaceTempView("municipalities");
            airportsDf.createOrReplaceTempView("airports");
            initialized = true;
            System.out.println("Finished initializing SedonaExecutor with master URL: " + masterUrl);
            }
    
        }
    }


    @Override
    public void execute (String sql, String queryName) {
        String threadName = Thread.currentThread().getName();
        Dataset<Row> result = null;
        if (sedona == null) {
            System.err.println("Sedona context is not initialized. Cannot execute query: " + queryName);
            return;
        }
        try {
            System.out.println(threadName + ": Executing Sedona SQL query: " + queryName);
            long startTime = System.currentTimeMillis();
            result = sedona.sql(sql);
            System.out.println(result.count());
            //result.show(5);
            long duration = System.currentTimeMillis() - startTime;

            System.out.printf("%s: Query '%s' returned in %d ms.%n",
                    threadName, queryName, duration);
        } catch (Exception e) {
            System.out.printf("Error executing Sedona query %s: %s%n", queryName, e.getMessage());
        }
    }
}
