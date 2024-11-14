package com.example;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFinder;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.data.SimpleFeatureStore;
import org.geotools.api.data.Transaction;
import org.geotools.api.feature.simple.SimpleFeature;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.filter.Filter;
import org.geotools.data.DataUtilities;
import org.geotools.data.DefaultTransaction;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQL;
public class Main {

    public static void main(String[] args) {
        if(args.length < 8){
            System.out.println("Please provide all the required arguments, in the following order: instanceName, username, password, catalog, zookeepers, queryType, threadCount, limit");
            System.exit(0);
        }

        System.out.println("Starting...");
        String instanceName = args[0];
        String username = args[1];
        String password = args[2];
        String catalog = args[3];
        String zookeepers = args[4];
        String queryType = args[5];
        Integer threadCount = Integer.parseInt(args[6]);

        CyclicBarrier barrier = new CyclicBarrier(threadCount, () -> {
            System.out.println("All threads reached the barrier and will proceed together.");
        });

        
        Integer limit = Integer.parseInt(args[7]);
        CountDownLatch startLatch = new CountDownLatch(1); // Latch to control start of all threads

        for (int i = 0; i < threadCount; i++) {
            Thread thread = new Thread(new Task(barrier,i,instanceName, username, password, catalog, zookeepers, queryType, limit, startLatch));
            thread.start();
        }

        // Start threads
        System.out.println("All threads ready. Starting tasks in parallel...");

        startLatch.countDown(); 
        //if arg count is less than 8, then exit
       
    }
}
class Task implements Runnable {
    private int threadNumber;
    private CountDownLatch startLatch;
    private String instanceName;
    private String username;
    private String password;
    private String catalog;
    private String zookeepers;
    private String queryType;
    private Integer limit;
    private CyclicBarrier barrier;


    public Task(CyclicBarrier barrier, int threadNumber, String instanceName, String username, String password, String catalog, String zookeepers, String queryType, Integer limit, CountDownLatch startLatch) {
        this.threadNumber = threadNumber;
        this.instanceName = instanceName;
        this.username = username;
        this.password = password;
        this.catalog = catalog;
        this.zookeepers = zookeepers;
        this.queryType = queryType;
        this.limit = limit;
        this.startLatch = startLatch;
        this.barrier = barrier;
    }

    @Override
    public void run() {
        try {
            startLatch.await(); // Wait until latch is counted down to 0
            System.out.println("Thread " + threadNumber + " is running.");
            System.out.println("Instance Name: "+instanceName);
            System.out.println("Username: "+username);
            System.out.println("Password: "+password);
            System.out.println("Catalog: "+catalog);
            System.out.println("Zookeepers: "+zookeepers);
            Map<String, String> parameters = new HashMap<>();
            parameters.put("accumulo.instance.name", instanceName);
            parameters.put("accumulo.user", username);
            parameters.put("accumulo.password", password);
            parameters.put("accumulo.catalog", catalog);
            parameters.put("accumulo.zookeepers", zookeepers);
            try{
                System.out.println("Trying to connect to the datastore");
                DataStore dataStore = DataStoreFinder.getDataStore(parameters);

                SimpleFeatureStore featureStore;

                //change if you want to write trip data
                String featureTypeName = "ride_data";


                try {
                    featureStore = (SimpleFeatureStore) dataStore.getFeatureSource(featureTypeName);
                } catch (ClassCastException e) {
                    throw new IOException("Feature source does not support writing");
                }

                System.out.println("Connected to the datastore "+dataStore);
                for (String str : dataStore.getTypeNames()) {
                    //System.out.println("Feature: "+str);
                    //SimpleFeatureSource source = dataStore.getFeatureSource(str);
                    //System.out.println("This is the source: "+source);
                    Filter filter=null;
                    
                    //switch case for different query types
                    //spatial
                    String startTime;
                    String endTime;
                    String midTime;
                    Double lat;
                    Double lon;
                    Double startLon = Math.floor(Math.random() * (13.50421822-13.22833252))+13.22833252;
                    Double startLat = Math.floor(Math.random() * (52.58061313-52.42922077))+52.42922077;
                    OffsetDateTime specificTimestamp = OffsetDateTime.of(
                2023, 7, 1, 12, 0, 0, 0, ZoneOffset.UTC);
                    // Format the timestamp
                    String formattedTimestamp = specificTimestamp.format(DateTimeFormatter.ISO_INSTANT);

                    System.out.println(formattedTimestamp);
                    Integer writes = 20000;
                    SimpleFeatureType featureType = dataStore.getSchema(featureTypeName);
                    SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(featureType);
                    List<SimpleFeature> features = new ArrayList<>();
                    switch(queryType){
                        //insert just a single point
                        case "single_insert":
                            try {
                                featureType = dataStore.getSchema("ride_data");
                                featureBuilder = new SimpleFeatureBuilder(featureType);
                                featureBuilder.set("rider_id", (int) Math.floor(Math.random() * 30));
                                featureBuilder.set("ride_id", (int) Math.floor(Math.random() * 300));
                                featureBuilder.set("latitude", startLat);
                                featureBuilder.set("longitude", startLon);
                                featureBuilder.set("geom", "POINT("+startLon+" "+startLat+")");
                                featureBuilder.set("x", "0");
                                featureBuilder.set("y", "0");
                                featureBuilder.set("z", "0");
                                featureBuilder.set("timestamp", formattedTimestamp);
                                SimpleFeature feature = featureBuilder.buildFeature("ride_data.feature-single-1");
                                Transaction transaction = new DefaultTransaction("writeTransaction");
                                barrier.await();
                                try {
                                    featureStore.setTransaction(transaction);
                                    featureStore.addFeatures(DataUtilities.collection(feature));
                                    transaction.commit();
                                } catch (Exception e) {
                                    transaction.rollback();
                                    e.printStackTrace();
                                } finally {
                                    transaction.close();
                                }
                            } catch (Exception e) {
                            }
                        //insert queries
                        case "insert": 
                            try {
                                
                                featureType = dataStore.getSchema("ride_data");
                                //generate random start point within Berlin
                                for (int i = 0; i < writes; i++) {
                                    //create a new feature
                                    
                                    //replace ride_data with trip_data when wanting to write trip data
                                    featureBuilder = new SimpleFeatureBuilder(featureType);
                                    featureBuilder.set("rider_id", (int) Math.floor(Math.random() * 30));
                                    featureBuilder.set("ride_id", (int) Math.floor(Math.random() * 300));
                                    featureBuilder.set("latitude", startLat);
                                    featureBuilder.set("longitude", startLon);
                                    featureBuilder.set("geom", "POINT("+startLon+" "+startLat+")");
                                    featureBuilder.set("x", "0");
                                    featureBuilder.set("y", "0");
                                    featureBuilder.set("z", "0");
                                    featureBuilder.set("timestamp", formattedTimestamp);
                                    
                                    
                                    SimpleFeature feature = featureBuilder.buildFeature("ride_data.feature-" + i);
                                    features.add(feature);
                                    //increment the startLon and startLat, simulating a diagonal trip
                                    startLon += 0.0001;
                                    startLat += 0.0001;
                                    //increment the timestamp by 1 second
                                    specificTimestamp = specificTimestamp.plusSeconds(1);
                                    formattedTimestamp = specificTimestamp.format(DateTimeFormatter.ISO_INSTANT);
                                }
                                //set number of writes that should be done
                                Transaction transaction = new DefaultTransaction("batchWriteTransaction");
                                barrier.await();
                                try {
                                    featureStore.setTransaction(transaction);
                                    featureStore.addFeatures(DataUtilities.collection(features));
                                    transaction.commit();
                                } catch (Exception e) {
                                    transaction.rollback();
                                    e.printStackTrace();
                                } finally {
                                    transaction.close();
                                }
                                //don't create a read filter,    but instead write to the database
                            }catch(Exception e){
                                System.out.println("Failed to write to the database");
                        }
                        case "surrounding":
                            try {
                                lat = Math.floor(Math.random() * (52.58061313-52.42922077))+52.42922077;
                                lon = Math.floor(Math.random() * (13.50421822-13.22833252))+13.22833252;
                                filter= CQL.toFilter("DWITHIN(geom, POINT("+lat+" "+lon+"), 2, kilometers)");
                            }catch(Exception e){
                                System.out.println("Failed to set filter");
                            }
                        case "ride_traffic":
                            //requires multiple queries
                            //generate random number between 0 and 300
                            Integer ride_id = (int) Math.floor(Math.random() * 300);
                            try {
                                filter= CQL.toFilter("ride_id = "+ride_id);
                            }catch(Exception e){
                                System.out.println("Failed to set filter");
                            }
                            //run query here, and then build second filter
                        // case "intersections":
                        //     try {
                        //         filter= CQL.toFilter("");
                        //     }catch(Exception e){
                        //         System.out.println("Failed to set filter");
                        //     }
                        case "bounding_box":
                            try {
                                filter= CQL.toFilter("BBBOX(geom, 13.22833252, 52.42922077, 13.50421822, 52.58061313, 'EPSG:4326')");
                            }catch(Exception e){
                                System.out.println("Failed to set filter");
                            }
                        case "polygonal_area":
                            try {
                                filter= CQL.toFilter("WITHIN(geom, POLYGON((13.22833252 52.42922077, 13.50421822 52.42922077, 13.50421822 52.58061313, 13.22833252 52.58061313, 13.22833252 52.42922077)))");
                            }catch(Exception e){
                                System.out.println("Failed to set filter");
                            }
                        //temporal
                        case "time_slice":
                            startTime = "2023-07-01T00:00:00Z";
                            endTime = "2023-07-01T23:59:59Z";
                            try {
                                filter= CQL.toFilter("timestamp BETWEEN "+startTime+" AND "+endTime);
                            }catch(Exception e){
                                System.out.println("Failed to set filter");
                            }
                        case "interval_around_timestamp":
                            midTime = "2023-07-01T12:00:00Z";
                            //set interval around timestamp to 1 hour, i.e. 3600000ms

                            String interval = "3600000"; 
                            try {
                                filter= CQL.toFilter("timestamp BETWEEN "+midTime+"-"+interval+" AND "+midTime+"+"+interval);
                            }catch(Exception e){
                                System.out.println("Failed to set filter");
                            }
                        //same functionality, can be potentially taken out?
                        // case "relative_time":
                        //     try {
                        //         filter= CQL.toFilter("");
                        //     }catch(Exception e){
                        //         System.out.println("Failed to set filter");
                        //     }
                        //     case "recurring":
                        //     try {
                        //         filter= CQL.toFilter("");
                        //     }catch(Exception e){
                        //         System.out.println("Failed to set filter");
                        //     }
                        // //spatiotemporal
                        case "get_trip":
                            try {
                                filter= CQL.toFilter("ride_id = "+(int) Math.floor(Math.random() * 300));
                            }catch(Exception e){
                                System.out.println("Failed to set filter");
                            }
                        //not possible within the current point data schema, would require programming outside of the query
                        case "get_trip_length":
                            try {
                                filter= CQL.toFilter("");
                            }catch(Exception e){
                                System.out.println("Failed to set filter");
                            }
                        //get first point of trip and last point of trip, calculate time difference
                        case "get_trip_duration":
                            try {
                                filter= CQL.toFilter("");
                            }catch(Exception e){
                                System.out.println("Failed to set filter");
                            }
                        //requires programming outside of the query to calculate speed
                        case "get_trip_speed":
                            try {
                                filter= CQL.toFilter("");
                            }catch(Exception e){
                                System.out.println("Failed to set filter");
                            }
                        case "get_trips_in_time_and_area":
                            startTime = "2023-07-01T00:00:00Z";
                            endTime = "2023-07-01T23:59:59Z";
                            lat = Math.floor(Math.random() * (52.58061313-52.42922077))+52.42922077;
                            lon = Math.floor(Math.random() * (13.50421822-13.22833252))+13.22833252;
                            try {
                                filter= CQL.toFilter("timestamp BETWEEN "+startTime+" AND "+endTime+" AND DWITHIN(geom, POINT("+lat+" "+lon+"), 2, kilometers)");
                            }catch(Exception e){
                                System.out.println("Failed to set filter");
                            }
                        //attribute
                        case "value":
                            //generate rider id between 0 and 30
                            Integer rider_id = (int) Math.floor(Math.random() * 30);
                            try {
                                filter= CQL.toFilter("rider_id = "+rider_id);
                            }catch(Exception e){
                                System.out.println("Failed to set filter");
                            }
                        case "value_threshold":
                            //generate rider id between 0 and 30
                            rider_id = (int) Math.floor(Math.random() * 30);
                            try {
                                filter= CQL.toFilter("rider_id <= "+rider_id);
                            }catch(Exception e){
                                System.out.println("Failed to set filter");
                            }
                        //complex, i.e value and spatiotemporal
                        case "value_and_spatiotemporal":
                            rider_id = (int) Math.floor(Math.random() * 30);
                            startTime = "2023-07-01T00:00:00Z";
                            endTime = "2023-07-01T23:59:59Z";
                            lat = Math.floor(Math.random() * (52.58061313-52.42922077))+52.42922077;
                            lon = Math.floor(Math.random() * (13.50421822-13.22833252))+13.22833252;
                            try {
                                filter= CQL.toFilter("rider_id = "+rider_id+" AND timestamp BETWEEN "+startTime+" AND "+endTime+" AND DWITHIN(geom, POINT("+lat+" "+lon+"), 2, kilometers)");
                            }catch(Exception e){
                                System.out.println("Failed to set filter");
                            }
                    }
                    //run the query
                    for (String name : dataStore.getTypeNames()) {
                        System.out.println("Feature: "+name);
                        SimpleFeatureSource source = dataStore.getFeatureSource(name);
                        //start the timer
                        long start = System.currentTimeMillis();
                        SimpleFeatureCollection allFeatures = source.getFeatures( filter );
                        //end the timer
                        long end = System.currentTimeMillis();
                        long duration = end - start;
                    }
                    //run the query
                    // System.out.println("Filter: "+filter);
                    // System.out.println("Limit: "+limit);
                    // System.out.println("Feature: "+str);
                    // System.out.println("This is the source: "+source);
                    // System.out.println("This is the filter: "+filter);
                    // System.out.println("This is the limit: "+limit);
                    
                }
                dataStore.dispose();
            } catch (IOException e) {
                System.out.println("Error connecting to the datastore");
            }

        // Connecting to the datastore
    
            
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Thread " + threadNumber + " was interrupted.");
        }
    }
}
