package com.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFinder;
import org.geotools.api.data.SimpleFeatureSource;
import org.geotools.api.feature.simple.SimpleFeatureType;
import org.geotools.api.feature.type.AttributeDescriptor;
import org.geotools.api.filter.Filter;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.filter.text.cql2.CQL;

public class Main {

    public static void runThreads(String instanceName, String username, String password, String catalog, String zookeepers,String queryType, Integer threadCount, Integer limit)
    {
        // Create threads
        CountDownLatch startLatch = new CountDownLatch(1); // Latch to control start of all threads

        for (int i = 0; i < threadCount; i++) {
            Thread thread = new Thread(new Task(i,instanceName, username, password, catalog, zookeepers, queryType, limit, startLatch));
            thread.start();
        }

        // Start threads
        System.out.println("All threads ready. Starting tasks in parallel...");

        startLatch.countDown(); // Release all threads to start simultaneously
        // Wait for threads to finish

    }    //
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
        Integer limit = Integer.parseInt(args[7]);
        
        //if arg count is less than 8, then exit
       
        runThreads(instanceName, username, password, catalog, zookeepers, queryType, threadCount, limit);
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


    public Task(int threadNumber, String instanceName, String username, String password, String catalog, String zookeepers, String queryType, Integer limit, CountDownLatch startLatch) {
        this.threadNumber = threadNumber;
        this.instanceName = instanceName;
        this.username = username;
        this.password = password;
        this.catalog = catalog;
        this.zookeepers = zookeepers;
        this.queryType = queryType;
        this.limit = limit;
        this.startLatch = startLatch;
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
                System.out.println("Connected to the datastore "+dataStore);
                for (String str : dataStore.getTypeNames()) {
                    System.out.println("Feature: "+str);
                    SimpleFeatureSource source = dataStore.getFeatureSource(str);
                    System.out.println("This is the source: "+source);
                    Filter filter=null;
                    Filter filter2=null;
                    
                    try {
                        filter= CQL.toFilter("rider_id <= 27");
                        filter2 = CQL.toFilter("rider_id <= 5");
                    }catch(Exception e){
                        System.out.println("Failed to set filter");
                    }
                    System.out.println("This is the filter: "+filter);
                    SimpleFeatureCollection allFeatures = source.getFeatures( filter2 );
                    SimpleFeatureCollection limitedFeatures = source.getFeatures( filter );
                    SimpleFeatureType schema = source.getSchema();
                    for (AttributeDescriptor descriptor : schema.getAttributeDescriptors()) {
                    System.out.println(descriptor.getLocalName() + " - " + descriptor.getType().getBinding());
                    }
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
