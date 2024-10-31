package com.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.geotools.api.data.DataStore;
import org.geotools.api.data.DataStoreFinder;
public class Main {
    public static void main(String[] args) {
        System.out.println("Starting...");
        String instanceName = args[0];
        String username = args[1];
        String password = args[2];
        String catalog = args[3];
        String zookeepers = args[4];
        
        //Check the variables by printing them
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
            // for (String str : dataStore.getTypeNames()) {
            //     System.out.println("Feature: "+str);
            //     SimpleFeatureSource source = dataStore.getFeatureSource(str);
            //     System.out.println("This is the source: "+source);
            //     Filter filter=null;
            //     // try {
            //     //     filter= CQL.toFilter("rider_id = 27");
            //     // }catch(Exception e){
            //     //     System.out.println("Failed to set filter");
            //     // }
            //     System.out.println("This is the filter: "+filter);
            //     SimpleFeatureCollection features = source.getFeatures();
            //     SimpleFeatureCollection limitedFeatures = source.getFeatures(filter);
            //     SimpleFeatureType schema = source.getSchema();
            //     for (AttributeDescriptor descriptor : schema.getAttributeDescriptors()) {
            //        System.out.println(descriptor.getLocalName() + " - " + descriptor.getType().getBinding());
            //     }
            //     int count = features.size();
            //     int count2 = limitedFeatures.size();
            //     System.out.println("Number of selected features:" + count+ " ,"+count2);
            // }
            dataStore.dispose();
        } catch (IOException e) {
            System.out.println("Error connecting to the datastore");
        }

        // Connecting to the datastore
    }
}
