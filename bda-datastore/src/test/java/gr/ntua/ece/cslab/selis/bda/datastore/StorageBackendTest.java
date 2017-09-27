package gr.ntua.ece.cslab.selis.bda.datastore;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

public class StorageBackendTest {
    public static void main(String[] args) throws Exception { // + function to initiate parameters from hashmap with factory for each object
        String EventLogFS;
        String DimensionTablesFS;

        /*Properties prop = new Properties();
        String filename = "datastore.properties";
        InputStream input = StorageBackendTest.class.getClassLoader().getResourceAsStream(filename);
        if (input == null) {
            System.out.println("Sorry, unable to find " + filename);
            return;
        }

        // load a properties file from class path, inside static method
        prop.load(input);

        // get the property value and print it out
        EventLogFS = new String(prop.getProperty("EventLogFS"));
        DimensionTablesFS = new String(prop.getProperty("DimensionTablesFS"));*/

        // Where are the event log and dimension tables stored
        EventLogFS = "bda-datastore/src/test/resources/output"; // hdfs or hbase
        DimensionTablesFS = "bda-datastore/src/test/resources/output"; // hdfs or postgres

        // List of dimension tables filenames
        ArrayList<String> dimensionTables = new ArrayList<String>();
        dimensionTables.add("bda-datastore/src/test/resources/trucks.csv");
        dimensionTables.add("bda-datastore/src/test/resources/warehouses.json");
        dimensionTables.add("bda-datastore/src/test/resources/RAs.csv");

        // Clean up the two filesystems before testing
        File temp = new File(EventLogFS);
        File[] files = temp.listFiles();
        if (files != null) for (File f : files) f.delete();
        temp = new File(DimensionTablesFS);
        files = temp.listFiles();
        if (files != null) for (File f : files) f.delete();

        // Create two new backends
        StorageBackend ELbackend = new StorageBackend(EventLogFS);
        StorageBackend DTbackend = new StorageBackend(DimensionTablesFS);

        // Create dimension tables
        DTbackend.create(dimensionTables);

        // Create EventLog
        ELbackend.init(dimensionTables);

        // Create example message for EventLog
        HashMap<String, String> hmap = new HashMap<String, String>();
        hmap.put("truck_platenr", "ZPO-3395");
        hmap.put("warehouse_id", "null");
        hmap.put("RA","AG.140");
        hmap.put("message","{latitude: 31.456, longitude: 36.542, timestamp: 2017-05-02.23:48:57}");

        // Insert message in EventLog
        ELbackend.insert(hmap);

        // Get last message from EventLog
        System.out.println(Arrays.toString(ELbackend.select("rows", 1)));

        // Get messages of last 3 days from Eventlog
        //ELbackend.select("days", 3);

        // Get all messages from EventLog
        System.out.println(Arrays.toString(ELbackend.select("rows", -1)));

        // Get info for specific entity from dimension table
        System.out.println(Arrays.toString(DTbackend.fetch("trucks","RA", "AG.072")));

        // Print EventLog format
        System.out.println(Arrays.toString(ELbackend.getSchema("")));

        // Print dimension table format
        System.out.println(Arrays.toString(DTbackend.getSchema("trucks")));
    }
}
