package gr.ntua.ece.cslab.selis.bda.datastore;

import java.io.File;
import java.io.InputStream;
import java.util.*;

public class StorageBackendTest {
    public static void main(String[] args) throws Exception {
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
        EventLogFS = "/home/evie/Desktop/output"; // hdfs or hbase
        DimensionTablesFS = "/home/evie/Desktop/output"; // hdfs or postgres

        // List of dimension tables filenames
        ArrayList<String> dimensionTables = new ArrayList<String>();
        dimensionTables.add("bda-datastore/src/test/resources/trucks.csv");
        dimensionTables.add("bda-datastore/src/test/resources/warehouses.json");
        dimensionTables.add("bda-datastore/src/test/resources/RAs.csv");

        // Clean up the two filesystems before testing
        //File temp = new File(EventLogFS);
        //File[] files = temp.listFiles();
        //if (files != null) for (File f : files) f.delete();
        //temp = new File(DimensionTablesFS);
        //files = temp.listFiles();
        //if (files != null) for (File f : files) f.delete();

        // Create a new backend to the BDA
        StorageBackend backend = new StorageBackend(EventLogFS, DimensionTablesFS);

        // Create EventLog and dimension tables
        //backend.init();

        // Create example message for EventLog
        HashMap<String, String> hmap = new HashMap<String, String>();
        hmap.put("warehouse_id", "1");
        hmap.put("latitude", "31.456");
        hmap.put("longitude", "36.542");
        hmap.put("timestamp", "2017-05-02.23:48:57");

        // Insert message in EventLog
        //backend.insert(hmap);

        // Get last message from EventLog
        //System.out.println(Arrays.toString(backend.fetch("rows", 1)));

        // Get messages of last 3 days from Eventlog
        //ELbackend.select("days", 3);

        // Get all messages from EventLog
        //System.out.println(Arrays.toString(backend.fetch("rows", -1)));

        // Get info for specific entities from dimension table
        //System.out.println(Arrays.toString(backend.select("trucks","RA", "AG.072")));

        // Get info for specific entities from EventLog
        //System.out.println(Arrays.toString(backend.select("","warehouse_id", "1")));

        // Print EventLog format
        //System.out.println(Arrays.toString(backend.getSchema("")));

        // Print dimension table format
        System.out.println(backend.getSchema("warehouses").getSchema().getColumnNames());

        // List dimension tables
        System.out.println(Arrays.toString(backend.listTables()));
    }
}
