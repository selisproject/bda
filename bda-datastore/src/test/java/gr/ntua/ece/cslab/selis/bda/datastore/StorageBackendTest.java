package gr.ntua.ece.cslab.selis.bda.datastore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StorageBackendTest {
    public static void main(String[] args) throws Exception { // function to initiate parameters from hashmap with factory for each object!!!!!!!!!!

        String EventLogFS = "hdfs://master:9000/"; // hdfs or hbase
        String DimensionTablesFS = "hdfs2://master:9000/"; // hdfs or postgres
        StorageBackend ELbackend = new StorageBackend(EventLogFS);
        StorageBackend DTbackend = new StorageBackend(DimensionTablesFS);

        List<String> dimensionTables = new ArrayList<String>();
        dimensionTables.add("trucks.csv");
        dimensionTables.add("warehouses.csv");
        dimensionTables.add("RAs.csv");
        ELbackend.create(dimensionTables);

        HashMap<String, String> ELkeys = ELbackend.getPrimaryKeys(dimensionTables);
        ELbackend.insert(ELkeys);

        HashMap<String, String> hmap = new HashMap<String, String>();
        hmap.put("key", "value");
        ELbackend.insert(hmap);
        ELbackend.select("rows", 3);
        ELbackend.select("days", 3);
        ELbackend.select("rows", -1);
        DTbackend.fetch("trucks","truck_platenr", "X1423");

        ELbackend.getSchema("");
        DTbackend.getSchema("trucks");
    }
}
