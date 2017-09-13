package gr.ntua.ece.cslab.selis.bda.datastore;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import gr.ntua.ece.cslab.selis.bda.datastore.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.datastore.connectors.ConnectorFactory;

public class StorageBackend {

    private Connector connector;

    public StorageBackend(String FS) {
        this.connector = ConnectorFactory.getInstance().generateConnector(FS);
    }

    // Create dimension tables
    public void create(List<String> dimensionTables) throws Exception { // get jdbc, json as input too!!!!!!!!!!!!!!!!!!!!!
        // put each table in DimensionTablesFS
        for (String table : dimensionTables) {
            InputStream input = StorageBackend.class.getClassLoader().getResourceAsStream(table);
            if ( input == null )
                throw new Exception("resource not found: " + table);
            connector.put(table);
        }
    }

    public HashMap<String, String> getPrimaryKeys(List<String> dimensionTables) throws Exception {
        HashMap<String, String> hmap = new HashMap<String, String>();
        for (String table : dimensionTables) {
            InputStream input = StorageBackend.class.getClassLoader().getResourceAsStream(table);
            if ( input == null )
                throw new Exception("resource not found: " + table);
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            hmap.put(reader.readLine().split("\t")[0], "");
            reader.close();
        }
        return hmap;
    }

    // Insert row in EventLog
    public void insert(HashMap<String, String> message){
        connector.put(message);
    }

    // Select rows from EventLog
    public HashMap<String, String> select(String type, Integer value) throws Exception {
        if (type.equals("rows"))
            return connector.getLast(value);
        else if (type.equals("days"))
            return connector.getFrom(value);
        else
            throw new Exception("type not found: " + type);
    }

    // Get entry id from dimension table
    public ArrayList<String> fetch(String table, String column, String value){
        return connector.get(table, column, value);
    }

    // function to get all table names and all tables fields!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    public static void main(String[] args) throws Exception { // test!!!!!!!!!! + function to initiate parameters from hashmap with factory for each object!!!!!!!!!!

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
    }
}