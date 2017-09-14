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

    // Get table names and schema
    public HashMap<String, String> getSchema(String table){
        return connector.describe(table);
    }

}