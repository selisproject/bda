package gr.ntua.ece.cslab.selis.bda.datastore;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

import gr.ntua.ece.cslab.selis.bda.datastore.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.datastore.connectors.ConnectorFactory;

public class StorageBackend {

    private Connector connector;

    public StorageBackend(String FS) {
        this.connector = ConnectorFactory.getInstance().generateConnector(FS);
    }

    // Create dimension tables
    public void create(ArrayList<String> dimensionTables) throws Exception { // get jdbc, json as input too!!!!!!!!!!!!!!!!!!!!!
        for (String table : dimensionTables)
            connector.put(table);
    }

    // Initialize eventLog
    public void init(ArrayList<String> dimensionTables) throws Exception {
        // create empty message as a hashmap whose field names are the first columns of the dimension tables and save it
        HashMap<String, String> hmap = new HashMap<String, String>();

        for (String table : dimensionTables) {
            InputStream input = StorageBackend.class.getClassLoader().getResourceAsStream(table);
            if ( input == null )
                throw new Exception("resource not found: " + table);
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            // save in a hashmap first column's name with empty content
            hmap.put(reader.readLine().split("\t")[0], "");
            reader.close();
        }
        // put empty message in EventLog
        connector.put(hmap);
    }

    // Insert row in EventLog
    public void insert(HashMap<String, String> message) throws IOException {
        connector.put(message);
    }

    // Select rows from EventLog
    public HashMap<String, String>[] select(String type, Integer value) throws Exception {
        if (type.equals("rows"))
            return connector.getLast(value);
        else if (type.equals("days"))
            return (HashMap<String, String>[]) connector.getFrom(value).toArray();
        else
            throw new Exception("type not found: " + type);
    }

    // Get entry from dimension table
    public HashMap<String, String> fetch(String table, String column, String value) throws IOException {
        return connector.get(table, column, value);
    }

    // Get table schema
    public String[] getSchema(String table) throws IOException {
        return connector.describe(table);
    }

}