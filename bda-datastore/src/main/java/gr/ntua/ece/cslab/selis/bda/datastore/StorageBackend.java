package gr.ntua.ece.cslab.selis.bda.datastore;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

import gr.ntua.ece.cslab.selis.bda.datastore.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.datastore.connectors.ConnectorFactory;

public class StorageBackend {

    private Connector connector;

    /** The StorageBackend constructor creates a new connection with the FS that is provided as an input String. **/
    public StorageBackend(String FS) {
        this.connector = ConnectorFactory.getInstance().generateConnector(FS);
    }

    /** Create and populate the dimension tables in the underlying FS.
     *  This method requires as input a list of strings (ArrayList<String>) that are the full paths to the files
     *  containing the tables master data. Each table must have a .csv or .json file named after the table name
     *  that has in the first line the column names and the rest lines are the master data. The name of the first
     *  column must be the primary key for the table that will be used as a foreign key in the eventLog. **/
    public void create(ArrayList<String> dimensionTables) throws Exception { // get jdbc as input too!!!!!!!!
        for (String table : dimensionTables)
            connector.put(table);
    }

    /** Initialize the eventLog table in the underlying FS.
     *  This method requires as input a list of strings (ArrayList<String>) that are the full paths to the files
     *  containing the dimension tables master data. Each table must have a .csv or .json file named after the
     *  table name that has in the first line the column names and the first column must be the primary key for
     *  the table that will be used to create a foreign key in the eventLog. Except for the foreign keys to all
     *  dimension tables an extra column is created in the eventLog that contains the actual message in json
     *  format. **/
    public void init(ArrayList<String> dimensionTables) throws Exception {
        // create empty message as a hashmap whose field names are the first columns of the dimension tables and save it
        HashMap<String, String> hmap = new HashMap<String, String>();
        for (String table : dimensionTables) {
            String[] tablename = table.split("\\.")[0].split("/"); // get tablename from properties?
            String[] columns = connector.describe(tablename[tablename.length -1]);
            hmap.put(columns[0], "");
        }
        // put empty message in EventLog
        connector.put(hmap);
    }

    /** Insert a new message in the EventLog.
     *  This method takes as input a message as a hashmap (HashMap<String, String>) that must have as keys all
     *  the column names of the eventLog table. **/
    public void insert(HashMap<String, String> message) throws IOException {
        connector.put(message);
    }

    /** Select rows from EventLog. Selects either the last n messages or the messages received the last n days.
     *  This method requires as input a string that denotes 'days' or 'rows' and an integer that denotes the
     *  number n. It returns an array of hashmaps (HashMap<String, String>[]) where each hashmap corresponds to
     *  a message that its keys are the eventLog columns. **/
    public HashMap<String, String>[] select(String type, Integer value) throws Exception {
        if (type.equals("rows"))
            return connector.getLast(value);
        else if (type.equals("days")){
            ArrayList<HashMap<String, String>> res = connector.getFrom(value);
            return res.toArray(new HashMap[0]);
        }
        else
            throw new Exception("type not found: " + type);
    }

    /** Get rows filtered in a specific column with a specific value from a table.
     *  This method requires as input a string which is the dimension table name or an empty string if it refers to
     *  the eventLog table, the column name and the column value as strings. The eventLog can be filtered in a column
     *  that is a foreign key to a dimension table, not in the actual message and the last 1000 messages are searched.
     *  It returns an array of hashmaps (HashMap<String, String>[]) where each hashmap corresponds to
     *  a row that its keys are the table columns. **/
    public HashMap<String, String>[] fetch(String table, String column, String value) throws Exception {
        if (column.equals("message") && table.matches(""))
            throw new Exception("Cannot filter the raw message in the eventLog.");
        ArrayList<HashMap<String, String>> res = connector.get(table, column, value);
        return res.toArray(new HashMap[0]);
    }

    /** Get table schema.
     *  This method takes as input a string which is the dimension table name or an empty string if it refers to
     *  the eventLog table and returns an array of strings (String[]) that contains the column names of the table. **/
    public String[] getSchema(String table) throws IOException {
        return connector.describe(table);
    }

}