package gr.ntua.ece.cslab.selis.bda.datastore;

import java.io.*;
import java.util.*;

import gr.ntua.ece.cslab.selis.bda.datastore.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.datastore.connectors.ConnectorFactory;

public class StorageBackend {

    private Connector connector;

    /** The StorageBackend constructor creates a new connection with the FS that is provided as an input String. **/
    public StorageBackend(String FS) {
        this.connector = ConnectorFactory.getInstance().generateConnector(FS);
    }

    /** Create and populate the dimension tables in the underlying FS.
     *  This method requires as input a list of Strings that are the full paths to the files containing the
     *  tables master data. Each table must have a .csv or .json file named after the table name that has in the
     *  first line the column names and the rest lines are the master data. The name of the first column must be
     *  the primary key for the table that will be used as a foreign key in the eventLog. **/
    public void create(List<String> dimensionTables) throws Exception { // get jdbc as input too!!!!!!!!
        for (String table : dimensionTables)
            connector.put(table);
    }

    /** Initialize the eventLog table in the underlying FS.
     *  This method requires as input a set of Strings that are the EventLog column names.
     *  These columns are essentially foreign keys to dimension tables columns. Except of these columns, an extra
     *  column named 'message' is created in the eventLog that contains the actual message (that will be in json
     *  format). **/
    public void init(Set<String> columns) throws Exception {
        HashMap<String, String> hmap = new HashMap<String, String>();
        for(String entry : columns)
            hmap.put(entry, "");
        connector.put(hmap);
    }

    /** Insert a new message in the EventLog.
     *  This method takes as input a message as a hashmap (HashMap<String, String>) and saves each key that matches
     *  with an EventLog column name in the relevant column of the eventLog table, while all the non-matching keys
     *  are saved as a blob in json format in the 'message' column of the eventLog table. **/
    public void insert(HashMap<String, String> message) throws IOException {
        connector.put(message);
    }

    /** Get rows from EventLog. Fetches either the last n messages or the messages received the last n days.
     *  This method requires as input a string that denotes 'days' or 'rows' and an integer that denotes the
     *  number n. It returns an array of hashmaps (HashMap<String, String>[]) where each hashmap corresponds to
     *  a message that its keys are the eventLog columns. **/
    public HashMap<String, String>[] fetch(String type, Integer value) throws Exception {
        if (type.equals("rows"))
            return connector.getLast(value);
        else if (type.equals("days")){
            ArrayList<HashMap<String, String>> res = connector.getFrom(value);
            return res.toArray(new HashMap[0]);
        }
        else
            throw new Exception("type not found: " + type);
    }

    /** Select rows filtered in a specific column with a specific value from a table.
     *  This method requires as input a string which is the dimension table name or an empty string if it refers to
     *  the eventLog table, the column name and the column value as strings. The eventLog can be filtered in a column
     *  that is a foreign key to a dimension table, not in the actual message and the last 1000 messages are searched.
     *  It returns an array of hashmaps (HashMap<String, String>[]) where each hashmap corresponds to
     *  a row that its keys are the table columns. **/
    public HashMap<String, String>[] select(String table, String column, String value) throws Exception {
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