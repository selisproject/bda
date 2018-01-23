package gr.ntua.ece.cslab.selis.bda.datastore;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;
import gr.ntua.ece.cslab.selis.bda.datastore.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.datastore.connectors.ConnectorFactory;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class StorageBackend {

    private Connector ELconnector;
    private Connector DTconnector;

    /** The StorageBackend constructor creates two new connections, one for the EventLog FS and one for the Dimension
     *  tables FS, using the FS parameters that are provided as input Strings. **/
    public StorageBackend(String EventLogFS, String DimensionTablesFS, String dbUsername, String dbPassword) {
        this.ELconnector = ConnectorFactory.getInstance().generateConnector(EventLogFS, dbUsername, dbPassword);
        this.DTconnector = ConnectorFactory.getInstance().generateConnector(DimensionTablesFS, dbUsername, dbPassword);
    }

    /** Initialize the eventLog and dimension tables in the underlying FS Using the masterData.
     *  MasterData include in json format for all the dimension tables the name, schema, data and the primary key of
     *  each table. Using the primary keys of the dimension tables the eventLog is created with these foreign keys
     *  and an extra column named 'message' is created in the eventLog that contains the actual message (that will
     *  be in json format). **/
    public void init(MasterData masterData) throws Exception {
        DTconnector.put(masterData);
        List<KeyValue> columns = new LinkedList<>();
        for (DimensionTable table: masterData.getTables()) {
            String key = table.getSchema().getPrimaryKey();
            String type = "";
            for (KeyValue column : table.getSchema().getColumnTypes())
                if (column.getKey().matches(key))
                    type = column.getValue();
            KeyValue field = new KeyValue(table.getName()+"_"+key, type);
            columns.add(field);
        }
        Message emptyMsg = new Message(new LinkedList<>(), columns);
        ELconnector.put(emptyMsg);
    }

    /** Insert a new message in the EventLog.
     *  This method takes as input a Message and saves each key that matches with an EventLog column name in the
     *  relevant column of the eventLog table, while all the non-matching keys are saved as a blob in json format
     *  in the 'message' column of the eventLog table. **/
    public void insert(Message message) throws Exception {
        ELconnector.put(message);
    }

    /** Get rows from EventLog. Fetches either the last n messages or the messages received the last n days.
     *  This method requires as input a string that denotes 'days' or 'rows' and an integer that denotes the
     *  number n. **/
    public List<Tuple> fetch(String type, Integer value) throws Exception {
        if (type.equals("rows"))
            return ELconnector.getLast(value);
        else if (type.equals("days"))
            return ELconnector.getFrom(value);
        else
            throw new Exception("type not found: " + type);
    }

    /** Select rows filtered in a specific column with a specific value from a dimension table.
     *  This method requires as input a string which is the dimension table name, the column name and the column value
     *  as strings. **/
    public List<Tuple> select(String table, String column, String value) throws Exception {
        return DTconnector.get(table, column, value);
    }

    /** Select rows filtered in a specific column with a specific value from the eventLog table.
     *  This method requires as input the column name and the column value as strings. The eventLog can be filtered in
     *  a column that is a foreign key to a dimension table, not in the actual message and the last 1000 messages are
     *  searched.**/
    public List<Tuple> select(String column, String value) throws Exception {
        return ELconnector.get("", column, value);
    }

    /** Get table schema.
     *  This method takes as input a string which is the dimension table name or an empty string if it refers to
     *  the eventLog table. **/
    public DimensionTable getSchema(String table) throws Exception {
        if (table.matches(""))
            return ELconnector.describe(table);
        else
            return DTconnector.describe(table);
    }

    /** List dimension tables. **/
    public List<String> listTables() {
            return DTconnector.list();
    }

    public void close(){
        DTconnector.close();
        ELconnector.close();
    }

}