package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;
import org.json.simple.JSONObject;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class PostgresqlConnector implements Connector {

    private String jdbcURL;
    private String user;
    private String password;
    private Connection connection;

    // The constructor creates a connection to the database provided in the 'jdbcURL' parameter.
    // The database should be up and running.
    public PostgresqlConnector(String jdbcURL, String Username, String Password){
        this.jdbcURL = jdbcURL;
        this.user = Username;
        this.password = Password;
        try {
            Class.forName("org.postgresql.Driver");

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("PostgreSQL JDBC Driver Registered!");

        try {
            connection = DriverManager.getConnection(jdbcURL, user, password);
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;
        }
        if (connection == null) {
            System.out.println("Failed to make connection!");
        }

        // make sure autocommit is off
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // Used to initialize or append a message in the EventLog
    public void put(Message row) throws Exception {
        try {
            DatabaseMetaData dbm = connection.getMetaData();
            ResultSet rs = dbm.getTables(null, null, "Events", null);
            if (rs.next()) {
                // Table exists
                JSONObject json = new JSONObject(); // to store blob
                List<String> fields = this.describe("").getSchema().getColumnNames();
                HashMap<String, String> message = new HashMap<>();
                for (KeyValue element : row.getEntries()) {
                    if (!fields.contains(element.getKey()))
                        json.put(element.getKey(), element.getValue());
                    else
                        message.put(element.getKey(), element.getValue());
                }
                for (String column : fields)
                    if (!message.containsKey(column))
                        message.put(column, "null");
                message.put("message", json.toJSONString());
                message.put("event_timestamp", String.valueOf(LocalDateTime.now()));

                if (message.containsKey("message") && message.containsKey("event_type") && message.size() == 3)
                    throw new Exception("Message does not contain any foreign keys.");
                else if (json.isEmpty() || !message.containsKey("event_type") || message.size() < 3)
                    throw new Exception("Message contains strange event format. Append aborted.");

                List<KeyValue> columns = new LinkedList<>(); // TODO: FIND EVENTLOG COLUMNS
                String values = "";
                String insertTableSQL = "INSERT INTO Events (";
                for (String field : fields) {
                    insertTableSQL += field + ",";
                    values += "?,";
                }
                insertTableSQL = insertTableSQL.substring(0, insertTableSQL.length() - 1) + ") VALUES (" + values.substring(0, values.length() - 1) + ");";

                PreparedStatement prepst = connection.prepareStatement(insertTableSQL);
                int i = 1;
                for (String field : fields){
                    for (KeyValue value : columns) {
                        if (value.getKey().equals(field)) {
                            if (value.getValue().contains("integer"))
                                prepst.setInt(i, Integer.valueOf(message.get(field)));
                            else if (value.getValue().contains("timestamp"))
                                prepst.setTimestamp(i, Timestamp.valueOf(message.get(field)));
                            else if (value.getValue().contains("bytea"))
                                prepst.setBytes(i, message.get(field).getBytes());
                            else if (value.getValue().contains("boolean"))
                                prepst.setBoolean(i, Boolean.parseBoolean(message.get(field)));
                            else
                                prepst.setString(i, message.get(field));
                        }
                    }
                    i++;
                }
                prepst.executeUpdate();
            }
            else {
                // Table does not exist
                Statement st = connection.createStatement();
                String q="CREATE TABLE Events (";
                for (KeyValue fields : row.getEntries()){
                    q += fields.getKey()+" "+fields.getValue()+",";
                }
                // add one more column named 'message' that will contain the blob
                q+="event_type TEXT, event_timestamp timestamp(3), message TEXT);"; //BYTEA
                System.out.println(q);
                st.executeUpdate(q);
                st.executeUpdate("ALTER TABLE Events OWNER TO "+ this.user+";");
            }
            connection.commit();
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            connection.rollback();
        }
    }

    // Create dimension table and populate it
    public void put(MasterData masterData) throws Exception {
        try {
            Statement st = connection.createStatement();
            for (DimensionTable table: masterData.getTables()) {
                List<KeyValue> columns = table.getSchema().getColumnTypes();
                String primaryKey = table.getSchema().getPrimaryKey();
                st.addBatch("DROP TABLE IF EXISTS "+table.getName()+";");

                String q="CREATE TABLE " + table.getName() + " (";
                for (KeyValue element : columns){
                    q+=element.getKey()+" "+element.getValue();
                    if (element.getKey().equals(primaryKey))
                        q+=" PRIMARY KEY";
                    q+=",";
                }
                q=q.substring(0, q.length() - 1)+");";
                System.out.println(q);
                st.addBatch(q);
                st.addBatch("ALTER TABLE " + table.getName() + " OWNER TO "+ this.user+";");
                st.executeBatch();

                // fill-in column values
                List<Tuple> data = table.getData();
                if (data.size()>0) {
                    String values = "";
                    String insertTableSQL = "INSERT INTO " + table.getName() + " (";
                    for (KeyValue element : data.get(0).getTuple()) {
                        insertTableSQL += element.getKey() + ",";
                        values += "?,";
                    }
                    insertTableSQL = insertTableSQL.substring(0, insertTableSQL.length() - 1) + ") VALUES (" + values.substring(0, values.length() - 1) + ");";

                    PreparedStatement prepst = connection.prepareStatement(insertTableSQL);
                    for (Tuple tuple : data) {
                        int i = 1;
                        for (KeyValue element : tuple.getTuple()) {
                            for (KeyValue field : columns) {
                                if (field.getKey().equals(element.getKey())) {
                                    if (field.getValue().contains("integer"))
                                        if (element.getValue().equalsIgnoreCase("null"))
                                            prepst.setNull(i,Types.INTEGER);
                                        else
                                            prepst.setInt(i, Integer.valueOf(element.getValue()));
                                    else if (field.getValue().contains("timestamp"))
                                        prepst.setTimestamp(i, Timestamp.valueOf(element.getValue()));
                                    else if (field.getValue().contains("bytea"))
                                        prepst.setBytes(i, element.getValue().getBytes());
                                    else if (field.getValue().contains("boolean"))
                                        prepst.setBoolean(i, Boolean.parseBoolean(element.getValue()));
                                    else
                                        prepst.setString(i, element.getValue());
                                }
                            }
                            i++;
                        }
                        prepst.addBatch();
                    }
                    prepst.executeBatch();
                }
                connection.commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            connection.rollback();
        }
    }

    // get last num rows from EventLog
    public List<Tuple> getLast(Integer num) throws Exception {
        List<Tuple> res = new LinkedList<>();
        try {
            Statement st = connection.createStatement();
            // Turn use of the cursor on.
            st.setFetchSize(1000);
            ResultSet rs = st.executeQuery("SELECT * FROM Events order by event_timestamp desc limit "+num+";");
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                List<KeyValue> entries = new LinkedList<>();
                for (int i = 1; i <= columnsNumber; i++) {
                    String columnValue = rs.getString(i);
                    if (!columnValue.equalsIgnoreCase("null") && !columnValue.matches(""))
                        entries.add(new KeyValue(rsmd.getColumnName(i), columnValue));
                }
                res.add(new Tuple(entries));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            connection.rollback();
        }
        return res;
    }

    public List<Tuple> getFrom(Integer args){
        System.out.println("get from PostgreSQL " + jdbcURL);
        return null;
    }

    // Get rows matching a specific column filter from a table
    public List<Tuple> get(String table, String column, String value) throws Exception {
        List<Tuple> res = new LinkedList<>();
        if (column.equals("message") && table.matches(""))
            throw new Exception("Cannot filter the raw message in the eventLog.");

        try {
            Statement st = connection.createStatement();
            // Turn use of the cursor on.
            st.setFetchSize(1000);
            ResultSet rs = st.executeQuery("SELECT * FROM "+table+" WHERE cast("+column+" as text) ='"+value+"';");
            ResultSetMetaData rsmd = rs.getMetaData();
            int columnsNumber = rsmd.getColumnCount();
            while (rs.next()) {
                List<KeyValue> entries = new LinkedList<>();
                for (int i = 1; i <= columnsNumber; i++) {
                    String columnValue = rs.getString(i);
                    entries.add(new KeyValue(rsmd.getColumnName(i), columnValue));
                }
                res.add(new Tuple(entries));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            connection.rollback();
        }
        return res;
    }

    // get column names and types for table 'args'
    public DimensionTable describe(String args) throws Exception {
        if (args.matches(""))
            args = "Events";
        List<String> columnNames = new LinkedList<>();
        List<KeyValue> columnTypes = new LinkedList<>();
        try {
            Statement st = connection.createStatement();
            // Turn use of the cursor on.
            st.setFetchSize(1000);
            ResultSet rs = st.executeQuery("select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = '"+args+"';");
            while (rs.next()) {
                columnNames.add(rs.getString(1));
                columnTypes.add(new KeyValue(rs.getString(1),rs.getString(2)));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            connection.rollback();
        }
        return new DimensionTable(args,
                new DimensionTableSchema(columnNames, columnTypes, ""),
                new LinkedList<>());
    }

    // List dimension tables in database
    public List<String> list() {
        List<String> tables = new LinkedList<>();
        try {
            DatabaseMetaData dbm = connection.getMetaData();
            ResultSet rs = dbm.getTables(null, null, "%", new String[] {"TABLE"});
            while (rs.next())
                if (!rs.getString("TABLE_NAME").equalsIgnoreCase("Events"))
                    tables.add(rs.getString("TABLE_NAME"));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return tables;
    }

    public void close(){
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    };
}
