package gr.ntua.ece.cslab.selis.bda.kpidb.connectors;

import gr.ntua.ece.cslab.selis.bda.kpidb.beans.*;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class PostgresqlConnector implements Connector{

    private String jdbcURL;
    private String user;
    private String password;
    private Connection connection;

    public PostgresqlConnector(String jdbcURL, String user, String password) {
        this.jdbcURL = jdbcURL;
        this.user = user;
        this.password = password;

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

    @Override
    public void create(KPITable kpi_table) throws Exception {
        Statement st = connection.createStatement();
        List<KeyValue> columns = kpi_table.getKpi_schema().getColumnTypes();
        st.addBatch("DROP TABLE IF EXISTS "+kpi_table.getKpi_name()+";");

        String q="CREATE TABLE " + kpi_table.getKpi_name() + " (id SERIAL PRIMARY KEY, timestamp bigint, ";
        for (KeyValue element : columns){
            q+=element.getKey()+" "+element.getValue();
            q+=",";
        }
        q=q.substring(0, q.length() - 1)+");";
        System.out.println(q);
        st.addBatch(q);
        st.addBatch("ALTER TABLE " + kpi_table.getKpi_name() + " OWNER TO "+ this.user+";");
        st.executeBatch();
        connection.commit();
    }

    @Override
    public void put(KPI kpi) throws Exception {

    }

    @Override
    public List<Tuple> get(String kpi_name, Tuple filters) throws Exception {
        return null;
    }

    @Override
    public List<Tuple> getLast(String kpi_name, Integer n) throws Exception {
        return null;
    }

    @Override
    public KPITable describe(String kpi_name) throws Exception {
        List<String> columnNames = new LinkedList<>();
        List<KeyValue> columnTypes = new LinkedList<>();
        try {
            Statement st = connection.createStatement();
            // Turn use of the cursor on.
            st.setFetchSize(1000);
            ResultSet rs = st.executeQuery("select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = '"+kpi_name+"';");
            while (rs.next()) {
                columnNames.add(rs.getString(1));
                columnTypes.add(new KeyValue(rs.getString(1),rs.getString(2)));
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            connection.rollback();
        }
        return new KPITable(kpi_name, new KPISchema(columnNames, columnTypes));
    }

    @Override
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

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
