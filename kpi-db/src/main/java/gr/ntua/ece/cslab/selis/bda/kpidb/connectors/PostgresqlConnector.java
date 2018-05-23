package gr.ntua.ece.cslab.selis.bda.kpidb.connectors;

import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPI;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPISchema;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.Tuple;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
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
    public void create(KPISchema kpi_schema) throws Exception {

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
    public KPISchema describe(String kpi_name) throws Exception {
        return null;
    }

    @Override
    public List<String> list() {
        return null;
    }

    @Override
    public void close() {

    }
}
