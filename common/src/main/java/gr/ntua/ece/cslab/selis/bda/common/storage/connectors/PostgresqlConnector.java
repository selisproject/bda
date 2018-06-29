package gr.ntua.ece.cslab.selis.bda.common.storage.connectors;

import java.sql.*;

public class PostgresqlConnector implements Connector {

    private String jdbcURL;
    private String user;
    private String password;
    private Connection connection;

    public PostgresqlConnector(String jdbcURL, String Username, String Password){
        this.jdbcURL = jdbcURL;
        this.user = Username;
        this.password = Password;
    }

    // The method creates a connection to the database provided in the 'jdbcURL' parameter.
    // The database should be up and running.
    public void init() {
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

    public void close(){
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
