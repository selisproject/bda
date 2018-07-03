package gr.ntua.ece.cslab.selis.bda.common.storage.connectors;

import java.sql.*;

public class PostgresqlConnector implements Connector {

    private String jdbcURL;
    private String username;
    private String password;
    private Connection connection;

    public PostgresqlConnector(){}

    // The method creates a connection to the database provided in the 'jdbcURL' parameter.
    // The database should be up and running.
    public PostgresqlConnector(String JdbcURL, String Username, String Password){
        this.jdbcURL = JdbcURL;
        this.username = Username;
        this.password = Password;

        try {
            Class.forName("org.postgresql.Driver");

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }
        System.out.println("PostgreSQL JDBC Driver Registered!");

        try {
            connection = DriverManager.getConnection(jdbcURL, username, password);
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

    public Connection getConnection() {
        return connection;
    }

    public void close(){
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
