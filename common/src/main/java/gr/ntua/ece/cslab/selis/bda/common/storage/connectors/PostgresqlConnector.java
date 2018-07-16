package gr.ntua.ece.cslab.selis.bda.common.storage.connectors;

import java.sql.*;

public class PostgresqlConnector implements Connector {

    private String jdbcURL;
    private String username;
    private String password;
    private Connection connection;

    private final static String CHECK_DATABASE_QUERY = "select count(*) as counter from pg_catalog.pg_database where datname = '%s';";

    private final static String CREATE_DATABASE_QUERY = "CREATE DATABASE %s WITH OWNER %s;";

    private final static String DROP_DATABASE_QUERY = "DROP DATABASE %s;";

    private final static String CREATE_DATABASE_SCHEMA_QUERY = "CREATE SCHEMA %s AUTHORIZATION %s;";
 
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

    public static String createDatabase(String jdbcUrl, String username, String password, 
                                        String owner, String dbname) throws SQLException {
        Connection localConnection = null;

        String postgresTemplateUrl = jdbcUrl + "template1";

        try {
            localConnection = DriverManager.getConnection(postgresTemplateUrl, username, password);
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            throw e;
        }

        PreparedStatement statement = localConnection.prepareStatement(
                String.format(CHECK_DATABASE_QUERY, dbname));

        ResultSet rs = statement.executeQuery();

        if (rs.next()){
            if (rs.getInt("counter")==1){
                localConnection.close();
                return jdbcUrl + dbname;
            }
        }
        statement = localConnection.prepareStatement(
                String.format(CREATE_DATABASE_QUERY, dbname, owner));

        statement.executeUpdate();

        localConnection.close();

        return jdbcUrl + dbname;
    }

    public static void createSchema(String jdbcUrl, String username, String password, 
                                    String owner, String schema) throws SQLException {
        Connection localConnection = null;

        try {
            localConnection = DriverManager.getConnection(jdbcUrl, username, password);
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            throw e;
        }

        PreparedStatement statement = localConnection.prepareStatement(
            String.format(CREATE_DATABASE_SCHEMA_QUERY, schema, owner));

        statement.executeUpdate();

        localConnection.close();
    }

    public static void dropDatabase(String jdbcUrl, String username, String password,
                        String owner, String dbname) throws SQLException {
        Connection localConnection = null;

        String postgresTemplateUrl = jdbcUrl + "template1";

        try {
            localConnection = DriverManager.getConnection(postgresTemplateUrl, username, password);
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            throw e;
        }

        PreparedStatement statement = localConnection.prepareStatement(
                String.format(DROP_DATABASE_QUERY, dbname));

        statement.executeUpdate();

        localConnection.close();
    }

    public Connection getConnection() {
        return connection;
    }

    public String getUsername() {
        return username;
    }

    public void close(){
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
