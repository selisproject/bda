package gr.ntua.ece.cslab.selis.bda.common.storage.connectors;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PostgresqlConnector implements Connector {
    static Logger LOGGER = Logger.getLogger(PostgresqlConnector.class.getCanonicalName());

    private String jdbcURL;
    private String username;
    private String password;
    private Connection connection;

    private final static String CREATE_DATABASE_QUERY = "CREATE DATABASE %s WITH OWNER %s;";

    private final static String DROP_DATABASE_QUERY = "DROP DATABASE %s;";

    private final static String CREATE_DATABASE_SCHEMA_QUERY = "CREATE SCHEMA %s AUTHORIZATION %s;";

    private final static String DROP_OPEN_CONNECTIONS_QUERY = "SELECT pg_terminate_backend(pg_stat_activity.pid) FROM "+
            "pg_stat_activity WHERE pg_stat_activity.datname = '%s' AND pid <> pg_backend_pid();";

    /**
     * Creates a new `PostgresqlConnector` instance, checks PostgreSQL availability and initializes a connection.
     * @param JdbcURL       The URL of the BDA db and the SCN's Dimension table database.
     * @param Username      The username to connect to the database.
     * @param Password      The password to connect to the database.
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public PostgresqlConnector(String JdbcURL, String Username, String Password) throws SQLException, ClassNotFoundException {
        this.jdbcURL = JdbcURL;
        this.username = Username;
        this.password = Password;

        try {
            Class.forName("org.postgresql.Driver");

        } catch (ClassNotFoundException e) {
            throw e;
        }
        LOGGER.log(Level.INFO, "PostgreSQL JDBC Driver Registered!");

        try {
            connection = DriverManager.getConnection(jdbcURL, username, password);
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Connection Failed! Check output console");
            throw e;
        }
        if (connection == null) {
            LOGGER.log(Level.SEVERE, "Failed to make connection!");
        }

        // make sure autocommit is off
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Creates a new database for the given SCN at the Dimension tables database.
     * @param jdbcUrl       The URL of the BDA db and the SCN's Dimension table database.
     * @param username      The username to connect to the database.
     * @param password      The password to connect to the database
     * @param owner         The username of the database owner.
     * @param dbname        The name of the new database to create.
     * @return the JDBC URL for the new database
     * @throws SQLException
     */
    public static String createDatabase(String jdbcUrl, String username, String password, 
                                        String owner, String dbname) throws SQLException {
        Connection localConnection = null;

        String postgresTemplateUrl = jdbcUrl + "template1";

        try {
            localConnection = DriverManager.getConnection(postgresTemplateUrl, username, password);
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Connection Failed! Check output console");
            throw e;
        }

        try {
            PreparedStatement statement = localConnection.prepareStatement(
                    String.format(CREATE_DATABASE_QUERY, dbname, owner));

            statement.executeUpdate();
            localConnection.close();
        }
        catch (Exception e){
            localConnection.close();
            throw e;
        }

        return jdbcUrl + dbname;
    }

    /**
     * Create a new schema in the given SCN database. This is used to store the
     * SCN metadata in a separate schema.
     * @param jdbcUrl       The full URL to the SCN's Dimension table database.
     * @param username      The username to connect to the database.
     * @param password      The password to connect to the database
     * @param owner         The username of the database owner.
     * @param schema        The name of the schema to create.
     * @throws SQLException
     */
    public static void createSchema(String jdbcUrl, String username, String password, 
                                    String owner, String schema) throws SQLException {
        Connection localConnection = null;

        try {
            localConnection = DriverManager.getConnection(jdbcUrl, username, password);
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Connection Failed! Check output console");
            throw e;
        }

        try {
            PreparedStatement statement = localConnection.prepareStatement(
                    String.format(CREATE_DATABASE_SCHEMA_QUERY, schema, owner));

            statement.executeUpdate();
            localConnection.close();
        }
        catch (Exception e){
            localConnection.close();
            throw e;
        }

    }

    /**
     * Deletes the Dimension tables database of a given SCN.
     * @param jdbcUrl       The URL of the SCN's Dimension table database.
     * @param username      The username to connect to the database.
     * @param password      The password to connect to the database
     * @param owner         The username of the database owner.
     * @param dbname        The name of the new database to create.
     * @throws SQLException
     */
    public static void dropDatabase(String jdbcUrl, String username, String password,
                        String owner, String dbname) throws SQLException {
        Connection localConnection = null;

        String postgresTemplateUrl = jdbcUrl + "template1";

        try {
            localConnection = DriverManager.getConnection(postgresTemplateUrl, username, password);
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Connection Failed! Check output console");
            throw e;
        }

        try {
            Statement st = localConnection.createStatement();
            ResultSet rs = st.executeQuery(String.format(DROP_OPEN_CONNECTIONS_QUERY,dbname));
            rs.close();

            LOGGER.log(Level.INFO, "Postgres connections to the database were closed.");

            PreparedStatement statement = localConnection.prepareStatement(
                    String.format(DROP_DATABASE_QUERY, dbname));

            statement.executeUpdate();
            localConnection.close();

            LOGGER.log(Level.INFO, "Postgres database dropped.");
        }
        catch (Exception e){
            localConnection.close();
            throw e;
        }
    }

    public Connection getConnection() {
        return connection;
    }

    /**
     * Extracts the port from a PostgresSQL Connection URL.
     */
    public static String getPostgresConnectionPort(String fs) {
        String[] tokens = fs.split(":");

        tokens = tokens[tokens.length - 1].split("/");

        return tokens[0];
    }

    /**
     * Extracts the host from a PostgreSQL Connection URL.
     */
    public static String getPostgresConnectionHost(String fs) {
        String[] tokens = fs.split("://")[1].split(":");

        return tokens[0];
    }

    public String getUsername() {
        return username;
    }

    /**
     * Close the connection to the PostgreSQL database.
     */
    public void close(){
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
