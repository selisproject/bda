package gr.ntua.ece.cslab.selis.bda.common.storage.connectors;

import java.util.Vector;
import java.sql.SQLException;
import java.lang.UnsupportedOperationException;

import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;

public class ConnectorFactory {
    private static ConnectorFactory connFactory;

    public static final int CONNECTOR_HDFS_TYPE = 0;
    public static final int CONNECTOR_HBASE_TYPE = 1;
    public static final int CONNECTOR_POSTGRES_TYPE = 2;
    public static final int CONNECTOR_LOCAL_FS_TYPE = 3;

    private ConnectorFactory() {}

    public static ConnectorFactory getInstance(){
        if (connFactory == null)
            connFactory = new ConnectorFactory();
        return connFactory;
    }

    /** Depending on the FS string format initialize a connector from a different class.
     *  Connectors are implemented for four different filesystems: local, HBase, HDFS, PostgreSQL. **/
    public Connector generateConnector(String FS, String Username, String Password){
        Connector connector;

        int connectorType = ConnectorFactory.getConnectorType(FS);

        if (connectorType == ConnectorFactory.CONNECTOR_HDFS_TYPE) {
            connector = new HDFSConnector(FS, Username, Password);
        } else if (connectorType == ConnectorFactory.CONNECTOR_HBASE_TYPE) {
            connector = new HBaseConnector(FS, Username, Password);
        } else if (connectorType == ConnectorFactory.CONNECTOR_POSTGRES_TYPE) {
            connector = new PostgresqlConnector(FS, Username, Password);
        } else {
            connector = new LocalFSConnector(FS, Username, Password);
        }

        return connector;
    }

    /** Creates a new database and specified schemas.
     *  Returns the jdbcUrl of the new database. **/
    public static String createNewDatabaseWithSchemas(String fs, String username, 
        String password, String owner, String dbname, Vector<String> schemas) 
        throws SystemConnectorException, UnsupportedOperationException {
        String databaseUrl = null;
        int connectorType = ConnectorFactory.getConnectorType(fs);

        if (connectorType == ConnectorFactory.CONNECTOR_HDFS_TYPE) {
            throw new UnsupportedOperationException("Creating a database in HDFS is not supported.");
        } else if (connectorType == ConnectorFactory.CONNECTOR_HBASE_TYPE) {
            databaseUrl = HBaseConnector.createNamespace(fs, username, password, dbname);
        } else if (connectorType == ConnectorFactory.CONNECTOR_POSTGRES_TYPE) {
            // 0. Create new database.
            try {
                databaseUrl = PostgresqlConnector.createDatabase(
                    fs, username, password, owner, dbname);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new SystemConnectorException("Could not create Postgresql database.");
            }
            // 1. Create schemas into the new database.
            if (!(schemas==null)) {
                for (String schema : schemas) {
                    try {
                        PostgresqlConnector.createSchema(databaseUrl, username, password, owner, schema);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        throw new SystemConnectorException("Could not create Postgresql schema.");
                    }
                }
            }
        } else {
            throw new UnsupportedOperationException("Creating a database in local FS is not supported.");
        }

        return databaseUrl;
    }

    /** Destroys a database. **/
    public static void dropDatabase(String fs, String username,
                                      String password, String owner, String dbname)
            throws UnsupportedOperationException, SystemConnectorException {

        int connectorType = ConnectorFactory.getConnectorType(fs);

        if (connectorType == ConnectorFactory.CONNECTOR_HDFS_TYPE) {
            throw new UnsupportedOperationException("Dropping a database in HDFS is not supported.");
        } else if (connectorType == ConnectorFactory.CONNECTOR_HBASE_TYPE) {
            HBaseConnector.dropNamespace(fs, username, password, dbname);
        } else if (connectorType == ConnectorFactory.CONNECTOR_POSTGRES_TYPE) {
            try {
                PostgresqlConnector.dropDatabase(fs, username, password, owner, dbname);
            } catch (SQLException e) {
                e.printStackTrace();
                throw new SystemConnectorException("Could not drop Postgresql database.");
            }
        } else {
            throw new UnsupportedOperationException("Dropping a database in local FS is not supported.");
        }

        return;
    }

    public static int getConnectorType(String fs) {
        if (fs.contains("hdfs")) {
            return ConnectorFactory.CONNECTOR_HDFS_TYPE;
        } else if (fs.contains("hbase")) {
            return ConnectorFactory.CONNECTOR_HBASE_TYPE;
        } else if (fs.contains("jdbc:postgresql")) {
            return ConnectorFactory.CONNECTOR_POSTGRES_TYPE;
        } else {
            return ConnectorFactory.CONNECTOR_LOCAL_FS_TYPE;
        }
    }
}
