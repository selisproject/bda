package gr.ntua.ece.cslab.selis.bda.common.storage.connectors;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HBaseConnector implements Connector {
    // TODO: Should setup connection using username/password.

    private final static Logger LOGGER = Logger.getLogger(HBaseConnector.class.getCanonicalName());

    private String port;
    private String hostname;
    private String namespace;
    private Connection connection;

    public HBaseConnector(String FS, String username, String password) throws IOException, ServiceException {
        // Store Connection Parameters.
        this.port = getHBaseConnectionPort(FS);
        this.hostname = getHBaseConnectionURL(FS);
        this.namespace = getHBaseNamespace(FS);

        LOGGER.log(Level.INFO, "Initializing HBase Connector...");

        // Initialize HBase Configuration.
        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.property.clientPort", this.port);
        conf.set("hbase.zookeeper.quorum", this.hostname);
        conf.set("hbase.client.keyvalue.maxsize","0");

        // Check HBase Availability.
        try {
            HBaseAdmin.checkHBaseAvailable(conf);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "HBase Availability Check Failed.");
            throw e;
        }

        // Initialize HBase Connection.
        this.connection = null;
        try {
            this.connection = ConnectionFactory.createConnection(conf);
            LOGGER.log(Level.INFO, "HBase connection initialized.");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Connection Failed! Check output console.");
            throw e;
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public static String createNamespace(String fs, String username, String password, String dbname) throws IOException, ServiceException {
        // Initialize HBase Configuration.
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort",getHBaseConnectionPort(fs));
        conf.set("hbase.zookeeper.quorum", getHBaseConnectionURL(fs));

        // Check HBase Availability.
        try {
            HBaseAdmin.checkHBaseAvailable(conf);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "HBase Availability Check Failed.");
            throw e;
        }

        // Initialize HBase Connection.
        Admin admin;
        try {
            admin = ConnectionFactory.createConnection(conf).getAdmin();
            LOGGER.log(Level.INFO, "HBase connection initialized.");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Admin Connection Failed! Check output console.");
            throw e;
        }

        // Create HBase namespace
        try {
            admin.createNamespace(NamespaceDescriptor.create(dbname).build());
            LOGGER.log(Level.INFO, "HBase namespace created.");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Creation of namespace failed! Check output console.");
            throw e;
        }

        try {
            HTableDescriptor desc = new HTableDescriptor(dbname + ":Events");
            desc.addFamily(new HColumnDescriptor("messages"));

            admin.createTable(desc);
            admin.close();
            LOGGER.log(Level.INFO, "HBase namespace created.");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Creation of Events table in namespace failed! Check output console.");
            throw e;
        }

        return fs + dbname;
    }

    public static void dropNamespace(String fs, String username, String password, String dbname) throws IOException, ServiceException {
        // Initialize HBase Configuration.
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort",getHBaseConnectionPort(fs));
        conf.set("hbase.zookeeper.quorum", getHBaseConnectionURL(fs));

        // Check HBase Availability.
        try {
            HBaseAdmin.checkHBaseAvailable(conf);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "HBase Availability Check Failed.");
            throw e;
        }

        // Initialize HBase Admin Connection.
        Admin admin;
        try {
            admin = ConnectionFactory.createConnection(conf).getAdmin();
            LOGGER.log(Level.INFO, "HBase Admin connection initialized.");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Admin Connection Failed! Check output console.");
            throw e;
        }

        // Delete HBase table and namespace
        try {
            TableName table = TableName.valueOf(dbname + ":Events");
            admin.disableTable(table);
            admin.deleteTable(table);
            admin.deleteNamespace(dbname);
            admin.close();
            LOGGER.log(Level.INFO, "HBase namespace deleted.");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Destroy of namespace failed! Check output console.");
            throw e;
        }
    }

    public void close(){
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Extracts the port from a HBase Connection URL.
     */
    private static String getHBaseConnectionPort(String FS) {
        String[] tokens = FS.split(":");
        
        tokens = tokens[tokens.length - 1].split("/");

        return tokens[0];
    }

    /**
     * Extracts the host from a HBase Connection URL.
     */
    private static String getHBaseConnectionURL(String FS) {
        String[] tokens = FS.split("://")[1].split(":");

        return tokens[0];
    }

    /**
     * Extracts the namespace from a HBase Connection URL.
     */
    private static String getHBaseNamespace(String FS) {
        String[] tokens = FS.split("/");

        return tokens[tokens.length - 1];
    }

    public String getNamespace() {
        return namespace;
    }
}
