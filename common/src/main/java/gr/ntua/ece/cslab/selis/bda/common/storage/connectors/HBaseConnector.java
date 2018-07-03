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
    private Connection connection;

    public HBaseConnector(){}

    public HBaseConnector(String FS, String username, String password) {
        // Store Connection Parameters.
        this.port = getHBaseConnectionPort(FS);
        this.hostname = getHBaseConnectionURL(FS);

        LOGGER.log(Level.INFO, "Initializing HBase Connector...");

        // Initialize HBase Configuration.
        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.property.clientPort", this.port);
        conf.set("hbase.zookeeper.quorum", this.hostname);
        conf.set("hbase.client.keyvalue.maxsize","0");

        // Check HBase Availability.
        try {
            HBaseAdmin.checkHBaseAvailable(conf);
        } catch (ServiceException e) {
            LOGGER.log(Level.SEVERE, "HBase Availability Check Failed.");
            e.printStackTrace();
            return;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "HBase Availability Check Failed.");
            e.printStackTrace();
            return;
        }

        // Initialize HBase Connection.
        this.connection = null;
        try {
            this.connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Connection Failed! Check output console.");
            e.printStackTrace();
            return;
        } finally {
            LOGGER.log(Level.INFO, "HBase connection initialized.");
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public void close(){
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    /**
     * Extracts the port from a HBase Connection URL.
     */
    private String getHBaseConnectionPort(String FS) {
        String[] tokens = FS.split(":");

        return tokens[1];
    }

    /**
     * Extracts the host from a HBase Connection URL.
     */
    private String getHBaseConnectionURL(String FS) {
        String[] tokens = FS.split(":");

        return tokens[0];
    }
}
