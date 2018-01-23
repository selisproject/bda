package gr.ntua.ece.cslab.selis.bda.controller;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class holds all the configuration options used by the Big Data Analytics component as a whole
 * (including the various subsystems).
 * Created by Giannis Giannakopoulos on 10/6/17.
 */
public class Configuration {
    private static Logger LOGGER =Logger.getLogger(Configuration.class.getCanonicalName());
    public final Server server;
    public final StorageBackend storageBackend;

    public class Server {
        private String address;
        private Integer port;

        public Server() {
        }

        public String getAddress() {
            return address;
        }

        public Integer getPort() {
            return port;
        }
    }
    public class StorageBackend {
        private String eventLogURL, dimensionTablesURL, dbUsername, dbPassword;

        public StorageBackend() {
        }

        public String getEventLogURL() {
            return eventLogURL;
        }

        public String getDimensionTablesURL() {
            return dimensionTablesURL;
        }

        public String getDbUsername() { return dbUsername; }

        public String getDbPassword() { return dbPassword; }
    }

    public Configuration() {
        this.server = new Server();
        this.storageBackend = new StorageBackend();
    }

    /**
     * parseConfiguration constructs a Configuration object through reading the provided configuration file.
     * @param configurationFile the path of the configuration file
     * @return the Configuration object
     */
    public static Configuration parseConfiguration(String configurationFile){
        Properties properties = new Properties();
        try {
            properties.load(new FileReader(configurationFile));
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, e.getMessage());
            return null;
        }

        Configuration conf = new Configuration();
        conf.server.address = properties.getProperty("server.address");
        try {
            conf.server.port = Integer.valueOf(properties.getProperty("server.port"));
        } catch (NumberFormatException e) {
            LOGGER.log(Level.SEVERE, e.getMessage());
            return null;
        }
        conf.storageBackend.eventLogURL = properties.getProperty("backend.url.event.log");
        conf.storageBackend.dimensionTablesURL = properties.getProperty("backend.url.dimension.tables");
        conf.storageBackend.dbUsername = properties.getProperty("backend.db.username");
        conf.storageBackend.dbPassword = properties.getProperty("backend.db.password");
        return conf;
    }
}
