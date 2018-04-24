package gr.ntua.ece.cslab.selis.bda.controller;

import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
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
    public final PubSubSubscriber subscriber;

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
        // TODO: Should add username/password for every StorageBackend.
        //       Modify Constructor accordingly.
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
    public class PubSubSubscriber {
        private String authHash, hostname;
        private int portNumber;
        private List<String> rules;

        public PubSubSubscriber(){
        }

        public String getAuthHash() { return authHash; }

        public String getHostname() { return hostname; }

        public int getPortNumber() { return portNumber; }

        public List<String> getRules() { return rules; }

    }

    public Configuration() {
        this.server = new Server();
        this.storageBackend = new StorageBackend();
        this.subscriber = new PubSubSubscriber();
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

        // Server Configuration.
        conf.server.address = properties.getProperty("server.address");
        try {
            conf.server.port = Integer.valueOf(properties.getProperty("server.port"));
        } catch (NumberFormatException e) {
            LOGGER.log(Level.SEVERE, e.getMessage());
            return null;
        }

        // Dimension Tables Configuration.
        conf.storageBackend.dbUsername = properties.getProperty("backend.db.dimension.username");
        conf.storageBackend.dbPassword = properties.getProperty("backend.db.dimension.password");
        conf.storageBackend.dimensionTablesURL = properties.getProperty("backend.db.dimension.url");

        // Event Log Configuration.

        // TODO: Should add username/password for every StorageBackend.
        // conf.storageBackend.eventLogUsername = properties.getProperty("backend.db.event.username");
        // conf.storageBackend.eventLogPassword = properties.getProperty("backend.db.event.password");

        conf.storageBackend.eventLogURL = properties.getProperty("backend.db.event.url");

        // Pub/Sub Configuration.
        conf.subscriber.hostname = properties.getProperty("pubsub.address");
        try {
            conf.subscriber.portNumber = Integer.valueOf(properties.getProperty("pubsub.port"));
        } catch (NumberFormatException e) {
            LOGGER.log(Level.SEVERE, e.getMessage());
            return null;
        }
        conf.subscriber.authHash = properties.getProperty("pubsub.authhash");
        conf.subscriber.rules = Arrays.asList(properties.getProperty("pubsub.rules.message.type").split(","));
        return conf;
    }
}
