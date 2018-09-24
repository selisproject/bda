package gr.ntua.ece.cslab.selis.bda.common;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;



import java.lang.IllegalStateException;



/**
 * This class holds all the configuration options used by the Big Data Analytics component as a whole
 * (including the various subsystems).
 */
public class Configuration {
    private static Logger LOGGER =Logger.getLogger(Configuration.class.getCanonicalName());

    private static Configuration configuration = null;

    public final Server server;
    public final StorageBackend storageBackend;
    public final ExecutionEngine execEngine;
    public final PubSubSubscriber subscriber;
    public final AuthClientBackend authClientBackend;
    public final KPIBackend kpiBackend;

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
        private String eventLogURL, dimensionTablesURL, bdaDatabaseURL, dbUsername, dbPassword;
        private String dbPrivilegedUsername, dbPrivilegedPassword;

        public StorageBackend() {
        }

        public String getEventLogURL() {
            return eventLogURL;
        }

        public String getDimensionTablesURL() {
            return dimensionTablesURL;
        }

        public String getBdaDatabaseURL() {
            return bdaDatabaseURL;
        }

        public String getDbUsername() { return dbUsername; }

        public String getDbPassword() { return dbPassword; }

        public String getDbPrivilegedUsername() { return dbPrivilegedUsername; }

        public String getDbPrivilegedPassword() { return dbPrivilegedPassword; }
    }
    public class ExecutionEngine {
        private String sparkMaster;
        private String sparkMasterURL;
        private String sparkDeployMode;
        private String sparkConfJars;
        private String sparkConfDriverMemory;
        private String sparkConfExecutorCores;
        private String sparkConfExecutorMemory;

        public ExecutionEngine(){}

        public String getSparkMaster() { return sparkMaster; }

        public String getSparkMasterURL() { return sparkMasterURL; }

        public String getSparkDeployMode() { return sparkDeployMode; }

        public String getSparkConfJars() { return sparkConfJars; }

        public String getSparkConfDriverMemory() { return sparkConfDriverMemory; }

        public String getSparkConfExecutorCores() { return sparkConfExecutorCores; }

        public String getSparkConfExecutorMemory() { return sparkConfExecutorMemory; }
    }
    public class PubSubSubscriber {
        private String authHash, hostname, certificateLocation;
        private int portNumber;

        public PubSubSubscriber(){
        }

        public String getAuthHash() { return authHash; }

        public String getHostname() { return hostname; }

        public String getCertificateLocation() { return certificateLocation; }

        public int getPortNumber() { return portNumber; }

    }
    public class AuthClientBackend {
        private String authServerUrl, realm, clientId, secret;

        public AuthClientBackend() { }

        public String getAuthServerUrl() { return authServerUrl; }

        public String getRealm() { return realm; }

        public String getClientId() { return clientId; }

        public String getSecret() { return secret; }
    }

    public class KPIBackend {
        private String dbUrl, dbUsername, dbPassword;

        public String getDbUrl() { return dbUrl; }

        public String getDbUsername() { return dbUsername; }

        public String getDbPassword() { return dbPassword; }

    }

    public static Configuration getInstance() throws IllegalStateException {
        if (configuration == null) {
            throw new IllegalStateException("Configuration not initialized.");
        }

        return configuration;
    }

    private Configuration() {
        this.server = new Server();
        this.storageBackend = new StorageBackend();
        this.subscriber = new PubSubSubscriber();
        this.authClientBackend = new AuthClientBackend();
        this.kpiBackend = new KPIBackend();
        this.execEngine = new ExecutionEngine();
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
        conf.storageBackend.dbPrivilegedUsername = properties.getProperty("backend.db.dimension.privileged_username");
        conf.storageBackend.dbPrivilegedPassword = properties.getProperty("backend.db.dimension.privileged_password");
        conf.storageBackend.dimensionTablesURL = properties.getProperty("backend.db.dimension.url");

        // BDA Database Configuration.

        // TODO: Should add username/password for every StorageBackend.
        // conf.storageBackend.bdaDatabaseUsername = properties.getProperty("backend.db.bda.username");
        // conf.storageBackend.bdaDatabasePassword = properties.getProperty("backend.db.bda.password");

        conf.storageBackend.bdaDatabaseURL = properties.getProperty("backend.db.bda.url");

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
        conf.subscriber.certificateLocation = properties.getProperty("pubsub.certificate.location");

        // Keycloak Auth Configuration.
        conf.authClientBackend.authServerUrl = properties.getProperty("keycloak.bda.url");
        conf.authClientBackend.realm = properties.getProperty("keycloak.bda.realm");
        conf.authClientBackend.clientId = properties.getProperty("keycloak.bda.clientid");
        conf.authClientBackend.secret = properties.getProperty("keycloak.bda.secret");

        // KPIDB configuration
        conf.kpiBackend.dbUrl = properties.getProperty("kpi.db.url");
        conf.kpiBackend.dbUsername = properties.getProperty("kpi.db.username");
        conf.kpiBackend.dbPassword = properties.getProperty("kpi.db.password");

        // Execution engine configuration
        conf.execEngine.sparkMaster = properties.getProperty("spark.master"); 
        conf.execEngine.sparkMasterURL = properties.getProperty("spark.master.url");
        conf.execEngine.sparkDeployMode = properties.getProperty("spark.deploy_mode");
        conf.execEngine.sparkConfJars = properties.getProperty("spark.conf.jars");
        conf.execEngine.sparkConfDriverMemory = properties.getProperty("spark.conf.driver_memory");
        conf.execEngine.sparkConfExecutorCores = properties.getProperty("spark.conf.executor_cores");
        conf.execEngine.sparkConfExecutorMemory = properties.getProperty("spark.conf.executor_memory");

        configuration = conf;

        return conf;
    }
}
