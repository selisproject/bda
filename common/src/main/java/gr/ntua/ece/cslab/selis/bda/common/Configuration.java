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
    public final PubSubServer pubsub;
    public final PubSubSubscriber subscriber;
    public final PubSubSubscriber externalSubscriber;
    public final AuthClientBackend authClientBackend;
    public final KPIBackend kpiBackend;

    /**
     * Class to hold the BDA server configuration.
     */
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

    /**
     * Class to hold the storage engines configuration. Includes the BDA db,
     * the EventLog FS, the Dimension tables FS and the HDFS options.
     */
    public class StorageBackend {
        // TODO: Should add username/password for every StorageBackend.
        //       Modify Constructor accordingly.
        private String eventLogURL, dimensionTablesURL, bdaDatabaseURL, dbUsername, dbPassword;
        private String dbPrivilegedUsername, dbPrivilegedPassword;

        private String eventLogMaster = null;
        private String eventLogQuorum = null;

        private String hdfsMasterURL;
        private String hdfsUsername;
        private String hdfsPassword;

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

        public String getEventLogMaster() { return eventLogMaster; }

        public String getEventLogQuorum() { return eventLogQuorum; }

        public String getHDFSMasterURL() { return hdfsMasterURL; }

        public String getHDFSUsername() { return hdfsUsername; }

        public String getHDFSPassword() { return hdfsPassword; }

    }

    /**
     * Class to hold the execution engines configuration. Includes Spark and Livy options,
     * as well as the storage folder for executables.
     */
    public class ExecutionEngine {
        private String sparkMaster;
        private String sparkMasterURL;
        private String sparkDeployMode;
        private String sparkConfJars;
        private String sparkConfPackages;
        private String sparkConfRepositories;
        private String sparkConfDriverMemory;
        private String sparkConfExecutorCores;
        private String sparkConfExecutorMemory;

        private String recipeStorageLocation;
        private String recipeStorageType;

        private String livyURL;

        public ExecutionEngine(){}

        public String getSparkMaster() { return sparkMaster; }

        public String getSparkMasterURL() { return sparkMasterURL; }

        public String getSparkDeployMode() { return sparkDeployMode; }

        public String getSparkConfJars() { return sparkConfJars; }

        public String getSparkConfPackages() { return sparkConfPackages; }

        public String getSparkConfRepositories() { return sparkConfRepositories; }

        public String getSparkConfDriverMemory() { return sparkConfDriverMemory; }

        public String getSparkConfExecutorCores() { return sparkConfExecutorCores; }

        public String getSparkConfExecutorMemory() { return sparkConfExecutorMemory; }

        public String getRecipeStorageLocation() { return recipeStorageLocation; }

        public String getRecipeStorageType() { return recipeStorageType; }

        public String getLivyURL() { return livyURL; }
    }

    /**
     * Class to hold the Pub/Sub server configuration.
     */
    public class PubSubServer {
        private String authHash, certificateLocation;

        public PubSubServer(){
        }

        public String getAuthHash() { return authHash; }

        public String getCertificateLocation() { return certificateLocation; }

    }

    /**
     * Class to hold the Pub/Sub subscriber configuration.
     */
    public class PubSubSubscriber {
        private String url;

        public PubSubSubscriber(){ }

        public String getUrl() { return url; }

    }

    /**
     * Class to hold the IAM server configuration.
     */
    public class AuthClientBackend {
        private String authServerUrl, realm, clientId, secret;

        public AuthClientBackend() { }

        public String getAuthServerUrl() { return authServerUrl; }

        public String getRealm() { return realm; }

        public String getClientId() { return clientId; }

        public String getSecret() { return secret; }
    }

    /**
     * Class to hold the KPI service configuration.
     */
    public class KPIBackend {
        private String dbUrl, dbUsername, dbPassword;

        public String getDbUrl() { return dbUrl; }

        public String getDbUsername() { return dbUsername; }

        public String getDbPassword() { return dbPassword; }

    }

    /**
     * This method is used by all the modules to get the existing configuration object.
     * @return the Configuration object that contains all configuration options
     * @throws IllegalStateException
     */
    public static Configuration getInstance() throws IllegalStateException {
        if (configuration == null) {
            throw new IllegalStateException("Configuration not initialized.");
        }

        return configuration;
    }

    /**
     * Default constructor.
     */
    private Configuration() {
        this.server = new Server();
        this.storageBackend = new StorageBackend();
        this.pubsub = new PubSubServer();
        this.subscriber = new PubSubSubscriber();
        this.externalSubscriber = new PubSubSubscriber();
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
        conf.storageBackend.eventLogMaster = properties.getProperty("backend.db.event.master.host");
        conf.storageBackend.eventLogQuorum = properties.getProperty("backend.db.event.quorum");

        conf.storageBackend.hdfsMasterURL = properties.getProperty("backend.hdfs.master.url");

        // Pub/Sub Configuration.
        conf.pubsub.authHash = properties.getProperty("pubsub.authhash");
        conf.pubsub.certificateLocation = properties.getProperty("pubsub.certificate.location");

        // Pub/Sub Subscriber Configuration.
        conf.subscriber.url = properties.getProperty("pubsub.subscriber.url");
        conf.externalSubscriber.url = properties.getProperty("pubsub.external.subscriber.url");

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
        conf.execEngine.sparkConfPackages = properties.getProperty("spark.conf.packages");
        conf.execEngine.sparkConfRepositories = properties.getProperty("spark.conf.repositories");
        conf.execEngine.sparkConfDriverMemory = properties.getProperty("spark.conf.driver_memory");
        conf.execEngine.sparkConfExecutorCores = properties.getProperty("spark.conf.executor_cores");
        conf.execEngine.sparkConfExecutorMemory = properties.getProperty("spark.conf.executor_memory");
        conf.execEngine.recipeStorageLocation = properties.getProperty("engines.recipe.storage.prefix");
        conf.execEngine.recipeStorageType = properties.getProperty("engines.recipe.storage.type");
        conf.execEngine.livyURL = properties.getProperty("spark.livy.url");

        configuration = conf;

        return conf;
    }
}
