/*
 * Copyright 2019 ICCS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    public final PubSubServer pubSubServer;
    public final PubSubSubscriber pubSubSubscriber;
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
    public class ExecutionEngine {
        private String sparkMaster;
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

    public class PubSubServer {
        private String address, certificateLocation, certificateFile;
        private Integer port;

        public PubSubServer(){ }

        public String getAddress() {
            return address;
        }

        public Integer getPort() {
            return port;
        }

        public String getCertificateLocation() {
            return certificateLocation;
        }

        public String getCertificateFile() {
            return certificateFile;
        }
    }

    public class PubSubSubscriber {
        private String authHash, url;

        public PubSubSubscriber(){ }

        public String getAuthHash() { return authHash; }

        public String getUrl() { return url; }

    }
    public class AuthClientBackend {
        private Boolean authEnabled;
        private String authServerUrl, realm, clientId, clientSecret, bdaUsername, bdaPassword;

        public AuthClientBackend() { }

        public Boolean isAuthEnabled() { return authEnabled; }

        public String getAuthServerUrl() { return authServerUrl; }

        public String getRealm() { return realm; }

        public String getClientId() { return clientId; }

        public String getClientSecret() { return clientSecret; }

        public String getBdaUsername() { return bdaUsername; }

        public String getBdaPassword() { return bdaPassword; }
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
        this.pubSubServer = new PubSubServer();
        this.pubSubSubscriber = new PubSubSubscriber();
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

        // Pub/Sub internal server Configuration
        conf.pubSubServer.certificateLocation = properties.getProperty("pubsub.certificate.storage.prefix");
        conf.pubSubServer.address = properties.getProperty("pubsub.address");
        conf.pubSubServer.port = Integer.valueOf(properties.getProperty("pubsub.port"));
        conf.pubSubServer.certificateFile = properties.getProperty("pubsub.certificate");

        // Pub/Sub Subscriber Configuration.
        conf.pubSubSubscriber.authHash = properties.getProperty("pubsub.subscriber.authhash");
        conf.pubSubSubscriber.url = properties.getProperty("pubsub.subscriber.url");

        // Keycloak Auth Configuration.
        conf.authClientBackend.authEnabled = Boolean.valueOf(properties.getProperty("keycloak.enabled"));
        conf.authClientBackend.authServerUrl = properties.getProperty("keycloak.bda.url");
        conf.authClientBackend.realm = properties.getProperty("keycloak.bda.realm");
        conf.authClientBackend.clientId = properties.getProperty("keycloak.bda.pubsub.client_id");
        conf.authClientBackend.clientSecret = properties.getProperty("keycloak.bda.pubsub.client_secret");
        conf.authClientBackend.bdaUsername = properties.getProperty("keycloak.bda.username");
        conf.authClientBackend.bdaPassword = properties.getProperty("keycloak.bda.password");

        // KPIDB configuration
        conf.kpiBackend.dbUrl = properties.getProperty("kpi.db.url");
        conf.kpiBackend.dbUsername = properties.getProperty("kpi.db.username");
        conf.kpiBackend.dbPassword = properties.getProperty("kpi.db.password");

        // Execution engine configuration
        conf.execEngine.sparkMaster = properties.getProperty("spark.master");
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
