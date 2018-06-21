package gr.ntua.ece.cslab.selis.bda.controller;

import gr.ntua.ece.cslab.selis.bda.analytics.AnalyticsInstance;
import gr.ntua.ece.cslab.selis.bda.analytics.AnalyticsSystem;
import gr.ntua.ece.cslab.selis.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.selis.bda.datastore.connectors.PostgresqlPooledDataSource;

import gr.ntua.ece.cslab.selis.bda.controller.connectors.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import org.json.JSONObject;
import org.keycloak.representations.idm.authorization.AuthorizationResponse;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.*;

/**
 * Created by Giannis Giannakopoulos on 8/31/17.
 */
public class Entrypoint {
    private final static Logger LOGGER = Logger.getLogger(Entrypoint.class.getCanonicalName());
    public static Configuration configuration;
    public static Thread subscriber;
    public static PubSubPublisher publisher;
    public static StorageBackend datastore;
    public static AnalyticsInstance analyticsComponent;

    private static void storageBackendInitialization() {
        LOGGER.log(Level.INFO, "Initializing storage backend...");
        datastore = new StorageBackend(
            configuration.storageBackend.getEventLogURL(),
            configuration.storageBackend.getDimensionTablesURL(),
            configuration.storageBackend.getDbUsername(),
            configuration.storageBackend.getDbPassword());

        LOGGER.log(Level.INFO, "Initializing Postgresql connection pool ...");
        PostgresqlPooledDataSource.init(
            configuration.storageBackend.getBdaDatabaseURL(),
            configuration.storageBackend.getDimensionTablesURL(),
            configuration.storageBackend.getDbUsername(),
            configuration.storageBackend.getDbPassword()
        );

        BDAdbConnector.init(
            configuration.storageBackend.getBdaDatabaseURL(),
            configuration.storageBackend.getDimensionTablesURL(),
            configuration.storageBackend.getDbUsername(),
            configuration.storageBackend.getDbPassword()
        );
    }


    private static void fetch_engines() {
        LOGGER.log(Level.INFO, "Fetch execution engines for analytics module.");
        Connection conn = BDAdbConnector.getInstance().getBdaConnection();

        Statement statement;
        ResultSet engines = null;

        try {
            statement = conn.createStatement();

            engines = statement.executeQuery("SELECT * FROM execution_engines;");

            if (engines != null) {
                while (engines.next()) {
                    Entrypoint.analyticsComponent.getEngineCatalog().addNewExecutEngine(
                            engines.getInt("id"),
                            engines.getString("name"),
                            engines.getString("engine_path"),
                            engines.getBoolean("local_engine"),
                            new JSONObject(engines.getString("args"))
                    );
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        LOGGER.log(Level.INFO, "Fetched engines : " + Entrypoint.analyticsComponent.
                getEngineCatalog().getAllExecutEngines() + "\n");

    }

    private static void fetch_recipes() {
        LOGGER.log(Level.INFO, "Fetch execution engines for analytics module.");
        Connection conn = BDAdbConnector.getInstance().getBdaConnection();

        Statement statement;
        ResultSet recipes = null;

        try {
            statement = conn.createStatement();

            recipes = statement.executeQuery("SELECT * FROM recipes;");

            if (recipes != null) {
                while (recipes.next()) {
                    Entrypoint.analyticsComponent.getKpiCatalog().addNewKpi(
                            recipes.getInt("id"),
                            recipes.getString("name"),
                            recipes.getString("description"),
                            recipes.getInt("engine_id"),
                            new JSONObject(recipes.getString("args")),
                            recipes.getString("executable_path")
                    );
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        LOGGER.log(Level.INFO, "Fetched recipes : " + Entrypoint.analyticsComponent.
                getKpiCatalog().getAllKpis() + "\n");


    }

    private static void analyticsModuleInitialization() {
        LOGGER.log(Level.INFO, "Initializing Analytics SubModule...");
        analyticsComponent = AnalyticsSystem.getInstance(
            configuration.kpiBackend.getDbUrl(),
            configuration.kpiBackend.getDbUsername(),
            configuration.kpiBackend.getDbPassword()
        );
        fetch_engines();
        fetch_recipes();
    }

    private static void pubSubConnectorsInitialization() {
        LOGGER.log(Level.INFO, "Initializing PubSub subscriber...");
        subscriber = new Thread(new PubSubSubscriber(configuration.subscriber.getAuthHash(),
                configuration.subscriber.getHostname(),
                configuration.subscriber.getPortNumber()), "subscriber");
        LOGGER.log(Level.INFO, "Initializing PubSub publisher...");
        publisher = new PubSubPublisher(configuration.subscriber.getHostname(),
                configuration.subscriber.getPortNumber());
    }

    private static void authClientBackendInitialization() {
        LOGGER.log(Level.INFO, "Initializing AuthClient backend...");

        AuthClientBackend.init(
            configuration.authClientBackend.getAuthServerUrl(),
            configuration.authClientBackend.getRealm(),
            configuration.authClientBackend.getClientId(),
            configuration.authClientBackend.getSecret()
        );
    }

    public static void create_folders() {
        LOGGER.log(Level.INFO, "Creating folders for uploaded recipes and recipe results");

        File theDir = new File("/uploads/");
        if (!theDir.exists()) {
            theDir.mkdir();
        }
        theDir = new File("/results/");
        if (!theDir.exists()) {
            theDir.mkdir();
        }

    }

    private static void testKeycloakAuthentication() {
        // TODO: This is just a proof of concept. Should be removed.
        AuthClientBackend authClientBackend = AuthClientBackend.getInstance();

        AuthorizationResponse response = authClientBackend.authzClient.authorization(
            "selis-user", "123456"
        ).authorize();

        String tokenString = response.getToken();

        assertNotNull(tokenString);
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Please provide a configuration file as a first argument");
            System.exit(1);
        }
        // parse configuration
        configuration = Configuration.parseConfiguration(args[0]);
        if(configuration==null) {
            System.exit(1);
        }

        // Datastore module initialization
        storageBackendInitialization();

        // KPI DB initialization
        analyticsModuleInitialization();

        // PubSub connectors initialization
        pubSubConnectorsInitialization();

        // AuthClient backend initialization.
        authClientBackendInitialization();

        // testKeycloakAuthentication();

        // Create folders for uploaded recipes and recipe results
        create_folders();

        // SIGTERM hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // TODO: stub method, add code for graceful shutdown here
            LOGGER.log(Level.INFO,"Terminating server");
        }));

        // Web Server configuration
        Server server = new Server(
                new InetSocketAddress(configuration.server.getAddress(), configuration.server.getPort()));

        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.packages("gr.ntua.ece.cslab.selis.bda.controller.resources");
        ServletHolder servlet = new ServletHolder(new ServletContainer(resourceConfig));

        ServletContextHandler handler = new ServletContextHandler(server, "/api");
        handler.addServlet(servlet, "/*");

        // run the server and the pubsub subscriber
        try {
            LOGGER.log(Level.INFO, "Starting server");
            server.start();
            subscriber.start();
            server.join();
            subscriber.join();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, e.getMessage());
            System.out.println(e);
            e.printStackTrace();
        } finally {
            LOGGER.log(Level.INFO,"Terminating server");
            server.destroy();
            subscriber.interrupt();
        }
    }
}
