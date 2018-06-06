package gr.ntua.ece.cslab.selis.bda.controller;

import gr.ntua.ece.cslab.selis.bda.kpidb.KPIBackend;
import gr.ntua.ece.cslab.selis.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.selis.bda.datastore.connectors.PostgresqlPooledDataSource;

import gr.ntua.ece.cslab.selis.bda.controller.connectors.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import org.keycloak.representations.idm.authorization.AuthorizationResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
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
    public static KPIBackend kpiDB;

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

    private static void kpiBackendInitialization() {
        LOGGER.log(Level.INFO, "Initializing kpi backend...");
        kpiDB = new KPIBackend(
            configuration.storageBackend.getBdaDatabaseURL(),
            configuration.storageBackend.getDbUsername(),
            configuration.storageBackend.getDbPassword());
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

        // PubSub connectors initialization
        pubSubConnectorsInitialization();

        // KPI DB initialization
        //kpiBackendInitialization();

        // AuthClient backend initialization.
        authClientBackendInitialization();

        // testKeycloakAuthentication();

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
            //subscriber.join();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, e.getMessage());
            e.printStackTrace();
        } finally {
            LOGGER.log(Level.INFO,"Terminating server");
            server.destroy();
            subscriber.interrupt();
        }
    }
}
