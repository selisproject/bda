package gr.ntua.ece.cslab.selis.bda.controller;

import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.selis.bda.common.Configuration;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.*;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import org.keycloak.representations.idm.authorization.AuthorizationResponse;

import java.io.File;
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

    public static void main(String[] args) throws SystemConnectorException {
        if (args.length < 1) {
            LOGGER.log(Level.WARNING, "Please provide a configuration file as a first argument");
            System.exit(1);
        }
        // parse configuration
        configuration = Configuration.parseConfiguration(args[0]);
        if(configuration==null) {
            System.exit(1);
        }

        SystemConnector.init(args[0]);

        // PubSub connectors initialization
        pubSubConnectorsInitialization();

        // AuthClient backend initialization.
        authClientBackendInitialization();

        // testKeycloakAuthentication();

        // Create folders for uploaded recipes and recipe results
        create_folders();

        // Hardcoded Analytics Job Run.
        /*
        AnalyticsInstance analyticsml = new AnalyticsInstance("sonae_slug");
        analyticsml.run(1, "666");

        // Hardcoded SparkLauncher example.
        SparkAppHandle handle = null;
        try {
            handle = new SparkLauncher()
                    .setMaster("yarn")
                    .setDeployMode("cluster")
                    .setAppResource("/uploads/2_pi.py")
                    .setVerbose(true)
                    .startApplication();
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Hardcoded SCN creation example.
        try {
            ScnDbInfo scn = new ScnDbInfo(
                "scn_slug", "scn_name", "scn_desc", "scn_db"
            );
            scn.save();

            StorageBackend.createNewScn(scn);
        } catch (Exception e) {
            System.out.println("Sometimes ...");
            e.printStackTrace();
        }
        */

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
            e.printStackTrace();
        } finally {
            LOGGER.log(Level.INFO,"Terminating server");
            server.destroy();
            subscriber.interrupt();
            SystemConnector.getInstance().close();
        }
    }
}
