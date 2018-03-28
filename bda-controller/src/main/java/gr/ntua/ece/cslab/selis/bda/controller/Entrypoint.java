package gr.ntua.ece.cslab.selis.bda.controller;

import gr.ntua.ece.cslab.selis.bda.datastore.KPIBackend;
import gr.ntua.ece.cslab.selis.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Giannis Giannakopoulos on 8/31/17.
 */
public class Entrypoint {
    private final static Logger LOGGER = Logger.getLogger(Entrypoint.class.getCanonicalName());
    public static Configuration configuration;
    public static PubSubSubscriber mySubscriber;
    public static PubSubPublisher myPublisher;
    public static StorageBackend myBackend;
    public static KPIBackend kpiDB;

    private static void storageBackendInitialization() {
        LOGGER.log(Level.INFO, "Initializing storage backend...");
        myBackend = new StorageBackend(configuration.storageBackend.getEventLogURL(),
                configuration.storageBackend.getDimensionTablesURL(),
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword());
    }

    private static void kpiBackendInitialization() {
        LOGGER.log(Level.INFO, "Initializing kpi backend...");
        kpiDB = new KPIBackend(configuration.storageBackend.getDimensionTablesURL(),
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword());
    }

    private static void pubSubConnectorsInitialization() {
        LOGGER.log(Level.INFO, "Initializing PubSub subscriber...");
        mySubscriber = new PubSubSubscriber(configuration.subscriber.getAuthHash(),
                configuration.subscriber.getHostname(),
                configuration.subscriber.getPortNumber(),
                configuration.subscriber.getRules());
        LOGGER.log(Level.INFO, "Initializing PubSub publisher...");
        myPublisher = new PubSubPublisher(configuration.subscriber.getHostname(),
                configuration.subscriber.getPortNumber());
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
        kpiBackendInitialization();
        
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

        Thread subscriber = new Thread(mySubscriber, "subscriber");
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
        }
    }
}