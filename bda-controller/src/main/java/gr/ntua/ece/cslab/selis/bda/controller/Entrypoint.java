package gr.ntua.ece.cslab.selis.bda.controller;

import gr.ntua.ece.cslab.selis.bda.datastore.StorageBackend;
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

    public static StorageBackend eventLog, dimensionTables;

    private static void storageBackendInitialization() {
        LOGGER.log(Level.INFO, "Configuring storage backend...");
        eventLog = new StorageBackend(configuration.storageBackend.getEventLogURL());
        dimensionTables = new StorageBackend(configuration.storageBackend.getDimensionTablesURL());
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

        // module initialization
        storageBackendInitialization();

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

        // run the server
        try {
            LOGGER.log(Level.INFO, "Starting server");
            server.start();
            server.join();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, e.getMessage());
            e.printStackTrace();
        } finally {
            LOGGER.log(Level.INFO,"Terminating server");
            server.destroy();
        }
    }
}