package gr.ntua.ece.cslab.selis.bda.controller;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Giannis Giannakopoulos on 8/31/17.
 */
public class Entrypoint {
    public final static Logger LOGGER = Logger.getLogger(Entrypoint.class.getSimpleName());

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Please provide a configuration file as a first argument");
            System.exit(1);
        }
        Properties conf = new Properties();
        conf.load(new FileReader(args[0]));

        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.packages("gr.ntua.ece.cslab.selis.bda.controller.resources");
        ServletHolder servlet = new ServletHolder(new ServletContainer(resourceConfig));

        Server server = new Server(
                new InetSocketAddress(conf.getProperty("server.address"),
                        new Integer(conf.getProperty("server.port"))));


        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                // TODO: stub method, add code for graceful shutdown here
                LOGGER.log(Level.INFO,"Terminating server");
            }
        }));

        ServletContextHandler handler = new ServletContextHandler(server, "/*");
        handler.addServlet(servlet, "/*");

        try {
            LOGGER.log(Level.INFO, "Starting server");

            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            LOGGER.log(Level.INFO,"Terminating server");
            server.destroy();
        }
    }
}