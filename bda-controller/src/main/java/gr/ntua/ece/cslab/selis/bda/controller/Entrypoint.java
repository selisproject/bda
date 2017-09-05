package gr.ntua.ece.cslab.selis.bda.controller;

import org.eclipse.jetty.server.Server;

import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Created by Giannis Giannakopoulos on 8/31/17.
 */
public class Entrypoint {
    private final static Logger LOGGER = Logger.getLogger(Entrypoint.class.getSimpleName());
    public static void main(String[] args) throws IOException {
        if(args.length < 1){
            System.err.println("Please provide a configuration file as a first argument");
            System.exit(1);
        }
        Properties conf = new Properties();
        conf.load(new FileReader(args[0]));
        Server server = new Server(
                new InetSocketAddress(conf.getProperty("server.address"),
                new Integer(conf.getProperty("server.port"))));
        try {
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
