package gr.ntua.ece.cslab.selis.bda.controller;

import org.eclipse.jetty.server.Server;

/**
 * Created by Giannis Giannakopoulos on 8/31/17.
 */
public class Entrypoint {
    public static void main(String[] args) {
        System.out.println("Hello SELIS World!");
        Server server = new Server(8080);

        // handlers
        // server.handle();

        try {
            server.start();
            server.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
