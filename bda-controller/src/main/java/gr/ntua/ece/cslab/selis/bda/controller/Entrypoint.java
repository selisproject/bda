package gr.ntua.ece.cslab.selis.bda.controller;

import gr.ntua.ece.cslab.selis.bda.analyticsml.RunnerInstance;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.selis.bda.common.Configuration;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;

import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.HDFSConnector;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.PubSubConnector;
import gr.ntua.ece.cslab.selis.bda.controller.cron.CronJobScheduler;
import org.apache.commons.io.IOUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import org.keycloak.representations.idm.authorization.AuthorizationResponse;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.junit.Assert.*;

/**
 * Created by Giannis Giannakopoulos on 8/31/17.
 */
public class Entrypoint {
    private final static Logger LOGGER = Logger.getLogger(Entrypoint.class.getCanonicalName());
    public static Configuration configuration;

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

        if (configuration.execEngine.getRecipeStorageType().startsWith("hdfs")) {
            // Use HDFS storage for recipes and libraries.

            ClassLoader classLoader = RunnerInstance.class.getClassLoader();
            InputStream fileInStream = classLoader.getResourceAsStream("RecipeDataLoader.py");

            byte[] recipeBytes = new byte[0];
            try {
                recipeBytes = IOUtils.toByteArray(fileInStream);

                fileInStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

            HDFSConnector connector = null;
            try {
                connector = (HDFSConnector)
                        SystemConnector.getInstance().getHDFSConnector();
            } catch (SystemConnectorException e) {
                e.printStackTrace();
            }

            org.apache.hadoop.fs.FileSystem fs = connector.getFileSystem();

            // Create HDFS file path object.
            org.apache.hadoop.fs.Path outputFilePath =
                    new org.apache.hadoop.fs.Path("/RecipeDataLoader.py");

            // Write to HDFS.
            org.apache.hadoop.fs.FSDataOutputStream outputStream = null;
            try {
                outputStream = fs.create(
                        outputFilePath
                );
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                outputStream.write(recipeBytes);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                fileInStream = new URL(configuration.execEngine.getSparkConfJars()).openStream();
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Spark jar clients download failed!! Please check the URLs");
            }

            byte[] postgresBytes = new byte[0];
            try {
                postgresBytes = IOUtils.toByteArray(fileInStream);

                fileInStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            String[] jar_name = configuration.execEngine.getSparkConfJars().split("/");
            outputFilePath =
                    new org.apache.hadoop.fs.Path(jar_name[jar_name.length-1]);
            try {
                outputStream = fs.create(
                        outputFilePath
                );
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                outputStream.write(postgresBytes);
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            // TODO: Use local storage?
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

        // start the server and the subscribers
        try {
            LOGGER.log(Level.INFO, "Starting server");
            server.start();
            PubSubConnector.init();
            CronJobScheduler.init_scheduler();
            server.join();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, e.getMessage());
            e.printStackTrace();
        } finally {
            LOGGER.log(Level.INFO,"Terminating server");
            server.destroy();
            PubSubConnector.getInstance().close();
            SystemConnector.getInstance().close();
        }
    }
}
