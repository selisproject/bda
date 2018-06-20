package gr.ntua.ece.cslab.selis.bda.analytics.connectors;

import org.apache.spark.launcher.SparkLauncher;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SparkConnector {
    private final static Logger LOGGER = Logger.getLogger(SparkLauncher.class.getCanonicalName());

    private String port;
    private String hostname;
    private static SparkLauncher sparkLauncher;

    public SparkConnector(String FS, String username, String password) {
       //sparkLauncher = new SparkLauncher().setMaster();
    }
}
