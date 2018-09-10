package gr.ntua.ece.cslab.selis.bda.analytics.runners;

import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.launcher.SparkAppHandle;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.ExecutEngineDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;
import gr.ntua.ece.cslab.selis.bda.kpidb.KPIBackend;

public class SparkRunner extends ArgumentParser implements Runnable {
    private final static Logger LOGGER = Logger.getLogger(SparkLauncher.class.getCanonicalName());

    String message;
    String scnSlug;
    String recipeResource;
    KpiDescriptor kpiDescriptor;
    ExecutEngineDescriptor engineDescriptor;

    public SparkRunner(KpiDescriptor kpi, ExecutEngineDescriptor engine,
                       String message, String scnSlug) {

        this.message = message;
        this.scnSlug = scnSlug;
        this.kpiDescriptor = kpi;
        this.engineDescriptor = engine;
        this.recipeResource = this.kpiDescriptor.getExecutable().getOsPath();
    }

    @Override
    public void run() {
        SparkAppHandle handle = null;

        try {
            SparkLauncher launcher = new SparkLauncher()
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setAppResource(this.recipeResource);
            if (message != "") {
                launcher.addAppArgs(message);
            }

            handle = launcher.startApplication();
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING,"Spark job failed to start!");
        }

        try {
            // TODO: This can be done better with a `SparkAppHandle.Listener`.
            while (!handle.getState().isFinal()) {
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING,"Spark job execution was interrupted!");
        }

        LOGGER.log(Level.INFO,"Spark job finished!");
    }
}
