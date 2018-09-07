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
        try {
            new SparkLauncher()
                .setMaster("yarn")
                .setDeployMode("cluster")
                .setAppResource(this.recipeResource)
                .addAppArgs(message)
                .startApplication();
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING,"Spark job failed to start!");
        }

        /*
        SparkAppHandle handle = null;
        try {
            handle = new SparkLauncher()
                    .setMaster("yarn")
                    .setDeployMode("cluster")
                    .setAppResource(recipe_part)
                    // the three properties below should be removed in the future
                    .setConf("spark.port.maxRetries","100")
                    .addSparkArg("--driver-class-path","/resources/postgresql-42.2.1.jar")
                    .addSparkArg("--jars","/resources/postgresql-42.2.1.jar")
                    //.redirectOutput(new File("/results/" + kpiDescriptor.getName() + ".out"))
                    .addAppArgs(message).startApplication();
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING,"Spark job failed to start!");
        }

        try {
            while (!handle.getState().equals(SparkAppHandle.State.FINISHED) && !handle.getState().equals(SparkAppHandle.State.FAILED) && !handle.getState().equals(SparkAppHandle.State.LOST))
                Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING,"Spark job execution was interrupted!");
        }
        handle.stop();
        handle.kill();
        LOGGER.log(Level.INFO,"Spark job finished!");
        */
    }
}
