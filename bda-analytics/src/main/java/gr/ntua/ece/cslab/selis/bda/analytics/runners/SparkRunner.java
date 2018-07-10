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

    String engine_part;
    String recipe_part;
    KpiDescriptor kpiDescriptor;
    ExecutEngineDescriptor engine;
    //KPIBackend kpidb;
    String message;

    public SparkRunner(KpiDescriptor kpi,
                       ExecutEngineDescriptor engine,
                       String message,
                       String scnSlug) {
        this.kpiDescriptor = kpi;
        this.engine = engine;
        this.message = message;
        //this.kpidb = kpidb;

        engine_part = "";
        recipe_part = "";

        // Set first the path of the engine
        engine_part += engine.getExecutionPreamble();

        if (engine.getArgs().length() != 0) {
            // Add code to support engine arguments
        }

        // Set the path of the recipe executable
        recipe_part += kpi.getExecutable().getOsPath();
    }

    @Override
    public void run() {
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
        /*try {
            while (!handle.getState().equals(SparkAppHandle.State.FINISHED) && !handle.getState().equals(SparkAppHandle.State.FAILED) && !handle.getState().equals(SparkAppHandle.State.LOST))
                Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING,"Spark job execution was interrupted!");
        }
        handle.stop();
        handle.kill();
        LOGGER.log(Level.INFO,"Spark job finished!");*/
    }

}
