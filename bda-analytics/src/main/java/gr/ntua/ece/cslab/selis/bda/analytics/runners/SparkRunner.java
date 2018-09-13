package gr.ntua.ece.cslab.selis.bda.analytics.runners;

import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.launcher.SparkAppHandle;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.SQLException;

import gr.ntua.ece.cslab.selis.bda.common.Configuration;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
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
            Configuration configuration = Configuration.getInstance();

            try {
                SparkLauncher launcher = new SparkLauncher()
                    .setMaster(configuration.execEngine.getSparkMaster())
                    .setDeployMode(configuration.execEngine.getSparkDeployMode())
                    .setConf(SparkLauncher.DRIVER_MEMORY,
                             configuration.execEngine.getSparkConfDriverMemory())
                    .setConf(SparkLauncher.EXECUTOR_MEMORY,
                             configuration.execEngine.getSparkConfExecutorMemory())
                    .setConf(SparkLauncher.EXECUTOR_CORES,
                             configuration.execEngine.getSparkConfExecutorCores())
                    .setAppResource(this.recipeResource);

                if (configuration.execEngine.getSparkConfJars() != null) {
                    launcher.addSparkArg("--jars",
                                         configuration.execEngine.getSparkConfJars());
                    launcher.addSparkArg("--driver-class-path",
                                         configuration.execEngine.getSparkConfJars());
                }
                if (message != "") {
                    launcher.addAppArgs(message);
                }

                ScnDbInfo scn =  ScnDbInfo.getScnDbInfoBySlug(scnSlug);

                launcher.addAppArgs(
                    "--selis-scn-db", scn.getDtDbname(),
                    "--selis-dt-url", configuration.storageBackend.getDimensionTablesURL(),
                    "--selis-dt-user", configuration.storageBackend.getDbUsername(),
                    "--selis-dt-pass", configuration.storageBackend.getDbPassword()
                );

                handle = launcher.startApplication();
            } catch (IOException e) {
                e.printStackTrace();
                LOGGER.log(Level.WARNING,"Spark job failed to start!");
            } catch (SQLException e) {
                e.printStackTrace();
                LOGGER.log(Level.WARNING,"Spark job failed. Unknown SCN.");
            }

            try {
                // TODO: This can be done better with a `SparkAppHandle.Listener`.
                while (!handle.getState().isFinal()) {
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Spark job execution was interrupted!");
            }

            LOGGER.log(Level.INFO, "Spark job finished!");
        } catch (IllegalStateException e) {
            LOGGER.log(Level.WARNING,
                       "Spark job execution failed. Uninitialized configuration!");
        }
    }
}
