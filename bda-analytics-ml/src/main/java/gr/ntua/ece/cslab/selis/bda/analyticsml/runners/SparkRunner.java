/*
 * Copyright 2019 ICCS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gr.ntua.ece.cslab.selis.bda.analyticsml.runners;

import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.launcher.SparkAppHandle;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.SQLException;

import gr.ntua.ece.cslab.selis.bda.common.Configuration;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionEngine;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Recipe;

public class SparkRunner extends ArgumentParser implements Runnable {
    private final static Logger LOGGER = Logger.getLogger(SparkLauncher.class.getCanonicalName());

    String messageId;
    String scnSlug;
    Recipe recipe;
    ExecutionEngine engine;

    public SparkRunner(Recipe recipe, ExecutionEngine engine,
                       String messageId, String scnSlug) {

        this.messageId = messageId;
        this.scnSlug = scnSlug;
        this.recipe = recipe;
        this.engine = engine;
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
                    .setAppResource(this.recipe.getExecutablePath());

                if (configuration.execEngine.getSparkConfJars() != null) {
                    launcher.addSparkArg("--jars",
                                         configuration.execEngine.getSparkConfJars());
                    launcher.addSparkArg("--driver-class-path",
                                         configuration.execEngine.getSparkConfJars());
                }
                if (messageId != "") {
                    launcher.addAppArgs(messageId);
                }

                ScnDbInfo scn =  ScnDbInfo.getScnDbInfoBySlug(scnSlug);

                launcher.addAppArgs(
                    "--selis-scn-name", scn.getName(),
                    "--selis-scn-slug", scn.getSlug(),
                    "--selis-scn-db", scn.getDtDbname(),
                    "--selis-kpi-db", scn.getKpiDbname(),
                    "--selis-kpi-table", recipe.getName(),
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
                LOGGER.log(Level.WARNING,"Spark job failed to start. Unknown SCN.");
            } catch (SystemConnectorException e){
                e.printStackTrace();
                LOGGER.log(Level.WARNING,"Spark job failed to start. Connection to get SCN failed.");
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
