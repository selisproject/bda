package gr.ntua.ece.cslab.selis.bda.analytics;

import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.ExecutEngineDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.Executable;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.ExecutEngineCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.KpiCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.runners.RunnerFactory;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionEngine;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Recipe;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.PostgresqlConnector;
import org.apache.hadoop.service.Service;
import org.json.JSONObject;

import java.sql.*;

public class AnalyticsInstance {

    private ExecutEngineCatalog engineCatalog;
    private KpiCatalog kpiCatalog;
    private String scnSlug;

    public AnalyticsInstance(String scnSlug) {
        this.kpiCatalog = new KpiCatalog();
        this.engineCatalog = new ExecutEngineCatalog();
        this.scnSlug = scnSlug;
    }

    public KpiCatalog getKpiCatalog() { return kpiCatalog; }

    public ExecutEngineCatalog getEngineCatalog() { return engineCatalog; }

    private ExecutEngineDescriptor fetch_engine(int engine_id) {
        ExecutEngineDescriptor engineDescriptor = null;

        try {
            ExecutionEngine engine = ExecutionEngine.getEngineById(engine_id);

            engineDescriptor = new ExecutEngineDescriptor(
                            engine.getName(),
                            engine.getEngine_path(),
                            engine.isLocal_engine(),
                            new JSONObject(engine.getArgs())
            );

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return engineDescriptor;
    }

    private KpiDescriptor fetch_kpi(int recipe_id) {
        Recipe recipe = Recipe.getRecipeById(scnSlug, recipe_id);
        return new KpiDescriptor(
                recipe.getName(),
                recipe.getDescription(),
                new Executable(
                        recipe.getEngineId(),
                        new JSONObject(recipe.getArgs()),
                        recipe.getExecutablePath()
                )
        );

    }

    public void run(int recipe_id, String message){
        KpiDescriptor kpi = fetch_kpi(recipe_id);

        ExecutEngineDescriptor engine =  fetch_engine(kpi.getExecutable().getEngineID());

        Runnable runner = RunnerFactory.getInstance().getRunner(kpi, engine, message, this.scnSlug);

        Thread t = new Thread(runner);
        t.start();

        /*
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        */
    }



}
