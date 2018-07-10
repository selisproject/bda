package gr.ntua.ece.cslab.selis.bda.analytics;

import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.ExecutEngineDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.Executable;
import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.ExecutEngineCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.KpiCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.runners.RunnerFactory;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.Recipe;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.PostgresqlConnector;
import org.apache.hadoop.service.Service;
import org.json.JSONObject;

import java.sql.*;

public class AnalyticsInstance {

    //private KPIBackend kpidb;
    private ExecutEngineCatalog engineCatalog;
    private KpiCatalog kpiCatalog;
    private String scnSlug;

    public AnalyticsInstance(String scnSlug) {
        //this.kpidb = new KPIBackend(kpidbURL, username, password);
        this.kpiCatalog = new KpiCatalog();
        this.engineCatalog = new ExecutEngineCatalog();
        this.scnSlug = scnSlug;
    }

    //public KPIBackend getKpidb() {
    //    return kpidb;
    //}

    public KpiCatalog getKpiCatalog() { return kpiCatalog; }

    public ExecutEngineCatalog getEngineCatalog() { return engineCatalog; }

    private static final String SELECT_ENGINE_QUERY =
            "SELECT * FROM execution_engines where id=?;";

    private ExecutEngineDescriptor fetch_engine(int engine_id) {
        PostgresqlConnector connector = (PostgresqlConnector) SystemConnector.getInstance().getBDAconnector();
        Connection conn = connector.getConnection();
        ExecutEngineDescriptor engineDescriptor = null;

        try {
            PreparedStatement statement = conn.prepareStatement(SELECT_ENGINE_QUERY);
            statement.setInt(1, engine_id);
            ResultSet engines = statement.executeQuery("SELECT * FROM execution_engines;");

            if (engines != null) {
                if (engines.next()) {
                    engineDescriptor = new ExecutEngineDescriptor(
                            engines.getString("name"),
                            engines.getString("engine_path"),
                            engines.getBoolean("local_engine"),
                            new JSONObject(engines.getString("args"))
                    );
                }
            }

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
                        recipe.getEngine_id(),
                        new JSONObject(recipe.getArgs()),
                        recipe.getExecutable_path()
                )
        );

    }

    public void run(int recipe_id, String message){
        KpiDescriptor kpi = fetch_kpi(recipe_id);

        ExecutEngineDescriptor engine =  fetch_engine(kpi.getExecutable().getEngineID());

        Runnable runner = RunnerFactory.getInstance().getRunner(kpi, engine, message, this.scnSlug);

        Thread t = new Thread(runner);
        t.start();

        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



}
