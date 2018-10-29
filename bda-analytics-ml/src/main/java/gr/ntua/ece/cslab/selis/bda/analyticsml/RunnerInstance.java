package gr.ntua.ece.cslab.selis.bda.analyticsml;

import gr.ntua.ece.cslab.selis.bda.analyticsml.runners.RunnerFactory;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionEngine;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.Recipe;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;

import java.sql.SQLException;

public class RunnerInstance {

    private String scnSlug;

    public RunnerInstance(String scnSlug) {
        this.scnSlug = scnSlug;
    }

    public void run(int recipeId, String executionType, String messageId) throws SystemConnectorException {
        Recipe recipe = Recipe.getRecipeById(scnSlug, recipeId);
        ExecutionEngine engine = null;
        try {
            engine = ExecutionEngine.getEngineById(recipe.getEngineId());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        Runnable runner = RunnerFactory.getInstance().getRunner(recipe, engine, messageId, this.scnSlug);

        Thread thread = new Thread(runner);

        thread.start();
    }
}
