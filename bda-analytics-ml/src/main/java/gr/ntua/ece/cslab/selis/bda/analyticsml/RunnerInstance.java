package gr.ntua.ece.cslab.selis.bda.analyticsml;

import gr.ntua.ece.cslab.selis.bda.analyticsml.runners.RunnerFactory;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionEngine;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Recipe;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.JobDescription;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class RunnerInstance {
    private final static Logger LOGGER = Logger.getLogger(RunnerInstance.class.getCanonicalName()+" [" + Thread.currentThread().getName() + "]");
    private String scnSlug;
    private MessageType msgInfo;
    private JobDescription job;
    private Recipe recipe;
    private ExecutionEngine engine;

    public RunnerInstance(String scnSlug) {
        this.scnSlug = scnSlug;
    }

    public void run(String messageType, String messageId) throws Exception {

        try {
            msgInfo = MessageType.getMessageByName(scnSlug, messageType);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new Exception("Message type not found.");
        }
        try {
            // TODO: handle multiple jobs related to a single message
            job = JobDescription.getJobByMessageId(scnSlug, msgInfo.getId());
        } catch (SQLException e) {
            LOGGER.log(Level.INFO, "No recipe found for message " + messageType + ".");
            return;
        }

        recipe = Recipe.getRecipeById(scnSlug, job.getRecipeId());

        try {
            engine = ExecutionEngine.getEngineById(recipe.getEngineId());
        } catch (SQLException e) {
            e.printStackTrace();
            throw new Exception("Execution engine not found.");
        }

        LOGGER.log(Level.INFO, "Launching " + job.getName() + " recipe.");
        Runnable runner = RunnerFactory.getInstance().getRunner(recipe, engine, msgInfo, messageId, this.scnSlug);
        Thread thread = new Thread(runner);

        thread.start();
    }

    public void schedule(){
        // TODO: create a new cron job
    }
}
