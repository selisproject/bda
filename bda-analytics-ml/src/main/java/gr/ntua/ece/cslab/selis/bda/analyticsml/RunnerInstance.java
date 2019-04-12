package gr.ntua.ece.cslab.selis.bda.analyticsml;

import gr.ntua.ece.cslab.selis.bda.analyticsml.runners.LivyRunner;
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
    public ExecutionEngine engine;

    public RunnerInstance(String scnSlug, String messageType) throws Exception {
        this.scnSlug = scnSlug;

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
            throw new Exception("No job found for message " + messageType + ".");
        }

        recipe = Recipe.getRecipeById(scnSlug, job.getRecipeId());

        try {
            engine = ExecutionEngine.getEngineById(recipe.getEngineId());
        } catch (SQLException e) {
            e.printStackTrace();
            throw new Exception("Execution engine not found.");
        }
    }

    public void loadLivySession(JobDescription job, Recipe recipe, MessageType messageType, String messageId) throws Exception{
        LOGGER.log(Level.INFO, "Creating session for " + job.getName() + " job.");
        LivyRunner runner = new LivyRunner(recipe, messageType, messageId, job, scnSlug);
        String sessionId = runner.createSession(runner.language);
        JobDescription.storeSession(scnSlug, job.getId(), Integer.valueOf(sessionId));
    }

    public void deleteLivySession(String sessionId) throws Exception{
        LivyRunner.deleteSession(sessionId);
        JobDescription.storeSession(scnSlug, job.getId(), null);
        LOGGER.log(Level.INFO, "Deleted session with id " + sessionId);
    }

    public void run(String messageId) throws Exception {

        LOGGER.log(Level.INFO, "Launching " + job.getName() + " recipe.");
        Runnable runner = RunnerFactory.getInstance().getRunner(recipe, engine, msgInfo, messageId, job, this.scnSlug);
        Thread thread = new Thread(runner);

        thread.start();
    }

    public void schedule(){
        // TODO: create a new cron job
    }

    public void unschedule(){
        // TODO: delete cron job
    }
}
