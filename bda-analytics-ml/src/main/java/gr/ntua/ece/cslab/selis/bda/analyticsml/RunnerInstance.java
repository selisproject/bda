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

    public RunnerInstance(String scnSlug, int jobId) throws Exception {
        this.scnSlug = scnSlug;
        this.msgInfo = null;

        job = JobDescription.getJobById(scnSlug, jobId);
        recipe = Recipe.getRecipeById(scnSlug, job.getRecipeId());

        try {
            engine = ExecutionEngine.getEngineById(recipe.getEngineId());
        } catch (SQLException e) {
            e.printStackTrace();
            throw new Exception("Execution engine not found.");
        }
    }

    public void loadLivySession(JobDescription j, Recipe r, MessageType m, String messageId){
        LOGGER.log(Level.INFO, "Creating session for " + j.getName() + " job.");
        new Thread(() -> {
            try {
                LivyRunner runner = new LivyRunner(r, m, messageId, j, scnSlug);
                String sessionId = runner.createSession();
                if (sessionId==null)
                    return;
                // TODO: Load dataframes in session
                JobDescription.storeSession(scnSlug, j.getId(), Integer.valueOf(sessionId));
                LOGGER.log(Level.INFO, "Session created.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }

    public static void deleteLivySession(String slug, JobDescription j){
        LOGGER.log(Level.INFO, "Destroying session with id " + j.getSessionId());
        new Thread(() -> {
            try {
                LivyRunner.deleteSession(String.valueOf(j.getSessionId()));
            } catch (Exception e) {
                e.printStackTrace();
            }
            try {
                JobDescription.storeSession(slug, j.getId(), null);
            } catch (Exception e) {
            }
        }).start();
    }

    public void run(String messageId) throws Exception {

        LOGGER.log(Level.INFO, "Launching " + job.getName() + " recipe.");
        Runnable runner = RunnerFactory.getInstance().getRunner(recipe, engine, msgInfo, messageId, job, this.scnSlug);
        Thread thread = new Thread(runner);

        thread.start();
    }

    public void schedule(){
        // TODO: create a new cron job
        new Thread(() -> {

        }).start();
    }

    public static void unschedule(){
        // TODO: delete cron job
        new Thread(() -> {

        }).start();
    }
}
