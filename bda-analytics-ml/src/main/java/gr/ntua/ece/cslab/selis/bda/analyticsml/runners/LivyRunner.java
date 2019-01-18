package gr.ntua.ece.cslab.selis.bda.analyticsml.runners;

import gr.ntua.ece.cslab.selis.bda.common.Configuration;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionEngine;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Recipe;

import org.json.JSONObject;
import javax.ws.rs.client.*;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LivyRunner extends ArgumentParser implements Runnable {
    private final static Logger LOGGER = Logger.getLogger(LivyRunner.class.getCanonicalName());

    private String messageId;
    private String scnSlug;
    private Recipe recipe;
    private ExecutionEngine engine;
    private WebTarget resource;
    private Configuration configuration;

    public LivyRunner(Recipe recipe, ExecutionEngine engine,
                       String messageId, String scnSlug) {

        this.messageId = messageId;
        this.scnSlug = scnSlug;
        this.recipe = recipe;
        this.engine = engine;

        this.configuration = Configuration.getInstance();

        Client client = ClientBuilder.newClient();
        this.resource = client.target(configuration.execEngine.getLivyURL());

    }

    private void deleteSession(String sessionId){
        Invocation.Builder request = resource.path("/sessions/"+sessionId).request();
        Response response = request.delete();
        if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
            LOGGER.log(Level.INFO,
                    "SUCCESS: Session has been deleted.");
        } else {
            LOGGER.log(Level.SEVERE,
                    "Request to delete session has failed, got error: {0}",
                    response.getStatusInfo().getReasonPhrase());
        }
    }

    @Override
    public void run() {
        ScnDbInfo scn;
        try {
            scn = ScnDbInfo.getScnDbInfoBySlug(scnSlug);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        String sessionId, statementId;

        // Create session and upload binaries
        Invocation.Builder request = resource.path("/sessions").request();
        JSONObject data = new JSONObject();
        data.put("kind","pyspark");
        List<String> files = new ArrayList<>();
        files.add(recipe.getExecutablePath());
        //files.add("file:///resources/RecipeDataLoader.py");
        data.put("files", files);
        if (configuration.execEngine.getSparkConfJars() != null) {
            List<String> jars = new ArrayList<>();
            jars.add("file://"+configuration.execEngine.getSparkConfJars());
            data.put("jars", jars);

            Map<String, String> classpath = new HashMap<>();
            classpath.put("spark.driver.extraClassPath","file://"+configuration.execEngine.getSparkConfJars());
            data.put("conf", classpath);
        }
        data.put("driverMemory", configuration.execEngine.getSparkConfDriverMemory());
        data.put("executorMemory", configuration.execEngine.getSparkConfExecutorMemory());
        data.put("executorCores", Integer.valueOf(configuration.execEngine.getSparkConfExecutorCores()));

        Response response = request.post(Entity.json(data.toString()));
        if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
            LOGGER.log(Level.INFO,
                    "SUCCESS: Request to create session has been sent. Waiting for session to start..");
            sessionId = new JSONObject(response.readEntity(String.class)).get("id").toString();
        } else {
            LOGGER.log(Level.SEVERE,
                    "Request to create session has failed, got error: {0}",
                    response.getStatusInfo().getReasonPhrase());
            return;
        }

        // Check that session is ready to receive job
        request = resource.path("/sessions/"+sessionId+"/state").request();
        response = request.get();
        String state = "not_started";
        while (!state.matches("idle")) {
            if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
                response = request.get();
                state = new JSONObject(response.readEntity(String.class)).get("state").toString();
                if (state.matches("shutting_down") || state.matches("error") || state.matches("dead") || state.matches("success")){
                    LOGGER.log(Level.SEVERE, "Session is not alive, status: {0}", state);
                    return;
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                LOGGER.log(Level.SEVERE,
                        "Request to get session status has failed, got error: {0}",
                        response.getStatusInfo().getReasonPhrase());
                deleteSession(sessionId);
                return;
            }
        }

        // Launch job
        request = resource.path("/sessions/"+sessionId+"/statements").request();
        data = new JSONObject();
        //String code = "import dataloader; dataloader.load(); import job; job.run(textFile);";
        String code = "skus_dataframe = spark.read.jdbc(url='"+configuration.storageBackend.getDimensionTablesURL()+scn.getDtDbname()+
                    "',properties={'user':'"+configuration.storageBackend.getDbUsername()+"','password':'"+
                    configuration.storageBackend.getDbPassword()+"'},table='inventoryitems'); skus_dataframe.show();";
        data.put("code",code);
        response = request.post(Entity.json(data.toString()));
        if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
            LOGGER.log(Level.INFO,
                    "SUCCESS: Job has been submitted.");
            statementId = new JSONObject(response.readEntity(String.class)).get("id").toString();
        } else {
            LOGGER.log(Level.SEVERE,
                    "Request to launch job has failed, got error: {0}",
                    response.getStatusInfo().getReasonPhrase());
            deleteSession(sessionId);
            return;
        }

        // Get job status
        request = resource.path("/sessions/"+sessionId+"/statements/"+statementId).request();
        response = request.get();
        state = "waiting";
        JSONObject result = null;
        while (!state.matches("available")) {
            if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
                response = request.get();
                result = new JSONObject(response.readEntity(String.class));
                state = result.get("state").toString();
                if (state.matches("error") || state.contains("cancel")){
                    LOGGER.log(Level.SEVERE, "Job failed, status: {0}", result.get("output"));
                    deleteSession(sessionId);
                    return;
                }
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                LOGGER.log(Level.SEVERE,
                        "Request to get job status has failed, got error: {0}",
                        response.getStatusInfo().getReasonPhrase());
                deleteSession(sessionId);
                return;
            }
        }
        String output = new JSONObject(result.get("output").toString()).get("data").toString();
        LOGGER.log(Level.INFO, "Job result: " +output);

        // Delete session
        deleteSession(sessionId);
    }
}
