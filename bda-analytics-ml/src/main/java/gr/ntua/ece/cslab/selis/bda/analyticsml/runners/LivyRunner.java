package gr.ntua.ece.cslab.selis.bda.analyticsml.runners;

import gr.ntua.ece.cslab.selis.bda.common.Configuration;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionEngine;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Recipe;

import org.json.JSONObject;
import javax.ws.rs.client.*;
import javax.ws.rs.core.Response;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LivyRunner extends ArgumentParser implements Runnable {
    private final static Logger LOGGER = Logger.getLogger(LivyRunner.class.getCanonicalName());

    String messageId;
    String scnSlug;
    Recipe recipe;
    ExecutionEngine engine;

    public LivyRunner(Recipe recipe, ExecutionEngine engine,
                       String messageId, String scnSlug) {

        this.messageId = messageId;
        this.scnSlug = scnSlug;
        this.recipe = recipe;
        this.engine = engine;
    }

    @Override
    public void run() {
        ScnDbInfo scn = null;
        try {
            scn = ScnDbInfo.getScnDbInfoBySlug(scnSlug);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (SystemConnectorException e) {
            e.printStackTrace();
        }
        Configuration configuration = Configuration.getInstance();

        Client client = ClientBuilder.newClient();
        WebTarget resource = client.target(configuration.execEngine.getLivyURL());
        String sessionId, statementId;

        Invocation.Builder request = resource.path("/sessions").request();

        // Create session and upload binaries
        JSONObject data = new JSONObject();
        data.put("kind","pyspark");
        List<String> files = new ArrayList<>();
        files.add(recipe.getExecutablePath());
        //files.add("/RecipeDataLoader.py");
        data.put("files", files);
        if (configuration.execEngine.getSparkConfJars() != null) {
            List<String> jars = new ArrayList<>();
            jars.add("local:/"+configuration.execEngine.getSparkConfJars());
            data.put("jars", jars);
            //data.put("conf", "{\"--driver-class-path\",\"" + configuration.execEngine.getSparkConfJars()+ "\"}");
        }
        data.put("driverMemory", configuration.execEngine.getSparkConfDriverMemory());
        data.put("executorMemory", configuration.execEngine.getSparkConfExecutorMemory());
        data.put("executorCores", Integer.valueOf(configuration.execEngine.getSparkConfExecutorCores()));

        Response response = request.post(Entity.json(data.toString()));
        if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
            LOGGER.log(Level.INFO,
                    "SUCCESS: Request to create session for executable has been sent.");
            sessionId = new JSONObject(response.readEntity(String.class)).get("id").toString();
            LOGGER.log(Level.INFO, sessionId);
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
                    "SUCCESS: Request to create session for executable has been sent.");
            statementId = new JSONObject(response.readEntity(String.class)).get("id").toString();
            LOGGER.log(Level.INFO, statementId);
        } else {
            LOGGER.log(Level.SEVERE,
                    "Request to launch job has failed, got error: {0}",
                    response.getStatusInfo().getReasonPhrase());
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
                    LOGGER.log(Level.SEVERE, "Job has failed, status: {0}", state);
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
                return;
            }
        }
        String output = new JSONObject(result.get("output").toString()).get("data").toString();
        LOGGER.log(Level.INFO, output);
    }
}
