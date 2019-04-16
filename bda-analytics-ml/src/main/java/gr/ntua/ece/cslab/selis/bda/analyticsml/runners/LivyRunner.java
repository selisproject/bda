package gr.ntua.ece.cslab.selis.bda.analyticsml.runners;

import gr.ntua.ece.cslab.selis.bda.common.Configuration;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.Connector;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionLanguage;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.PostgresqlConnector;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.JobDescription;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Recipe;

import org.json.JSONObject;
import javax.ws.rs.client.*;
import javax.ws.rs.core.Response;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LivyRunner extends ArgumentParser implements Runnable {
    private final static Logger LOGGER = Logger.getLogger(LivyRunner.class.getCanonicalName());

    private String messageId;
    private String scnSlug;
    private MessageType msgInfo;
    private Recipe recipe;
    private JobDescription job;
    private static WebTarget resource;
    private static Configuration configuration;
    private String recipeClass;
    public String language;
    private String sessionId;

    public LivyRunner(Recipe recipe, MessageType msgInfo,
                      String messageId, JobDescription job, String scnSlug) throws Exception{

        this.messageId = messageId;
        this.msgInfo = msgInfo;
        this.scnSlug = scnSlug;
        this.recipe = recipe;
        this.job = job;
        configuration = Configuration.getInstance();

        Client client = ClientBuilder.newClient();
        resource = client.target(configuration.execEngine.getLivyURL());

        String[] recipe_export = recipe.getExecutablePath().split("\\.");
        recipe_export = recipe_export[recipe_export.length - 2].split("/");
        this.recipeClass = recipe_export[recipe_export.length - 1];

        try {
            this.language = ExecutionLanguage.getLanguageById(recipe.getLanguageId()).getName();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE,"Getting the execution language has failed. Abort creating runner.");
            throw e;
        }

        this.sessionId = String.valueOf(job.getLivySessionId());
    }

    public String createSession(){
        String kind = null, dataLoaderLibrary = null, sessionId;
        if (language.matches("python")){
            kind = "pyspark";
            dataLoaderLibrary = "/RecipeDataLoader.py";
        }

        Invocation.Builder request = resource.path("/sessions").request();
        JSONObject data = new JSONObject();
        data.put("kind",kind);

        Map<String, String> classpath = new HashMap<>();
        List<String> files = new ArrayList<>();
        files.add(recipe.getExecutablePath());
        files.add(dataLoaderLibrary);
        //client mode
        //data.put("files", files);
        //cluster mode
        classpath.put("spark.yarn.dist.pyFiles", String.join(",",files));

        if (configuration.execEngine.getSparkConfJars() != null) {
            List<String> jars = new ArrayList<>();
            jars.add("file://" + configuration.execEngine.getSparkConfJars());
            data.put("jars", jars);
            classpath.put("spark.driver.extraClassPath","file://" + configuration.execEngine.getSparkConfJars());
        }
        if (configuration.execEngine.getSparkConfPackages() != null) {
            classpath.put("spark.jars.packages",configuration.execEngine.getSparkConfPackages());
        }
        if (configuration.execEngine.getSparkConfRepositories() != null) {
            classpath.put("spark.jars.repositories", configuration.execEngine.getSparkConfRepositories());
        }
        if (!classpath.isEmpty())
            data.put("conf", classpath);
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
            return null;
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
                    return null;
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
                return null;
            }
        }

        return sessionId;
    }

    public static void deleteSession(String sessionId){
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

    private List<String> buildDataLoadingPythonCode(ScnDbInfo scn) throws SQLException, SystemConnectorException {

        StringBuilder builder = new StringBuilder();
        builder.append("import RecipeDataLoader; import ").append(recipeClass).append("; ");

        StringBuilder arguments = new StringBuilder();
        arguments.append(msgInfo.getName());

        List<String> dimension_tables = recipe.getArgs().getDimension_tables();
        for (String dimension_table: dimension_tables) {
            builder.append(dimension_table).append(" = RecipeDataLoader.fetch_from_master_data(spark, '")
                    .append(configuration.storageBackend.getDimensionTablesURL()).append(scn.getDtDbname()).append("','")
                    .append(configuration.storageBackend.getDbUsername()).append("','")
                    .append(configuration.storageBackend.getDbPassword()).append("','")
                    .append(dimension_table).append("'); ");
            arguments.append(",").append(dimension_table);
        }

        List<String> eventlog_messages = recipe.getArgs().getMessage_types();
        for (String eventlog_message: eventlog_messages) {
            MessageType msg = MessageType.getMessageByName(this.scnSlug, eventlog_message);
            List<String> columns = msg.getMessageColumns();
            builder.append(eventlog_message).append(" = RecipeDataLoader.fetch_from_eventlog(spark, '")
                    .append(scn.getElDbname()).append("','")
                    .append(eventlog_message).append("','")
                    .append(columns).append("'); ");
            arguments.append(",").append(eventlog_message);
        }

        List<String> columns = msgInfo.getMessageColumns();
        builder.append(msgInfo.getName()).append(" = RecipeDataLoader.fetch_from_eventlog_one(spark, '")
                .append(scn.getElDbname()).append("','")
                .append(messageId).append("','")
                .append(columns).append("'); ");

        return Arrays.asList(builder.toString(),arguments.toString());
    }

    private String buildRecipePythonCode(ScnDbInfo scn, String args) throws SQLException, SystemConnectorException {

        StringBuilder builder = new StringBuilder();
        StringBuilder arguments = new StringBuilder();
        arguments.append(args);

        List<String> other_args = recipe.getArgs().getOther_args();
        for (String arg: other_args)
            arguments.append(",").append(arg);

        builder.append("result = ").append(recipeClass).append(".run(spark, ").append(arguments).append("); ");
        if (this.job.isJobResultPersist()){
            List<String> columns = msgInfo.getMessageColumns();
            builder.append("RecipeDataLoader.save_result_to_kpidb('")
                    .append(PostgresqlConnector.getPostgresConnectionHost(configuration.kpiBackend.getDbUrl())).append("','")
                    .append(PostgresqlConnector.getPostgresConnectionPort(configuration.kpiBackend.getDbUrl())).append("','")
                    .append(scn.getKpiDbname()).append("','")
                    .append(configuration.kpiBackend.getDbUsername()).append("','")
                    .append(configuration.kpiBackend.getDbPassword()).append("','")
                    .append(recipe.getName()).append("',")
                    .append(msgInfo.getName()).append(",'")
                    .append(columns).append("',result);");
        } else {
            Connector conn = Connector.getConnectorInfoById(scn.getConnectorId());
            builder.append("RecipeDataLoader.publish_result('")
                    .append(conn.getAddress()).append("','")
                    .append(conn.getPort()).append("','")
                    .append(configuration.pubsub.getCertificateLocation()).append("','")
                    .append(scn.getSlug()).append("','")
                    .append(recipe.getName()).append("_").append(job.getId()).append("',result);");
        }
        return builder.toString();
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

        String statementId, code = null;

        // Prepare code
        try {
            if (this.language.matches("python")){
                List<String> dataLoadingandArgs = buildDataLoadingPythonCode(scn);
                String dataLoadingCode = dataLoadingandArgs.get(0);
                String dataArgs = dataLoadingandArgs.get(1);
                String recipeCode = buildRecipePythonCode(scn, dataArgs);
                code = dataLoadingCode + recipeCode;
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE,"Building the code has failed. Aborting launching the job.");
            e.printStackTrace();
            return;
        }

        // Create session if it is required
        if (job.getJobType().matches("batch") && sessionId.matches("null")){
            LOGGER.log(Level.INFO,"Creating session for batch job..");
            sessionId = createSession();
        }
        else if (job.getJobType().matches("batch") && !sessionId.matches("null")) {
            LOGGER.log(Level.SEVERE,"Found existing session for batch job. This should never happen!");
            return;
        }
        else if (job.getJobType().matches("streaming") && sessionId.matches("null")){
            LOGGER.log(Level.SEVERE,"Streaming job has no open session. This should never happen!");
            return;
        }
        if (sessionId==null || sessionId.matches("null"))
            return;

        // Launch job
        Invocation.Builder request = resource.path("/sessions/"+sessionId+"/statements").request();
        JSONObject data = new JSONObject();
        data.put("code",code);
        Response response = request.post(Entity.json(data.toString()));
        if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
            LOGGER.log(Level.INFO,
                    "SUCCESS: Job has been submitted.");
            statementId = new JSONObject(response.readEntity(String.class)).get("id").toString();
        } else {
            LOGGER.log(Level.SEVERE,
                    "Request to launch job has failed, got error: {0}",
                    response.getStatusInfo().getReasonPhrase());
            if (job.getJobType().matches("batch"))
                deleteSession(sessionId);
            return;
        }

        // Get job status
        request = resource.path("/sessions/"+sessionId+"/statements/"+statementId).request();
        response = request.get();
        String state = "waiting";
        JSONObject result = null;
        while (!state.matches("available")) {
            if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
                response = request.get();
                result = new JSONObject(response.readEntity(String.class));
                state = result.get("state").toString();
                if (state.matches("error") || state.contains("cancel")){
                    LOGGER.log(Level.SEVERE, "Job failed, status: {0}", result.get("output"));
                    if (job.getJobType().matches("batch"))
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
                if (job.getJobType().matches("batch"))
                    deleteSession(sessionId);
                return;
            }
        }
        if (new JSONObject(result.get("output").toString()).get("status").toString().equals("error")) {
            LOGGER.log(Level.SEVERE,
                    "Job error: {0}",
                    new JSONObject(result.get("output").toString()).get("traceback").toString());
        } else {
            String output = new JSONObject(result.get("output").toString()).get("data").toString();
            LOGGER.log(Level.INFO, "Job result: " + output);
        }

        // Delete session
        if (job.getJobType().matches("batch"))
            deleteSession(sessionId);
    }
}
