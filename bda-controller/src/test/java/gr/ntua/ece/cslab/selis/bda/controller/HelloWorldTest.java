package gr.ntua.ece.cslab.selis.bda.controller;

import gr.ntua.ece.cslab.selis.bda.analytics.AnalyticsSystem;
import gr.ntua.ece.cslab.selis.bda.controller.beans.JobDescription;
import gr.ntua.ece.cslab.selis.bda.controller.beans.MessageType;
import gr.ntua.ece.cslab.selis.bda.controller.beans.Recipe;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.BDAdbConnector;
import gr.ntua.ece.cslab.selis.bda.controller.resources.JobResource;
import gr.ntua.ece.cslab.selis.bda.controller.resources.MessageResource;
import gr.ntua.ece.cslab.selis.bda.controller.resources.RecipeResource;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.HttpOutput;
import org.eclipse.jetty.server.Response;
import org.json.JSONObject;

import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Created by Giannis Giannakopoulos on 8/31/17.
 */
public class HelloWorldTest {

    Logger LOGGER = Logger.getLogger(HelloWorldTest.class.getCanonicalName());

    MessageType msgType;
    Recipe recipe;
    JobDescription jobDescription;

    MessageResource messageResource;
    RecipeResource recipeResource;
    JobResource jobResource;


    private static final String MSG_TYPE_STRING =
            "{" +
                    "\"name\":\"exampleMsgType\","+
                    "\"description\":\"exampleMsgType\"," +
                    "\"active\": true," +
                    "\"format\":\"{" +
                        "'message_type': 'exampleMsgType', " +
                        "'payload': {" +
                            "'supplier_id': 'integer'," +
                            "'sales_forecast': 'jsonb'," +
                            "'warehouse_id': 'integer'" +
                        "}" +
                    "}\"" +
            "}";

    private static final String RECIPE_STRING =
            "{"+
                "\"name\" : \"recipe\"," +
                "\"description\" : \"recipe\"," +
                "\"executable_path\" : \"\"," +
                "\"engine_id\" : 1," +
                "\"args\" : {" +
                    "\"intarg\" : 1," +
                    "\"strarg\" : \"string\"" +
                "}" +
            "}";

    String jobDescriptionString;

    private static final String DELETE_MSG_TYPE_QUERY =
            "DELETE FROM message_type " +
            "WHERE id = ?;";

    private static final String DELETE_RECIPE_QUERY =
            "DELETE FROM recipes " +
            "WHERE id = ?;";

    private static final String DELETE_JOB_QUERY =
            "DELETE FROM jobs " +
            "WHERE id = ?;";

    private void execute_delete(String query, int id) {
        Connection connection = BDAdbConnector.getInstance().getBdaConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(query);
            statement.setInt(1, id);

            statement.executeUpdate();

        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    private MessageType messageTypeFromString(String msg) {
        JSONObject obj = new JSONObject(msg);

        return new MessageType(obj.getString("name"),
                obj.getString("description"),
                obj.getBoolean("active"),
                obj.getString("format")
        );

    }

    private Recipe recipeFromString(String recipe) {
        JSONObject obj = new JSONObject(recipe);

        return new Recipe(obj.getString("name"),
                obj.getString("description"),
                obj.getString("executable_path"),
                obj.getInt("engine_id"),
                obj.getJSONObject("args").toString());
    }
    private JobDescription jobDescriptionFromString(String job) {
        JSONObject obj = new JSONObject(job);

        return new JobDescription(obj.getString("name"),
                obj.getString("description"),
                obj.getBoolean("active"),
                obj.getInt("messageTypeId"),
                obj.getInt("recipeId"),
                obj.getString("job_type"));
    }

    private ResultSet fetch_engines() {
        LOGGER.log(Level.INFO, "Fetch execution engines for analytics module.");
        Connection conn = BDAdbConnector.getInstance().getBdaConnection();

        Statement statement;
        ResultSet engines = null;

        try {
            statement = conn.createStatement();

            engines = statement.executeQuery("SELECT * FROM execution_engines;");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return engines;
    }

    private void initialize_components() {
        LOGGER.log(Level.INFO, "Initializing BDADB Connector...");
        BDAdbConnector.init(
                Entrypoint.configuration.storageBackend.getBdaDatabaseURL(),
                Entrypoint.configuration.storageBackend.getDimensionTablesURL(),
                Entrypoint.configuration.storageBackend.getDbUsername(),
                Entrypoint.configuration.storageBackend.getDbPassword()
        );


        LOGGER.log(Level.INFO, "Initializing Analytics SubModule...");
        Entrypoint.analyticsComponent = AnalyticsSystem.getInstance(
                Entrypoint.configuration.kpiBackend.getDbUrl(),
                Entrypoint.configuration.kpiBackend.getDbUsername(),
                Entrypoint.configuration.kpiBackend.getDbPassword(),
                fetch_engines()
        );

        LOGGER.log(Level.INFO, "Creating folders for uploaded recipes and recipe results");

        File theDir = new File("/uploads/");
        if (!theDir.exists()) {
            theDir.mkdir();
        }
        theDir = new File("/results/");
        if (!theDir.exists()) {
            theDir.mkdir();
        }
    }


    @org.junit.Before
    public void setUp() throws Exception {
        Entrypoint.configuration = Configuration.
                parseConfiguration("/code/conf/bda.properties");

        initialize_components();


        messageResource = new MessageResource();
        recipeResource = new RecipeResource();
        jobResource = new JobResource();

        msgType = messageTypeFromString(MSG_TYPE_STRING);
        //jobDescription = jobDescriptionFromString();

    }

    @org.junit.Test
    public void test() throws Exception {

        LOGGER.log(Level.INFO, "About to insert new messageType...");
        messageResource.insert(null , msgType);
        msgType = MessageType.getMessageByName(msgType.getName());
        LOGGER.log(Level.INFO, "Inserted : \n\t" + msgType.toString());

        LOGGER.log(Level.INFO, "About to insert new recipe...");
        recipeResource.insert(null , RECIPE_STRING);
        recipe = Recipe.getRecipeByName(recipeFromString(RECIPE_STRING).getName());
        LOGGER.log(Level.INFO, "Inserted : \n\t" + recipe.toString());

        LOGGER.log(Level.INFO, "About to upload recipe file...");
        InputStream uploadedFile = new FileInputStream(new File("/code/examples/recipe.py"));
        recipeResource.upload(recipe.getId(), recipe.getName(), uploadedFile);
        LOGGER.log(Level.INFO, "File uploaded");



    }

    @org.junit.After
    public void tearDown() throws Exception {

        LOGGER.log(Level.INFO, "Delete test message type from database");
        execute_delete(DELETE_MSG_TYPE_QUERY, msgType.getId());

        LOGGER.log(Level.INFO, "Delete recipe from database");
        execute_delete(DELETE_RECIPE_QUERY, recipe.getId());

        LOGGER.log(Level.INFO, "Closing connections...");
        BDAdbConnector.getInstance().getBdaConnection().close();
        BDAdbConnector.getInstance().getLabConnection().close();
        Entrypoint.analyticsComponent.getKpidb().stop();
        
    }



}