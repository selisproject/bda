package gr.ntua.ece.cslab.selis.bda.controller;

import gr.ntua.ece.cslab.selis.bda.analytics.AnalyticsInstance;
import gr.ntua.ece.cslab.selis.bda.common.storage.TestConnector;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.JobDescription;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Recipe;
import gr.ntua.ece.cslab.selis.bda.controller.resources.JobResource;
import gr.ntua.ece.cslab.selis.bda.controller.resources.MessageResource;
import gr.ntua.ece.cslab.selis.bda.controller.resources.RecipeResource;
import org.json.JSONObject;

import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EntrypointTest {


    Logger LOGGER = Logger.getLogger(EntrypointTest.class.getCanonicalName());

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

    /*
    private static final String MESSAGE = "{" +
            "'message_type': 'recipe', " +
            "'payload': {" +
                "'supplier_id': 1," +
                "'sales_forecast': {'mpla1' : 1, 'mpla2' : 'mpla'}," +
                "'warehouse_id': 1" +
                "}" +
            "}";
    }
    */
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


    public static void create_folders() {

        File theDir = new File("/testuploads/");
        if (!theDir.exists()) {
            theDir.mkdir();
        }
        theDir = new File("/testresults/");
        if (!theDir.exists()) {
            theDir.mkdir();
        }

    }

    private void destroy_folders() {

        File theDir = new File("/testuploads/");
        if (theDir.exists()) {
            theDir.delete();
        }
        theDir = new File("/testresults/");
        if (theDir.exists()) {
            theDir.delete();
        }
    }


    @org.junit.Before
    public void setUp() {
        TestConnector.init("../conf/bda.properties");
        LOGGER.log(Level.INFO, "Creating folders for uploaded recipes and recipe results");
        create_folders();
    }

    @org.junit.Test
    public void test() throws Exception {
        String SCNslug = "testll";
        // TODO: Create SCN

        LOGGER.log(Level.INFO, "About to insert new messageType...");
        MessageType msgType = messageTypeFromString(MSG_TYPE_STRING);
        messageResource.insert(null, SCNslug, msgType);
        msgType = MessageType.getMessageByName(SCNslug, msgType.getName());
        LOGGER.log(Level.INFO, "Inserted : \t" + msgType.toString());

        LOGGER.log(Level.INFO, "About to insert new recipe...");
        recipeResource.insert(null, SCNslug, RECIPE_STRING);
        recipe = Recipe.getRecipeByName(SCNslug, recipeFromString(RECIPE_STRING).getName());
        LOGGER.log(Level.INFO, "Inserted : \t" + recipe.toString());

        LOGGER.log(Level.INFO, "About to upload recipe file...");
        InputStream uploadedFile = new FileInputStream(new File("/code/examples/recipe.py"));
        recipeResource.upload(SCNslug, recipe.getId(), recipe.getName() + ".py", uploadedFile);
        LOGGER.log(Level.INFO, "File uploaded");

        jobDescription = new JobDescription("recipe_job", "recipe_job", true,
            msgType.getId(), recipe.getId(), "");

        LOGGER.log(Level.INFO, "About to insert new job...");
        jobResource.insert(null, SCNslug, jobDescription);
        LOGGER.log(Level.INFO, "After job insertion...");
        jobDescription = JobDescription.getJobByMessageId(SCNslug, msgType.getId());
        LOGGER.log(Level.INFO, "Inserted : \t" + jobDescription.toString());
        LOGGER.log(Level.INFO, "Running recipe with message");
        (new AnalyticsInstance(SCNslug)).run(jobDescription.getRecipeId(), String.valueOf(msgType.getId()));
        LOGGER.log(Level.INFO, "Recipe result : ");
        try (BufferedReader br = new BufferedReader(new FileReader("/results/recipe.out"))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                System.out.println("\t" + line);
            }
        }
        catch (Exception e) {
            System.out.println(e);
        }
        // TODO: Get res from kpidb
        /*List<Tuple> results = Entrypoint.analyticsComponent.getKpidb().fetch(recipe.getName(), "rows", 1);
        LOGGER.log(Level.INFO, "KPIDB entry:" );
        int i = 0;
        for (Tuple t : results) {
            LOGGER.log(Level.INFO, "\tResult #" + i);
            for (KeyValue e : t.getTuple()) {
                LOGGER.log(Level.INFO, "\t\t" + e.toString());
            }
            i++;
        }*/

        // TODO: Destroy SCN
    }

    @org.junit.After
    public void tearDown(){
        TestConnector.getInstance().close();
        destroy_folders();
        // TODO: Destroy BDA test db
    }

}
