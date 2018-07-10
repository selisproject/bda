package gr.ntua.ece.cslab.selis.bda.controller;

import gr.ntua.ece.cslab.selis.bda.common.Configuration;
import gr.ntua.ece.cslab.selis.bda.analytics.AnalyticsSystem;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.ConnectorFactory;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.PostgresqlConnector;
import gr.ntua.ece.cslab.selis.bda.controller.beans.JobDescription;
import gr.ntua.ece.cslab.selis.bda.controller.beans.MessageType;
import gr.ntua.ece.cslab.selis.bda.controller.beans.Recipe;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.BDAdbPooledConnector;
import gr.ntua.ece.cslab.selis.bda.controller.resources.JobResource;
import gr.ntua.ece.cslab.selis.bda.controller.resources.MessageResource;
import gr.ntua.ece.cslab.selis.bda.controller.resources.RecipeResource;
import gr.ntua.ece.cslab.selis.bda.datastore.connectors.DatastoreConnector;
import org.json.JSONObject;

import java.io.*;
import java.sql.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Created by Giannis Giannakopoulos on 8/31/17.
 */
public class HelloWorldTest {

    Logger LOGGER = Logger.getLogger(HelloWorldTest.class.getCanonicalName());

    Configuration configuration;
    Connector dtConnector, kpiConnector;
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


    private static final String MESSAGE = "{" +
            "'message_type': 'recipe', " +
            "'payload': {" +
                "'supplier_id': 1," +
                "'sales_forecast': {'mpla1' : 1, 'mpla2' : 'mpla'}," +
                "'warehouse_id': 1" +
                "}" +
            "}";

    private static final String DELETE_MSG_TYPE_QUERY =
            "DELETE FROM message_type " +
            "WHERE id = ?;";

    private static final String DELETE_RECIPE_QUERY =
            "DELETE FROM recipes " +
            "WHERE id = ?;";

    private static final String DELETE_JOB_QUERY =
            "DELETE FROM jobs " +
            "WHERE id = ?;";

    private static final String DROP_DATABASE_SCHEMA_QUERY =
            "DROP SCHEMA IF EXISTS %s CASCADE;";
    /*
    private void execute_delete(String query, int id) {
        Connection connection = BDAdbPooledConnector.getInstance().getBdaConnection();

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

    private void fetch_engines() {
        LOGGER.log(Level.INFO, "Fetch execution engines for analytics module.");
        Connection conn = BDAdbPooledConnector.getInstance().getBdaConnection();

        Statement statement;
        ResultSet engines = null;

        try {
            statement = conn.createStatement();

            engines = statement.executeQuery("SELECT * FROM execution_engines;");

            if (engines != null) {
                while (engines.next()) {
                    Entrypoint.analyticsComponent.getEngineCatalog().addNewExecutEngine(
                            engines.getInt("id"),
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

        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        LOGGER.log(Level.INFO, "Fetched engines : " + Entrypoint.analyticsComponent.
                getEngineCatalog().getAllExecutEngines() + "\n");

    }

    private void initialize_components() {
        LOGGER.log(Level.INFO, "Initializing BDADB KPIConnector...");
        BDAdbPooledConnector.init(
                Entrypoint.configuration.storageBackend.getBdaDatabaseURL(),
                Entrypoint.configuration.storageBackend.getDimensionTablesURL(),
                Entrypoint.configuration.storageBackend.getDbUsername(),
                Entrypoint.configuration.storageBackend.getDbPassword()
        );


        LOGGER.log(Level.INFO, "Initializing Analytics SubModule...");
        Entrypoint.analyticsComponent = AnalyticsSystem.getInstance(
                Entrypoint.configuration.kpiBackend.getDbUrl(),
                Entrypoint.configuration.kpiBackend.getDbUsername(),
                Entrypoint.configuration.kpiBackend.getDbPassword()
        );
        fetch_engines();

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

    private void destroy_fs_data(List<String> files) {
        for (String path : files) {
            File theFile = new File(path);
            if (theFile.exists()) {
                theFile.delete();
            }
        }
        File theDir = new File("/uploads/");
        if (theDir.exists()) {
            theDir.delete();
        }
        theDir = new File("/results/");
        if (theDir.exists()) {
            theDir.delete();
        }
    }
*/
    @org.junit.Before
    public void setUp() throws Exception {
        SystemConnector.init("/code/conf/bda.properties");

        /*configuration = Configuration.parseConfiguration("/code/conf/bda.properties");


        LOGGER.log(Level.INFO, "About to create schemas in test database");


        try {
            KPIPostgresqlConnector.createSchema(configuration.testDb.getDbUrl(),
                    configuration.testDb.getDbUsername(),
                    configuration.testDb.getDbPassword(),
                    configuration.testDb.getDbUsername(),
                    "metadata");
        } catch (SQLException e) {
            e.printStackTrace();
            throw new SystemConnectorException("Could not create Postgresql schema.");
        }


        dtConnector = KPIConnectorFactory.getInstance().generateConnector(
                configuration.testDb.getDbUrl(),
                configuration.testDb.getDbUsername(),
                configuration.testDb.getDbPassword()
        );

        try {
            KPIPostgresqlConnector.createSchema(configuration.testDb.getDbUrl(),
                    configuration.testDb.getDbUsername(),
                    configuration.testDb.getDbPassword(),
                    configuration.testDb.getDbUsername(),
                    "kpi");
        } catch (SQLException e) {
            e.printStackTrace();
            throw new SystemConnectorException("Could not create Postgresql schema.");
        }


        kpiConnector = KPIConnectorFactory.getInstance().generateConnector(
                configuration.testDb.getDbUrl(),
                configuration.testDb.getDbUsername(),
                configuration.testDb.getDbPassword()
        );

        DatastoreConnector localDtConnector = gr.ntua.ece.cslab.selis.bda.datastore.connectors.
                KPIConnectorFactory.getInstance().generateConnector(dtConnector);

        localDtConnector.createMetaTables();
*/
//        messageResource = new MessageResource();
  //    recipeResource = new RecipeResource();
     //   jobResource = new JobResource();

    //    msgType = messageTypeFromString(MSG_TYPE_STRING);

    }

    @org.junit.Test
    public void test() throws Exception {

        /*LOGGER.log(Level.INFO, "About to insert new messageType...");
        messageResource.insert(null , msgType);
        msgType = MessageType.getMessageByName(msgType.getName());
        LOGGER.log(Level.INFO, "Inserted : \t" + msgType.toString());

        LOGGER.log(Level.INFO, "About to insert new recipe...");
        recipeResource.insert(null , RECIPE_STRING);
        recipe = Recipe.getRecipeByName(recipeFromString(RECIPE_STRING).getName());
        LOGGER.log(Level.INFO, "Inserted : \t" + recipe.toString());

        LOGGER.log(Level.INFO, "About to upload recipe file...");
        InputStream uploadedFile = new FileInputStream(new File("/code/examples/recipe.py"));
        recipeResource.upload(recipe.getId(), recipe.getName() + ".py", uploadedFile);
        LOGGER.log(Level.INFO, "File uploaded");

        jobDescription = new JobDescription("recipe_job", "recipe_job", true,
            msgType.getId(), recipe.getId(), "");

        LOGGER.log(Level.INFO, "About to insert new job...");
        jobResource.insert(null, jobDescription);
        LOGGER.log(Level.INFO, "After job insertion...");
        jobDescription = JobDescription.getJobByMessageId(msgType.getId());
        LOGGER.log(Level.INFO, "Inserted : \t" + jobDescription.toString());
        LOGGER.log(Level.INFO, "Running recipe with message");
        Entrypoint.analyticsComponent.run(recipe.getId(), MESSAGE);
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
        List<Tuple> results = Entrypoint.analyticsComponent.getKpidb().fetch(recipe.getName(), "rows", 1);
        LOGGER.log(Level.INFO, "KPIDB entry:" );
        int i = 0;
        for (Tuple t : results) {
            LOGGER.log(Level.INFO, "\tResult #" + i);
            for (KeyValue e : t.getTuple()) {
                LOGGER.log(Level.INFO, "\t\t" + e.toString());
            }
            i++;
        }

*/
    }

    @org.junit.After
    public void tearDown() throws Exception {

        Connection localConnection = null;

        try {
            localConnection = DriverManager.getConnection(
                    configuration.testDb.getDbUrl(),
                    configuration.testDb.getDbUsername(),
                    configuration.testDb.getDbPassword()
            );
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            throw e;
        }

        PreparedStatement statement = localConnection.prepareStatement(
                String.format(DROP_DATABASE_SCHEMA_QUERY, "metadata"));

        statement.executeUpdate();

        statement = localConnection.prepareStatement(
                String.format(DROP_DATABASE_SCHEMA_QUERY, "kpi"));

        statement.executeUpdate();

        localConnection.close();

    }



}
