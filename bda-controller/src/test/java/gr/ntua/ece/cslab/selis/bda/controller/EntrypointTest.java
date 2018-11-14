package gr.ntua.ece.cslab.selis.bda.controller;

import gr.ntua.ece.cslab.selis.bda.common.storage.AbstractTestConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Recipe;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.controller.resources.DatastoreResource;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;
import gr.ntua.ece.cslab.selis.bda.controller.resources.JobResource;
import gr.ntua.ece.cslab.selis.bda.controller.resources.MessageResource;
import gr.ntua.ece.cslab.selis.bda.controller.resources.RecipeResource;

import org.codehaus.jackson.map.ObjectMapper;
import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EntrypointTest extends AbstractTestConnector  {
    Logger LOGGER = Logger.getLogger(EntrypointTest.class.getCanonicalName());

    MessageResource messageResource;
    RecipeResource recipeResource;
    JobResource jobResource;
    DatastoreResource datastoreResource;

    ScnDbInfo scnDbInfo;
    MessageType msgType;
    Recipe recipe;
    JobDescription jobDescription;

    String SCNslug = "testll";

    @org.junit.Before
    public void setUp() throws SystemConnectorException {
        super.setUp();
        messageResource = new MessageResource();
        recipeResource = new RecipeResource();
        jobResource = new JobResource();
        datastoreResource = new DatastoreResource();

        scnDbInfo = new ScnDbInfo(SCNslug,"LLtest","","lltestdb");
        //datastoreResource.createNewScn(null, scnDbInfo);
    }

    @org.junit.After
    public void tearDown() throws SystemConnectorException {
        //datastoreResource.destroyScn(null, scnDbInfo.getId());
        super.tearDown();
    }

    @org.junit.Test
    public void test() throws Exception {

        /*MasterData masterData = new ObjectMapper().readValue(new File("/code/examples/master_data.json"), MasterData.class);
        datastoreResource.bootstrap(null, SCNslug, masterData);

        LOGGER.log(Level.INFO, "About to insert new messageType...");
        msgType = new ObjectMapper().readValue(new File("/code/examples/msgtype.json"), MessageType.class);
        messageResource.insert(null, SCNslug, msgType);
        msgType = MessageType.getMessageByName(SCNslug, msgType.getName());
        LOGGER.log(Level.INFO, "Inserted : \t" + msgType.toString());
        
        LOGGER.log(Level.INFO, "About to insert new recipe...");
        recipe = new ObjectMapper().readValue(new File("/code/examples/recipe.json"), Recipe.class);
        recipeResource.insert(null, SCNslug, recipe);
        recipe = Recipe.getRecipeByName(SCNslug, recipe.getName());
        LOGGER.log(Level.INFO, "Inserted : \t" + recipe.toString());

        LOGGER.log(Level.INFO, "About to upload recipe file...");
        InputStream uploadedFile = new FileInputStream(new File("/code/examples/recipe.py"));
        recipeResource.upload(SCNslug, recipe.getId(), recipe.getName() + ".py", uploadedFile);
        LOGGER.log(Level.INFO, "File uploaded");

        jobDescription = new JobDescription("recipe_job", "recipe_job", true,
            msgType.getId(), recipe.getId(), "");

        LOGGER.log(Level.INFO, "About to insert new job...");
        jobResource.insert(null, SCNslug, jobDescription);
        jobDescription = JobDescription.getJobByMessageId(SCNslug, msgType.getId());
        LOGGER.log(Level.INFO, "Inserted : \t" + jobDescription.toString());*/

        /*LOGGER.log(Level.INFO, "Running recipe with message");
        (new AnalyticsInstance(SCNslug)).run(jobDescription.getRecipeId(), String.valueOf(msgType.getId()));
        LOGGER.log(Level.INFO, "Recipe result : ");
        try (BufferedReader br = new BufferedReader(new FileReader("/results/recipe.out"))) {
            String line = null;
            while ((line = br.readLine()) != null) {
                LOGGER.log(Level.INFO, "\t" + line);
            }
        }
        catch (Exception e) {
            LOGGER.log(Level.INFO, e);
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
        }*/
    }

}
