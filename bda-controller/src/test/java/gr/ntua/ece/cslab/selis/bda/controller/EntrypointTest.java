package gr.ntua.ece.cslab.selis.bda.controller;

import gr.ntua.ece.cslab.selis.bda.analytics.AnalyticsInstance;
import gr.ntua.ece.cslab.selis.bda.common.storage.AbstractTestConnector;
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

    @org.junit.Before
    public void setUp() {
        super.setUp();
    }

    @org.junit.After
    public void tearDown(){
        super.tearDown();
    }

    @org.junit.Test
    public void test() throws Exception {
        MessageResource messageResource = new MessageResource();
        RecipeResource recipeResource = new RecipeResource();
        JobResource jobResource = new JobResource();
        DatastoreResource datastoreResource = new DatastoreResource();
        String SCNslug = "testll";

        ScnDbInfo scnDbInfo = new ScnDbInfo(SCNslug,"LLtest","","lltestdb");
        datastoreResource.createNewScn(null, scnDbInfo);

        MasterData masterData = new ObjectMapper().readValue(new File("/code/examples/master_data.json"), MasterData.class);
        datastoreResource.bootstrap(null, SCNslug, masterData);

        LOGGER.log(Level.INFO, "About to insert new messageType...");
        MessageType msgType = new ObjectMapper().readValue(new File("/code/examples/msgtype.json"), MessageType.class);
        messageResource.insert(null, SCNslug, msgType);
        msgType = MessageType.getMessageByName(SCNslug, msgType.getName());
        LOGGER.log(Level.INFO, "Inserted : \t" + msgType.toString());

        LOGGER.log(Level.INFO, "About to insert new recipe...");
        Recipe recipe = new ObjectMapper().readValue(new File("/code/examples/recipe.json"), Recipe.class);
        recipeResource.insert(null, SCNslug, recipe);
        LOGGER.log(Level.INFO, "Inserted : \t" + recipe.toString());

        /*LOGGER.log(Level.INFO, "About to upload recipe file...");
        InputStream uploadedFile = new FileInputStream(new File("/code/examples/recipe.py"));
        recipeResource.upload(SCNslug, recipe.getId(), recipe.getName() + ".py", uploadedFile);
        LOGGER.log(Level.INFO, "File uploaded");

        JobDescription jobDescription = new JobDescription("recipe_job", "recipe_job", true,
            msgType.getId(), recipe.getId(), "");

        LOGGER.log(Level.INFO, "About to insert new job...");
        jobResource.insert(null, SCNslug, jobDescription);
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

        datastoreResource.destroyScn(null, scnDbInfo.getId());
    }

}
