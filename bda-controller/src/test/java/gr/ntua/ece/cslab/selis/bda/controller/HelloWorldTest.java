package gr.ntua.ece.cslab.selis.bda.controller;

import gr.ntua.ece.cslab.selis.bda.controller.beans.Recipe;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.BDAdbConnector;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;


/**
 * Created by Giannis Giannakopoulos on 8/31/17.
 */
public class HelloWorldTest {



    @org.junit.Before
    public void setUp() throws Exception {
        String username = "selis";
        String password = "123456";
        String bda_url      = "jdbc:postgresql://selis-postgres:5432/selis_bda_db";
        String lab_url = "jdbc:postgresql://selis-postgres:5432/selis_lab_db";
        BDAdbConnector.init(bda_url, lab_url, username, password);

    }

    @org.junit.Test
    public void test() throws Exception {
        Recipe recipe = new Recipe("dummy", "dummy",
                "mypath", 1,
                new JSONObject("{\"mpla1\" : 1, \"mpla2\" : \"mpla\"}"));
        Recipe recipe2 = new Recipe("dummy2", "dummy2",
                "mypath2", 1,
                new JSONObject("{\"mpla1\" : 1, \"mpla2\" : \"mpla\"}"));
        recipe.save();
        recipe2.save();
        List<Recipe> recipes = Recipe.getRecipes();
        for (Recipe r : recipes) {
            System.out.println(r.toString());
        }
    }

    @org.junit.After
    public void tearDown() throws Exception {
        Connection connection = BDAdbConnector.getInstance().getBdaConnection();
        PreparedStatement statement = connection.prepareStatement("DELETE FROM recipes WHERE name=\'dummy\' or name = \'dummy2\';");
        statement.executeUpdate();
        
    }



}