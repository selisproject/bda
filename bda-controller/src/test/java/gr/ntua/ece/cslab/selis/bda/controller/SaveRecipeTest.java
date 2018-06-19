package gr.ntua.ece.cslab.selis.bda.controller;

import gr.ntua.ece.cslab.selis.bda.controller.beans.Recipe;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.BDAdbConnector;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;


public class SaveRecipeTest extends AbstractBdaTest {
    @org.junit.Test
    public void test() throws Exception {
        Recipe recipe = new Recipe("dummy", "dummy",
                "mypath", 1,
                "{\"mpla1\" : 1, \"mpla2\" : \"mpla\"}");
        Recipe recipe2 = new Recipe("dummy2", "dummy2",
                "mypath2", 1,
                "{\"mpla1\" : 1, \"mpla2\" : \"mpla\"}");
        recipe.save();
        recipe2.save();
        List<Recipe> recipes = Recipe.getRecipes();
        for (Recipe r : recipes) {
            System.out.println(r.toString());
        }
    }
}
