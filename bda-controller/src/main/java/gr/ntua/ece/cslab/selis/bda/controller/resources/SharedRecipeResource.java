package gr.ntua.ece.cslab.selis.bda.controller.resources;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.Recipe;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

@Path("sharedrecipes")
public class SharedRecipeResource {
    private final static Logger LOGGER = Logger.getLogger(SharedRecipeResource.class.getCanonicalName());

    /**
     * Returns all the registered shared recipes.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<Recipe> getSharedRecipes() {
        List<Recipe> recipes = new LinkedList<>();

        try {
            recipes = Recipe.getSharedRecipes();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return recipes;
    }
}
