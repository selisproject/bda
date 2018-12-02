package gr.ntua.ece.cslab.selis.bda.controller.resources;

import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionEngine;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.SharedRecipe;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Recipe;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.RequestResponse;
import org.apache.commons.io.IOUtils;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.*;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.List;
import java.util.LinkedList;


@Path("recipe")
public class RecipeResource {
    private final static Logger LOGGER = Logger.getLogger(RecipeResource.class.getCanonicalName());

    /**
     * Recipe insert method
     * @param r the recipe to insert
     */
    @PUT
    @Path("{slug}")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public RequestResponse insert(@Context HttpServletResponse response,
                                  @PathParam("slug") String slug,
                                  Recipe r) {

        String status = "OK";
        String details = "";

        try {

            List<ExecutionEngine> engines = ExecutionEngine.getEngines();

            boolean correctEngine = false;
            for (ExecutionEngine engine : engines) {
                if (engine.getId() == r.getEngineId()) {
                    correctEngine = true;
                }
            }

            if (correctEngine) {

                // If recipe id is provided in recipe definition, it means that a shared recipe
                // is about to be used. Import data from the corresponding SharedRecipe object.
                if (r.getId() != 0) {
                    SharedRecipe shRecipe = SharedRecipe.getSharedRecipeById(r.getId());
                    r.setDescription(shRecipe.getDescription());
                    r.setEngineId(shRecipe.getEngine_id());
                    r.setExecutablePath(shRecipe.getExecutable_path());
                    r.setShared(true);
                }
                r.save(slug);

                details = Integer.toString(r.getId());

                if (response != null) {
                    response.setStatus(HttpServletResponse.SC_CREATED);
                }
            }
            else {
                LOGGER.log(Level.WARNING, "Bad engine id provided!");
                if (response != null) {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.SEVERE, e.toString());

            status = "ERROR";
            if (response != null) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }
        }

        try {
            if (response != null) {
                response.flushBuffer();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new RequestResponse(status, details);
    }

    @PUT
    @Path("{slug}/upload/{id}/{filename}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public RequestResponse upload(@Context HttpServletResponse response,
                                  @PathParam("slug") String slug,
                                  @PathParam("id") int recipeId,
                                  @PathParam("filename") String recipeName,
                                  InputStream recipeBinary)  {


        String status = "OK";
        String details = "";

        try {
            // If upload is about to be performed for a shared recipe,
            // cancel upload
            Recipe recipe = Recipe.getRecipeById(slug, recipeId);
            if (recipe.isShared()) {
                LOGGER.log(Level.WARNING, "Cannot modify shared recipe executable!");
                status="BAD REQUEST";
                if (response != null) {
                    response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                }
            }
            else {
                String binaryPath = "/uploads/" + recipeId + "_" + recipeName;

                saveFile(recipeBinary, binaryPath);

                recipe.setExecutablePath(binaryPath);
                recipe.save(slug);
            }
        } catch (Exception e) {

            e.printStackTrace();
            status="ERROR";
            if (response != null) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }
        }
        return new RequestResponse(status, details);
    }

    /**
     * Returns all the registered recipes.
     */
    @GET
    @Path("{slug}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<Recipe> getRecipesView(@PathParam("slug") String slug) {
        List<Recipe> recipes = new LinkedList<Recipe>();

        try {
            recipes = Recipe.getRecipes(slug);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return recipes;
    }


    private void saveFile(InputStream uploadedInputStream, String serverLocation) {

        try {
            File outputFile = new File(serverLocation);
            OutputStream outputStream = new FileOutputStream(outputFile);

            IOUtils.copy(uploadedInputStream, outputStream);

            outputStream.close();
            uploadedInputStream.close();

        } catch (IOException e) {

            e.printStackTrace();
        }

    }

}
