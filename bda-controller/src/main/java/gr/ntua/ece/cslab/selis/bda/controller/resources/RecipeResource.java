package gr.ntua.ece.cslab.selis.bda.controller.resources;

import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionEngine;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionLanguage;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Recipe;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.RequestResponse;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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

            List<ExecutionLanguage> languages = ExecutionLanguage.getLanguages();

            boolean correctLanguage = false;
            for (ExecutionLanguage language : languages) {
                if (language.getId() == r.getLanguageId()) {
                    correctLanguage = true;
                }
            }

            if (correctEngine && correctLanguage) {
                r.save(slug);

                details = Integer.toString(r.getId());

                if (response != null) {
                    response.setStatus(HttpServletResponse.SC_CREATED);
                }
            }
            else {
                LOGGER.log(Level.WARNING, "Bad engine id or language id provided!");
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

    /**
     * Uploads a binary for the specified recipe id, SCN.
     *
     * Given an existing `Recipe` object, this REST endpoint, for a given SCN, receives a binary
     * and stores it in the configured storage backend. Then links the `Recipe` object to the
     * stored binary by running `recipe.setExecutablePath()`.
     *
     * TODO: Requires authentication/authorization.
     * TODO: Tests.
     *
     * @param slug          The SCN's slug.
     * @param recipeId      The `id` of the recipe object to which this binary corresponds.
     * @param recipeName    The name of the recipe's binary.
     * @param recipeBinary  The actual recipe binary.
     * @return              An javax `Response` object.
     */
    @PUT
    @Path("{slug}/upload/{id}/{filename}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    public Response upload(@PathParam("slug") String slug,
                           @PathParam("id") int recipeId,
                           @PathParam("filename") String recipeName,
                           InputStream recipeBinary)  {
        // Ensure a `Recipe` object with the given `id` exists for the specified SCN.
        Recipe recipe;
        try {
            recipe = Recipe.getRecipeById(slug, recipeId);
        } catch (SQLException | SystemConnectorException e) {
            e.printStackTrace();

            return Response.serverError().entity(
                new RequestResponse("ERROR", "Upload recipe FAILED")
            ).build();
        }

        // Ensure the storage location for the specified SCN exists.
        try {
            Recipe.ensureStorageForSlug(slug);
        } catch (IOException | SystemConnectorException  e) {
            e.printStackTrace();

            return Response.serverError().entity(
                new RequestResponse("ERROR", "Upload recipe FAILED")
            ).build();
        }

        // Save the binary file to the specified location.
        String recipeFilename;
        try {
            recipeFilename = Recipe.saveRecipeForSlug(
                slug, recipeBinary, recipeName
            );
        } catch (IOException | SystemConnectorException  e) {
            e.printStackTrace();

            return Response.serverError().entity(
                new RequestResponse("ERROR", "Upload recipe FAILED")
            ).build();
        }

        // Update the `Recipe` object.
        recipe.setExecutablePath(recipeFilename);

        try {
            recipe.save(slug);
        } catch (SQLException | SystemConnectorException e) {
            e.printStackTrace();

            return Response.serverError().entity(
                new RequestResponse("ERROR", "Upload recipe FAILED")
            ).build();
        }

        return Response.ok(
                new RequestResponse("OK", "")
        ).build();
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
}
