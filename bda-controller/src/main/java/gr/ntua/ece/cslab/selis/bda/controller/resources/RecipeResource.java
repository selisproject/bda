/*
 * Copyright 2019 ICCS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gr.ntua.ece.cslab.selis.bda.controller.resources;

import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionEngine;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionLanguage;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Recipe;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.RequestResponse;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.List;
import java.util.LinkedList;


@Path("recipes")
public class RecipeResource {
    private final static Logger LOGGER = Logger.getLogger(RecipeResource.class.getCanonicalName());

    /**
     * Recipe insert method
     * @param r the recipe to insert
     */
    @POST
    @Path("{slug}")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response insert(@PathParam("slug") String slug,
                                  Recipe r) {
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

            }
            else {
                LOGGER.log(Level.WARNING, "Bad engine id or language id provided!");
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Could not create Job. Invalid json.")
                ).build();
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.SEVERE, e.toString());

            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not create Job.")
            ).build();
        }

        return Response.ok(
                new RequestResponse("OK", details)
        ).build();
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
    @Path("{slug}/{recipeId}/{filename}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    @Produces(MediaType.APPLICATION_JSON)
    public Response upload(@PathParam("slug") String slug,
                           @PathParam("recipeId") int recipeId,
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

    /**
     * Returns information about a specific recipe.
     */
    @GET
    @Path("{slug}/{recipeId}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Recipe getRecipeInfo(@PathParam("slug") String slug,
                                @PathParam("recipeId") Integer id) {
        Recipe recipe = null;

        try {
            recipe = Recipe.getRecipeById(slug, id);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return recipe;
    }

    /**
     * Delete a specific recipe.
     */
    @DELETE
    @Path("{slug}/{recipeId}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response deleteRecipe(@PathParam("slug") String slug,
                                 @PathParam("recipeId") Integer id) {
        try {
            Recipe.destroy(slug, id);
        } catch (Exception e) {
            e.printStackTrace();

            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not delete Recipe.")
            ).build();
        }

        return Response.ok(
                new RequestResponse("OK", "")
        ).build();
    }
}
