package gr.ntua.ece.cslab.selis.bda.controller.resources;


import gr.ntua.ece.cslab.selis.bda.controller.beans.Recipe;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.RequestResponse;
import org.apache.commons.io.IOUtils;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.*;
import java.sql.SQLException;
import java.util.logging.Logger;


@Path("recipe")
public class RecipeResource {
    private final static Logger LOGGER = Logger.getLogger(RecipeResource.class.getCanonicalName());

    @GET
    @Path("check")
    public String checker () {
        System.out.println("mpla");
        return "mpla";
    }
    /**
     * Job description insert method
     * @param m the job description to insert
     */
    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public RequestResponse insert(@Context HttpServletResponse response, String m) {

        String status = "OK";
        String details = "";
        JSONObject obj = new JSONObject(m);

        Recipe r = new Recipe(obj.getString("name"),
                obj.getString("description"),
                obj.getString("executable_path"),
                obj.getInt("engine_id"),
                obj.getJSONObject("args").toString());

        try {
            r.save();

            response.setStatus(HttpServletResponse.SC_CREATED);
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(e.toString());

            status = "ERROR";
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }

        try {
            response.flushBuffer();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new RequestResponse(status, details);
    }

    @PUT
    @Path("upload/{id}/{filename}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public RequestResponse upload(@PathParam("id") int recipe_id,
                         @PathParam("filename") String recipe_name,
                         InputStream recipe)  {

        String status = "OK";
        String details = "";

        String binaryPath = "/uploads/" + recipe_id + "_" + recipe_name;
        saveFile(recipe, binaryPath);

        Recipe r = Recipe.getRecipeById(recipe_id);
        System.out.println(r.toString());
        r.setExecutable_path(binaryPath);
        System.out.println(r.toString());

        try {
            r.updateBinaryPath();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return new RequestResponse(status, details);
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
