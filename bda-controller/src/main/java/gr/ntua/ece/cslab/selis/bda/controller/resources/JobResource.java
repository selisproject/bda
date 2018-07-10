package gr.ntua.ece.cslab.selis.bda.controller.resources;

import gr.ntua.ece.cslab.selis.bda.controller.Entrypoint;
import gr.ntua.ece.cslab.selis.bda.controller.beans.JobDescription;
import gr.ntua.ece.cslab.selis.bda.controller.beans.MessageType;
import gr.ntua.ece.cslab.selis.bda.controller.beans.Recipe;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.RequestResponse;
import gr.ntua.ece.cslab.selis.bda.kpidb.KPIBackend;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPISchema;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPITable;
import org.json.JSONObject;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("job")
public class JobResource {
    private final static Logger LOGGER = Logger.getLogger(JobResource.class.getCanonicalName());

    /**
     * Job description insert method
     * @param m the job description to insert
     */
    @PUT
    @Path("{slug}")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})

    public RequestResponse insert(@Context HttpServletResponse response, 
                                  @PathParam("slug") String slug,
                                  JobDescription m) {
        String status = "OK";
        String details = "";

        try {
            m.save(slug);

            LOGGER.log(Level.INFO, "Inserted job.");
            if (response != null) {
                response.setStatus(HttpServletResponse.SC_CREATED);
            }

            MessageType msg = MessageType.getMessageById(slug,m.getMessageTypeId());
            Recipe r = Recipe.getRecipeById(slug, m.getRecipeId());
            JSONObject msgFormat = new JSONObject(msg.getFormat());
            LOGGER.log(Level.INFO, "Create kpidb table..");
            (new KPIBackend(slug)).create(new KPITable(r.getName(),
                    new KPISchema(msgFormat)));
            //Entrypoint.analyticsComponent.getKpiCatalog().addNewKpi(r.getId(), r.getName(), r.getDescription(),
            //        r.getEngine_id(), new JSONObject(r.getArgs()), r.getExecutable_path());

        } catch (Exception e) {
            e.printStackTrace();

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
}
