package gr.ntua.ece.cslab.selis.bda.controller.resources;

import gr.ntua.ece.cslab.selis.bda.controller.beans.KPIDescription;
import gr.ntua.ece.cslab.selis.bda.controller.beans.RequestResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.LinkedList;
import java.util.List;

/**
 * KPIResource holds the method for the analytics module.
 * Created by Giannis Giannakopoulos on 10/11/17.
 */
@Path("kpi")
public class KPIResource {
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<KPIDescription> getKPIList() {
        // TODO: implement the method
        return new LinkedList<>();
    }

    @POST
    @Path("{id}/run")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public RequestResponse runKPI(@PathParam("id") String id) {
        // TODO: implement the method
        return new RequestResponse("Done", "Double.toString()");
    }
}
