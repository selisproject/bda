package gr.ntua.ece.cslab.selis.bda.controller.resources;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.KPIDescription;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

/**
 * KPIResource holds the method for the analytics module.
 * Created by Giannis Giannakopoulos on 10/11/17.
 */
@Path("kpi")
public class KPIResource {
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public KPIDescription getKPIList() {
        // TODO: implement the method
        return new KPIDescription();
    }
}
