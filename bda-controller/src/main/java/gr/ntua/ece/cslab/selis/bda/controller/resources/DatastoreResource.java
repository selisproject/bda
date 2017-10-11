package gr.ntua.ece.cslab.selis.bda.controller.resources;

import gr.ntua.ece.cslab.selis.bda.controller.beans.*;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class holds the REST API of the datastore object.
 * Created by Giannis Giannakopoulos on 10/11/17.
 */
@Path("datastore")
public class DatastoreResource {
    private final static Logger LOGGER = Logger.getLogger(DatastoreResource.class.getCanonicalName());

    /**
     * Message insertion method
     * @param m the message to insert
     */
    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public RequestResponse insert(@Context HttpServletResponse response, Message m) {
        // TODO: implement it
        LOGGER.log(Level.INFO, m.toString());
        // placeholder
        response.setStatus(HttpServletResponse.SC_CREATED);
        try {
            response.flushBuffer();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new RequestResponse("OK", "");
    }


    /**
     * Responsible for datastore bootstrapping
     * @param masterData the schema of the dimension tables along with their content
     * @return a response for the status of bootstrapping
     */
    @POST
    @Path("boot")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public RequestResponse bootstrap(MasterData masterData) {
        // TODO: implement it
        return new RequestResponse("ERROR", "Not implemented");
    }

    /**
     * Returns the content of a given dimension table
     * @param tableName is the name of the table to fetch
     * @return the content of the dimension table
     */
    @GET
    @Path("dtable")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public DimensionTable getTable(@QueryParam("name") String tableName) {
        // TODO: implement it
        return new DimensionTable();
    }

    /**
     * Returns the schema of all dimension tables
     * @return
     */
    @GET
    @Path("schema")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public DimensionTableSchema getSchema() {
        // TODO: implement it
        return new DimensionTableSchema();

    }

    /**
     * Returns the last entries (i.e., messages) stored in the event log.
     * @param type one of days, count
     * @param n the number of days/messages to fetch
     * @return the denormalized messages
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<Message> getEntries(@QueryParam("type") String type,
                                           @QueryParam("n") Integer n) {
        // TODO: implement it
        return new LinkedList();

    }
}
