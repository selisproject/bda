package gr.ntua.ece.cslab.selis.bda.controller.resources;

import gr.ntua.ece.cslab.selis.bda.common.storage.beans.Connector;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.RequestResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("connectors")
public class ConnectorResource {
    private final static Logger LOGGER = Logger.getLogger(ConnectorResource.class.getCanonicalName());

    /**
     * Create new SCN Connector.
     * @param connector a Connector description.
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response createNewConnector(Connector connector) {
        LOGGER.log(Level.INFO, connector.toString());

        String details = "";
        try {
            connector.save();
            details = Integer.toString(connector.getId());
        } catch (Exception e) {
            e.printStackTrace();

            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not register new Connector.")
            ).build();
        }

        return Response.ok(
                new RequestResponse("OK", details)
        ).build();
    }

    /**
     * Returns all the registered Connectors.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<Connector> getConnectorsView() {
        List<Connector> connectors = new LinkedList<Connector>();

        try {
            connectors = Connector.getConnectors();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return connectors;
    }

    /**
     * Returns information about a specific Connector.
     * @param id a Connector id.
     */
    @GET
    @Path("{connectorId}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Connector getConnectorInfo(@PathParam("connectorId") Integer id) {
        Connector connector = null;

        try {
            connector = Connector.getConnectorInfoById(id);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return connector;
    }

    /**
     * Delete a Connector.
     * @param id a Connector id.
     */
    @DELETE
    @Path("{connectorId}")
    public Response deleteConnector(@PathParam("connectorId") Integer id) {

        try {
            Connector.destroy(id);
        } catch (Exception e) {
            e.printStackTrace();

            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not delete Connector instance.")
            ).build();
        }

        return Response.ok(
                new RequestResponse("OK", "")
        ).build();
    }
}
