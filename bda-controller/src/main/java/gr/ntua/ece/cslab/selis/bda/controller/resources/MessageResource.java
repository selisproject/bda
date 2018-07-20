package gr.ntua.ece.cslab.selis.bda.controller.resources;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.RequestResponse;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.PubSubSubscriber;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.List;
import java.util.LinkedList;

@Path("message")
public class MessageResource {
    private final static Logger LOGGER = Logger.getLogger(MessageResource.class.getCanonicalName());

    /**
     * Message description insert method
     * @param m the message description to insert
     */
    @PUT
    @Path("{slug}")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public RequestResponse insert(@Context HttpServletResponse response, 
                                  @PathParam("slug") String slug,
                                  MessageType m) {
        String status = "OK";
        String details = "";

        try {
            m.save(slug);

            details = Integer.toString(m.getId());

            if (response != null) {
                response.setStatus(HttpServletResponse.SC_CREATED);
            }
            PubSubSubscriber.reloadMessageTypes();
        } catch (Exception e) {
            e.printStackTrace();

            if (response != null) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }

            return new RequestResponse("ERROR", "Could not insert new Message Type.");
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
     * Returns all the registered message types.
     */
    @GET
    @Path("{slug}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<MessageType> getMessageTypeView(@PathParam("slug") String slug) {
        List<MessageType> messageTypes = new LinkedList<MessageType>();

        try {
            messageTypes = MessageType.getMessageTypes(slug);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return messageTypes;
    }
}
