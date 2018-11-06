package gr.ntua.ece.cslab.selis.bda.controller.resources;

import de.tu_dresden.selis.pubsub.Message;

import gr.ntua.ece.cslab.selis.bda.controller.Entrypoint;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.PubSubMessage;
import gr.ntua.ece.cslab.selis.bda.controller.beans.PubSubSubscription;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.RequestResponse;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.PubSubSubscriber;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

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
        } catch (Exception e) {
            e.printStackTrace();

            if (response != null) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }

            return new RequestResponse("ERROR", "Could not insert new Message Type.");
        }

        try {
            PubSubSubscription subscriptions = PubSubSubscription.getActiveSubscriptions();
            PubSubMessage.externalSubscribe(Entrypoint.configuration.subscriber.getHostname(),
                    Entrypoint.configuration.subscriber.getPortNumber(),
                    subscriptions);
            //PubSubSubscriber.reloadMessageTypes(subscriptions);
        } catch (Exception e) {
            e.printStackTrace();
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

    /**
     * Handle incoming PubSub message method
     * @param message the PubSub message
     */
    @POST
    @Path("/insert")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public static RequestResponse handleMessage(@Context HttpServletResponse response, Message message) {

        String status = "OK";
        String details = "";

        try {
            PubSubMessage.handleMessage(message);
            LOGGER.log(Level.WARNING,"PubSub message successfully inserted in the BDA.");
            if (response != null) {
                response.setStatus(HttpServletResponse.SC_CREATED);
            }
        } catch (Exception e) {
            e.printStackTrace();
            if (response != null) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }

            return new RequestResponse("ERROR", "Could not insert new PubSub message.");
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
     * Message subscriptions reload method
     * @param messageTypes the message types names to subscribe to
     */
    @POST
    @Path("/reload")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public RequestResponse reload(@Context HttpServletResponse response,
                                  PubSubSubscription messageTypes) {
        String status = "OK";
        String details = "";

        PubSubSubscriber.reloadMessageTypes(messageTypes);

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
