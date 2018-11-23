package gr.ntua.ece.cslab.selis.bda.controller.resources;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import de.tu_dresden.selis.pubsub.Message;

import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.PubSubConnector;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.PubSubMessageHandler;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.RequestResponse;

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

        PubSubConnector.getInstance().reloadSubscriptions(slug);

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
    @Path("{slug}/insert")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public static RequestResponse handleMessage(@Context HttpServletResponse response,
                                                @PathParam("slug") String scnSlug,
                                                String message) {
        String status = "OK";
        String details = "";

        try {
            Message msg = new Message();
            Map<String, Object> retMap = new Gson().fromJson(
                    message, new TypeToken<HashMap<String, Object>>() {}.getType()
            );
            for (Map.Entry<String, Object> entry: retMap.entrySet())
                msg.put(entry.getKey(),entry.getValue());
            PubSubMessageHandler.handleMessage(msg, scnSlug);
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
     */
    @GET
    @Path("/reload")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public RequestResponse reload(@Context HttpServletResponse response) {
        String status = "OK";
        String details = "";

        try {
            for (ScnDbInfo scn: ScnDbInfo.getScnDbInfo())
                PubSubConnector.getInstance().reloadSubscriptions(scn.getSlug());
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING, "Failed to get SCN info. Aborting reload of all subscriptions.");

            if (response != null) {
                response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            }

            return new RequestResponse("ERROR", "Could not reload subscriptions.");
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
