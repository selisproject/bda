package gr.ntua.ece.cslab.selis.bda.controller.resources;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import de.tu_dresden.selis.pubsub.Message;

import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.controller.beans.PubSubSubscription;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.PubSubConnector;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.PubSubMessageHandler;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.RequestResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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
    public Response insert(@PathParam("slug") String slug,
                           MessageType m) {
        String details;
        try {
            m.save(slug);
            details = Integer.toString(m.getId());
        } catch (Exception e) {
            e.printStackTrace();

            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not insert new Message Type.")
            ).build();
        }

        PubSubConnector.getInstance().reloadSubscriptions(slug);

        return Response.ok(
                new RequestResponse("OK", details)
        ).build();
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
    public static Response handleMessage(@PathParam("slug") String scnSlug,
                                         String message) {
        try {
            Message msg = new Message();
            Map<String, Object> retMap = new Gson().fromJson(
                    message, new TypeToken<HashMap<String, Object>>() {}.getType()
            );
            for (Map.Entry<String, Object> entry: retMap.entrySet())
                msg.put(entry.getKey(),entry.getValue());
            PubSubMessageHandler.handleMessage(msg, scnSlug);
            LOGGER.log(Level.INFO,"PubSub message successfully inserted in the BDA.");
        } catch (Exception e) {
            e.printStackTrace();

            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not insert new PubSub message.")
            ).build();
        }

        return Response.ok(
                new RequestResponse("OK", "")
        ).build();
    }

    /**
     * Message subscriptions reload method
     */
    @GET
    @Path("/reload")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public HashMap<String, PubSubSubscription> reload() {
        HashMap<String, PubSubSubscription> subscriptions = null;
        try {
            for (ScnDbInfo scn: ScnDbInfo.getScnDbInfo())
                subscriptions.put(scn.getSlug(), PubSubSubscription.getMessageSubscriptions(scn.getSlug()));
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING, "Failed to get SCN info. Aborting reload of all subscriptions.");
        }

        return subscriptions;
    }

}
