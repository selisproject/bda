package gr.ntua.ece.cslab.selis.bda.controller.resources;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import de.tu_dresden.selis.pubsub.Message;

import gr.ntua.ece.cslab.selis.bda.common.storage.beans.Connector;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.controller.beans.PubSubSubscription;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.PubSubConnector;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.PubSubMessageHandler;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.RequestResponse;
import org.json.JSONObject;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Date;

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
            if (m.getExternalConnectorId()!=null){
                Connector conn = Connector.getConnectorInfoById(m.getExternalConnectorId());
                if (!conn.isExternal())
                    return Response.serverError().entity(
                            new RequestResponse("ERROR", "Could not insert new Message Type. Connector is not external.")
                    ).build();
                if (!conn.getMetadata().getDatasources().contains(m.getDatasource())){
                    return Response.serverError().entity(
                            new RequestResponse("ERROR", "Could not insert new Message Type. Invalid datasource specified for connector.")
                    ).build();
                }
            }
            m.save(slug);
            details = Integer.toString(m.getId());
        } catch (Exception e) {
            e.printStackTrace();

            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not insert new Message Type.")
            ).build();
        }

        boolean externalConnector = !(m.getExternalConnectorId() == null);
        PubSubConnector.getInstance().reloadSubscriptions(slug, externalConnector);

        return Response.ok(
                new RequestResponse("OK", details)
        ).build();
    }

    /**
     * Message service to update external connector and metadata
     */
    @PUT
    @Path("{slug}/{message_id}/newconnector")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response insert(@PathParam("slug") String slug,
                           @PathParam("message_id") Integer messageId,
                           @QueryParam("connector_id") Integer connectorId,
                           @QueryParam("datasource") String datasource) {
        String details;
        MessageType m;
        Connector conn;
        try {
            m = MessageType.getMessageById(slug, messageId);
        } catch (Exception e) {
            e.printStackTrace();

            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not create new service for Message Type. Invalid message id.")
            ).build();
        }
        try {
            conn = Connector.getConnectorInfoById(connectorId);
            if (!conn.isExternal())
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Could not insert new Message Type. Connector is not external.")
                ).build();
        } catch (Exception e) {
            e.printStackTrace();

            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not create new service for Message Type. Invalid connector id.")
            ).build();
        }
        try {
            if (!conn.getMetadata().getDatasources().contains(m.getDatasource())){
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Could not create new service for Message Type. Invalid datasource specified for connector.")
                ).build();
            }
            m.setExternalConnectorId(connectorId);
            m.setDatasource(datasource);
            m.save(slug);
            details = Integer.toString(m.getId());
        } catch (Exception e) {
            e.printStackTrace();

            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not create new service for Message Type.")
            ).build();
        }

        boolean externalConnector = !(m.getExternalConnectorId() == null);
        PubSubConnector.getInstance().reloadSubscriptions(slug, externalConnector);

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
            //create file to record timestamps
            File f = new File("/code/timestamps.csv");
            if(!f.exists()){
                f.createNewFile();
            }else{
                String textToAppend = Long.toString((new Date()).getTime());
                FileWriter fileWriter = new FileWriter(f, true);
                PrintWriter printWriter = new PrintWriter(fileWriter);
                printWriter.print(textToAppend);  //New line
                printWriter.close();
            }
            Message msg = new Message();
            /*Map<String, Object> retMap = new Gson().fromJson(
                    message, new TypeToken<HashMap<String, Object>>() {}.getType()
            );*/

            JSONObject obj = new JSONObject(message);
            //for (Map.Entry<String, Object> entry: retMap.entrySet())
            Iterator<String> keys = obj.keys();
            while(keys.hasNext()) {
                String key = keys.next();
                msg.put(key, obj.get(key));
            }
            PubSubMessageHandler.handleMessage(msg, scnSlug);
            LOGGER.log(Level.INFO,"PubSub message successfully inserted in the BDA.");
            //create file to record timestamps
            if(!f.exists()){
                f.createNewFile();
            }else{
                String textToAppend = ","+ Long.toString((new Date()).getTime());
                FileWriter fileWriter = new FileWriter(f, true);
                PrintWriter printWriter = new PrintWriter(fileWriter);
                printWriter.print(textToAppend);  //New line
                printWriter.close();
            }
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
    @Produces(MediaType.APPLICATION_JSON)
    public List<PubSubSubscription> reload(@QueryParam("external") boolean externalConnector) {
        List subscriptions = new Vector();
        try {
            for (ScnDbInfo scn: ScnDbInfo.getScnDbInfo())
                if (externalConnector && MessageType.checkExternalMessageTypesExist(scn.getSlug()))
                    subscriptions.add(PubSubSubscription.getMessageSubscriptions(scn.getSlug(), externalConnector));
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING, "Failed to get SCN info. Aborting reload of all subscriptions.");
        }

        return subscriptions;
    }

}
