/*
 * Copyright 2019 ICCS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("messages")
public class MessageResource {
    private final static Logger LOGGER = Logger.getLogger(MessageResource.class.getCanonicalName());

    /**
     * Message description insert method
     * @param m the message description to insert
     */
    @POST
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
                if (!conn.getMetadata().getDatasources().contains(m.getExternal_datasource())){
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
    @Path("{slug}/{messageTypeId}")
    public Response insert(@PathParam("slug") String slug,
                           @PathParam("messageTypeId") Integer messageId,
                           @QueryParam("connector_id") Integer connectorId,
                           @QueryParam("external_datasource") String datasource) {
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
            if (!conn.getMetadata().getDatasources().contains(m.getExternal_datasource())){
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Could not create new service for Message Type. Invalid datasource specified for connector.")
                ).build();
            }
            m.setExternalConnectorId(connectorId);
            m.setExternal_datasource(datasource);
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
     * Returns information about a specific message type.
     */
    @GET
    @Path("{slug}/{messageTypeId}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public MessageType getMessageTypeInfo(@PathParam("slug") String slug,
                                          @PathParam("messageTypeId") Integer id) {
        MessageType messageType = null;

        try {
            messageType = MessageType.getMessageById(slug, id);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return messageType;
    }

    /**
     * Delete a specific message type.
     */
    @DELETE
    @Path("{slug}/{messageTypeId}")
    public Response deleteMessageType(@PathParam("slug") String slug,
                                      @PathParam("messageTypeId") Integer id) {

        try {
            MessageType.destroy(slug, id);
        } catch (Exception e) {
            e.printStackTrace();

            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not destroy Message Type.")
            ).build();
        }

        return Response.ok(
                new RequestResponse("OK", "")
        ).build();
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
    @Path("/subscribe/{connectorId}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<PubSubSubscription> getSubscriptions(@PathParam("connectorId") Integer id) {
        List subscriptions = new Vector();
        try {
            Connector connector = Connector.getConnectorInfoById(id);
            for (ScnDbInfo scn: ScnDbInfo.getScnDbInfo())
                if (connector.isExternal() && MessageType.checkExternalMessageTypesExist(scn.getSlug()))
                    subscriptions.add(PubSubSubscription.getMessageSubscriptions(scn.getSlug(), true));
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING, "Failed to get subscriptions.");
        }

        return subscriptions;
    }

}
