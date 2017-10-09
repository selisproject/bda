package gr.ntua.ece.cslab.selis.bda.controller.resources;

import gr.ntua.ece.cslab.selis.bda.controller.beans.KeyValue;
import gr.ntua.ece.cslab.selis.bda.controller.beans.Message;
import gr.ntua.ece.cslab.selis.bda.controller.beans.RequestResponse;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Giannis Giannakopoulos on 10/5/17.
 * This class represents the Message resource.
 */
@Path("message")
public class MessageResource {
    private final static Logger LOGGER = Logger.getLogger(MessageResource.class.getCanonicalName());

    /**
     * Demo method - just a proof of concept that the messages are correctly serialized/deserialized
     * @return
     */
    @GET
    @Path("demo")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Message demo() {
        Message m = new Message();
        m.setEntries(new LinkedList<>());
        m.getEntries().add(new KeyValue("time", "1"));
        m.getEntries().add(new KeyValue("place", "2"));
        Message m2 = new Message();
        m2.setEntries(new LinkedList<>());
        m2.getEntries().add(new KeyValue("foo", "bar"));
        m.setNested(new LinkedList<>());
        m.getNested().add(m2);
        return m;
    }


    /**
     * Message insertion method
     * @param m the method to insert
     */
    @PUT
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public RequestResponse insert(@Context HttpServletResponse response, Message m) {
        // FIXME: do something after getting a message
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
}
