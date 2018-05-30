package gr.ntua.ece.cslab.selis.bda.controller.resources;

import gr.ntua.ece.cslab.selis.bda.controller.beans.MessageType;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.RequestResponse;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
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
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public RequestResponse insert(@Context HttpServletResponse response, MessageType m) {
        LOGGER.log(Level.INFO, m.toString());
        try {
            m.save();
        } catch (Exception e) {
            e.printStackTrace();
        }
        response.setStatus(HttpServletResponse.SC_CREATED);
        try {
            response.flushBuffer();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new RequestResponse("OK", "");
    }
}
