package gr.ntua.ece.cslab.selis.bda.controller.resources;

import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionEngine;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

@Path("xengines")
public class ExecutionEngineResource {
    private final static Logger LOGGER = Logger.getLogger(ExecutionEngineResource.class.getCanonicalName());

    /**
     * Returns all the available execution engines.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<ExecutionEngine> getExecutionEngines() {
        List<ExecutionEngine> engines = new LinkedList<>();

        try {
            engines = ExecutionEngine.getEngines();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return engines;
    }
}
