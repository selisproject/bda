package gr.ntua.ece.cslab.selis.bda.controller.resources;

import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionLanguage;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

@Path("xlanguages")
public class ExecutionLanguageResource {
    private final static Logger LOGGER = Logger.getLogger(ExecutionLanguageResource.class.getCanonicalName());

    /**
     * Returns all the available execution languages.
     */
    @GET
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<ExecutionLanguage> getSharedRecipes() {
        List<ExecutionLanguage> languages = new LinkedList<>();

        try {
            languages = ExecutionLanguage.getLanguages();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return languages;
    }
}
