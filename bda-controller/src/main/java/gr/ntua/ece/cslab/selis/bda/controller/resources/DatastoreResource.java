package gr.ntua.ece.cslab.selis.bda.controller.resources;

import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import com.google.common.base.Splitter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class holds the REST API of the datastore object.
 * Created by Giannis Giannakopoulos on 10/11/17.
 */
@Path("datastore")
public class DatastoreResource {
    private final static Logger LOGGER = Logger.getLogger(DatastoreResource.class.getCanonicalName());

    /**
     * Create new SCN databases/schemas/tables.
     * @param scn an SCN description.
     */
    @POST
    @Path("create")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public RequestResponse createNewScn(@Context HttpServletResponse response, ScnDbInfo scn) {
        LOGGER.log(Level.INFO, scn.toString());

        String status = "OK";
        String details = "";

        try {
            scn.save();

            details = Integer.toString(scn.getId());
        } catch (Exception e) {
            e.printStackTrace();

            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            return new RequestResponse("ERROR", "Could not register new SCN.");
        }

        try {
            StorageBackend.createNewScn(scn);
            if (response != null) {
                response.setStatus(HttpServletResponse.SC_CREATED);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.INFO, "Clearing SCN registry and databases after failure.");
            try {
                StorageBackend.destroyScn(scn);
            } catch (Exception e1) {
            }
            try {
                ScnDbInfo.destroy(scn.getId());
            } catch (Exception e1) {
                e1.printStackTrace();
                LOGGER.log(Level.SEVERE, "Could not clear SCN registry, after databases creation failed!");
            }
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            return new RequestResponse("ERROR", "Could not create new SCN.");
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
     * Destroy an SCN's databases/schemas/tables.
     * @param scnId an SCN id.
     */
    @POST
    @Path("destroy")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public RequestResponse destroyScn(@Context HttpServletResponse response,
            @QueryParam("scnId") Integer scnId) {

        try {
            ScnDbInfo scn = ScnDbInfo.getScnDbInfoById(scnId);
            StorageBackend.destroyScn(scn);
        } catch (Exception e) {
            e.printStackTrace();

            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            return new RequestResponse("ERROR", "Could not destroy SCN databases.");
        }

        try {
            ScnDbInfo.destroy(scnId);
            if (response != null) {
                response.setStatus(HttpServletResponse.SC_CREATED);
            }
        } catch (Exception e) {
            e.printStackTrace();

            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            return new RequestResponse("ERROR", "Could not destroy SCN.");
        }

        try {
            if (response != null) {
                response.flushBuffer();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new RequestResponse("OK", "");
    }

    /**
     * Message insertion method
     * @param m the message to insert
     */
    @PUT
    @Path("{slug}")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public RequestResponse insert(@Context HttpServletResponse response, 
                                  @PathParam("slug") String slug,
                                  Message m) {
        LOGGER.log(Level.INFO, m.toString());
        try {
            new StorageBackend(slug).insert(m);
            if (response != null) {
                response.setStatus(HttpServletResponse.SC_CREATED);
            }
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
        return new RequestResponse("OK", "");
    }


    /**
     * Responsible for datastore bootstrapping
     * @param masterData the schema of the dimension tables along with their content
     * @return a response for the status of bootstrapping
     */
    @POST
    @Path("{slug}/boot")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public RequestResponse bootstrap(@Context HttpServletResponse response,
                                     @PathParam("slug") String slug,
                                     MasterData masterData) {
        try {
            new StorageBackend(slug).init(masterData);
            if (response != null) {
                response.setStatus(HttpServletResponse.SC_CREATED);
            }
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
        return new RequestResponse("OK", "");
    }

    /**
     * Returns the filtered content of a given dimension table
     * @param tableName is the name of the table to search
     * @param filters contains the names and values of the columns to be filtered
     * @return the selected content of the dimension table
     */
    @GET
    @Path("{slug}/dtable")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<Tuple> getTable(
            @QueryParam("tableName") String tableName,
            @QueryParam("filters") String filters,
            @PathParam("slug") String slug
    ) {
        try {
            HashMap<String, String> mapfilters;
            if(filters != null && !filters.isEmpty()) {
                Map<String, String> map = Splitter.on(';').withKeyValueSeparator(":").split(filters);
                mapfilters = new HashMap<String, String>(map);
            }
            else {
                mapfilters = new HashMap<String, String>();
            }
            return new StorageBackend(slug).select(tableName, mapfilters);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new LinkedList<>();
    }

    /**
     * Returns all the registered SCNs.
     */
    @GET
    @Path("scns")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<ScnDbInfo> getScnDbInfoView() {
        List<ScnDbInfo> scns = new LinkedList<ScnDbInfo>();

        try {
            scns = ScnDbInfo.getScnDbInfo();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return scns;
    }

    /**
     * Returns the schema of all dimension tables
     * @return a list of the dimension tables schemas
     */
    @GET
    @Path("{slug}/schema")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<DimensionTable> getSchema(@PathParam("slug") String slug) {
        try {
            List<String> tables = new StorageBackend(slug).listTables();
            List<DimensionTable> res = new LinkedList<>();
            for (String table: tables){
                DimensionTable schema = new StorageBackend(slug).getSchema(table);
                LOGGER.log(Level.INFO, "Table: " +table + ", Columns: "+ schema.getSchema().getColumnNames());
                res.add(schema);
            }
            return res;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return new LinkedList<>();
    }

    /**
     * Returns the last entries (i.e., messages) stored in the event log.
     * @param type one of days, count
     * @param n the number of days/messages to fetch
     * @return the denormalized messages
     */
    @GET
    @Path("{slug}/entries")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<Tuple> getEntries(@QueryParam("type") String type,
                                  @QueryParam("n") Integer n,
                                  @PathParam("slug") String slug) {
        try {
            return new StorageBackend(slug).fetch(type,n);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new LinkedList<>();
    }

    /**
     * Returns the filtered entries (i.e., messages) stored in the event log.
     * @param filters contains the names and values of the columns to be filtered
     * @return the denormalized messages
     */
    @GET
    @Path("{slug}/select")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<Tuple> getSelectedEntries(
            @QueryParam("filters") String filters,
            @PathParam("slug") String slug
    ) {
        try {
            Map<String,String> map= Splitter.on(';').withKeyValueSeparator(":").split(filters);
            HashMap<String, String> mapfilters = new HashMap<String, String>(map);
            return new StorageBackend(slug).select(mapfilters);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return new LinkedList<>();
    }
}
