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

import gr.ntua.ece.cslab.selis.bda.analyticsml.RunnerInstance;
import gr.ntua.ece.cslab.selis.bda.common.Configuration;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.Connector;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionEngine;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ExecutionLanguage;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.HDFSConnector;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.PubSubConnector;
import gr.ntua.ece.cslab.selis.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.*;
import com.google.common.base.Splitter;
import org.apache.commons.io.IOUtils;

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
     * Perform initial upload of libraries and shared recipes in HDFS and populate
     * shared_recipes, execution_engines and execution_languages tables.
     */
    @POST
    @Path("init")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response initialUploads() {

        Configuration configuration = Configuration.getInstance();
        ClassLoader classLoader = RunnerInstance.class.getClassLoader();

        try {
            ExecutionLanguage lang = new ExecutionLanguage("python");
            lang.save();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Failed to populate execution languages table.")
            ).build();
        }

        try {
            ExecutionEngine eng = new ExecutionEngine("python3", "/usr/bin/python3", true, "{}");
            eng.save();
            eng = new ExecutionEngine("spark-livy", "http://selis-livy:8998", false, "{}");
            eng.save();
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Failed to populate execution engines table.")
            ).build();
        }

        if (configuration.execEngine.getRecipeStorageType().startsWith("hdfs")) {
            // Use HDFS storage for recipes and libraries.
            HDFSConnector connector;
            try {
                connector = (HDFSConnector)
                        SystemConnector.getInstance().getHDFSConnector();
            } catch (SystemConnectorException e) {
                e.printStackTrace();
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Failed to get HDFS connector.")
                ).build();
            }
            org.apache.hadoop.fs.FileSystem fs = connector.getFileSystem();

            InputStream fileInStream = classLoader.getResourceAsStream("RecipeDataLoader.py");

            byte[] recipeBytes;
            try {
                recipeBytes = IOUtils.toByteArray(fileInStream);

                fileInStream.close();
            } catch (IOException e) {
                e.printStackTrace();
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Failed to create stream of Recipes library file.")
                ).build();
            }

            // Create HDFS file path object.
            org.apache.hadoop.fs.Path outputFilePath =
                    new org.apache.hadoop.fs.Path("/RecipeDataLoader.py");

            // Write to HDFS.
            org.apache.hadoop.fs.FSDataOutputStream outputStream = null;
            try {
                outputStream = fs.create(
                        outputFilePath
                );
            } catch (IOException e) {
                e.printStackTrace();
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Failed to create Recipes library file in HDFS.")
                ).build();
            }

            try {
                outputStream.write(recipeBytes);
            } catch (IOException e) {
                e.printStackTrace();
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Failed to upload Recipes library to HDFS.")
                ).build();
            } finally {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                String path = System.getProperty("user.dir").replaceFirst("bda-controller","bda-analytics-ml/src/main/resources/shared_recipes/");
                org.apache.hadoop.fs.Path folderToUpload = new org.apache.hadoop.fs.Path(path);

                // Create HDFS file path object.
                outputFilePath = new org.apache.hadoop.fs.Path("/");
                fs.copyFromLocalFile(folderToUpload, outputFilePath);
            } catch (Exception e) {
                e.printStackTrace();
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Failed to upload shared recipes to HDFS.")
                ).build();
            }

            try {
                fileInStream = new URL(configuration.execEngine.getSparkConfJars()).openStream();
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Spark jar clients download failed!! Please check the URLs");
            }

            byte[] postgresBytes;
            try {
                postgresBytes = IOUtils.toByteArray(fileInStream);

                fileInStream.close();
            } catch (IOException e) {
                e.printStackTrace();
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Failed to create Spark jar stream.")
                ).build();
            }
            String[] jar_name = configuration.execEngine.getSparkConfJars().split("/");
            outputFilePath =
                    new org.apache.hadoop.fs.Path("/" + jar_name[jar_name.length - 1]);

            try {
                outputStream = fs.create(
                        outputFilePath
                );
            } catch (IOException e) {
                e.printStackTrace();
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Failed to create Spark jar file in HDFS.")
                ).build();
            }

            try {
                outputStream.write(postgresBytes);
            } catch (IOException e) {
                e.printStackTrace();
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Failed to upload Spark jars to HDFS.")
                ).build();
            } finally {
                try {
                    outputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            try {
                RecipeArguments args = new RecipeArguments();
                List<String> args_list = new LinkedList<>();
                args_list.add("labelColumnName");
                args_list.add("maxIter");
                args_list.add("regParam");
                args_list.add("elasticNetParam");
                args.setOther_args(args_list);
                List<String> input_df_args_list = new LinkedList<>();
                input_df_args_list.add("eventLogMessageType1");
                args.setMessage_types(input_df_args_list);

                Recipe r = new Recipe("Linear Regression model train", "Simple regression training algorithm using Spark MLlib",
                        1, "hdfs:///shared_recipes/linear_regression_train.py", 2, args);
                r.save_as_shared();

                r = new Recipe("Binomial Logistic Regression model train", "Simple binary classification training algorithm using Spark MLlib",
                        1, "hdfs:///shared_recipes/binomial_logistic_regression_train.py", 2, args);
                r.save_as_shared();

                args = new RecipeArguments();
                r = new Recipe("Linear Regression prediction", "Simple regression prediction using Spark MLlib",
                        1, "hdfs:///shared_recipes/linear_regression_predict.py", 2, args);
                r.save_as_shared();
                r = new Recipe("Binomial Logistic Regression prediction", "Simple binary classification prediction using Spark MLlib",
                        1, "hdfs:///shared_recipes/binomial_logistic_regression_predict.py", 2, args);
                r.save_as_shared();
            } catch (Exception e) {
                e.printStackTrace();
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Failed to populate shared recipes table.")
                ).build();
            }
        }
        else {
            // Upload any shared recipes for local execution
        }

        return Response.ok(
                new RequestResponse("OK", "")
        ).build();
    }

    /**
     * Create new SCN databases/schemas/tables.
     * @param scn an SCN description.
     */
    @POST
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response createNewScn(ScnDbInfo scn) {
        LOGGER.log(Level.INFO, scn.toString());

        String details = "";

        try {
            List<Connector> connectors = Connector.getConnectors();

            boolean correctConnector = false;
            for (Connector connector : connectors) {
                if (connector.getId() == scn.getConnectorId()) {
                    correctConnector = true;
                }
            }

            if (correctConnector) {
                scn.save();
            } else {
                LOGGER.log(Level.WARNING, "Bad connector id provided!");
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Connector id does not exist.")
                ).build();
            }

            details = Integer.toString(scn.getId());
        } catch (Exception e) {
            e.printStackTrace();

            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not register new SCN.")
            ).build();
        }

        try {
            StorageBackend.createNewScn(scn);
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
            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not create new SCN.")
            ).build();
        }

        return Response.ok(
                new RequestResponse("OK", details)
        ).build();
    }

    /**
     * Returns all the registered SCNs.
     */
    @GET
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
     * Returns information about a specific SCN.
     * @param id the SCN registry id.
     */
    @GET
    @Path("{scnId}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public ScnDbInfo getScnInfo(@PathParam("scnId") Integer id) {
        ScnDbInfo scn = null;

        try {
            scn = ScnDbInfo.getScnDbInfoById(id);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return scn;
    }

    /**
     * Destroy an SCN's databases/schemas/tables.
     * @param scnId an SCN id.
     */
    @DELETE
    @Path("{scnId}")
    public Response destroyScn(@PathParam("scnId") Integer scnId) {

        ScnDbInfo scn;
        Boolean externalConnectors;

        try {
            scn = ScnDbInfo.getScnDbInfoById(scnId);
            externalConnectors = MessageType.checkExternalMessageTypesExist(scn.getSlug());
            StorageBackend.destroyScn(scn);
        } catch (Exception e) {
            e.printStackTrace();

            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not destroy SCN databases.")
            ).build();
        }

        try {
            ScnDbInfo.destroy(scnId);
        } catch (Exception e) {
            e.printStackTrace();

            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not destroy SCN.")
            ).build();
        }

        PubSubConnector.getInstance().reloadSubscriptions(scn.getSlug(), false);
        if (externalConnectors)
            PubSubConnector.getInstance().reloadSubscriptions(scn.getSlug(), true);

        return Response.ok(
                new RequestResponse("OK", "")
        ).build();
    }

    /**
     * Message insertion method
     * @param m the message to insert
     */
    @POST
    @Path("{slug}")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response insert(@PathParam("slug") String slug,
                           Message m) {
        LOGGER.log(Level.INFO, m.toString());
        try {
            new StorageBackend(slug).insert(m);
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not insert new message.")
            ).build();
        }
        return Response.ok(
                new RequestResponse("OK", "")
        ).build();
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
    public Response bootstrap(@PathParam("slug") String slug,
                                     MasterData masterData) {
        try {
            new StorageBackend(slug).init(masterData);
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not insert master data.")
            ).build();
        }
        return Response.ok(
                new RequestResponse("OK", "")
        ).build();
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
