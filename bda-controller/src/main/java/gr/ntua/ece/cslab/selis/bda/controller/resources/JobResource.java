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
import gr.ntua.ece.cslab.selis.bda.controller.cron.CronJobScheduler;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Job;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Recipe;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.RequestResponse;
import gr.ntua.ece.cslab.selis.bda.kpidb.KPIBackend;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPISchema;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPITable;
import org.json.JSONObject;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.List;
import java.util.LinkedList;

@Path("jobs")
public class JobResource {
    private final static Logger LOGGER = Logger.getLogger(JobResource.class.getCanonicalName());

    /**
     * Job description insert method
     */
    @POST
    @Path("{slug}")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response insert(@PathParam("slug") String slug,
                           Job j) {

        String details = "";
        j.setJobType();

        try {
            Job parentJob = null;
            if (j.getDependJobId() != null) {
                parentJob = Job.getJobById(slug, j.getDependJobId());
                if (parentJob == null)
                    return Response.serverError().entity(
                            new RequestResponse("ERROR", "Could not insert new Job depending on non-existing Job.")
                    ).build();
            }

            if ((j.getMessageTypeId() == null) && (j.getScheduleInfo() == null)) {
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Could not insert new Job. Job must have schedule info or be connected to a message type")
                ).build();
            }

            if ((j.getResultStorage() == null) || !(j.getResultStorage().matches("kpidb") || j.getResultStorage().matches("pubsub") || (j.getResultStorage().matches("hdfs")))) {
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Could not insert new Job. Job result storage must be either 'kpidb', or 'pubsub' or 'hdfs'")
                ).build();
            }

            j.save(slug);
            details = Integer.toString(j.getId());
            LOGGER.log(Level.INFO, "Inserted job.");

            Recipe r = Recipe.getRecipeById(slug, j.getRecipeId());
            MessageType msg = null;
            String messageId = "";

            if (j.getMessageTypeId() != null)  {
                msg = MessageType.getMessageById(slug, j.getMessageTypeId());
                if (j.getResultStorage().equals("kpidb")) {
                    messageId = String.valueOf(j.getMessageTypeId());
                    JSONObject msgFormat = new JSONObject(msg.getFormat());
                    LOGGER.log(Level.INFO, "Create kpidb table..");

                    (new KPIBackend(slug)).create(new KPITable(r.getName(),
                            new KPISchema(msgFormat)));
                }
            }
            else {
                if (j.getResultStorage().equals("kpidb")) {
                    JSONObject schema = new JSONObject("{}");
                    LOGGER.log(Level.INFO, "Create kpidb table..");

                    (new KPIBackend(slug)).create(new KPITable(r.getName(),
                            new KPISchema(schema)));
                }
            }

            if (j.getDependJobId() != null) {
                LOGGER.log(Level.INFO, "Getting session id from parent job..");
                if (parentJob.getSessionId() != null) {
                    parentJob.setChildrenSessionId(slug);
                }
            }
            else if (j.getJobType().matches("streaming")){
                RunnerInstance runner = new RunnerInstance(slug, msg.getName());
                if (runner.engine.getName().matches("spark-livy"))
                    runner.loadLivySession(j, r, msg, messageId);
            }

            if (j.getScheduleInfo() != null)
                CronJobScheduler.schedule_job(slug, j);

        } catch (Exception e) {
            e.printStackTrace();

            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not insert new Job.")
            ).build();
        }

        return Response.ok(
                new RequestResponse("OK", details)
        ).build();
    }

    /**
     * Returns all the registered jobs.
     */
    @GET
    @Path("{slug}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public List<Job> getJobsView(@PathParam("slug") String slug) {
        List<Job> jobs = new LinkedList<Job>();

        try {
            jobs = Job.getJobs(slug);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return jobs;
    }

    /**
     * Returns information about a specific job.
     */
    @GET
    @Path("{slug}/{jobId}")
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Job getJobInfo(@PathParam("slug") String slug,
                          @PathParam("jobId") Integer id) {
        Job job = null;

        try {
            job = Job.getJobById(slug, id);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return job;
    }

    @DELETE
    @Path("{slug}/{jobId}")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response delete(@PathParam("slug") String slug,
                           @PathParam("jobId") int jobId) {

        try {
            Job job = Job.getJobById(slug, jobId);
            if (job.getJobType().matches("streaming") && job.getSessionId()!=null && job.getDependJobId() == null){
                RunnerInstance.deleteLivySession(slug, job);
            }
            //TODO: delete kpi table, unschedule cron
            //(new KPIBackend(slug)).delete(new KPITable(r.getName()));
            // RunnerInstance.unschedule();
            Job.destroy(slug, jobId);
        } catch (Exception e) {
            e.printStackTrace();
            return Response.serverError().entity(
                    new RequestResponse("ERROR", "Could not delete Job.")
            ).build();
        }

        return Response.ok(
                new RequestResponse("OK", "")
        ).build();
    }
}
