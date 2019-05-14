package gr.ntua.ece.cslab.selis.bda.controller.resources;

import gr.ntua.ece.cslab.selis.bda.analyticsml.RunnerInstance;
import gr.ntua.ece.cslab.selis.bda.controller.cron.CronJobScheduler;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.JobDescription;
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

@Path("job")
public class JobResource {
    private final static Logger LOGGER = Logger.getLogger(JobResource.class.getCanonicalName());

    /**
     * Job description insert method
     */
    @PUT
    @Path("{slug}")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    @Produces({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})

    public Response insert(@PathParam("slug") String slug,
                                  JobDescription m) {
        String details = "";

        try {
            if (!(m.getJobType().matches("batch") || m.getJobType().matches("streaming")))
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Could not insert new Job. Invalid job type.")
                ).build();

            if (!((m.getMessageTypeId() == null) ^ (m.getScheduleTime() == 0))) {
                return Response.serverError().entity(
                        new RequestResponse("ERROR", "Could not insert new Job. Job is either cron or connected to a message type")
                ).build();
            }

            m.save(slug);
            details = Integer.toString(m.getId());
            LOGGER.log(Level.INFO, "Inserted job.");

            Recipe r = Recipe.getRecipeById(slug, m.getRecipeId());
            MessageType msg = null;
            String messageId = "";

            if (m.getMessageTypeId() != null) {
                msg = MessageType.getMessageById(slug, m.getMessageTypeId());
                messageId = String.valueOf(m.getMessageTypeId());
                JSONObject msgFormat = new JSONObject(msg.getFormat());
                LOGGER.log(Level.INFO, "Create kpidb table..");

                (new KPIBackend(slug)).create(new KPITable(r.getName(),
                        new KPISchema(msgFormat)));
            }

            if (m.getJobType().matches("streaming")){
                RunnerInstance runner = new RunnerInstance(slug, msg.getName());
                if (runner.engine.getName().matches("livy"))
                    runner.loadLivySession(m, r, msg, messageId);
            }

            if (m.getScheduleTime() > 0)
                CronJobScheduler.schedule_job(slug, m);

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
    public List<JobDescription> getJobsView(@PathParam("slug") String slug) {
        List<JobDescription> jobs = new LinkedList<JobDescription>();

        try {
            jobs = JobDescription.getJobs(slug);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return jobs;
    }

    @DELETE
    @Path("{slug}/{id}")
    @Consumes({MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML})
    public Response delete(@PathParam("slug") String slug,
                           @PathParam("id") int jobId) {

        try {
            JobDescription job = JobDescription.getJobById(slug, jobId);
            if (job.getJobType().matches("streaming") && job.getLivySessionId()!=null){
                RunnerInstance.deleteLivySession(slug, job);
            }
            //TODO: delete kpi table
            //(new KPIBackend(slug)).delete(new KPITable(r.getName()));
            // RunnerInstance.unschedule();

            // if job is cron, de-schedule it
            if (job.getScheduleTime() > 0) {
                CronJobScheduler.cancel_job(slug, job);
            }

            JobDescription.delete(slug, jobId);
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
