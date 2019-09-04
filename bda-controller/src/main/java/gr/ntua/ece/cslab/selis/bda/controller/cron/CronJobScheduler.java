package gr.ntua.ece.cslab.selis.bda.controller.cron;

import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.controller.resources.JobResource;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.JobDescription;

import javax.jnlp.IntegrationService;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CronJobScheduler {
    private final static Logger LOGGER = Logger.getLogger(JobResource.class.getCanonicalName());

    public static void init_scheduler() {
        Calendar.getInstance().add(Calendar.SECOND, 1);

        try {
            for (ScnDbInfo scn : ScnDbInfo.getScnDbInfo()) {
                for (JobDescription job : JobDescription.getActiveJobs(scn.getSlug())) {
                    int scheduleTime = Integer.parseInt(job.getScheduleInfo());
                    if (scheduleTime > 0) {
                        schedule_job(scn.getSlug(), job);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING, "Could not retrieve information to start defined cron jobs.");
        }
    }

    public static void schedule_job(String scn_slug, JobDescription jobDescription) {
        LOGGER.log(Level.INFO, "About to schedule cron job with id " + jobDescription.getId());
        Thread thread = new Thread(new ScheduledTaskRunnable(scn_slug, jobDescription));
        thread.start();
    }
}
