package gr.ntua.ece.cslab.selis.bda.controller.cron;

import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.controller.resources.JobResource;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.JobDescription;

import java.util.Calendar;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CronJobScheduler {
    private final static Logger LOGGER = Logger.getLogger(JobResource.class.getCanonicalName());
    private static HashMap<String, ScheduledTaskThread> runningThreadVault;

    public static void init_scheduler() {
        Calendar.getInstance().add(Calendar.SECOND, 1);

        runningThreadVault = new HashMap<>();
        try {
            for (ScnDbInfo scn : ScnDbInfo.getScnDbInfo()) {
                for (JobDescription job : JobDescription.getActiveJobs(scn.getSlug())) {
                    if (job.getScheduleTime() > 0) {
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
        ScheduledTaskThread thread = new ScheduledTaskThread(scn_slug, jobDescription);
        runningThreadVault.put(scn_slug + jobDescription.getId(), thread);
        thread.start();
    }

    public static void cancel_job(String scn_slug, JobDescription jobDescription) {
        ScheduledTaskThread thread = runningThreadVault.get(scn_slug + jobDescription.getId());
        LOGGER.log(Level.INFO, "About to de-schedule cron job with id " + jobDescription.getId() +
                " for scn with slug " + scn_slug);
        thread.cancelTimer();
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
