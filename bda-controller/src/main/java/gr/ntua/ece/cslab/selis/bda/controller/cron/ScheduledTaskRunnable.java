package gr.ntua.ece.cslab.selis.bda.controller.cron;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.JobDescription;

import java.util.Calendar;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ScheduledTaskRunnable implements Runnable {

    private ScheduledTask scheduledTask;
    private int scheduledTime;

    public ScheduledTaskRunnable(String scn_slug, JobDescription jobDescription) {
        this.scheduledTask = new ScheduledTask(scn_slug, jobDescription);
        this.scheduledTime = jobDescription.getScheduleTime();
    }


    @Override
    public void run() {
        Timer timer = new Timer();
        timer.schedule(this.scheduledTask, Calendar.getInstance().getTime(), TimeUnit.SECONDS.toMillis(scheduledTime));
    }
}
