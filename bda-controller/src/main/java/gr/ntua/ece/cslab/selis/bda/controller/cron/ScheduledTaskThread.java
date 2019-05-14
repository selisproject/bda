package gr.ntua.ece.cslab.selis.bda.controller.cron;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.JobDescription;

import java.util.Calendar;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ScheduledTaskThread extends Thread {

    private ScheduledTask scheduledTask;
    private int scheduledTime;
    private Timer timer;

    public ScheduledTaskThread(String scn_slug, JobDescription jobDescription) {
        this.scheduledTask = new ScheduledTask(scn_slug, jobDescription);
        this.scheduledTime = jobDescription.getScheduleTime();
        this.timer = new Timer();
    }

    public void cancelTimer() {
        this.timer.cancel();
    }

    @Override
    public void run() {
        timer.schedule(this.scheduledTask, Calendar.getInstance().getTime(), TimeUnit.SECONDS.toMillis(scheduledTime));
    }
}
