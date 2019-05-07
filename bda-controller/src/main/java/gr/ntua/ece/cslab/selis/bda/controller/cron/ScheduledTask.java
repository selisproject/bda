package gr.ntua.ece.cslab.selis.bda.controller.cron;

import gr.ntua.ece.cslab.selis.bda.analyticsml.RunnerInstance;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.PubSubPublisher;
import gr.ntua.ece.cslab.selis.bda.controller.resources.JobResource;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.JobDescription;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;
import org.json.JSONObject;

import javax.swing.plaf.PanelUI;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ScheduledTask extends TimerTask {
    private final static Logger LOGGER = Logger.getLogger(JobResource.class.getCanonicalName());

    private int jobId;
    private String scn_slug;

    public ScheduledTask(String scn_slug, JobDescription jobDescription) {
        this.jobId = jobDescription.getId();
        this.scn_slug = scn_slug;
    }


    @Override
    public void run() {
        LOGGER.log(Level.INFO, "Cron Task for job " + this.jobId + " launched at " + new Date());

        try {
            (new RunnerInstance(this.scn_slug, this.jobId)).run("");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
