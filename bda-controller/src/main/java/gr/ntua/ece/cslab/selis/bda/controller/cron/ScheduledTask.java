package gr.ntua.ece.cslab.selis.bda.controller.cron;

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
    private HashMap<String, String> msgTable;

    public ScheduledTask(String scn_slug, JobDescription jobDescription) {
        this.jobId = jobDescription.getId();
        this.msgTable = new HashMap<>();
        try {
            MessageType messageType = MessageType.getMessageById(scn_slug, jobDescription.getMessageTypeId());

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING, "Could not fetch cron message information.");
        }

        JSONObject payload = new JSONObject();
        payload.put("supplier_id", 0);
        payload.put("sales_forecast", new JSONObject());
        payload.put("warehouse_id", 0);

        msgTable.put("message_type", "SonaeSalesForecast");
        msgTable.put("scn_slug", "scn_slug");
        msgTable.put("payload", payload.toString());
    }


    @Override
    public void run() {
        LOGGER.log(Level.INFO, "Cron Task for job " + this.jobId + " launched at " + new Date());
        //Enumeration<String> enumeration = msgTable.keySet();
        //while (enumeration.hasMoreElements()) {
            //String k = enumeration.nextElement();
            //LOGGER.log(Level.INFO, k + "    " + msgTable.get(k));
        //}
        PubSubPublisher publisher = new PubSubPublisher("selis-pubsub", 20000);
        publisher.publish(msgTable);
    }
}
