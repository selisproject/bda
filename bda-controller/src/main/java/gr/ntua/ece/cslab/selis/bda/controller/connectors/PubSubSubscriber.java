package gr.ntua.ece.cslab.selis.bda.controller.connectors;

import gr.ntua.ece.cslab.selis.bda.analytics.AnalyticsInstance;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.controller.beans.JobDescription;
import gr.ntua.ece.cslab.selis.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;
import gr.ntua.ece.cslab.selis.bda.controller.Entrypoint;
import gr.ntua.ece.cslab.selis.bda.controller.beans.MessageType;

import de.tu_dresden.selis.pubsub.*;
import de.tu_dresden.selis.pubsub.PubSubException;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PubSubSubscriber implements Runnable {
    private final static Logger LOGGER = Logger.getLogger(PubSubSubscriber.class.getCanonicalName()+" [" + Thread.currentThread().getName() + "]");

    private static String authHash;
    private static String hostname;
    private static int portNumber;
    private List<String> messageTypeNames;

    private static volatile boolean reloadMessageTypesFlag = true;

    public PubSubSubscriber(String authHash, String hostname, int portNumber) {
        this.authHash = authHash;
        this.hostname = hostname;
        this.portNumber = portNumber;

        this.messageTypeNames = new Vector<String>();
    }

    public static void reloadMessageTypes() {
        reloadMessageTypesFlag = true;
    }

    @Override
    public void run() {
        PubSub pubsub = null;

        while (reloadMessageTypesFlag) {
            reloadMessageTypesFlag = false;

            try {
                pubsub = new PubSub(this.hostname, this.portNumber);
                List<ScnDbInfo> SCNs = new LinkedList<>();
                try {
                    SCNs = ScnDbInfo.getScnDbInfo();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                if (!(SCNs.isEmpty())) {
                    for (ScnDbInfo SCN : SCNs) {
                        messageTypeNames.addAll(MessageType.getActiveMessageTypeNames(SCN.getSlug()));
                    }

                    for (String messageTypeName : messageTypeNames) {
                        Subscription subscription = new Subscription(this.authHash);

                        subscription.add(new Rule("message_type", messageTypeName, RuleType.EQ));

                        pubsub.subscribe(subscription, new Callback() {
                            @Override
                            public void onMessage(Message message) {
                                try {
                                    handleMessage(message);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                    }

                    LOGGER.log(Level.INFO,
                            "SUCCESS: Subscribed to {0} message types",
                            messageTypeNames.size());
                }
            } catch (PubSubException ex) {
                LOGGER.log(Level.WARNING,
                           "Could not subscribe, got error: {0}",
                           ex.getMessage());
                pubsub.close();
            } catch (Exception e) {
                e.printStackTrace();
                pubsub.close();
            }

            while (true) {
                try {
                    Thread.sleep(300);

                    if (reloadMessageTypesFlag) {
                        pubsub.close();
                        break;
                    }
                } catch (InterruptedException e) {
                    LOGGER.log(Level.WARNING,"Subscriber was interrupted.");
                    pubsub.close();
                    break;
                }
            }
        }
        pubsub.close();
        LOGGER.log(Level.INFO,"Subscriber finished.");
    }

    private void handleMessage(Message message) throws Exception {

        gr.ntua.ece.cslab.selis.bda.datastore.beans.Message bdamessage = new gr.ntua.ece.cslab.selis.bda.datastore.beans.Message();
        String messageType = "";
        String message_id = "";
        for (Map.Entry<String, Object> entry : message.entrySet()) {
            String key = entry.getKey() != null ? entry.getKey() : "";
            if (key.matches("message_type")) {
                String value = entry.getValue() != null ? entry.getValue().toString() : "";
                if (messageTypeNames.contains(value))
                    messageType = value;
                else
                    throw new Exception("Subscriber[" + authHash + "], received unknown message type: " + value + ". This should never happen.");
            }
        }
        if (messageType.matches(""))
            throw new Exception("Subscriber[" + authHash + "], received no message type. This should never happen.");

        List<KeyValue> entries = new LinkedList<>();
        for (Map.Entry<String, Object> entry : message.entrySet()) {
            String key = entry.getKey() != null ? entry.getKey() : "";
            if (key.matches("payload")) {
                String value = entry.getValue() != null ? entry.getValue().toString() : "";
                value = value.replaceAll("=", "\":\"").replaceAll("\\s+", "").replaceAll(":\"\\[", ":[").replaceAll("\\{", "{\"").replaceAll(",", "\",\"").replaceAll("}", "\"}").replaceAll("}\",\"\\{", "},{").replaceAll("]\",", "],");
                JsonObject payloadjson = new JsonParser().parse(value).getAsJsonObject();
                Set<Map.Entry<String, JsonElement>> entrySet = payloadjson.entrySet();
                entries.add(new KeyValue("topic", messageType));
                for (Map.Entry<String, JsonElement> field : entrySet) {
                    // This is serialization garbage from CLMS adapter - should be removed in the future
                    if (!field.getKey().contains("Key")) {
                        try {
                            entries.add(new KeyValue(field.getKey(), field.getValue().getAsString()));
                        } catch (Exception e) {
                            entries.add(new KeyValue("message", "{\"" + field.getKey() + "\": " + field.getValue() + "}"));
                        }
                    }
                }
            }
        }
        bdamessage.setEntries(entries);
        try {
            // TODO: get scn name
            message_id = new StorageBackend("").insert(bdamessage);
            LOGGER.info("Subscriber[" + authHash + "], Received and persisted " + messageType + " message.");
        } catch (Exception e) {
            e.printStackTrace();
        }

        MessageType msgInfo = MessageType.getMessageByName("", messageType);
        try {
            JobDescription job = JobDescription.getJobByMessageId("", msgInfo.getId());
            LOGGER.log(Level.INFO, "Subscriber[" + authHash + "], Launching " + job.getName() + " recipe.");
            // TODO: check job.getJob_type()
            (new AnalyticsInstance("")).run(job.getRecipeId(), message_id);
        } catch (SQLException e) {
            LOGGER.log(Level.INFO, "Subscriber[" + authHash + "], No recipe found for message " + messageType + ".");
        }
    }
}
