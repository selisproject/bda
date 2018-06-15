package gr.ntua.ece.cslab.selis.bda.controller.connectors;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import gr.ntua.ece.cslab.selis.bda.analytics.AnalyticsSystem;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.ExecutEngineCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.ExecutableCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.KpiCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.kpis.Kpi;
import gr.ntua.ece.cslab.selis.bda.analytics.kpis.KpiFactory;

import gr.ntua.ece.cslab.selis.bda.controller.beans.JobDescription;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;

import gr.ntua.ece.cslab.selis.bda.controller.Entrypoint;
import gr.ntua.ece.cslab.selis.bda.controller.beans.MessageType;

import de.tu_dresden.selis.pubsub.*;
import de.tu_dresden.selis.pubsub.PubSubException;

import java.sql.SQLException;
import java.util.*;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PubSubSubscriber implements Runnable {
    private final static Logger LOGGER = Logger.getLogger(PubSubSubscriber.class.getCanonicalName()+" [" + Thread.currentThread().getName() + "]");

    private final static int DEFAULT_VECTOR_SIZE = 10;

    private static String authHash;
    private static String hostname;
    private static int portNumber;
    private List<String> messageTypeNames;

    private static volatile boolean reloadMessageTypesFlag = true;

    public PubSubSubscriber(String authHash, String hostname, int portNumber) {
        this.authHash = authHash;
        this.hostname = hostname;
        this.portNumber = portNumber;
    }

    public static void reloadMessageTypes() {
        reloadMessageTypesFlag = true;
    }

    @Override
    public void run() {
        PubSub pubsub;
        Vector<Subscription> subscriptions = new Vector<Subscription>(DEFAULT_VECTOR_SIZE);

        while (reloadMessageTypesFlag) {
            reloadMessageTypesFlag = false;
            subscriptions.clear();

            try {
                pubsub = new PubSub(this.hostname, this.portNumber);

                messageTypeNames = MessageType.getActiveMessageTypeNames();

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

                    subscriptions.addElement(subscription);
                }

                LOGGER.log(Level.INFO, 
                           "SUCCESS: Subscribed to {0} message types", 
                           messageTypeNames.size());

            } catch (PubSubException ex) {
                LOGGER.log(Level.WARNING, 
                           "Could not subscribe, got error: {0}", 
                           ex.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
            }

            while (true) {
                try {
                    Thread.sleep(300);

                    if (reloadMessageTypesFlag) {
                        break;
                    }
                } catch (InterruptedException e) {
                    LOGGER.log(Level.WARNING,"Subscriber was interrupted.");
                    break;
                }
            }
        }
        LOGGER.log(Level.INFO,"Subscriber finished.");
    }

    private void handleMessage(Message message) throws Exception {

        gr.ntua.ece.cslab.selis.bda.datastore.beans.Message bdamessage = new gr.ntua.ece.cslab.selis.bda.datastore.beans.Message();
        String messageType = "";
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
            Entrypoint.datastore.insert(bdamessage);
            LOGGER.info("Subscriber[" + authHash + "], Received and persisted " + messageType + " message.");
        } catch (Exception e) {
            e.printStackTrace();
        }

        MessageType msgInfo = MessageType.getMessageByName(messageType);
        try {
            JobDescription job = JobDescription.getJobByMessageId(msgInfo.getId());
            LOGGER.log(Level.INFO, "Subscriber[" + authHash + "], Launching " + job.getName() + " recipe.");
            /*List<String> messageArguments = Arrays.asList(bdamessage.toString());
            kpi.setArguments(messageArguments);
            (new Thread(kpi)).start();*/
        } catch (SQLException e) {
            LOGGER.log(Level.INFO, "Subscriber[" + authHash + "], No recipe found for message " + messageType + ".");
        }
    }
}
