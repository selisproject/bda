package gr.ntua.ece.cslab.selis.bda.controller.connectors;

import gr.ntua.ece.cslab.selis.bda.analyticsml.RunnerInstance;
import gr.ntua.ece.cslab.selis.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;

import de.tu_dresden.selis.pubsub.*;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import gr.ntua.ece.cslab.selis.bda.controller.beans.PubSubSubscription;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Tuple;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PubSubSubscriber implements Runnable {
    private final static Logger LOGGER = Logger.getLogger(PubSubSubscriber.class.getCanonicalName()+" [" + Thread.currentThread().getName() + "]");

    private static String authHash;
    private static String hostname;
    private static int portNumber;
    private static String certificateLocation;

    private static volatile PubSubSubscription messageTypeNames = new PubSubSubscription();
    private static volatile boolean reloadMessageTypesFlag = true;

    public PubSubSubscriber(String authHash, String hostname, int portNumber, String cert) {
        this.authHash = authHash;
        this.hostname = hostname;
        this.portNumber = portNumber;
        this.certificateLocation = cert;
    }

    public static void reloadMessageTypes(PubSubSubscription messageTypes) {
        messageTypeNames = messageTypes;
        reloadMessageTypesFlag = true;
    }

    @Override
    public void run() {
        PubSub pubsub = null;

        while (reloadMessageTypesFlag) {
            reloadMessageTypesFlag = false;

            try {
                pubsub = new PubSub(this.certificateLocation, this.hostname, this.portNumber);

                if (!(messageTypeNames.getSubscriptions().isEmpty())) {

                    for (Tuple messageTypeName : messageTypeNames.getSubscriptions()) {
                        Subscription subscription = new Subscription(this.authHash);

                        for (KeyValue rule: messageTypeName.getTuple())
                            subscription.add(new Rule(rule.getKey(), rule.getValue(), RuleType.EQ));

                        pubsub.subscribe(subscription, new Callback() {
                            @Override
                            public void onMessage(Message message) {
                                try {
                                    PubSubMessage.handleMessage(message);
                                    LOGGER.log(Level.WARNING,"PubSub message successfully inserted in the BDA.");
                                    //handleMessage(message);
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    LOGGER.log(Level.WARNING,"Could not insert new PubSub message.");
                                }
                            }
                        });
                    }

                    LOGGER.log(Level.INFO,
                            "SUCCESS: Subscribed to {0} message types",
                            messageTypeNames.getSubscriptions().size());
                }
                else
                    LOGGER.log(Level.INFO,
                            "No registered messages to subscribe to.");
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
        String scnSlug = "";

        for (Map.Entry<String, Object> entry : message.entrySet()) {
            String key = entry.getKey() != null ? entry.getKey() : "";
            if (key.matches("scn_slug")) {
                scnSlug = entry.getValue() != null ? entry.getValue().toString() : "";
            }
        }
        if (scnSlug.matches("")) {
            throw new Exception("Subscriber[" + authHash + "], received message with no SCN slug. This should never happen.");
        }
        for (Map.Entry<String, Object> entry : message.entrySet()) {
            String key = entry.getKey() != null ? entry.getKey() : "";
            if (key.matches("message_type")) {
                List<String> messageTypeNames;
                try {
                    messageTypeNames = MessageType.getActiveMessageTypeNames(scnSlug);
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Could not get registered message types to validate incoming message.");
                    throw e;
                }
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
        message_id = new StorageBackend(scnSlug).insert(bdamessage);

        try {
            (new RunnerInstance(scnSlug)).run(messageType, message_id);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING,"Could not send request to start message related jobs.");
        }
    }
}
