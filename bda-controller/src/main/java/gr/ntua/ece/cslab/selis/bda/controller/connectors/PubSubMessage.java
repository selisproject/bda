package gr.ntua.ece.cslab.selis.bda.controller.connectors;

import de.tu_dresden.selis.pubsub.Message;

import gr.ntua.ece.cslab.selis.bda.analyticsml.RunnerInstance;
import gr.ntua.ece.cslab.selis.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PubSubMessage {
    private final static Logger LOGGER = Logger.getLogger(PubSubMessage.class.getCanonicalName()+" [" + Thread.currentThread().getName() + "]");

    public static void handleMessage(Message message) throws Exception {

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
            LOGGER.log(Level.WARNING,"Received message with no SCN slug. This should never happen.");
            throw new Exception("Could not insert new PubSub message. Missing SCN identifier.");
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
                else {
                    LOGGER.log(Level.WARNING, "Received unknown message type: " + value + ". This should never happen.");
                    throw new Exception("Could not insert new PubSub message. Unregistered message type.");
                }
            }
        }
        if (messageType.matches("")){
            LOGGER.log(Level.WARNING,"Received no message type. This should never happen.");
            throw new Exception("Could not insert new PubSub message. Empty message type.");
        }

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
