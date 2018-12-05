package gr.ntua.ece.cslab.selis.bda.controller.connectors;

import de.tu_dresden.selis.pubsub.Message;

import gr.ntua.ece.cslab.selis.bda.analyticsml.RunnerInstance;
import gr.ntua.ece.cslab.selis.bda.datastore.StorageBackend;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PubSubMessageHandler {
    private final static Logger LOGGER = Logger.getLogger(PubSubMessageHandler.class.getCanonicalName()+" [" + Thread.currentThread().getName() + "]");

    public static void handleMessage(Message message, String SCNslug) throws Exception {

        gr.ntua.ece.cslab.selis.bda.datastore.beans.Message bdamessage = new gr.ntua.ece.cslab.selis.bda.datastore.beans.Message();
        String messageType;
        String messageId;
        String scnSlug;

        if (!message.containsKey("scn_slug")){
            LOGGER.log(Level.SEVERE,"Received message with no SCN slug.");
            throw new Exception("Could not insert new PubSub message. Missing SCN identifier.");
        }
        Object scnEntry = message.get("scn_slug");
        scnSlug = scnEntry != null ? scnEntry.toString() : "";
        if (!scnSlug.matches(SCNslug)) {
            LOGGER.log(Level.SEVERE,"Received message with wrong SCN slug. This should never happen.");
            throw new Exception("Could not insert new PubSub message. Mismatch in SCN identifier.");
        }

        if (!message.containsKey("message_type")) {
            LOGGER.log(Level.SEVERE, "Received message with no message type.");
            throw new Exception("Could not insert new PubSub message. Missing message type.");
        }
        Object messageEntry = message.get("message_type");
        messageType = messageEntry != null ? messageEntry.toString() : "";

        List<String> messageTypeNames;
        try {
            messageTypeNames = MessageType.getActiveMessageTypeNames(scnSlug);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Could not get registered message types to validate incoming message.");
            throw e;
        }
        if (!messageTypeNames.contains(messageType)){
            LOGGER.log(Level.SEVERE, "Received unknown message type: " + messageType + ". This should never happen.");
            throw new Exception("Could not insert new PubSub message. Unregistered or empty message type.");
        }

        if (!message.containsKey("payload")){
            LOGGER.log(Level.SEVERE,"Received message with no payload.");
            throw new Exception("Could not insert new PubSub message. Missing payload.");
        }
        Object payloadEntry = message.get("payload");
        String payload = payloadEntry != null ? payloadEntry.toString() : "";
        if (payload.isEmpty()){
            LOGGER.log(Level.SEVERE,"Received message with empty payload.");
            throw new Exception("Could not insert new PubSub message. Empty payload.");
        }

        List<KeyValue> entries = new LinkedList<>();
        entries.add(new KeyValue("topic", messageType));
        entries.add(new KeyValue("message", payload));
        // parse payload from clms adapter
        //value = payload.replaceAll("=", "\":\"").replaceAll("\\s+", "").replaceAll(":\"\\[", ":[").replaceAll("\\{", "{\"").replaceAll(",", "\",\"").replaceAll("}", "\"}").replaceAll("}\",\"\\{", "},{").replaceAll("]\",", "],");
        //JsonObject payloadjson = new JsonParser().parse(value).getAsJsonObject();
        //Set<Map.Entry<String, JsonElement>> entrySet = payloadjson.entrySet();
        //for (Map.Entry<String, JsonElement> field : entrySet) {
        //    try {
        //        entries.add(new KeyValue(field.getKey(), field.getValue().getAsString()));
        //    } catch (Exception e) {
        //        entries.add(new KeyValue("message", "{\"" + field.getKey() + "\": " + field.getValue() + "}"));
        //    }
        //}

        for (Map.Entry<String, Object> entry : message.entrySet()) {
            String key = entry.getKey() != null ? entry.getKey() : "";
            if (!key.matches("payload") & !key.matches("message_type") & !key.matches("scn_slug")) {
                String value = entry.getValue() != null ? entry.getValue().toString() : "";
                entries.add(new KeyValue(key, value));
            }
        }
        bdamessage.setEntries(entries);

        messageId = new StorageBackend(scnSlug).insert(bdamessage);

        try {
            (new RunnerInstance(scnSlug)).run(messageType, messageId);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING,"Could not send request to start message related jobs.");
        }
    }
}
