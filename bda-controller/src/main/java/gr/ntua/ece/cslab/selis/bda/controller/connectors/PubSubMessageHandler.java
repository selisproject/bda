/*
 * Copyright 2019 ICCS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gr.ntua.ece.cslab.selis.bda.controller.connectors;

import de.tu_dresden.selis.pubsub.Message;

import com.google.gson.Gson;

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
        String payload;

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
        try {
            payload = new Gson().toJson(payloadEntry);
        } catch (Exception e) {
            payload = payloadEntry != null ? payloadEntry.toString() : "";
        }
        if (payload.isEmpty()){
            LOGGER.log(Level.SEVERE,"Received message with empty payload.");
            throw new Exception("Could not insert new PubSub message. Empty payload.");
        }

        List<KeyValue> entries = new LinkedList<>();
        entries.add(new KeyValue("message_type", messageType));
        entries.add(new KeyValue("payload", payload));
        // parse payload from clms adapter
        //value = payload.replaceAll("=", "\":\"").replaceAll("\\s+", "").replaceAll(":\"\\[", ":[").replaceAll("\\{", "{\"").replaceAll(",", "\",\"").replaceAll("}", "\"}").replaceAll("}\",\"\\{", "},{").replaceAll("]\",", "],");
        //JsonObject payloadjson = new JsonParser().parse(value).getAsJsonObject();
        //Set<Map.Entry<String, JsonElement>> entrySet = payloadjson.entrySet();
        //for (Map.Entry<String, JsonElement> field : entrySet) {
        //    try {
        //        entries.add(new KeyValue(field.getKey(), field.getValue().getAsString()));
        //    } catch (Exception e) {
        //        entries.add(new KeyValue("payload", "{\"" + field.getKey() + "\": " + field.getValue() + "}"));
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
            (new RunnerInstance(scnSlug, messageType)).run(messageId);
        } catch (Exception e) {
            LOGGER.log(Level.INFO, "Did not launch job. "+e.getMessage());
        }
    }
}
