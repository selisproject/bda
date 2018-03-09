package gr.ntua.ece.cslab.selis.bda.controller.connectors;

import de.tu_dresden.selis.pubsub.PubSub;
import de.tu_dresden.selis.pubsub.Message;
import de.tu_dresden.selis.pubsub.PubSubException;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PubSubPublisher {
    private final static Logger LOG = Logger.getLogger(PubSubPublisher.class.getCanonicalName());
    private static PubSub publisher;

    public PubSubPublisher(String hostname, int portNumber) {
        this.publisher = new PubSub(hostname, portNumber);
    }

    public void publish(HashMap<String,String> message) {

        try {
            Message msg = new Message();
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String,String> field: message.entrySet()) {
                String key = field.getKey();
                String value = field.getValue();
                msg.put(key, value);
                sb.append(key).append("=").append(value).append(", ");
            }
            publisher.publish(msg);
            LOG.log(Level.INFO,"Published "+ sb.toString());
        } catch (PubSubException ex) {
            LOG.log(Level.WARNING,"Could not publish messages, got exception: {}", ex.getMessage());
        }

    }
}
