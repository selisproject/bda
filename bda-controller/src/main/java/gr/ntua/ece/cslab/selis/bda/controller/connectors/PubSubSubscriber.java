package gr.ntua.ece.cslab.selis.bda.controller.connectors;

import gr.ntua.ece.cslab.selis.bda.controller.Entrypoint;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;

import de.tu_dresden.selis.pubsub.*;
import de.tu_dresden.selis.pubsub.PubSubException;

import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Map;

public class PubSubSubscriber implements Runnable {
    private final static Logger LOG = Logger.getLogger(PubSubSubscriber.class.getCanonicalName()+" [" + Thread.currentThread().getName() + "]");
    private static String authHash;
    private static String hostname;
    private static int portNumber;
    private static List<String> rules;

    public PubSubSubscriber(String authHash, String hostname, int portNumber, List<String> rules) {
        this.authHash = authHash;
        this.hostname = hostname;
        this.portNumber = portNumber;
        this.rules = rules;
    }

    public void run() {

        try (PubSub c = new PubSub(this.hostname, this.portNumber)) {
            Subscription subscription = new Subscription(this.authHash);

            //this line can throw exception if we provide value of invalid type. Check ValueType for allowed values
            for (String rule : this.rules){
                subscription.add(new Rule("message_type", rule, RuleType.EQ));
            }
            //subscription.add(new Rule("_type", "PKI", RuleType.EQ));
            //subscription.add(new Rule("AvgPrice", price, RuleType.GE));
            // subscription.add(new Rule("quality", quality, RuleType.GE)); //this line is equal to the below line:
            // subscription.add(Rule.intRule("quality", quality, RuleType.GE));

            c.subscribe(subscription, new Callback() {
                @Override
                public void onMessage(Message message) {
                    gr.ntua.ece.cslab.selis.bda.datastore.beans.Message bdamessage = new gr.ntua.ece.cslab.selis.bda.datastore.beans.Message();
                    List<KeyValue> entries = new LinkedList<>();
                    StringBuilder sb = new StringBuilder();
                    for (Map.Entry<String, Object> entry : message.entrySet()) {
                        String key = entry.getKey() != null ? entry.getKey() : "";
                        String value = entry.getValue() != null ? entry.getValue().toString() : "";

                        sb.append(key).append("=").append(value).append(", ");
                        entries.add(new KeyValue(key,value));
                    }
                    bdamessage.setEntries(entries);
                    try {
                        Entrypoint.myBackend.insert(bdamessage);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    LOG.log(Level.INFO,"Subscriber["+authHash+"], Received "+ sb.toString());
                }
            });

            while (true) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    break;
                }
            }
        } catch (PubSubException ex) {
            LOG.log(Level.WARNING,"Could not subscribe, got error: {}", ex.getMessage());
        }

        LOG.log(Level.INFO,"Finishing");
    }
}
