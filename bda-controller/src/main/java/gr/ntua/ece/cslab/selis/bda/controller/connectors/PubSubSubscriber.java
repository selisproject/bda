package gr.ntua.ece.cslab.selis.bda.controller.connectors;

import de.tu_dresden.selis.pubsub.*;

import gr.ntua.ece.cslab.selis.bda.controller.beans.PubSubSubscription;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Tuple;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;

import java.util.logging.Level;
import java.util.logging.Logger;

public class PubSubSubscriber implements Runnable {
    private final static Logger LOGGER = Logger.getLogger(PubSubSubscriber.class.getCanonicalName()+" [" + Thread.currentThread().getName() + "]");

    private static String authHash;
    private static String hostname;
    private static int portNumber;
    private static String certificateLocation;
    private static String SCNslug;

    private static volatile PubSubSubscription subscriptions = new PubSubSubscription();
    private static volatile boolean reloadSubscriptionsFlag = true;

    public PubSubSubscriber(String authHash, String hostname, int portNumber, String cert, String scn) {
        this.authHash = authHash;
        this.hostname = hostname;
        this.portNumber = portNumber;
        this.certificateLocation = cert;
        this.SCNslug = scn;
    }

    public String getSCNslug() { return SCNslug; }

    public void reloadSubscriptions(PubSubSubscription subscriptions) {
        this.subscriptions = subscriptions;
        this.reloadSubscriptionsFlag = true;
    }

    @Override
    public void run() {
        PubSub pubsub = null;

        while (reloadSubscriptionsFlag) {
            reloadSubscriptionsFlag = false;

            try {
                if (subscriptions.getPubSubHostname() != null)
                    this.hostname = subscriptions.getPubSubHostname();
                if (subscriptions.getPubSubPort() != null)
                    this.portNumber = subscriptions.getPubSubPort();
                pubsub = new PubSub(this.certificateLocation, this.hostname, this.portNumber);

                if (!(subscriptions.getSubscriptions().isEmpty())) {

                    for (Tuple messageTypeName : subscriptions.getSubscriptions()) {
                        Subscription subscription = new Subscription(this.authHash);

                        for (KeyValue rule: messageTypeName.getTuple())
                            subscription.add(new Rule(rule.getKey(), rule.getValue(), RuleType.EQ));

                        pubsub.subscribe(subscription, new Callback() {
                            @Override
                            public void onMessage(Message message) {
                                try {
                                    PubSubMessageHandler.handleMessage(message, SCNslug);
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
                            subscriptions.getSubscriptions().size());
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

                    if (reloadSubscriptionsFlag) {
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
}
