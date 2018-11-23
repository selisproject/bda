package gr.ntua.ece.cslab.selis.bda.controller.connectors;

import gr.ntua.ece.cslab.selis.bda.common.Configuration;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.controller.Entrypoint;
import gr.ntua.ece.cslab.selis.bda.controller.beans.PubSubSubscription;

import javax.ws.rs.client.*;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PubSubConnector {
    private final static Logger LOGGER = Logger.getLogger(PubSubConnector.class.getCanonicalName());
    private static Configuration configuration;
    private static PubSubConnector pubSubConnector;

    private static boolean isExternal;
    private HashMap<String, PubSubSubscriber> subscriberRunners;
    private HashMap<String, Thread> subscribers;
    //private PubSubPublisher publisher;

    public PubSubConnector() {
        this.subscriberRunners = new HashMap<>();
        this.subscribers = new HashMap<>();
        isExternal = true;
    }

    public static PubSubConnector getInstance() {
        if (pubSubConnector == null){
            pubSubConnector = new PubSubConnector();
            pubSubConnector.initSCNsubscribers();
        }
        return pubSubConnector;
    }

    public static void init() {
        // parse configuration
        configuration = Entrypoint.configuration;

        if (pubSubConnector == null) {
            pubSubConnector = new PubSubConnector();
            pubSubConnector.initSCNsubscribers();
        }
    }

    private void initSCNsubscribers() {
        if (configuration.subscriber.getHostname().isEmpty() || String.valueOf(configuration.subscriber.getPortNumber()).isEmpty()){
            isExternal = false;

            LOGGER.log(Level.INFO, "Initializing internal PubSub subscribers...");
            try {
                for (ScnDbInfo scn : ScnDbInfo.getScnDbInfo()) {
                    reloadSubscriptions(scn.getSlug());
                }
            } catch (Exception e) {
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Failed to retrieve SCN info to start pub sub subscribers.");
            }

            //LOGGER.log(Level.INFO, "Initializing PubSub publisher...");
            //publisher = new PubSubPublisher(configuration.pubsub.getHostname(),
            //        configuration.pubsub.getPortNumber());
        }
    }

    public void removeSubscriber(String SCNslug) {
        if (!isExternal) {
            subscribers.get(SCNslug).interrupt();
            subscribers.remove(SCNslug);
            subscriberRunners.remove(SCNslug);
        }
        else {
            Client client = ClientBuilder.newClient();

            try{
                WebTarget resource = client.target("http://" + configuration.subscriber.getHostname() + ":" + configuration.subscriber.getPortNumber() + "/api").path("/message/"+SCNslug+"/remove");
                Invocation.Builder request = resource.request();

                Response response = request.get();
                if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
                    LOGGER.log(Level.INFO,
                            "SUCCESS: Request to remove scn subscriber has been sent.");
                } else {
                    LOGGER.log(Level.WARNING,
                            "Request to remove subscriber has failed, got error: {0}",
                            response.getStatusInfo().getReasonPhrase());
                }
            } catch (Exception e){
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Could not connect to subscriber.");
            }
        }
    }

    public void reloadSubscriptions(String SCNslug) {
        PubSubSubscription subscriptions = null;
        try {
            subscriptions = PubSubSubscription.getActiveSubscriptions(SCNslug);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING, "Failed to get subscriptions.");
        }

        if (!isExternal) {
            if (!subscriberRunners.containsKey(SCNslug)) {
                try {
                    ScnDbInfo scn = ScnDbInfo.getScnDbInfoBySlug(SCNslug);
                    PubSubSubscriber subscriber = new PubSubSubscriber(configuration.pubsub.getAuthHash(),
                            scn.getPubsubaddress(),
                            scn.getPubsubport(),
                            configuration.pubsub.getCertificateLocation(),
                            scn.getSlug());
                    subscriberRunners.put(scn.getSlug(), subscriber);
                    Thread s = new Thread(subscriber, "Subscriber_" + scn.getSlug());
                    subscribers.put(scn.getSlug(), s);
                    s.start();
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.log(Level.WARNING, "Could not create internal subscriber.");
                }
            }
            subscriberRunners.get(SCNslug).reloadSubscriptions(subscriptions);
        }
        else {
            Client client = ClientBuilder.newClient();

            try{
                WebTarget resource = client.target("http://" + configuration.subscriber.getHostname() + ":" + configuration.subscriber.getPortNumber() + "/api").path("/message/"+SCNslug+"/reload");
                Invocation.Builder request = resource.request();

                Response response = request.post(Entity.json(subscriptions));
                if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
                    LOGGER.log(Level.INFO,
                            "SUCCESS: Request to subscribe to {0} message types has been sent.",
                            subscriptions.getSubscriptions().size());
                } else {
                    LOGGER.log(Level.WARNING,
                            "Request to subscribe has failed, got error: {0}",
                            response.getStatusInfo().getReasonPhrase());
                }
            } catch (Exception e){
                e.printStackTrace();
                LOGGER.log(Level.WARNING, "Could not connect to subscriber.");
            }
        }
    }

    public void close(){
        if (!isExternal)
            for (Map.Entry<String, Thread> subscriber: subscribers.entrySet()){
                subscriber.getValue().interrupt();
            }
    }
}
