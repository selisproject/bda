package gr.ntua.ece.cslab.selis.bda.controller.connectors;

import gr.ntua.ece.cslab.selis.bda.common.Configuration;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.Connector;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.controller.beans.PubSubSubscription;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;

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
        configuration = Configuration.getInstance();

        if (pubSubConnector == null) {
            pubSubConnector = new PubSubConnector();
            pubSubConnector.initSCNsubscribers();
        }
    }

    private void initSCNsubscribers() {
        isExternal = (
            configuration.subscriber.getUrl() != null &&
            !configuration.subscriber.getUrl().isEmpty()
        );

        try {
            for (ScnDbInfo scn : ScnDbInfo.getScnDbInfo()) {
                if (!isExternal) {
                    LOGGER.log(Level.INFO, "Initializing internal PubSub subscriber for "+scn.getSlug());
                    reloadSubscriptions(scn.getSlug(), false);
                }
                if (MessageType.checkExternalMessageTypesExist(scn.getSlug()))
                    reloadSubscriptions(scn.getSlug(), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING, "Failed to retrieve SCN info to start pub sub subscribers.");
        }

        //LOGGER.log(Level.INFO, "Initializing PubSub publisher...");
        //publisher = new PubSubPublisher(configuration.pubsub.getHostname(),
        //        configuration.pubsub.getPortNumber());

    }

    public void reloadSubscriptions(String SCNslug, boolean externalConnector) {
        PubSubSubscription subscriptions;
        try {
            subscriptions = PubSubSubscription.getMessageSubscriptions(SCNslug, externalConnector);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING, "Failed to get subscriptions.");
            return;
        }

        if (!isExternal && !externalConnector) {
            if (subscriptions.getSubscriptions().isEmpty() & subscriberRunners.containsKey(SCNslug)){
                subscribers.get(SCNslug).interrupt();
                subscribers.remove(SCNslug);
                subscriberRunners.remove(SCNslug);
                return;
            }
            if (!subscriberRunners.containsKey(SCNslug)) {
                try {
                    ScnDbInfo scn = ScnDbInfo.getScnDbInfoBySlug(SCNslug);
                    Connector conn = Connector.getConnectorInfoById(scn.getConnectorId());
                    PubSubSubscriber subscriber = new PubSubSubscriber(configuration.pubsub.getAuthHash(),
                            conn.getAddress(),
                            conn.getPort(),
                            configuration.pubsub.getCertificateLocation(),
                            scn.getSlug());
                    subscriber.reloadSubscriptions(subscriptions);
                    subscriberRunners.put(scn.getSlug(), subscriber);
                    Thread s = new Thread(subscriber, "Subscriber_" + scn.getSlug());
                    subscribers.put(scn.getSlug(), s);
                    s.start();
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.log(Level.WARNING, "Could not create internal subscriber.");
                }
            }
            else
                subscriberRunners.get(SCNslug).reloadSubscriptions(subscriptions);
        }
        else {
            Client client = ClientBuilder.newClient();
            WebTarget resource;
            if (externalConnector)
                resource = client.target(configuration.externalSubscriber.getUrl());
            else
                resource = client.target(configuration.subscriber.getUrl());
            Invocation.Builder request = resource.request();

            Response response = request.post(Entity.json(subscriptions));
            if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
                LOGGER.log(Level.INFO,
                        "SUCCESS: Request to reload subscriptions of SCN {0} has been sent.",
                        subscriptions.getScnSlug());
            } else {
                LOGGER.log(Level.SEVERE,
                        "Request to reload subscriptions has failed, got error: {0}",
                        response.getStatusInfo().getReasonPhrase());
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
