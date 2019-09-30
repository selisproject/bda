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

import gr.ntua.ece.cslab.selis.bda.common.Configuration;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.Connector;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.controller.beans.PubSubSubscription;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.*;
import javax.ws.rs.core.Response;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PubSubConnector {
    private final static Logger LOGGER = Logger.getLogger(PubSubConnector.class.getCanonicalName());
    private static Configuration configuration;
    private static PubSubConnector pubSubConnector;

    private HashMap<String, PubSubSubscriber> subscriberRunners;
    private HashMap<String, Thread> subscribers;
    //private PubSubPublisher publisher;

    public PubSubConnector() {
        this.subscriberRunners = new HashMap<>();
        this.subscribers = new HashMap<>();
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

        try {
            for (ScnDbInfo scn : ScnDbInfo.getScnDbInfo()) {
                reloadSubscriptions(scn.getSlug(), false);

                if (MessageType.checkExternalMessageTypesExist(scn.getSlug()))
                    reloadSubscriptions(scn.getSlug(), true);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.SEVERE, "Failed to retrieve SCN info to start pub sub subscribers. Aborting.");
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
            LOGGER.log(Level.WARNING, "Failed to get subscriptions. Aborting reload of subscriber for "+SCNslug);
            return;
        }

        Connector conn;
        try {
            if (!externalConnector)
                conn = Connector.getConnectorInfoById(ScnDbInfo.getScnDbInfoBySlug(SCNslug).getConnectorId());
            else {
                Integer len = subscriptions.getSubscriptions().size();
                String msg = subscriptions.getSubscriptions().get(len-1).getTuple().get(1).getValue();
                conn = Connector.getConnectorInfoById(MessageType.getMessageByName(SCNslug, msg).getExternalConnectorId());
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.WARNING, "Failed to get connector info. Aborting reload of subscriber for "+SCNslug);
            return;
        }

        if (conn.isInternal() && !externalConnector) {
            if (subscriptions.getSubscriptions().isEmpty() & subscriberRunners.containsKey(SCNslug)){
                subscribers.get(SCNslug).interrupt();
                subscribers.remove(SCNslug);
                subscriberRunners.remove(SCNslug);
                return;
            }
            if (!subscriberRunners.containsKey(SCNslug)) {
                LOGGER.log(Level.INFO, "Initializing internal PubSub subscriber for "+SCNslug);
                try {
                    PubSubSubscriber subscriber = new PubSubSubscriber(
                            configuration.subscriber.getAuthHash(),
                            configuration.subscriber.getCertificateLocation(),
                            SCNslug);
                    subscriber.reloadSubscriptions(subscriptions);
                    subscriberRunners.put(SCNslug, subscriber);
                    Thread s = new Thread(subscriber, "Subscriber_" + SCNslug);
                    subscribers.put(SCNslug, s);
                    s.start();
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.log(Level.SEVERE, "Could not create internal subscriber for "+SCNslug);
                }
            }
            else
                subscriberRunners.get(SCNslug).reloadSubscriptions(subscriptions);
        }
        else {
            Client client = ClientBuilder.newClient();
            String address = conn.getAddress();
            Integer port = conn.getPort();
            WebTarget resource = client.target(configuration.subscriber.getUrl().replaceFirst("\\{}", address).replace("\\{\\}", port.toString()));
            Invocation.Builder request = resource.request();

            try {
                Response response = request.post(Entity.json(subscriptions));
                if (response.getStatusInfo().getFamily() == Response.Status.Family.SUCCESSFUL) {
                    LOGGER.log(Level.INFO,
                            "SUCCESS: Request to reload subscriptions of SCN {0} has been sent.",
                            subscriptions.getScnSlug());
                } else {
                    LOGGER.log(Level.WARNING,
                            "Request to reload subscriptions has failed, got error: {0}",
                            response.getStatusInfo().getReasonPhrase());
                }
            } catch (ProcessingException e){
                LOGGER.log(Level.WARNING,"Request to reload subscriptions has failed as subscriber seems down. Error details: ");
                e.printStackTrace();
            }
        }
    }

    public void close(){
        for (Map.Entry<String, Thread> subscriber: subscribers.entrySet()){
            subscriber.getValue().interrupt();
        }
    }
}
