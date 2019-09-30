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

import de.tu_dresden.selis.pubsub.*;

import gr.ntua.ece.cslab.selis.bda.controller.AuthClientBackend;
import gr.ntua.ece.cslab.selis.bda.controller.beans.PubSubSubscription;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Tuple;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;

import java.util.logging.Level;
import java.util.logging.Logger;

public class PubSubSubscriber implements Runnable {
    private final static Logger LOGGER = Logger.getLogger(PubSubSubscriber.class.getCanonicalName()+" [" + Thread.currentThread().getName() + "]");

    private String authHash;
    private String SCNslug;
    private volatile PubSubSubscription subscriptions = new PubSubSubscription();
    private volatile boolean reloadSubscriptionsFlag = true;

    public PubSubSubscriber(String authHash, String scn) {
        this.authHash = authHash;
        this.SCNslug = scn;
    }

    public void reloadSubscriptions(PubSubSubscription subscriptions) {
        if (!subscriptions.getScnSlug().matches(this.SCNslug)){
            LOGGER.log(Level.SEVERE, "Asked to reload subscriptions of different SCN subscriber! This should never happen.");
            return;
        }
        this.subscriptions = subscriptions;
        this.reloadSubscriptionsFlag = true;
    }


    @Override
    public void run() {
        PubSub pubsub = null;

        while (reloadSubscriptionsFlag) {
            reloadSubscriptionsFlag = false;

            try {
                String hostname = subscriptions.getPubSubHostname();
                Integer portNumber = subscriptions.getPubSubPort();
                String certificateFile = subscriptions.getPubSubCertificate();
                pubsub = new PubSub(certificateFile, hostname, portNumber);

                if (!(subscriptions.getSubscriptions().isEmpty())) {

                    for (Tuple messageTypeName : subscriptions.getSubscriptions()) {
                        if (authHash.matches(""))
                            authHash = AuthClientBackend.getAccessToken();

                        Subscription subscription = new Subscription(authHash);

                        for (KeyValue rule: messageTypeName.getTuple())
                            subscription.add(new Rule(rule.getKey(), rule.getValue(), RuleType.EQ));

                        pubsub.subscribe(subscription, new Callback() {
                            @Override
                            public void onMessage(Message message) {
                                try {
                                    PubSubMessageHandler.handleMessage(message, SCNslug);
                                    LOGGER.log(Level.INFO,"PubSub message successfully inserted in the BDA.");

                                } catch (Exception e) {
                                    e.printStackTrace();
                                    LOGGER.log(Level.SEVERE,"Could not insert new PubSub message.");
                                }
                            }
                        });
                    }

                    LOGGER.log(Level.INFO,
                            String.format("SUCCESS: %s subscriber subscribed to %d message types",
                                    SCNslug, subscriptions.getSubscriptions().size()));
                }
                else
                    LOGGER.log(Level.INFO,
                            "{0} subscriber: No registered messages to subscribe to.", SCNslug);
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
                    LOGGER.log(Level.WARNING,"{0} subscriber was interrupted.", SCNslug);
                    pubsub.close();
                    break;
                }
            }
        }
        pubsub.close();
        LOGGER.log(Level.INFO,"{0} subscriber finished.", SCNslug);
    }
}
