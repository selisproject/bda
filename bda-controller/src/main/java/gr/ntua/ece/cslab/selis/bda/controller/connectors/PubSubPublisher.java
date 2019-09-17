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
        Configuration configuration = Configuration.getInstance();

        String certificateLocation = configuration.pubsub.getCertificateLocation();

        this.publisher = new PubSub(certificateLocation, hostname, portNumber);
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
