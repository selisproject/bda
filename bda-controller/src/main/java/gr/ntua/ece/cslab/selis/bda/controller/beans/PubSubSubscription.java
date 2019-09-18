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

package gr.ntua.ece.cslab.selis.bda.controller.beans;

import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.Connector;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Tuple;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;

@XmlRootElement(name = "messageSubscription")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class PubSubSubscription implements Serializable {
    private String scnSlug;
    private List<Tuple> subscriptions;
    private String pubSubHostname;
    private Integer pubSubPort;
    private List<Tuple> metadata;

    public PubSubSubscription() {
        this.subscriptions = new Vector<>();
    }

    public List<Tuple> getSubscriptions() {
        return subscriptions;
    }

    public void setSubscriptions(List<Tuple> subscriptions) {
        this.subscriptions = subscriptions;
    }

    public String getPubSubHostname() {
        return pubSubHostname;
    }

    public void setPubSubHostname(String pubSubHostname) {
        this.pubSubHostname = pubSubHostname;
    }

    public Integer getPubSubPort() {
        return pubSubPort;
    }

    public void setPubSubPort(Integer pubSubPort) { this.pubSubPort = pubSubPort; }

    public String getScnSlug() { return scnSlug; }

    public void setScnSlug(String scnSlug) { this.scnSlug = scnSlug; }

    public List<Tuple> getMetadata() { return metadata; }

    public void setMetadata(List<Tuple> metadata) { this.metadata = metadata; }

    public static PubSubSubscription getMessageSubscriptions(String SCNslug, Boolean external) throws SystemConnectorException {
        ScnDbInfo scn;
        Connector connector;
        List<MessageType> messageTypes;
        PubSubSubscription subscriptions = new PubSubSubscription();
        subscriptions.setScnSlug(SCNslug);

        try {
            scn = ScnDbInfo.getScnDbInfoBySlug(SCNslug);
            messageTypes = MessageType.getActiveMessageTypes(SCNslug, external);
            if(!external){
                connector = Connector.getConnectorInfoById(scn.getConnectorId());
            }
            else{
                Integer conn = null;
                for (MessageType messageType: messageTypes) {
                    if (conn == null) conn = messageType.getExternalConnectorId();
                    else
                        if (messageType.getExternalConnectorId().equals(conn))
                            throw new SystemConnectorException("A single external connector is supported");
                }
                connector = Connector.getConnectorInfoById(conn);
            }

        } catch (SQLException e){
            return subscriptions;
        } catch (SystemConnectorException e) {
            throw e;
        }

        String pubsubhost = connector.getAddress();
        Integer pubsubport = connector.getPort();

        List<Tuple> messageTypeNames = new Vector<>();
        List<Tuple> messagesMetadata = new Vector<>();
        if (external) {
            Tuple messageMetadata = new Tuple();
            List<KeyValue> metadata = new Vector<>();
            metadata.add(new KeyValue("username", connector.getMetadata().getUsername()));
            metadata.add(new KeyValue("password", connector.getMetadata().getPassword()));
            messageMetadata.setTuple(metadata);
            messagesMetadata.add(messageMetadata);
        }

        for (MessageType messageType: messageTypes) {
            Tuple subscription = new Tuple();
            List<KeyValue> rules = new Vector<>();
            rules.add(new KeyValue("scn_slug", SCNslug));
            rules.add(new KeyValue("message_type", messageType.getName()));
            subscription.setTuple(rules);
            messageTypeNames.add(subscription);

            if (!(messageType.getExternalConnectorId() == null) && !(messageType.getExternal_datasource() == null)) {
                Tuple messageMetadata = new Tuple();
                List<KeyValue> metadata = new Vector<>();
                metadata.add(new KeyValue("message_type", messageType.getName()));
                metadata.add(new KeyValue("data_source", messageType.getExternal_datasource()));
                messageMetadata.setTuple(metadata);
                messagesMetadata.add(messageMetadata);
            }
        }

        subscriptions.setSubscriptions(messageTypeNames);
        subscriptions.setPubSubHostname(pubsubhost);
        subscriptions.setPubSubPort(pubsubport);
        subscriptions.setMetadata(messagesMetadata);
        return subscriptions;
    }
}
