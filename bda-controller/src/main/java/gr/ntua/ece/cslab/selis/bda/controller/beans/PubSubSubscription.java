package gr.ntua.ece.cslab.selis.bda.controller.beans;

import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Tuple;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

@XmlRootElement(name = "messageSubscription")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class PubSubSubscription implements Serializable {
    private List<Tuple> subscriptions;

    public PubSubSubscription() {
        this.subscriptions = new LinkedList<>();
    }

    public List<Tuple> getSubscriptions() {
        return subscriptions;
    }

    public void setSubscriptions(List<Tuple> subscriptions) {
        this.subscriptions = subscriptions;
    }

    public static PubSubSubscription getActiveSubscriptions() throws SQLException, SystemConnectorException {
        PubSubSubscription messageTypeNames = new PubSubSubscription();
        List<Tuple> subscriptions = new LinkedList<>();
        List<ScnDbInfo> SCNs = ScnDbInfo.getScnDbInfo();

        if (!(SCNs.isEmpty()))
        {
            for (ScnDbInfo SCN : SCNs) {
                for (String messageType: MessageType.getActiveMessageTypeNames(SCN.getSlug())) {
                    Tuple subscription = new Tuple();
                    List<KeyValue> rules = new LinkedList<>();
                    //rules.add(new KeyValue("scn_slug", SCN.getSlug()));
                    rules.add(new KeyValue("message_type", messageType));
                    subscription.setTuple(rules);
                    subscriptions.add(subscription);
                }
            }
        }
        messageTypeNames.setSubscriptions(subscriptions);
        return messageTypeNames;
    }
}
