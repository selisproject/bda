package gr.ntua.ece.cslab.selis.bda.controller.beans;

import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MessageType;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.Tuple;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

@XmlRootElement(name = "messageSubscription")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class PubSubSubscription implements Serializable {
    private List<Tuple> subscriptions;
    private String pubSubHostname;
    private Integer pubSubPort;

    public PubSubSubscription() {
        this.subscriptions = new LinkedList<>();
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

    public static PubSubSubscription getActiveSubscriptions(String SCNslug) throws Exception {
        ScnDbInfo scn = ScnDbInfo.getScnDbInfoBySlug(SCNslug);
        String pubsubhost = scn.getPubsubaddress();
        Integer pubsubport = scn.getPubsubport();

        List<Tuple> messageTypeNames = new LinkedList<>();
        for (String messageType: MessageType.getActiveMessageTypeNames(SCNslug)) {
            Tuple subscription = new Tuple();
            List<KeyValue> rules = new LinkedList<>();
            //rules.add(new KeyValue("scn_slug", SCNslug));
            rules.add(new KeyValue("message_type", messageType));
            subscription.setTuple(rules);
            messageTypeNames.add(subscription);
        }

        PubSubSubscription subscriptions = new PubSubSubscription();
        subscriptions.setSubscriptions(messageTypeNames);
        subscriptions.setPubSubHostname(pubsubhost);
        subscriptions.setPubSubPort(pubsubport);
        return subscriptions;
    }
}
