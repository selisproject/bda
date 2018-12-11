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
import java.util.List;
import java.util.Vector;

@XmlRootElement(name = "messageSubscription")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class PubSubSubscription implements Serializable {
    private String scnSlug;
    private List<Tuple> subscriptions;
    private String pubSubHostname;
    private Integer pubSubPort;

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

    public static PubSubSubscription getMessageSubscriptions(String SCNslug) throws SystemConnectorException {
        ScnDbInfo scn;
        PubSubSubscription subscriptions = new PubSubSubscription();
        subscriptions.setScnSlug(SCNslug);

        try {
            scn = ScnDbInfo.getScnDbInfoBySlug(SCNslug);
        } catch (SQLException e){
            return subscriptions;
        } catch (SystemConnectorException e) {
            throw e;
        }
        String pubsubhost = scn.getPubsubaddress();
        Integer pubsubport = scn.getPubsubport();

        List<Tuple> messageTypeNames = new Vector<>();
        for (String messageType: MessageType.getActiveMessageTypeNames(SCNslug)) {
            Tuple subscription = new Tuple();
            List<KeyValue> rules = new Vector<>();
            rules.add(new KeyValue("scn_slug", SCNslug));
            rules.add(new KeyValue("message_type", messageType));
            subscription.setTuple(rules);
            messageTypeNames.add(subscription);
        }

        subscriptions.setSubscriptions(messageTypeNames);
        subscriptions.setPubSubHostname(pubsubhost);
        subscriptions.setPubSubPort(pubsubport);
        return subscriptions;
    }
}
