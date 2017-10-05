package gr.ntua.ece.cslab.selis.bda.controller.beans;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.List;

/**
 * Created by Giannis Giannakopoulos on 10/5/17.
 */
@XmlRootElement(name = "message")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class Message implements Serializable {
    private List<Message> nested;
    private List<KeyValue> entries;

    public Message() {
    }

    public List<KeyValue> getEntries() {
        return entries;
    }

    public void setEntries(List<KeyValue> entries) {
        this.entries = entries;
    }

    public List<Message> getNested() {
        return nested;
    }

    public void setNested(List<Message> nested) {
        this.nested = nested;
    }
}
