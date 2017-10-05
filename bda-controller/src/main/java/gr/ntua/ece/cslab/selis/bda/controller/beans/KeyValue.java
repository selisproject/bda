package gr.ntua.ece.cslab.selis.bda.controller.beans;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

/**
 * Created by Giannis Giannakopoulos on 10/5/17.
 * This class represents a key value pair
 */
@XmlRootElement(name = "kv")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class KeyValue implements Serializable{
    private String key;
    private String value;

    public KeyValue() {}

    public KeyValue(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
