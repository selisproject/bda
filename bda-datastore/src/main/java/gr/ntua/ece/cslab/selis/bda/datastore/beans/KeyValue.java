package gr.ntua.ece.cslab.selis.bda.datastore.beans;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

/**
 * Created by Giannis Giannakopoulos on 10/5/17.
 * This class represents a key value pair, used by the {@link Message} class.
 */
@XmlRootElement(name = "KeyValue")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class KeyValue implements Serializable{
    private String key;
    private String value;

    /**
     * Default constructor
     */
    public KeyValue() {}

    /**
     * Default constructor that initializes the key and the value.
     * @param key
     * @param value
     */
    public KeyValue(String key, String value) {
        this.key = key;
        this.value = value;
    }

    /**
     * Getter for the key
     * @return key
     */
    public String getKey() {
        return key;
    }

    /**
     * Setter for the key
     * @param key
     */
    public void setKey(String key) {
        this.key = key;
    }

    /**
     * Getter for the value
     * @return value
     */
    public String getValue() {
        return value;
    }

    /**
     * Setter for the value
     * @param value
     */
    public void setValue(String value) {
        this.value = value;
    }
}
