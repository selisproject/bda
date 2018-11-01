package gr.ntua.ece.cslab.selis.bda.kpidb.beans;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

@XmlRootElement(name = "KeyValue")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class KeyValue implements Serializable {
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
     * @return
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
     * @return
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

    @Override
    public String toString() {
        return "KeyValue{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                "}\n";
    }
}
