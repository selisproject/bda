package gr.ntua.ece.cslab.selis.bda.datastore.beans;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Giannis Giannakopoulos on 10/5/17.
 * Message class represents the message that BDA receives. This is a simple class that only provides getters and
 * setters.
 */
@XmlRootElement(name = "message")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class Message implements Serializable {
    private List<Message> nested;
    private List<KeyValue> entries;

    /**
     * Default constructor for the Message object
     */
    public Message() {
        this.nested = new LinkedList<>();
    }

    /**
     * Constructor that initializes the object's fields
     * @param nested the nested message components
     * @param entries the root-level list of entries
     */
    public Message(List<Message> nested, List<KeyValue> entries) {
        this.nested = nested;
        this.entries = entries;
    }

    /**
     * Getter for the Entries component
     * @return
     */
    public List<KeyValue> getEntries() {
        return entries;
    }

    /**
     * Setter for the entries component
     * @param entries
     */
    public void setEntries(List<KeyValue> entries) {
        this.entries = entries;
    }

    /**
     * Returns the nested messages
     * @return
     */
    public List<Message> getNested() {
        return nested;
    }

    /**
     * Sets a list of nested messages
     * @param nested
     */
    public void setNested(List<Message> nested) {
        this.nested = nested;
    }

    @Override
    /**
     * Default toString method that recursively prints the message objects
     */
    public String toString() {
        String entries = this.entries.stream().
                map(a -> "["+a.getKey() + "," + a.getValue()+"]").
                reduce((a, b) -> a + "," + b).
                get();
        for (Message m : this.nested) {
            entries += "[" + m.toString() + "]";
        }
        return entries;
    }
}
