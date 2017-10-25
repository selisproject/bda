package gr.ntua.ece.cslab.selis.bda.datastore.beans;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

@XmlRootElement(name = "tuple")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class Tuple implements Serializable {
    private List<KeyValue> fields;

    public Tuple(){ this.fields = new LinkedList<>();};

    public Tuple(List<KeyValue> fields) {
        this.fields = fields;
    }

    public List<KeyValue> getFields() {
        return fields;
    }

    public void setFields(List<KeyValue> fields) {
        this.fields = fields;
    }

    public String toString() {
        String row = this.fields.stream().
                map(a -> "["+a.getKey() + "," + a.getValue()+"]").
                reduce((a, b) -> a + "," + b).
                get();
        return row;
    }
}
