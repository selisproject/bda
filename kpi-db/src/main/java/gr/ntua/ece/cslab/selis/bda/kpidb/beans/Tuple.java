package gr.ntua.ece.cslab.selis.bda.kpidb.beans;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

@XmlRootElement(name = "data")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class Tuple implements Serializable {
    private List<KeyValue> tuple;

    public Tuple(){ this.tuple = new LinkedList<>();}

    public Tuple(List<KeyValue> tuple) {
        this.tuple = tuple;
    }

    public List<KeyValue> getTuple() {
        return this.tuple;
    }

    public void setTuple(List<KeyValue> tuple) {
        this.tuple = tuple;
    }

    public String toString() {
        String row = this.tuple.stream().
                map(a -> "["+a.getKey() + "," + a.getValue()+"]").
                reduce((a, b) -> a + "," + b).
                get();
        return row;
    }
}