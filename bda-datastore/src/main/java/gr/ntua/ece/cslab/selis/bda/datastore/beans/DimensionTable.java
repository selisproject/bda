package gr.ntua.ece.cslab.selis.bda.datastore.beans;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Giannis Giannakopoulos on 10/11/17.
 */
@XmlRootElement(name = "DimensionTable")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class DimensionTable {
    private String name;
    private DimensionTableSchema schema;
    private List<Tuple> data;

    public DimensionTable(){ this.data = new LinkedList<>();}

    public DimensionTable(String name, DimensionTableSchema schema, List<Tuple> data) {
        this.name = name;
        this.schema = schema;
        this.data = data;
    }

    public DimensionTableSchema getSchema() {
        return schema;
    }

    public void setSchema(DimensionTableSchema schema) {
        this.schema = schema;
    }

    public List<Tuple> getData() {
        return data;
    }

    public void setData(List<Tuple> data) {
        this.data = data;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
