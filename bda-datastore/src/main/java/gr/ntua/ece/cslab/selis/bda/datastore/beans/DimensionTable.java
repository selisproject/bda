package gr.ntua.ece.cslab.selis.bda.datastore.beans;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Giannis Giannakopoulos on 10/11/17.
 * This class represents a Dimension (Entity) Table.
 *
 * Entity Tables are modeled as resources identified by a name, a schema, and the data they
 * are storing.
 *
 * @see DimensionTableSchema
 */
@XmlRootElement(name = "DimensionTable")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class DimensionTable {
    private String name;
    private DimensionTableSchema schema;
    private List<Tuple> data;

    /**
     * A simple placeholder constructor.
     */
    public DimensionTable(){ this.data = new LinkedList<>();}

    /**
     * @param name the table name
     * @param schema the table schema as defined in {@link DimensionTableSchema}
     * @param data the table data in the form of a list (must comply with the schema in {@param schema})
     */
    public DimensionTable(String name, DimensionTableSchema schema, List<Tuple> data) {
        this.name = name;
        this.schema = schema;
        this.data = data;
    }

    /**
     * @return
     */
    public DimensionTableSchema getSchema() {
        return schema;
    }

    /**
     * @param schema
     */
    public void setSchema(DimensionTableSchema schema) {
        this.schema = schema;
    }

    /**
     * @return
     */
    public List<Tuple> getData() {
        return data;
    }

    /**
     * @param data
     */
    public void setData(List<Tuple> data) {
        this.data = data;
    }

    /**
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }
}
