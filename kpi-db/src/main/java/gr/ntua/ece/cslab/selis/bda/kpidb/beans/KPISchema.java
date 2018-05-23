package gr.ntua.ece.cslab.selis.bda.kpidb.beans;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement(name = "DimensionTableSchema")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class KPISchema {
    private List<String> columnNames;
    private List<KeyValue> columnTypes;

    /* Empty constructor */
    public KPISchema() {
    }

    /* Constructor on fields*/
    public KPISchema(List<String> columnNames, List<KeyValue> columnTypes) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    /* Getters and Setters */
    public List<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    public List<KeyValue> getColumnTypes() {
        return columnTypes;
    }

    public void setColumnTypes(List<KeyValue> columnTypes) {
        this.columnTypes = columnTypes;
    }
}
