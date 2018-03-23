package gr.ntua.ece.cslab.selis.bda.datastore.beans;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by Giannis Giannakopoulos on 10/11/17.
 */
public class KPIDescription {
    String kpi_name;
    long timestamp;
    List<KeyValue> entries;

    /*
        Empty Constructor
     */
    public KPIDescription() {
        this.entries = new LinkedList<>();
    }

    public KPIDescription(String kpi_name, long timestamp, List<KeyValue> entries) {
        this.kpi_name = kpi_name;
        this.timestamp = timestamp;
        this.entries = entries;
    }

    public String getKpi_name() {
        return kpi_name;
    }

    public void setKpi_name(String kpi_name) {
        this.kpi_name = kpi_name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public List<KeyValue> getEntries() {
        return entries;
    }

    public void setEntries(List<KeyValue> entries) {
        this.entries = entries;
    }
}
