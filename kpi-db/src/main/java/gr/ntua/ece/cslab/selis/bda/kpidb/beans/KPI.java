package gr.ntua.ece.cslab.selis.bda.kpidb.beans;

import java.util.LinkedList;
import java.util.List;

public class KPI {
    String kpi_name;
    long timestamp;
    List<KeyValue> entries;

    /*
        Empty Constructor
     */
    public KPI() {
        this.entries = new LinkedList<>();
    }

    public KPI(String kpi_name, long timestamp, List<KeyValue> entries) {
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
