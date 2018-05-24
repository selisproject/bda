package gr.ntua.ece.cslab.selis.bda.kpidb.beans;

public class KPITable {
    private String kpi_name;
    private KPISchema kpi_schema;

    public KPITable(String kpi_name, KPISchema kpi_schema) {
        this.kpi_name = kpi_name;
        this.kpi_schema = kpi_schema;
    }

    public String getKpi_name() {
        return kpi_name;
    }

    public void setKpi_name(String kpi_name) {
        this.kpi_name = kpi_name;
    }

    public KPISchema getKpi_schema() {
        return kpi_schema;
    }

    public void setKpi_schema(KPISchema kpi_schema) {
        this.kpi_schema = kpi_schema;
    }
}
