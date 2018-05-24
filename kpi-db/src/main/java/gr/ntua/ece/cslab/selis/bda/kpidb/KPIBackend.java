package gr.ntua.ece.cslab.selis.bda.kpidb;

import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPI;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPITable;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.Tuple;
import gr.ntua.ece.cslab.selis.bda.kpidb.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.kpidb.connectors.ConnectorFactory;

import java.util.List;

public class KPIBackend {
    private Connector KPIConnector;

    public KPIBackend(String KPIFS, String dbUsername, String dbPassword) {
        this.KPIConnector = ConnectorFactory.getInstance().generateConnector(KPIFS, dbUsername, dbPassword);
    }

    public void create(KPITable kpiTable) throws Exception {
        this.KPIConnector.create(kpiTable);
    }

    public void insert(KPI kpi) throws Exception {
        this.KPIConnector.put(kpi);

    }

    public List<Tuple> fetch(String kpi_name, String type, Integer value) throws Exception {
        if (type.equals("rows")) {
            System.out.println("Before calling kpiconnector");
            return this.KPIConnector.getLast(kpi_name, value);
        }
        else
            throw new Exception("type not found: " + type);
    }

    public List<Tuple> select(String kpi_name, Tuple filters) throws Exception {
        return this.KPIConnector.get(kpi_name, filters);
    }

    public KPITable getSchema(String kpi_name) throws Exception {
        return this.KPIConnector.describe(kpi_name);
    }

    public void stop() {
        this.KPIConnector.close();
    }
}
