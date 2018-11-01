package gr.ntua.ece.cslab.selis.bda.kpidb;

import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPI;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.KPITable;
import gr.ntua.ece.cslab.selis.bda.kpidb.beans.Tuple;
import gr.ntua.ece.cslab.selis.bda.kpidb.connectors.KPIConnectorFactory;
import gr.ntua.ece.cslab.selis.bda.kpidb.connectors.KPIConnector;

import java.util.List;

public class KPIBackend {
    private KPIConnector kpiConnector;

    public KPIBackend(String slug) throws SystemConnectorException {
        this.kpiConnector = KPIConnectorFactory.getInstance().generateConnector(SystemConnector.getInstance().getKPIconnector(slug));
    }

    public KPIBackend(Connector conn) {

    }

    public void create(KPITable kpiTable) throws Exception {
        this.kpiConnector.create(kpiTable);
    }

    public void insert(KPI kpi) throws Exception {
        this.kpiConnector.put(kpi);

    }

    public List<Tuple> fetch(String kpi_name, String type, Integer value) throws Exception {
        if (type.equals("rows")) {
            System.out.println("Before calling kpiconnector");
            return this.kpiConnector.getLast(kpi_name, value);
        }
        else
            throw new Exception("type not found: " + type);
    }

    public List<Tuple> select(String kpi_name, Tuple filters) throws Exception {
        return this.kpiConnector.get(kpi_name, filters);
    }

    public KPITable getSchema(String kpi_name) throws Exception {
        return this.kpiConnector.describe(kpi_name);
    }

}
