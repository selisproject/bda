package gr.ntua.ece.cslab.selis.bda.datastore;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;
import gr.ntua.ece.cslab.selis.bda.datastore.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.datastore.connectors.ConnectorFactory;
import org.apache.hadoop.mapreduce.jobhistory.TaskUpdatedEvent;

import java.security.Key;
import java.util.List;

public class KPIBackend {
    private Connector KPIConnector;

    public KPIBackend(String KPIFS, String dbUsername, String dbPassword) {
        this.KPIConnector = ConnectorFactory.getInstance().generateConnector(KPIFS, dbUsername, dbPassword);
    }

    public void init(MasterData masterData) throws Exception {
        this.KPIConnector.put(masterData);
    }

    public void insert(KPIDescription kpi) throws Exception {
        this.KPIConnector.put(kpi);

    }

    public List<Tuple> fetch(String kpi_name, String type, Integer value) throws Exception {
        if (type.equals("rows")) {
            System.out.println("Before calling kpiconnector");
            return this.KPIConnector.getLastKPIs(kpi_name, value);
        }
        else
            throw new Exception("type not found: " + type);
    }

    public List<Tuple> select(String kpi_name, List<KeyValue> args) throws Exception {
        return this.KPIConnector.getKPIs(kpi_name, args);
    }

    public DimensionTable getSchema(String table) throws Exception {
        return this.KPIConnector.describe(table);
    }

    public void stop() {
        this.KPIConnector.close();
    }
}
