package gr.ntua.ece.cslab.selis.bda.datastore;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.DimensionTableSchema;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.MasterData;
import gr.ntua.ece.cslab.selis.bda.datastore.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.datastore.connectors.ConnectorFactory;

public class KPIBackend {
    private Connector KPIConnector;

    public KPIBackend(String KPIFS, String dbUsername, String dbPassword) {
        this.KPIConnector = ConnectorFactory.getInstance().generateConnector(KPIFS, dbUsername, dbPassword);
    }

    public void init(MasterData masterData) throws Exception {
        this.KPIConnector.put(masterData);
    }

    public void insert() {

    }

    public void fetch() {

    }

    public void select() {

    }

    public void stop() {
        this.KPIConnector.close();
    }
}
