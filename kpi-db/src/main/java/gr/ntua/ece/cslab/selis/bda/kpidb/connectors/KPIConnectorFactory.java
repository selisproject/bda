package gr.ntua.ece.cslab.selis.bda.kpidb.connectors;

import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.HBaseConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.LocalFSConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.PostgresqlConnector;

public class KPIConnectorFactory {
    private static KPIConnectorFactory connFactory;

    private KPIConnectorFactory() {}

    public static KPIConnectorFactory getInstance(){
        if (connFactory == null)
            connFactory = new KPIConnectorFactory();
        return connFactory;
    }

    public KPIConnector generateConnector(Connector conn){


        KPIConnector connector = null;
        if (conn instanceof PostgresqlConnector) {
            connector = new KPIPostgresqlConnector( (PostgresqlConnector) conn );
        }
        return connector;
    }
}
