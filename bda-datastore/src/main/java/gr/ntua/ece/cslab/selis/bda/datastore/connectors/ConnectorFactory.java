package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.HBaseConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.LocalFSConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.PostgresqlConnector;

public class ConnectorFactory {
    private static ConnectorFactory connFactory;

    private ConnectorFactory() {}

    public static ConnectorFactory getInstance(){
        if (connFactory == null)
            connFactory = new ConnectorFactory();
        return connFactory;
    }

    /** Depending on the FS string format initialize a connector from a different class.
     *  Connectors are implemented for four different filesystems: local, HBase, HDFS, PostgreSQL. **/
    public DatastoreConnector generateConnector(Connector conn){
        DatastoreConnector connector = null;
        if (conn.getClass().getCanonicalName().equalsIgnoreCase("HBaseConnector")){
            connector = new DatastoreHBaseConnector( (HBaseConnector) conn);
        }
        else if (conn.getClass().getCanonicalName().equalsIgnoreCase("PostgresqlConnector")) {
            connector = new DatastorePostgresqlConnector( (PostgresqlConnector) conn);
        }
        else if (conn.getClass().getCanonicalName().equalsIgnoreCase("LocalFSConnector")) {
            connector = new DatastoreLocalFSConnector( (LocalFSConnector) conn);
        }
        return connector;
    }
}
