package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

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
    public DatastoreConnector generateConnector(String FS, String Username, String Password){
        DatastoreConnector connector;
        if (FS.contains("hbase")){
            connector = new DatastoreHBaseConnector();
        }
        else if (FS.contains("jdbc:postgresql")) {
            connector = new DatastorePostgresqlConnector();
        }
        else
            connector = new DatastorePostgresqlConnector();
        return connector;
    }
}
