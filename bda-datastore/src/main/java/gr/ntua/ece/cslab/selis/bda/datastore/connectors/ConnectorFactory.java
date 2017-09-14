package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

public class ConnectorFactory {

    private static ConnectorFactory connFactory;

    private ConnectorFactory() {}

    public static ConnectorFactory getInstance(){
        if (connFactory == null)
            connFactory = new ConnectorFactory();
        return connFactory;
    }

    public Connector generateConnector(String FS){
        Connector connector = null;
        if (FS.contains("hdfs")){
            connector = new HDFSConnector(FS);
        }
        else if (FS.contains("hbase")){
            connector = new HBaseConnector(FS);
        }
        else if (FS.contains("jdbc")) {
            connector = new PostgresqlConnector(FS);
        }
        else
            connector = new LocalFSConnector(FS);
        return connector;
    }

}
