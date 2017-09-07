package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

public class ConnectorFactory {

    public Connector getConnector(String FS){
        Connector connector = null;
        if (FS.contains("hdfs")){
            connector = new HDFSConnector();
        }
        else if (FS.contains("hbase")){
            connector = new HBaseConnector();
        }
        else
            connector = new PostgresqlConnector();
        return connector;
    }

}
