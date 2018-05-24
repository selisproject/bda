package gr.ntua.ece.cslab.selis.bda.kpidb.connectors;

public class ConnectorFactory {
    private static ConnectorFactory connFactory;

    private ConnectorFactory() {}

    public static ConnectorFactory getInstance(){
        if (connFactory == null)
            connFactory = new ConnectorFactory();
        return connFactory;
    }

    public Connector generateConnector(String FS, String Username, String Password){
        Connector connector = null;
        if (FS.contains("jdbc:postgresql")) {
            connector = new PostgresqlConnector(FS, Username, Password);
        }
        return connector;
    }
}
