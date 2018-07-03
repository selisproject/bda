package gr.ntua.ece.cslab.selis.bda.common.storage;

import gr.ntua.ece.cslab.selis.bda.common.Configuration;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.ConnectorFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SystemConnector {
    private final static Logger LOGGER = Logger.getLogger(SystemConnector.class.getCanonicalName());
    public static Configuration configuration;
    private static Connector BDAconnector;
    private static HashMap<String, Connector> ELconnectors;
    private static HashMap<String, Connector> DTconnectors;
    private static HashMap<String, Connector> KPIconnectors;

    private static SystemConnector systemConnector;
    public SystemConnector() {}

    public static SystemConnector getInstance(){
        if (systemConnector == null)
            systemConnector = new SystemConnector();
        return systemConnector;
    }

    /** The method creates new connections for the EventLog FS, the Dimension
     *  tables FS and the KPI db per LL as well as the BDA db. **/
    public static void init(){
        LOGGER.log(Level.INFO, "Initializing BDA db connector...");
        BDAconnector = ConnectorFactory.getInstance().generateConnector(
                configuration.storageBackend.getBdaDatabaseURL(),
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword()
        );

        // TODO: get SCNs from BDA db
        LinkedList<String> SCNs = new LinkedList<>();
        for (String SCN: SCNs){
            LOGGER.log(Level.INFO, "Initializing EventLog connector...");
            ELconnectors.put(SCN, ConnectorFactory.getInstance().generateConnector(
                    configuration.storageBackend.getEventLogURL(),
                    configuration.storageBackend.getDbUsername(),
                    configuration.storageBackend.getDbPassword()
            ));

            LOGGER.log(Level.INFO, "Initializing Dimension tables connector...");
            DTconnectors.put(SCN, ConnectorFactory.getInstance().generateConnector(
                    configuration.storageBackend.getDimensionTablesURL(),
                    configuration.storageBackend.getDbUsername(),
                    configuration.storageBackend.getDbPassword()
            ));
            /*PostgresqlPooledDataSource.init(
                    configuration.storageBackend.getBdaDatabaseURL(),
                    configuration.storageBackend.getDimensionTablesURL(),
                    configuration.storageBackend.getDbUsername(),
                    configuration.storageBackend.getDbPassword()
            );*/

            LOGGER.log(Level.INFO, "Initializing KPI db connector...");
            KPIconnectors.put(SCN, ConnectorFactory.getInstance().generateConnector(
                    configuration.kpiBackend.getDbUrl(),
                    configuration.kpiBackend.getDbUsername(),
                    configuration.kpiBackend.getDbPassword()
            ));
        }
    }

    public static Connector getBDAconnector() {
        return BDAconnector;
    }

    public static Connector getELconnector(String SCN) {
        return ELconnectors.get(SCN);
    }

    public static Connector getDTconnector(String SCN) {
        return DTconnectors.get(SCN);
    }

    public static Connector getKPIconnector(String SCN) {
        return KPIconnectors.get(SCN);
    }
}