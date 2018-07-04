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
    private Connector BDAconnector;
    private HashMap<String, Connector> ELconnectors;
    private HashMap<String, Connector> DTconnectors;
    private HashMap<String, Connector> KPIconnectors;

    private static SystemConnector systemConnector;

    /** The constructor creates new connections for the EventLog FS, the Dimension
     *  tables FS and the KPI db per LL as well as the BDA db. **/
    public SystemConnector() {
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

    public static SystemConnector getInstance(){
        if (systemConnector == null)
            systemConnector = new SystemConnector();
        return systemConnector;
    }

    public static void init(){
        if (systemConnector == null)
            systemConnector = new SystemConnector();
    }

    public Connector getBDAconnector() {
        return BDAconnector;
    }

    public Connector getELconnector(String SCN) {
        return ELconnectors.get(SCN);
    }

    public Connector getDTconnector(String SCN) { return DTconnectors.get(SCN); }

    public Connector getKPIconnector(String SCN) {
        return KPIconnectors.get(SCN);
    }
}