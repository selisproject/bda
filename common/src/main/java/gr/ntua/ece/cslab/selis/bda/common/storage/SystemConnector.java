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
    private static Configuration configuration;
    private static SystemConnector systemConnector;

    private Connector BDAconnector;
    private HashMap<String, Connector> ELconnectors;
    private HashMap<String, Connector> DTconnectors;
    private HashMap<String, Connector> KPIconnectors;

    /** The constructor creates new connections for the EventLog FS, the Dimension
     *  tables FS and the KPI db per LL as well as the BDA db. **/
    public SystemConnector() {
        this.ELconnectors = new HashMap<String, Connector>();
        this.DTconnectors = new HashMap<String, Connector>();
        this.KPIconnectors = new HashMap<String, Connector>();

        LOGGER.log(Level.INFO, "Initializing BDA db connector...");
        BDAconnector = ConnectorFactory.getInstance().generateConnector(
                configuration.storageBackend.getBdaDatabaseURL(),
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword()
        );

        // TODO: get SCNs from BDA db
        LinkedList<String> SCNs = new LinkedList<>();
        SCNs.add("");
        LOGGER.log(Level.INFO, "Initializing SCN connectors...");
        for (String SCN: SCNs){
            ELconnectors.put(SCN, ConnectorFactory.getInstance().generateConnector(
                    configuration.storageBackend.getEventLogURL(),
                    configuration.storageBackend.getDbUsername(),
                    configuration.storageBackend.getDbPassword()
            ));

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

    public static void init(String args){
        // parse configuration
        configuration = Configuration.parseConfiguration(args);
        if(configuration==null) {
            System.exit(1);
        }
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