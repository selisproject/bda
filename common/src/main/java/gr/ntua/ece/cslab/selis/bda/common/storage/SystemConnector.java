package gr.ntua.ece.cslab.selis.bda.common.storage;

import gr.ntua.ece.cslab.selis.bda.common.Configuration;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.ConnectorFactory;

import java.util.logging.Level;
import java.util.logging.Logger;

public class SystemConnector {
    private final static Logger LOGGER = Logger.getLogger(SystemConnector.class.getCanonicalName());
    public static Configuration configuration;
    private Connector ELconnector;
    private Connector DTconnector;
    private Connector KPIconnector;
    private Connector BDAdbconnector;

    /** The constructor creates new connections for the EventLog FS, the Dimension
     *  tables FS, the KPI db and the BDA db. **/
    public SystemConnector(String SCNname) {
        LOGGER.log(Level.INFO, "Initializing EventLog connector...");
        this.ELconnector = ConnectorFactory.getInstance().generateConnector(
                configuration.storageBackend.getEventLogURL(),
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword());

        LOGGER.log(Level.INFO, "Initializing Dimension tables connector...");
        this.DTconnector = ConnectorFactory.getInstance().generateConnector(
                configuration.storageBackend.getDimensionTablesURL(),
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword()
        );

        LOGGER.log(Level.INFO, "Initializing BDA db connector...");
        this.BDAdbconnector = ConnectorFactory.getInstance().generateConnector(
                configuration.storageBackend.getBdaDatabaseURL(),
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword()
        );

        LOGGER.log(Level.INFO, "Initializing KPI db connector...");
        this.BDAdbconnector = ConnectorFactory.getInstance().generateConnector(
                configuration.kpiBackend.getDbUrl(),
                configuration.kpiBackend.getDbUsername(),
                configuration.kpiBackend.getDbPassword()
        );
    }
}