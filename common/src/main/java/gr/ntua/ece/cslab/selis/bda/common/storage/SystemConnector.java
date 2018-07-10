package gr.ntua.ece.cslab.selis.bda.common.storage;

import gr.ntua.ece.cslab.selis.bda.common.Configuration;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.ConnectorFactory;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;

import java.sql.SQLException;
import java.util.List;
import java.util.Vector;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.lang.UnsupportedOperationException;

public class SystemConnector {
    private final static Logger LOGGER = Logger.getLogger(SystemConnector.class.getCanonicalName());
    private static Configuration configuration;
    private static SystemConnector systemConnector;

    private Connector bdaConnector;
    private HashMap<String, Connector> elConnectors;
    private HashMap<String, Connector> dtConnectors;
    private HashMap<String, Connector> kpiConnectors;

    /** The constructor creates new connections for the EventLog FS, the Dimension
     *  tables FS and the KPI db per LL as well as the BDA db. **/
    public SystemConnector() {
        this.elConnectors = new HashMap<String, Connector>();
        this.dtConnectors = new HashMap<String, Connector>();
        this.kpiConnectors = new HashMap<String, Connector>();

        LOGGER.log(Level.INFO, "Initializing BDA db connector...");
        bdaConnector = ConnectorFactory.getInstance().generateConnector(
                configuration.storageBackend.getBdaDatabaseURL(),
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword()
        );
    }

    public static SystemConnector getInstance(){
        if (systemConnector == null){
            systemConnector = new SystemConnector();
            systemConnector.initSCNconnections();
        }
        return systemConnector;
    }

    public static void init(String args){
        // parse configuration
        configuration = Configuration.parseConfiguration(args);
        if(configuration==null) {
            System.exit(1);
        }
        if (systemConnector == null) {
            systemConnector = new SystemConnector();
            systemConnector.initSCNconnections();
        }
    }

    private void initSCNconnections(){
        List<ScnDbInfo> SCNs = new LinkedList<>();
        try {
            SCNs = ScnDbInfo.getScnDbInfo();
            LOGGER.log(Level.INFO, "Initializing SCN connectors...");
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        for (ScnDbInfo SCN: SCNs){
            elConnectors.put(SCN.getSlug(), ConnectorFactory.getInstance().generateConnector(
                    configuration.storageBackend.getEventLogURL()+SCN.getDbname(),
                    configuration.storageBackend.getDbUsername(),
                    configuration.storageBackend.getDbPassword()
            ));

            dtConnectors.put(SCN.getSlug(), ConnectorFactory.getInstance().generateConnector(
                    configuration.storageBackend.getDimensionTablesURL()+SCN.getDbname(),
                    configuration.storageBackend.getDbUsername(),
                    configuration.storageBackend.getDbPassword()
            ));
            /*PostgresqlPooledDataSource.init(
                    configuration.storageBackend.getBdaDatabaseURL(),
                    configuration.storageBackend.getDimensionTablesURL(),
                    configuration.storageBackend.getDbUsername(),
                    configuration.storageBackend.getDbPassword()
            );*/

            kpiConnectors.put(SCN.getSlug(), ConnectorFactory.getInstance().generateConnector(
                    configuration.kpiBackend.getDbUrl()+SCN.getDbname(),
                    configuration.kpiBackend.getDbUsername(),
                    configuration.kpiBackend.getDbPassword()
            ));
        }
    }

    public void createScnDatabase(String scnSlug, String dbname) 
        throws SystemConnectorException, UnsupportedOperationException {

        Vector<String> schemas = new Vector<String>(1);
        schemas.add("metadata");

        String databaseUrl = ConnectorFactory.createNewDatabaseWithSchemas(
            configuration.storageBackend.getDimensionTablesURL(),
            configuration.storageBackend.getDbPrivilegedUsername(),
            configuration.storageBackend.getDbPrivilegedPassword(),
            configuration.storageBackend.getDbUsername(),
            dbname, 
            schemas
        );

        Connector dtConnector = ConnectorFactory.getInstance().generateConnector(
                databaseUrl,
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword()
        );

        dtConnectors.put(scnSlug, dtConnector);

        databaseUrl = ConnectorFactory.createNewDatabaseWithSchemas(
                configuration.storageBackend.getEventLogURL(),
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword(),
                configuration.storageBackend.getDbUsername(),
                dbname,
                null
        );

        Connector elConnector = ConnectorFactory.getInstance().generateConnector(
                databaseUrl,
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword()
        );

        elConnectors.put(scnSlug, elConnector);

        // TODO: create KPI db too
    }

    public Connector getBDAconnector() {
        return bdaConnector;
    }

    public Connector getELconnector(String SCN) {
        return elConnectors.get(SCN);
    }

    public Connector getDTconnector(String SCN) { 
        return dtConnectors.get(SCN);
    }

    public Connector getKPIconnector(String SCN) {
        return kpiConnectors.get(SCN);
    }
}
