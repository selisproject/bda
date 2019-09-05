package gr.ntua.ece.cslab.selis.bda.common.storage;

import gr.ntua.ece.cslab.selis.bda.common.Configuration;
import gr.ntua.ece.cslab.selis.bda.common.storage.beans.ScnDbInfo;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.ConnectorFactory;

import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.lang.UnsupportedOperationException;

/**
 * This class is used by all the BDA modules to connect to the underlying storage engines.
 * It keeps open connections to every database that the BDA contains.
 */
public class SystemConnector {
    private final static Logger LOGGER = Logger.getLogger(SystemConnector.class.getCanonicalName());
    private static Configuration configuration;
    private static SystemConnector systemConnector;

    private Connector bdaConnector;
    private Connector hdfsConnector;
    private HashMap<String, Connector> elConnectors;
    private HashMap<String, Connector> dtConnectors;
    private HashMap<String, Connector> kpiConnectors;

    /**
     * Default constructor.
     * It creates new connections for the BDA db and the HDFS and initializes
     * objects to store the connections of the SCN databases.
     */
    private SystemConnector() throws SystemConnectorException {
        this.elConnectors = new HashMap<String, Connector>();
        this.dtConnectors = new HashMap<String, Connector>();
        this.kpiConnectors = new HashMap<String, Connector>();

        LOGGER.log(Level.INFO, "Initializing BDA db connector...");
        bdaConnector = ConnectorFactory.getInstance().generateConnector(
                configuration.storageBackend.getBdaDatabaseURL(),
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword(),
                configuration
        );

        hdfsConnector = ConnectorFactory.getInstance().generateConnector(
            configuration.storageBackend.getHDFSMasterURL(),
            configuration.storageBackend.getHDFSUsername(),
            configuration.storageBackend.getHDFSPassword(),
            configuration
        );
    }

    /**
     * This method is used by all the BDA modules to retrieve and use the
     * existing database connections.
     * @return the SystemConnector object containing all the database connections.
     * @throws SystemConnectorException
     */
    public static SystemConnector getInstance() throws SystemConnectorException {
        if (systemConnector == null){
            systemConnector = new SystemConnector();
            systemConnector.initSCNconnections();
        }
        return systemConnector;
    }

    /**
     * The init method is called when the BDA server is launched.
     * It loads the configuration and opens connections to all the databases.
     * @param args the configuration file location
     * @throws SystemConnectorException
     */
    public static void init(String args) throws SystemConnectorException {
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

    /**
     * This method is used to create new connections for the EventLog FS, the Dimension
     * tables FS and the KPI db per LL. **/
    private void initSCNconnections() throws SystemConnectorException {
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
                configuration.storageBackend.getEventLogURL() + SCN.getElDbname(),
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword(),
                configuration
            ));

            dtConnectors.put(SCN.getSlug(), ConnectorFactory.getInstance().generateConnector(
                configuration.storageBackend.getDimensionTablesURL() + SCN.getDtDbname(),
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword(),
                configuration
            ));
            /*PostgresqlPooledDataSource.init(
                    configuration.storageBackend.getBdaDatabaseURL(),
                    configuration.storageBackend.getDimensionTablesURL(),
                    configuration.storageBackend.getDbUsername(),
                    configuration.storageBackend.getDbPassword()
            );*/

            kpiConnectors.put(SCN.getSlug(), ConnectorFactory.getInstance().generateConnector(
                configuration.kpiBackend.getDbUrl() + SCN.getKpiDbname(),
                configuration.kpiBackend.getDbUsername(),
                configuration.kpiBackend.getDbPassword(),
                configuration
            ));
        }
    }

    /**
     * This method is used upon SCN creation to create the required databases
     * and open connections to them.
     * @param scn the ScnDbInfo object that contains information such as the slug
     *            and scn database name
     * @throws SystemConnectorException
     * @throws UnsupportedOperationException
     */
    public void createScnDatabase(ScnDbInfo scn)
        throws SystemConnectorException, UnsupportedOperationException {

        String scnSlug = scn.getSlug();
        Vector<String> schemas = new Vector<String>(1);
        schemas.add("metadata");

        String databaseUrl = ConnectorFactory.createNewDatabaseWithSchemas(
            configuration.storageBackend.getDimensionTablesURL(),
            configuration.storageBackend.getDbPrivilegedUsername(),
            configuration.storageBackend.getDbPrivilegedPassword(),
            configuration,
            configuration.storageBackend.getDbUsername(),
            scn.getDtDbname(),
            schemas
        );

        Connector dtConnector = ConnectorFactory.getInstance().generateConnector(
                databaseUrl,
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword(),
                configuration
        );

        dtConnectors.put(scnSlug, dtConnector);

        databaseUrl = ConnectorFactory.createNewDatabaseWithSchemas(
                configuration.storageBackend.getEventLogURL(),
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword(),
                configuration,
                configuration.storageBackend.getDbUsername(),
                scn.getElDbname(),
                null
        );

        Connector elConnector = ConnectorFactory.getInstance().generateConnector(
                databaseUrl,
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword(),
                configuration
        );

        elConnectors.put(scnSlug, elConnector);

        databaseUrl = ConnectorFactory.createNewDatabaseWithSchemas(
                configuration.kpiBackend.getDbUrl(),
                configuration.storageBackend.getDbPrivilegedUsername(),
                configuration.storageBackend.getDbPrivilegedPassword(),
                configuration,
                configuration.kpiBackend.getDbUsername(),
                scn.getKpiDbname(),
                null
        );

        Connector kpiConnector = ConnectorFactory.getInstance().generateConnector(
                databaseUrl,
                configuration.kpiBackend.getDbUsername(),
                configuration.kpiBackend.getDbPassword(),
                configuration
        );

        kpiConnectors.put(scnSlug, kpiConnector);
    }

    /**
     * This method is used upon SCN deletion to delete the scn databases
     * and close the open connections to them.
     * @param scn the ScnDbInfo object that contains information such as the slug
     *            and scn database name
     * @throws UnsupportedOperationException
     * @throws SystemConnectorException
     */
    public void destroyScnDatabase(ScnDbInfo scn)
            throws UnsupportedOperationException, SystemConnectorException {

        String scnSlug = scn.getSlug();
        if (dtConnectors.containsKey(scnSlug))
            getDTconnector(scnSlug).close();
        if (elConnectors.containsKey(scnSlug))
            getELconnector(scnSlug).close();
        if (kpiConnectors.containsKey(scnSlug))
            getKPIconnector(scnSlug).close();

        ConnectorFactory.dropDatabase(
                configuration.storageBackend.getDimensionTablesURL(),
                configuration.storageBackend.getDbPrivilegedUsername(),
                configuration.storageBackend.getDbPrivilegedPassword(),
                configuration,
                configuration.storageBackend.getDbUsername(),
                scn.getDtDbname()
        );
        dtConnectors.remove(scnSlug);

        ConnectorFactory.dropDatabase(
                configuration.storageBackend.getEventLogURL(),
                configuration.storageBackend.getDbUsername(),
                configuration.storageBackend.getDbPassword(),
                configuration,
                configuration.storageBackend.getDbUsername(),
                scn.getElDbname()
        );
        elConnectors.remove(scnSlug);

        ConnectorFactory.dropDatabase(
                configuration.kpiBackend.getDbUrl(),
                configuration.storageBackend.getDbPrivilegedUsername(),
                configuration.storageBackend.getDbPrivilegedPassword(),
                configuration,
                configuration.kpiBackend.getDbUsername(),
                scn.getKpiDbname()
        );
        kpiConnectors.remove(scnSlug);
    }

    /**
     * This method is used when the BDA server is shutdown to close all open database
     * connections.
     */
    public void close(){
        bdaConnector.close();

        for (Map.Entry<String, Connector> conn: elConnectors.entrySet()){
            conn.getValue().close();
        }
        for (Map.Entry<String, Connector> conn: dtConnectors.entrySet()){
            conn.getValue().close();
        }
        for (Map.Entry<String, Connector> conn: kpiConnectors.entrySet()){
            conn.getValue().close();
        }
    }

    /**
     * Retrieve the existing connection to the BDA db.
     * @return the Connector object that contains the open connection
     */
    public Connector getBDAconnector() {
        return bdaConnector;
    }

    /**
     * Retrieve the existing connection to the HDFS.
     * @return the Connector object that contains the open connection
     */
    public Connector getHDFSConnector() {
        return hdfsConnector;
    }

    /**
     * Retrieve the existing connection to an SCN's Event Log.
     * @param SCN the scn slug
     * @return the Connector object that contains the open connection
     */
    public Connector getELconnector(String SCN) {
        return elConnectors.get(SCN);
    }

    /**
     * Retrieve the existing connection to an SCN's Dimension Tables.
     * @param SCN the scn slug
     * @return the Connector object that contains the open connection
     */
    public Connector getDTconnector(String SCN) { 
        return dtConnectors.get(SCN);
    }

    /**
     * Retrieve the existing connection an SCN's KPI database.
     * @param SCN the scn slug
     * @return the Connector object that contains the open connection
     */
    public Connector getKPIconnector(String SCN) {
        return kpiConnectors.get(SCN);
    }
}
