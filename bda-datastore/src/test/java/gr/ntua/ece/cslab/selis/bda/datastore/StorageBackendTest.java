package gr.ntua.ece.cslab.selis.bda.datastore;

import gr.ntua.ece.cslab.selis.bda.common.storage.AbstractTestConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StorageBackendTest extends AbstractTestConnector {
    Logger LOGGER = Logger.getLogger(StorageBackendTest.class.getCanonicalName());

    @org.junit.Before
    public void setUp() throws SystemConnectorException {
        super.setUp();
    }

    @org.junit.After
    public void tearDown() throws SystemConnectorException {
        super.tearDown();
    }

    @org.junit.Test
    public void test() throws SystemConnectorException {
        String SCNslug = "testll";
        StorageBackend backend = new StorageBackend(SCNslug);

        // List of dimension tables filenames
        ArrayList<String> dimensionTables = new ArrayList<String>();
        dimensionTables.add("bda-datastore/src/test/resources/trucks.csv");
        dimensionTables.add("bda-datastore/src/test/resources/warehouses.json");
        dimensionTables.add("bda-datastore/src/test/resources/RAs.csv");

        // Create example message for EventLog
        HashMap<String, String> hmap = new HashMap<String, String>();
        hmap.put("warehouse_id", "1");
        hmap.put("latitude", "31.456");
        hmap.put("longitude", "36.542");
        hmap.put("timestamp", "2017-05-02.23:48:57");

        // Insert message in EventLog
        //backend.insert(hmap);

        // Get last message from EventLog
        //LOGGER.log(Level.INFO, Arrays.toString(backend.fetch("rows", 1)));

        // Get messages of last 3 days from Eventlog
        //backend.select("days", 3);

        // Get all messages from EventLog
        //LOGGER.log(Level.INFO, Arrays.toString(backend.fetch("rows", -1)));

        // Get info for specific entities from dimension table
        //LOGGER.log(Level.INFO, Arrays.toString(backend.select("trucks","RA", "AG.072")));

        // Get info for specific entities from EventLog
        //LOGGER.log(Level.INFO, Arrays.toString(backend.select("","warehouse_id", "1")));

        // Print EventLog format
        //LOGGER.log(Level.INFO, Arrays.toString(backend.getSchema("")));

        // Print dimension table format
        //LOGGER.log(Level.INFO, backend.getSchema("warehouses").getSchema().getColumnNames());

        // List dimension tables
        //LOGGER.log(Level.INFO, backend.listTables().toString());
    }
}
