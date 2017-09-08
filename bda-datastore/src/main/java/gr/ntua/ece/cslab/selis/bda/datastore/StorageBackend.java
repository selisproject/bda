package gr.ntua.ece.cslab.selis.bda.datastore;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import gr.ntua.ece.cslab.selis.bda.datastore.connectors.Connector;
import gr.ntua.ece.cslab.selis.bda.datastore.connectors.ConnectorFactory;

public class StorageBackend {

    private static Connector ELConnector = null;
    private static Connector DTConnector = null;

    // Create datastore with star schema
    public void create(String EventLogFS, String DimensionTablesFS, List<String> dimensionTables) throws Exception {
        ConnectorFactory factory = new ConnectorFactory();
        ELConnector = factory.getConnector(EventLogFS);
        DTConnector = factory.getConnector(DimensionTablesFS);
        List<String> primaryKeys = new ArrayList<String>();

        // put each table in DimensionTablesFS + create columns in EventLog
        for (String table : dimensionTables) {
            InputStream input = StorageBackend.class.getClassLoader().getResourceAsStream(table);
            if ( input == null )
                throw new Exception("resource not found: " + table);

            DTConnector.put(table);
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            primaryKeys.add(reader.readLine().split("\t")[0]);
            reader.close();
        }
        ELConnector.put(primaryKeys.toString());
    }

    // Insert row in EventLog
    public void insert(List<String> field){
        ELConnector.put(String.valueOf(field));
    }

    // Select rows from EventLog
    public ArrayList<String> select(String type, Integer value){
        return ELConnector.get(String.valueOf(value));
    }

    // Get entry id from dimension table
    public ArrayList<String> fetch(String column, String value){
        return DTConnector.get(String.valueOf(value));
    }

    public static void main(String[] args) throws Exception {
        StorageBackend mybackend = new StorageBackend();

        String EventLogFS = "hdfs://master:9000/"; // hdfs or hbase
        String DimensionTablesFS = "hdfs://master:9000/"; // hdfs or postgres
        List<String> dimensionTables = new ArrayList<String>();
        dimensionTables.add("trucks.csv");
        dimensionTables.add("warehouses.csv");
        dimensionTables.add("RAs.csv");
        mybackend.create(EventLogFS, DimensionTablesFS, dimensionTables);

        mybackend.insert(new ArrayList<String>());
        mybackend.select("rows", 3);
        mybackend.select("days", 3);
        mybackend.select("raws", -1);
        mybackend.fetch("truck_platenr", "X1423");
    }
}