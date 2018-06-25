package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import com.google.protobuf.ServiceException;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HBaseConnector implements Connector {
    // TODO: Should setup connection using username/password.

    private final static Logger LOGGER = Logger.getLogger(HBaseConnector.class.getCanonicalName());

    private String port;
    private String hostname;
    private Connection connection;

    public HBaseConnector(String FS, String username, String password) {
        LOGGER.log(Level.INFO, "Initializing HBase Connector...");

        // Store Connection Parameters.
        this.port = getHBaseConnectionPort(FS);
        this.hostname = getHBaseConnectionURL(FS);

        // Initialize HBase Configuration.
        Configuration conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.property.clientPort", this.port);
        conf.set("hbase.zookeeper.quorum", this.hostname);
        conf.set("hbase.client.keyvalue.maxsize","0");

        // Check HBase Availability.
        try {
            HBaseAdmin.checkHBaseAvailable(conf);
        } catch (ServiceException e) {
            LOGGER.log(Level.SEVERE, "HBase Availability Check Failed.");
            e.printStackTrace();
            return;
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "HBase Availability Check Failed.");
            e.printStackTrace();
            return;
        }

        // Initialize HBase Connection.
        this.connection = null;
        try {
            this.connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Connection Failed! Check output console.");
            e.printStackTrace();
            return;
        } finally {
            LOGGER.log(Level.INFO, "HBase connection initialized.");
        }
    }

    public String put(Message row) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("Events");
        String rowkey = null;
        if (!admin.tableExists(tableName)) {
            HTableDescriptor desc = new HTableDescriptor(tableName);
            desc.addFamily(new HColumnDescriptor("messages"));
            admin.createTable(desc);
        }
        else {
            Table table = connection.getTable(tableName);
            Long timestamp = Timestamp.valueOf(LocalDateTime.now()).getTime();
            String topic="";
            for (KeyValue fields: row.getEntries()){
                if (fields.getKey().matches("topic"))
                    topic = fields.getValue();
            }
            rowkey = timestamp + "_"+topic;
            Put p = new Put(Bytes.toBytes(rowkey));
            for (KeyValue fields: row.getEntries())
                p.addColumn(Bytes.toBytes("messages"), Bytes.toBytes(fields.getKey()), Bytes.toBytes(fields.getValue()));
            table.put(p);
        }
        admin.close();
        return rowkey;
    }

    public void put(MasterData masterData){
        System.out.println("put in HBase ");
    }

    public List<Tuple> getLast(Integer args) throws IOException {
        List<Tuple> res = new LinkedList<>();
        TableName tableName = TableName.valueOf("Events");
        Table table = connection.getTable(tableName);
        Scan s = new Scan();
        s.addFamily(Bytes.toBytes("messages"));
        s.setReversed(true);
        ResultScanner scanner = table.getScanner(s);
        Iterator<Result> it = scanner.iterator();
        if (args==-1) args=1000;
        for ( int i=0; i<args && it.hasNext(); i++) {
            List<KeyValue> entries = new LinkedList<>();
            Result result = it.next();
            for(Cell cell: result.listCells()) {
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                entries.add(new KeyValue(qualifier, value));
            }
            res.add(new Tuple(entries));
        }
        scanner.close();
        return res;
    }

    public List<Tuple> getFrom(Integer args) throws IOException {
        List<Tuple> res = new LinkedList<>();
        TableName tableName = TableName.valueOf("Events");
        Table table = connection.getTable(tableName);
        Scan s = new Scan();
        s.addFamily(Bytes.toBytes("messages"));
        s.setTimeRange(Timestamp.valueOf(LocalDateTime.now().minusDays(args)).getTime(),Timestamp.valueOf(LocalDateTime.now()).getTime());
        ResultScanner scanner = table.getScanner(s);
        for (Result result : scanner) {
            List<KeyValue> entries = new LinkedList<>();
            for(Cell cell: result.listCells()) {
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String v = Bytes.toString(CellUtil.cloneValue(cell));
                entries.add(new KeyValue(qualifier, v));
            }
            res.add(new Tuple(entries));
        }
        scanner.close();
        return res;
    }

    public List<Tuple> get(String tablename, HashMap<String,String> filters) throws IOException {
        List<Tuple> res = new LinkedList<>();
        if (tablename=="")
            tablename = "Events";
        TableName tableName = TableName.valueOf(tablename);
        Table table = connection.getTable(tableName);
        Scan s = new Scan();
        s.setReversed(true);
        s.addFamily(Bytes.toBytes("messages"));
        FilterList filterList = new FilterList();
        for (Map.Entry<String,String> f: filters.entrySet()) {
            if (f.getKey().equalsIgnoreCase("key")){
                RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                        new SubstringComparator(f.getValue()));
                filterList.addFilter(filter);
            }
            else {
                SingleColumnValueFilter filter = new SingleColumnValueFilter(
                        Bytes.toBytes("messages"),
                        Bytes.toBytes(f.getKey()),
                        CompareFilter.CompareOp.EQUAL,
                        new SubstringComparator(f.getValue()));
                filter.setFilterIfMissing(true);
                filterList.addFilter(filter);
            }
        }
        s.setFilter(filterList);
        ResultScanner scanner = table.getScanner(s);
        for (Result result : scanner) {
            List<KeyValue> entries = new LinkedList<>();
            for(Cell cell: result.listCells()) {
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String v = Bytes.toString(CellUtil.cloneValue(cell));
                entries.add(new KeyValue(qualifier, v));
            }
            res.add(new Tuple(entries));
            break;
        }
        scanner.close();
        return res;
    }

    public DimensionTable describe(String args){
        System.out.println("print HBase schema " );
        return null;
    }

    public List<String> list() {
        return null;
    }

    public void close(){
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    };

    /**
     * Extracts the port from a HBase Connection URL.
     */
    private String getHBaseConnectionPort(String FS) {
        String[] tokens = FS.split(":");

        return tokens[1];
    }

    /**
     * Extracts the host from a HBase Connection URL.
     */
    private String getHBaseConnectionURL(String FS) {
        String[] tokens = FS.split(":");

        return tokens[0];
    }
}
