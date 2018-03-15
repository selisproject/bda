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
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class HBaseConnector implements Connector {

    private String port;
    private String hostname;
    private Connection connection;

    public HBaseConnector(String FS){
        this.port = "2181";
        this.hostname = "localhost";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", port);
        conf.set("hbase.zookeeper.quorum", hostname);
        try {
            HBaseAdmin.checkHBaseAvailable(conf);
        } catch (ServiceException e) {
            e.printStackTrace();
            return;
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            System.out.println("HBase connection initialized!");
        } catch (IOException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;
        }

    }

    public void put(Message row) throws IOException {
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("Events");
        if (!admin.tableExists(tableName)) {
            HTableDescriptor desc = new HTableDescriptor(tableName);
            desc.addFamily(new HColumnDescriptor("messages"));
            admin.createTable(desc);
        }
        else {
            Table table = connection.getTable(tableName);
            Long timestamp = null;
            String topic="";
            for (KeyValue fields: row.getEntries()){
                if (fields.getKey().matches ("timestamp"))
                    timestamp = Timestamp.valueOf(LocalDateTime.parse(fields.getValue(), DateTimeFormatter.ISO_DATE_TIME)).getTime();
                else if (fields.getKey().matches("topic"))
                    topic = fields.getValue();
            }
            String rowkey = timestamp + "_"+topic;
            Put p = new Put(Bytes.toBytes(rowkey));
            for (KeyValue fields: row.getEntries())
                p.addColumn(Bytes.toBytes("messages"), Bytes.toBytes(fields.getKey()), Bytes.toBytes(fields.getValue()));
            table.put(p);
        }
        admin.close();
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

    public List<Tuple> get(String tablename, String column, String value) throws IOException {
        List<Tuple> res = new LinkedList<>();
        if (tablename=="")
            tablename = "Events";
        TableName tableName = TableName.valueOf(tablename);
        Table table = connection.getTable(tableName);
        Scan s = new Scan();
        s.addFamily(Bytes.toBytes("messages"));
        Filter filter = new SingleColumnValueFilter(
                Bytes.toBytes("messages"),
                Bytes.toBytes(column),
                CompareFilter.CompareOp.EQUAL,
                new org.apache.hadoop.hbase.filter.SubstringComparator(value));
        s.setFilter(filter);
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
}
