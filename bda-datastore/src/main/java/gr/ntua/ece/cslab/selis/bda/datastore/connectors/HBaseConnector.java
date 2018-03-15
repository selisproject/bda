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
            String timestamp="", topic="";
            for (KeyValue fields: row.getEntries()){
                if (fields.getKey().matches ("timestamp"))
                    timestamp = fields.getValue();
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
        s.setReversed(true);
        s.setMaxResultSize(args);
        ResultScanner scanner = table.getScanner(s);
        for (Result result : scanner) {
            List<KeyValue> entries = new LinkedList<>();
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

    public List<Tuple> getFrom(Integer args){
        System.out.println("get from HBase " );
        return null;
    }

    public List<Tuple> get(String tablename, String column, String value) throws IOException {
        List<Tuple> res = new LinkedList<>();
        TableName tableName = TableName.valueOf(tablename);
        Table table = connection.getTable(tableName);
        Scan s = new Scan();
        Filter filter = new QualifierFilter(
                CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator(column));
        Filter filter2 = new ValueFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(value));
        s.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, filter, filter2));
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
