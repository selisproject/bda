package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
import java.util.*;

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
            Long timestamp = Timestamp.valueOf(LocalDateTime.now()).getTime();
            String topic="";
            String payload="";
            for (KeyValue fields: row.getEntries()){
                if (fields.getKey().matches("message_type"))
                    topic = fields.getValue();
                else if (fields.getKey().matches("payload"))
                    payload = fields.getValue();
            }
            String rowkey = timestamp + "_"+topic;
            Put p = new Put(Bytes.toBytes(rowkey));
            JsonObject payloadjson=new JsonParser().parse(payload.replaceAll("=","\":\"").replaceAll("\\s+","").replaceAll(":\"\\[",":[").replaceAll("\\{","{\"").replaceAll(",","\",\"").replaceAll("}","\"}").replaceAll("}\",\"\\{","},{").replaceAll("]\",","],")).getAsJsonObject();
            Set<Map.Entry<String, JsonElement>> entrySet = payloadjson.entrySet();
            for(Map.Entry<String,JsonElement> entry : entrySet){
                if ((entry.getKey().matches("sales_forecast")) || (entry.getKey().matches("stock_levels"))){
                    p.addColumn(Bytes.toBytes("messages"), Bytes.toBytes("topic"), Bytes.toBytes(entry.getKey()));
                    p.addColumn(Bytes.toBytes("messages"), Bytes.toBytes("message"), Bytes.toBytes("{\""+entry.getKey()+"\": "+entry.getValue()+"}"));
                }
                else
                    p.addColumn(Bytes.toBytes("messages"), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().getAsString()));
                }
            table.put(p);
        }
        admin.close();
    }

    public void put(MasterData masterData){
        System.out.println("put in HBase ");
    }

    @Override
    public void put(KPIDescription args) throws Exception {

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

    @Override
    public List<Tuple> getLastKPIs(String kpi_name, Integer args) throws Exception {
        return null;
    }

    @Override
    public List<Tuple> getKPIs(String kpi_name, List<KeyValue> args) throws Exception {
        return null;
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
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    Bytes.toBytes("messages"),
                    Bytes.toBytes(f.getKey()),
                    CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator(f.getValue()));
            filter.setFilterIfMissing(true);
            filterList.addFilter(filter);
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
}
