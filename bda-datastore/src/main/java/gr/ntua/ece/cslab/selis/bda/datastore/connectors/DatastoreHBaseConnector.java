package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import com.google.protobuf.ServiceException;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.HBaseConnector;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;
import gr.ntua.ece.cslab.selis.bda.datastore.beans.KeyValue;
import gr.ntua.ece.cslab.selis.bda.datastore.DatastoreException;
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

public class DatastoreHBaseConnector implements DatastoreConnector {
    // TODO: Should setup connection using username/password.

    private final static Logger LOGGER = Logger.getLogger(HBaseConnector.class.getCanonicalName());

    HBaseConnector conn;
    public DatastoreHBaseConnector(HBaseConnector conn) {
        this.conn = conn;
    }

    public void createMetaTables() throws DatastoreException, UnsupportedOperationException {
        throw new java.lang.UnsupportedOperationException("Creating Metadata tables in HBase is not supported.");
    }

    public String put(Message row) throws IOException {
        Admin admin = conn.getConnection().getAdmin();
        TableName tableName = TableName.valueOf(conn.getNamespace()+":Events");
        String rowkey = null;
        if (!admin.tableExists(tableName)) {
            HTableDescriptor desc = new HTableDescriptor(tableName);
            desc.addFamily(new HColumnDescriptor("messages"));
            admin.createTable(desc);
        }
        else {
            Table table = conn.getConnection().getTable(tableName);
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

    public void put(MasterData masterData) {
        throw new java.lang.UnsupportedOperationException("Inserting master data in HBase is not supported.");
    }

    public List<Tuple> getLast(Integer args) throws IOException {
        List<Tuple> res = new LinkedList<>();
        TableName tableName = TableName.valueOf(conn.getNamespace()+":Events");
        Table table = conn.getConnection().getTable(tableName);
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
        TableName tableName = TableName.valueOf(conn.getNamespace()+":Events");
        Table table = conn.getConnection().getTable(tableName);
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
            tablename = conn.getNamespace()+":Events";
        else
            throw new java.lang.UnsupportedOperationException("Cannot query a dimension table. HBase contains only the EventLog.");
        TableName tableName = TableName.valueOf(tablename);
        Table table = conn.getConnection().getTable(tableName);
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
        throw new java.lang.UnsupportedOperationException("HBase column schema is dynamic and shall not be described.");
    }

    public List<String> list() {
        throw new java.lang.UnsupportedOperationException("HBase only contains the EventLog table.");
    }

}
