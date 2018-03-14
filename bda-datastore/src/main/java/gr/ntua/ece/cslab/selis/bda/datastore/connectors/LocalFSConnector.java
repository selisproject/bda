package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;

import org.apache.commons.io.input.ReversedLinesFileReader;
import java.io.*;
import java.util.*;

public class LocalFSConnector implements Connector {
    private String FS;

    // The constructor creates the filesystem folder using the 'FS' parameter.
    // If this folder exists, it should be initially empty (before the bootstraping).
    public LocalFSConnector(String FS){
        File fs = new File(FS);
        if (!fs.exists())
            fs.mkdir();
        this.FS = FS;
    }

    // Used to initialize or append a message in the EventLog which is a csv file
    public void put(Message row) throws Exception {
        File evlog = new File(FS + "/EventLog.csv");
        FileWriter fw;
        BufferedWriter bw;
        if (!evlog.exists()) {
            // Initialize eventLog by writing column names in first line
            fw = new FileWriter(evlog);
            bw = new BufferedWriter(fw);
            for (KeyValue fields : row.getEntries()){
                String key = fields.getKey();
                bw.write( key + "\t");
            }
            // add one more column named 'message' that will contain the blob
            bw.write("event_type\tevent_timestamp\tmessage");
            bw.newLine();
        }
        else {
            // Append message in csv
            BufferedReader reader = new BufferedReader(new FileReader(FS + "/EventLog.csv"));
            String[] fields = reader.readLine().split("\t");
            reader.close();
            fw = new FileWriter(evlog, true);
            bw = new BufferedWriter(fw);
            List<KeyValue> entries = row.getEntries();
            for (String field : fields)
                for (KeyValue entry: entries){
                    if (entry.getKey().equalsIgnoreCase(field)) {
                        bw.write(entry.getValue() + "\t");
                        break;
                    }
                }
            bw.newLine();
        }
        bw.close();
        fw.close();
    }

    // Create table, populate it and store it in csv file
    public void put(MasterData masterData) throws Exception {
        for (DimensionTable table: masterData.getTables()) {
            String output = table.getName() + ".csv"; // save in csv
            FileWriter fw = new FileWriter(FS + '/' + output);
            BufferedWriter bw = new BufferedWriter(fw);

            List<Tuple> data = table.getData();
            // write column names
            List<KeyValue> fields = data.get(0).getTuple();
            for (KeyValue element : fields)
                bw.write(element.getKey() + "\t");
            bw.newLine();
            // fill-in column values
            for (Tuple tuple : data) {
                for (KeyValue element : tuple.getTuple())
                    bw.write(element.getValue() + "\t");
                bw.newLine();
            }
            bw.close();
            fw.close();
        }
    }

    // get last num rows from EventLog
    public List<Tuple> getLast(Integer num) throws IOException {
        List<Tuple> res = new LinkedList<>();
        File file = new File(FS + "/EventLog.csv");
        List<String> fields = describe("").getSchema().getColumnNames();
        LineNumberReader lnr;

        // If num is negative get total number of rows
        if (num < 0) {
            lnr = new LineNumberReader(new FileReader(file));
            lnr.skip(Long.MAX_VALUE);
            num = lnr.getLineNumber()-1;
            lnr.close();
        }

        int counter = 0;
        String line;
        ReversedLinesFileReader reader = new ReversedLinesFileReader(file);
        // Read from end of file with a counter
        while((line = reader.readLine()) != null && counter < num){
            List<KeyValue> entries = new LinkedList<>();
            String[] values = line.split("\t");
            for (int i=0; i < fields.size(); i++) {
                String columnValue = values[i];
                if (!columnValue.equalsIgnoreCase("null") && !columnValue.matches(""))
                    entries.add(new KeyValue(fields.get(i),columnValue));
            }
            res.add(new Tuple(entries));
            counter++;
        }
        reader.close();
        return res;
    }

    // Get rows for last num days from EventLog
    public List<Tuple> getFrom(Integer num){
        System.out.println("get from " + FS);
        return new LinkedList<>();
    }

    // Get rows matching a specific column filter from a table
    public List<Tuple> get(String table, String column, String value) throws Exception {
        List<Tuple> res = new LinkedList<>();
        if (column.equals("message") && table.matches(""))
            throw new Exception("Cannot filter the raw message in the eventLog.");

        List<String> fields = describe(table).getSchema().getColumnNames();
        Integer pos = fields.indexOf(column);

        String line;
        int counter = 0;
        if (table.matches(""))
            table = "EventLog";
        ReversedLinesFileReader reader = new ReversedLinesFileReader(new File(FS + "/" + table + ".csv"));
        // Read the table line by line and filter the specified column. In the eventLog only the last 1000 rows are searched.
        while((line = reader.readLine()) != null && (!table.equals("EventLog") || counter < 1000)){
            String[] values = line.split("\t");
            if (values[pos].equals(value)) {
                List<KeyValue> entries = new LinkedList<>();
                for (int i = 0; i < fields.size(); i++)
                    entries.add(new KeyValue(fields.get(i), values[i]));
                res.add(new Tuple(entries));
            }
            counter++;
        }
        reader.close();
        return res;
    }

    // get column names for table args
    public DimensionTable describe(String args) throws IOException {
        String table;
        if (args.matches(""))
            table = FS + "/EventLog.csv";
        else
            table = FS + "/" + args + ".csv";
        BufferedReader reader = new BufferedReader(new FileReader(table));
        String[] fields = reader.readLine().split("\t");
        reader.close();
        return new DimensionTable(args,
                new DimensionTableSchema(Arrays.asList(fields), new LinkedList<>(), ""),
                new LinkedList<>());
    }

    // List dimension tables in FS
    public List<String> list() {
        File folder = new File(FS);
        File[] dimensiontables = folder.listFiles((dir, name) -> (!name.contains("EventLog")));
        List<String> tables = new LinkedList<>();
        for (File file: dimensiontables)
            tables.add(file.getName().split("\\.")[0]);
        return tables;
    }

    public void close(){};
}
