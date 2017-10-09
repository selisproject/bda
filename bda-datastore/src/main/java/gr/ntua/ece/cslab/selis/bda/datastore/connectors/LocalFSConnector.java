package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import java.io.*;
import java.util.*;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.codehaus.jackson.map.ObjectMapper;

public class LocalFSConnector implements Connector {
    private String FS;

    public LocalFSConnector(String FS){
        this.FS = FS;
    }

    // Append message in EventLog which is a csv file
    public void put(HashMap<String, String> row) throws IOException {
        File evlog = new File(FS + "/EventLog.csv");
        FileWriter fw = new FileWriter(evlog, true);
        BufferedWriter bw = new BufferedWriter(fw);

        for (String value : new TreeSet<String>(row.values()))
            bw.write(value + "\t");
        bw.newLine();

        bw.close();
        fw.close();
    }

    // Create table with columns from csv or json file and store it in csv file
    public void put(String file) throws Exception {
        String[] output_path = file.replace("json","csv").split("/"); // save in csv
        String output = output_path[output_path.length-1];
        FileWriter fw = new FileWriter(FS + '/' + output);
        BufferedWriter bw = new BufferedWriter(fw);
        String ext = FilenameUtils.getExtension(file);

        // if file is a csv read line by line
        if (ext.equals("csv")) {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                bw.write(line);
                bw.newLine();
            }
            reader.close();
        }
        // if file is a json read as an arraylist of linked hashmaps to retain columns order
        else if (ext.equals("json")) {
            ObjectMapper objectMapper = new ObjectMapper();
            ArrayList<LinkedHashMap<String, Object>> rows =
                    objectMapper.readValue(new File(file), objectMapper.getTypeFactory().constructCollectionType(ArrayList.class, LinkedHashMap.class));
            // write column names
            for (Map.Entry<String, Object> row : rows.get(0).entrySet())
                bw.write(row.getKey() + "\t");
            // if we create the eventLog add one more column named 'message' that will contain the blob
            if (output.matches("EventLog.csv"))
                bw.write("message");
            bw.newLine();
            // if we create a dimension table populate it with the master data too
            if (!(output.matches("EventLog.csv"))){
                // fill-in column values
                for (HashMap<String, Object> row : rows) {
                    for (Map.Entry<String, Object> line : row.entrySet())
                        bw.write(line.getValue() + "\t");
                    bw.newLine();
                }
            }
        }
        bw.close();
        fw.close();
    }

    // get last num rows from EventLog
    public HashMap<String, String>[] getLast(Integer num) throws IOException {
        File file = new File(FS + "/EventLog.csv");
        String[] fields = describe("");
        LineNumberReader lnr;

        // If num is negative get total number of rows
        if (num < 0) {
            lnr = new LineNumberReader(new FileReader(file));
            lnr.skip(Long.MAX_VALUE);
            num = lnr.getLineNumber()-1;
            lnr.close();
        }
        HashMap<String, String>[] rows = new HashMap[num];

        int counter = 0;
        String line;
        ReversedLinesFileReader reader = new ReversedLinesFileReader(file);
        // Read from end of file with a counter
        while((line = reader.readLine()) != null && counter < num){
            HashMap<String, String> hmap = new HashMap<String, String>();
            String[] values = line.split("\t");
            for (int i=0; i < fields.length; i++) {
                hmap.put(fields[i],values[i]);
            }
            rows[counter]=hmap;
            counter++;
        }
        reader.close();
        return rows;
    }

    // Get rows for last num days from EventLog
    public ArrayList<HashMap<String, String>> getFrom(Integer num){
        System.out.println("get from " + FS);
        return new ArrayList<HashMap<String, String>>();
    }

    // Get rows matching a specific column filter from a table
    public ArrayList<HashMap<String, String>> get(String table, String column, String value) throws IOException {
        String[] fields = describe(table);
        Integer pos = Arrays.asList(fields).indexOf(column);
        ArrayList<HashMap<String, String>> rows = new ArrayList<HashMap<String, String>>();

        String line;
        int counter = 0;
        if (table.matches(""))
            table = "EventLog";
        ReversedLinesFileReader reader = new ReversedLinesFileReader(new File(FS + "/" + table + ".csv"));
        // Read the table line by line and filter the specified column. In the eventLog only the last 1000 rows are searched.
        while((line = reader.readLine()) != null && (!table.equals("EventLog") || counter < 1000)){
            String[] values = line.split("\t");
            if (values[pos].equals(value)) {
                HashMap<String, String> hmap = new HashMap<String, String>();
                for (int i = 0; i < fields.length; i++) {
                    hmap.put(fields[i], values[i]);
                }
                rows.add(hmap);
            }
            counter++;
        }
        reader.close();
        return rows;
    }

    // get column names for table args
    public String[] describe(String args) throws IOException {
        if (args.matches(""))
            args = FS + "/EventLog.csv";
        else
            args = FS + "/" + args + ".csv";
        BufferedReader reader = new BufferedReader(new FileReader(args));
        String[] fields = reader.readLine().split("\t");
        reader.close();
        return fields;
    }

    public void close(){};
}
