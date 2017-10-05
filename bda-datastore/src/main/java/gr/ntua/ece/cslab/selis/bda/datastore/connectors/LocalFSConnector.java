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
        FileWriter fw;
        BufferedWriter bw;

        // If it does not exist create it with provided empty message columns and an extra field to store message content
        if (!evlog.exists()) {
            fw = new FileWriter(evlog);
            bw = new BufferedWriter(fw);
            for (String field : new TreeSet<String>(row.keySet()))
                bw.write(field + "\t");
            bw.write("message");
            bw.newLine();
        }
        // else append message
        else {
            fw = new FileWriter(evlog, true);
            bw = new BufferedWriter(fw);
            for (String value : new TreeSet<String>(row.values()))
                bw.write(value + "\t");
            bw.newLine();
        }
        bw.close();
        fw.close();
    }

    // Store dimension table from csv or json file to csv file
    public void put(String file) throws Exception {
        String[] output = file.replace("json","csv").split("/"); // save in csv
        FileWriter fw = new FileWriter(FS + '/' + output[output.length-1]);
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
            bw.newLine();
            // write values
            for (HashMap<String, Object> row : rows) {
                for (Map.Entry<String, Object> line : row.entrySet())
                    bw.write(line.getValue() + "\t");
                bw.newLine();
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
