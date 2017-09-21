package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeSet;
import org.apache.commons.io.input.ReversedLinesFileReader;

public class LocalFSConnector implements Connector {
    private String FS;

    public LocalFSConnector(String FS){
        this.FS = FS;
    }

    // append message in EventLog
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

    // Store dimension table
    public void put(String file) throws Exception {
        InputStream input = LocalFSConnector.class.getClassLoader().getResourceAsStream(file);
        if ( input == null )
            throw new Exception("resource not found: " + file);

        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        FileWriter fw = new FileWriter(FS + '/' + file);
        BufferedWriter bw = new BufferedWriter(fw);
        String line;
        while ((line = reader.readLine()) != null) {
            bw.write(line);
            bw.newLine();
        }
        reader.close();
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

    // Get all info for specific entity from dimension table
    public HashMap<String, String> get(String table, String column, String value) throws IOException {
        String[] fields = describe(table);
        Integer pos = Arrays.asList(fields).indexOf(column);
        HashMap<String, String> hmap = new HashMap<String, String>();

        String line;
        BufferedReader reader = new BufferedReader(new FileReader(FS + "/" + table + ".csv"));
        while((line = reader.readLine()) != null){
            String[] values = line.split("\t");
            if (values[pos].equals(value)) {
                for (int i = 0; i < fields.length; i++) {
                    hmap.put(fields[i], values[i]);
                }
                break;
            }
        }
        reader.close();
        return hmap;
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
