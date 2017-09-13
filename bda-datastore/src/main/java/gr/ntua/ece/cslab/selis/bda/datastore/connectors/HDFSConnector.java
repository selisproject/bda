package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import java.util.ArrayList;
import java.util.HashMap;

public class HDFSConnector implements Connector {

    private String FS;

    public HDFSConnector(String FS){
        this.FS = FS;
    }

    public void put(HashMap<String, String> row){
        System.out.println("put in HDFS " + FS);
    }

    public void put(String file){
        System.out.println("put in HDFS " + FS);
    }

    public HashMap<String, String> getLast(Integer args){
        System.out.println("get from HDFS " + FS);
        return new HashMap<String, String>();
    }

    public HashMap<String, String> getFrom(Integer args){
        System.out.println("get from HDFS " + FS);
        return new HashMap<String, String>();
    }

    public ArrayList<String> get(String args, String args2, String args3){
        System.out.println("get from HDFS " + FS);
        return new ArrayList<String>();
    }

    public void close(){};
}
