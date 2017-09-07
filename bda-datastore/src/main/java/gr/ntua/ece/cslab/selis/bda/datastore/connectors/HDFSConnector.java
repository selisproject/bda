package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import java.util.ArrayList;

public class HDFSConnector implements Connector {

    public HDFSConnector(){

    }

    public void put(String args){
        System.out.println("put in HDFS");
    }

    public ArrayList<String> get(String args){
        System.out.println("get from HDFS");
        return new ArrayList<String>();
    }

    public void close(){};
}
