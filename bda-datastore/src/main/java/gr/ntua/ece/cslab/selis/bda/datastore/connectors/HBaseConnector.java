package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import java.util.ArrayList;

public class HBaseConnector implements Connector {

    public HBaseConnector(){

    }

    public void put(String args){
        System.out.println("put in HBase");
    }

    public ArrayList<String> get(String args){
        System.out.println("get from HBase");
        return new ArrayList<String>();
    }

    public void close(){};
}
