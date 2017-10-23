package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;
import java.util.ArrayList;
import java.util.List;

public class HBaseConnector implements Connector {

    private String FS;

    public HBaseConnector(String FS){
        this.FS = FS;
    }

    public void put(Message message){
        System.out.println("put in HBase " + FS);
    }

    public void put(MasterData masterData){
        System.out.println("put in HBase " + FS);
    }

    public List<Message> getLast(Integer args){
        System.out.println("get from HBase " + FS);
        return new ArrayList<>();
    }

    public List<Message> getFrom(Integer args){
        System.out.println("get from HBase " + FS);
        return new ArrayList<>();
    }

    public List<Tuple> get(String args, String args2, String args3){
        System.out.println("get from HBase " + FS);
        return new ArrayList<>();
    }

    public String[] describe(String args){
        System.out.println("print HBase schema " + FS);
        return new String[0];
    }

    public String[] list() {
        return new String[0];
    }

    public void close(){};
}
