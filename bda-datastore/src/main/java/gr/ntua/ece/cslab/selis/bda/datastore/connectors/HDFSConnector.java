package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;
import java.util.List;

public class HDFSConnector implements Connector {

    private String FS;

    public HDFSConnector(String FS){
        this.FS = FS;
    }

    public void put(Message message){
        System.out.println("put in HDFS " + FS);
    }

    public void put(MasterData masterData){
        System.out.println("put in HDFS " + FS);
    }

    public List<Message> getLast(Integer args){
        System.out.println("get from HDFS " + FS);
        return null;
    }

    public List<Message> getFrom(Integer args){
        System.out.println("get from HDFS " + FS);
        return null;
    }

    public List<Tuple> get(String args, String args2, String args3){
        System.out.println("get from HDFS " + FS);
        return null;
    }

    public String[] describe(String args){
        System.out.println("print HDFS schema " + FS);
        return null;
    }

    public String[] list() {
        return new String[0];
    }

    public void close(){};
}
