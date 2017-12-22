package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;
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

    public List<Tuple> getLast(Integer args){
        System.out.println("get from HBase " + FS);
        return null;
    }

    public List<Tuple> getFrom(Integer args){
        System.out.println("get from HBase " + FS);
        return null;
    }

    public List<Tuple> get(String args, String args2, String args3){
        System.out.println("get from HBase " + FS);
        return null;
    }

    public DimensionTable describe(String args){
        System.out.println("print HBase schema " + FS);
        return null;
    }

    public List<String> list() {
        return null;
    }

    public void close(){};
}
