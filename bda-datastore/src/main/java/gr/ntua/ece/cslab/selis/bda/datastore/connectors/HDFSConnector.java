package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;

import java.util.HashMap;
import java.util.List;

public class HDFSConnector implements Connector {

    private String FS;

    public HDFSConnector(String FS){
        this.FS = FS;
    }

    public String put(Message message){
        throw new java.lang.UnsupportedOperationException();
    }

    public void put(MasterData masterData){
        throw new java.lang.UnsupportedOperationException();
    }

    public List<Tuple> getLast(Integer args){
        throw new java.lang.UnsupportedOperationException();
    }

    public List<Tuple> getFrom(Integer args){
        throw new java.lang.UnsupportedOperationException();
    }

    public List<Tuple> get(String args,  HashMap<String,String> filters){
        throw new java.lang.UnsupportedOperationException();
    }

    public DimensionTable describe(String args){
        throw new java.lang.UnsupportedOperationException();
    }

    public List<String> list() {
        throw new java.lang.UnsupportedOperationException();
    }

    public void close(){
        throw new java.lang.UnsupportedOperationException();
    }
}
