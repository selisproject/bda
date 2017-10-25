package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;
import java.util.List;

public class PostgresqlConnector implements Connector {

    private String FS;

    public PostgresqlConnector(String FS){
        this.FS = FS;
    }

    public void put(Message message){
        System.out.println("put in PostgreSQL " + FS);
    }

    public void put(MasterData masterData){
        System.out.println("put in PostgreSQL " + FS);
    }

    public List<Message> getLast(Integer args){
        System.out.println("get from PostgreSQL " + FS);
        return null;
    }

    public List<Message> getFrom(Integer args){
        System.out.println("get from PostgreSQL " + FS);
        return null;
    }

    public List<Tuple> get(String args, String args2, String args3){
        System.out.println("get from PostgreSQL " + FS);
        return null;
    }

    public DimensionTable describe(String args){
        System.out.println("print PostgreSQL schema " + FS);
        return null;
    }

    public String[] list() {
        return new String[0];
    }

    public void close(){};
}
