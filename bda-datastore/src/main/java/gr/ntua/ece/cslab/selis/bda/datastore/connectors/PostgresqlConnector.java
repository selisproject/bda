package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import java.util.ArrayList;
import java.util.HashMap;

public class PostgresqlConnector implements Connector {

    private String FS;

    public PostgresqlConnector(String FS){
        this.FS = FS;
    }

    public void put(HashMap<String, String> row){
        System.out.println("put in PostgreSQL " + FS);
    }

    public void put(String file){
        System.out.println("put in PostgreSQL " + FS);
    }

    public HashMap<String, String>[] getLast(Integer args){
        System.out.println("get from PostgreSQL " + FS);
        return new HashMap[0];
    }

    public ArrayList<HashMap<String, String>> getFrom(Integer args){
        System.out.println("get from PostgreSQL " + FS);
        return new ArrayList<HashMap<String, String>>();
    }

    public ArrayList<HashMap<String, String>> get(String args, String args2, String args3){
        System.out.println("get from PostgreSQL " + FS);
        return new ArrayList<HashMap<String, String>>();
    }

    public String[] describe(String args){
        System.out.println("print PostgreSQL schema " + FS);
        return new String[0];
    }

    public void close(){};
}
