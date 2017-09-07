package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import java.util.ArrayList;

public class PostgresqlConnector implements Connector {

    public PostgresqlConnector(){

    }

    public void put(String args){
        System.out.println("put in PostgreSQL");
    }

    public ArrayList<String> get(String args){
        System.out.println("get from PostgreSQL");
        return new ArrayList<String>();
    }

    public void close(){};
}
