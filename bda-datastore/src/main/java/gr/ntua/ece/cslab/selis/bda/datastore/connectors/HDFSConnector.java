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

    @Override
    public void put(KPIDescription args) throws Exception {

    }

    public List<Tuple> getLast(Integer args){
        System.out.println("get from HDFS " + FS);
        return null;
    }

    @Override
    public List<Tuple> getLastKPIs(String kpi_name, Integer args) throws Exception {
        return null;
    }

    @Override
    public List<Tuple> getKPIs(String kpi_name, List<KeyValue> args) throws Exception {
        return null;
    }

    public List<Tuple> getFrom(Integer args){
        System.out.println("get from HDFS " + FS);
        return null;
    }

    public List<Tuple> get(String args, String args2, String args3){
        System.out.println("get from HDFS " + FS);
        return null;
    }

    public DimensionTable describe(String args){
        System.out.println("print HDFS schema " + FS);
        return null;
    }

    public List<String> list() {
        return null;
    }

    public void close(){};
}
