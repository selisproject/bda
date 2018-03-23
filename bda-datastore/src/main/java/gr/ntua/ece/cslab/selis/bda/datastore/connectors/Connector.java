package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import java.io.IOException;
import java.util.List;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;

/** Methods that a connector should implement for accessing the filesystem. **/
public interface Connector {
    void put(Message args) throws Exception;
    void put(MasterData args) throws Exception;
    void put(KPIDescription args) throws Exception;
    List<Tuple> getLast(Integer args) throws Exception;
    List<Tuple> getLastKPIs(String kpi_name, Integer args) throws Exception;
    List<Tuple> getKPIs(String kpi_name, List<KeyValue> args) throws Exception;
    List<Tuple> getFrom(Integer args) throws Exception;
    List<Tuple> get(String args, String args2, String args3) throws Exception;
    DimensionTable describe(String args) throws Exception;
    List<String> list();
    void close();
}
