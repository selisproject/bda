package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import java.util.HashMap;
import java.util.List;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;

/** Methods that a connector should implement for accessing the filesystem. **/
public interface Connector {
    void put(Message args) throws Exception;
    void put(MasterData args) throws Exception;
    void put(KPIDescription args) throws Exception;
    List<Tuple> getLast(Integer args) throws Exception;
    List<Tuple> getFrom(Integer args) throws Exception;
    List<Tuple> get(String args, HashMap<String,String> args2) throws Exception;
    DimensionTable describe(String args) throws Exception;
    List<String> list();
    void close();
}
