package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.Connector;
import java.util.HashMap;
import java.util.List;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;
import gr.ntua.ece.cslab.selis.bda.datastore.DatastoreException;

/** Methods that a connector should implement for accessing the filesystem. **/
public interface DatastoreConnector {
    String put(Message args) throws Exception;
    void put(MasterData args) throws Exception;
    List<Tuple> getLast(Integer args) throws Exception;
    List<Tuple> getFrom(Integer args) throws Exception;
    List<Tuple> get(String args, HashMap<String,String> args2) throws Exception;
    DimensionTable describe(String args) throws Exception;
    List<String> list();
    void createMetaTables() throws DatastoreException, UnsupportedOperationException;
}
