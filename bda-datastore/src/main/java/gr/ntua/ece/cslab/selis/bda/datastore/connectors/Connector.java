package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import java.io.IOException;
import java.util.List;

import gr.ntua.ece.cslab.selis.bda.datastore.beans.*;

/** Methods that a connector should implement for accessing the filesystem. **/
public interface Connector {
    void put(Message args) throws Exception;
    void put(MasterData args) throws Exception;
    List<Message> getLast(Integer args) throws IOException;
    List<Message> getFrom(Integer args);
    List<Tuple> get(String args, String args2, String args3) throws Exception;
    DimensionTable describe(String args) throws IOException;
    String[] list();
    void close();
}
