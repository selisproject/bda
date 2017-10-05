package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

/** Methods that a connector should implement for accessing the filesystem. **/
public interface Connector {
    void put(HashMap<String, String> args) throws IOException;
    void put(String args) throws Exception;
    HashMap<String, String>[] getLast(Integer args) throws IOException;
    ArrayList<HashMap<String, String>> getFrom(Integer args);
    ArrayList<HashMap<String, String>> get(String args, String args2, String args3) throws IOException;
    String[] describe(String args) throws IOException;
    void close();
}
