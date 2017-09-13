package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import java.util.ArrayList;
import java.util.HashMap;

public interface Connector {
    void put(HashMap<String, String> args);
    void put(String args);
    HashMap<String, String> getLast(Integer args);
    HashMap<String, String> getFrom(Integer args);
    ArrayList<String> get(String args, String args2, String args3);
    void close();
}
