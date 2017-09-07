package gr.ntua.ece.cslab.selis.bda.datastore.connectors;

import java.util.ArrayList;

public interface Connector {
    void put(String args);
    ArrayList<String> get(String args);
    void close();
}
