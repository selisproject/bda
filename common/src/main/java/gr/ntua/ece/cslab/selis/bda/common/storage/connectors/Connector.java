package gr.ntua.ece.cslab.selis.bda.common.storage.connectors;

/** General methods that a storage engine connector should implement. **/
public interface Connector {
    /**
     * Close the connection.
     */
    void close();
}
