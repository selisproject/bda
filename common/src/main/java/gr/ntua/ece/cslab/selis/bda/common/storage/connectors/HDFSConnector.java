package gr.ntua.ece.cslab.selis.bda.common.storage.connectors;

public class HDFSConnector implements Connector {

    private String FS;

    public HDFSConnector(String FS, String username, String password){
        this.FS = FS;
    }

    public void close(){
        throw new UnsupportedOperationException();
    }
}
