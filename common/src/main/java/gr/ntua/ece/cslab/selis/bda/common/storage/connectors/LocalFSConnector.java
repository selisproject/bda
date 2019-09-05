package gr.ntua.ece.cslab.selis.bda.common.storage.connectors;

import java.io.*;

public class LocalFSConnector implements Connector {
    private String FS;

    /**
     * Creates a new `LocalFSConnector` instance. This method creates the
     * filesystem folder using the 'FS' parameter. If this folder exists,
     * it should be initially empty (before the bootstraping).
     * @param FS            The local folder to create.
     * @param username      The username.
     * @param password      The password.
     */
    public LocalFSConnector(String FS, String username, String password){
        this.FS = FS;

        File fs = new File(FS);
        if (!fs.exists())
            fs.mkdir();
    }

    public String getFS() {
        return FS;
    }

    public void close(){
        throw new UnsupportedOperationException();
    }
}
