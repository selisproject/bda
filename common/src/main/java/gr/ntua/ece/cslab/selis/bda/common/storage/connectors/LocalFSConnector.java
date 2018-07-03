package gr.ntua.ece.cslab.selis.bda.common.storage.connectors;

import java.io.*;

public class LocalFSConnector implements Connector {
    private String FS;

    public LocalFSConnector(LocalFSConnector conn){
        this (conn.FS,"","");
    };

    // This method creates the filesystem folder using the 'FS' parameter.
    // If this folder exists, it should be initially empty (before the bootstraping).
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
