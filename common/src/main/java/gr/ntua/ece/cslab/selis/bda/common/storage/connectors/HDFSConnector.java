package gr.ntua.ece.cslab.selis.bda.common.storage.connectors;

import java.net.URI;
import java.io.IOException;
import gr.ntua.ece.cslab.selis.bda.common.Configuration;

public class HDFSConnector implements Connector {
    private org.apache.hadoop.fs.FileSystem fileSystem;
    private org.apache.hadoop.conf.Configuration hadoopConfiguration;

    /**
     * Creates a new `HDFSConnector` instance and checks its availability.
     *
     * TODO: Should use `usename`, `password` for connection.
     *
     * @param fs            The URL of the HDFS.
     * @param username      The username to connect to HDFS.
     * @param password      The password to connect to HDFS.
     * @param configuration The Global BDA configuration.
     * @throws IOException
     */
    public HDFSConnector(String fs, String username, String password, 
                         Configuration configuration) throws IOException {
        try {
            hadoopConfiguration = new org.apache.hadoop.conf.Configuration();

            hadoopConfiguration.set(
                "fs.defaultFS", configuration.storageBackend.getHDFSMasterURL()
            );

            URI uri = URI.create(configuration.storageBackend.getHDFSMasterURL());

            fileSystem = org.apache.hadoop.fs.FileSystem.get(uri, hadoopConfiguration);
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void close(){
        throw new UnsupportedOperationException();
    }

    public org.apache.hadoop.fs.FileSystem getFileSystem() {
        return fileSystem;
    }
}
