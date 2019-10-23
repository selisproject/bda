/*
 * Copyright 2019 ICCS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
