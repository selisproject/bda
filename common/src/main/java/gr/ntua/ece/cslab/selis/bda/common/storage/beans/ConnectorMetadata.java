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

package gr.ntua.ece.cslab.selis.bda.common.storage.beans;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.List;

/**
 * This class represents a ConnectorMetadata which contains information
 * required for connecting with the Pub/Sub server like the username, password and
 * the available message topics i.e. datasources to listen to.
 */
@XmlRootElement(name = "ConnectorMetadata")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class ConnectorMetadata implements Serializable {
    private String username;
    private String password;
    private List<String> datasources;

    public ConnectorMetadata() {}

    /**
     * Default constructor.
     * @param username
     * @param password
     * @param datasources
     */
    public ConnectorMetadata(String username, String password, List<String> datasources) {
        this.username = username;
        this.password = password;
        this.datasources = datasources;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public List<String> getDatasources() {
        return datasources;
    }

    public void setDatasources(List<String> datasources) {
        this.datasources = datasources;
    }

    @Override
    public String toString() {
        return "ConnectorMetadata{" +
                "username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", datasources=" + datasources +
                '}';
    }
}
