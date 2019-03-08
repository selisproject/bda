package gr.ntua.ece.cslab.selis.bda.common.storage.beans;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.util.List;

@XmlRootElement(name = "ConnectorMetadata")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class ConnectorMetadata implements Serializable {
    private String username;
    private String password;
    private List<String> datasources;

    public ConnectorMetadata() {}

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
