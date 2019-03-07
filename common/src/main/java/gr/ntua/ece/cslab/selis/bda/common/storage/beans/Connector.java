package gr.ntua.ece.cslab.selis.bda.common.storage.beans;

import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.PostgresqlConnector;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

@XmlRootElement(name = "ExecutionEngine")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class Connector implements Serializable {
    private transient int id;
    private String name;
    private String address;
    private Integer port;
    private String username;
    private String password;
    private String metadata;
    private boolean isExternal;

    private final static String INSERT_CONNECTOR_QUERY =
            "INSERT INTO connectors (name, address, port, username, encrypted_password, metadata, is_external) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?) " +
            "RETURNING id;";

    private final static String GET_CONNECTOR_BY_ID_QUERY =
            "SELECT id, name, address, port, username, encrypted_password, metadata, is_external " +
            "FROM connectors " +
            "WHERE id = ?;";

    private final static String GET_CONNECTOR_QUERY =
            "SELECT id, name, address, port, username, encrypted_password, metadata, is_external " +
            "FROM connectors;";

    private boolean exists = false;

    public Connector(){}

    public Connector(String name, String address, Integer port, String username, String password, String metadata, boolean isExternal) {
        this.name = name;
        this.address = address;
        this.port = port;
        this.username = username;
        this.password = password;
        this.metadata = metadata;
        this.isExternal = isExternal;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
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

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }

    public boolean isExternal() {
        return isExternal;
    }

    public void setExternal(boolean external) {
        isExternal = external;
    }

    @Override
    public String toString() {
        return "Connector{" +
                "name='" + name + '\'' +
                ", address='" + address + '\'' +
                ", port=" + port +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", metadata='" + metadata + '\'' +
                ", isExternal=" + isExternal +
                '}';
    }

    public void save() throws SQLException, UnsupportedOperationException, SystemConnectorException {
        if (!this.exists) {
            // The object does not exist, it should be inserted.
            PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
            Connection connection = connector.getConnection();

            try {
                PreparedStatement statement = connection.prepareStatement(INSERT_CONNECTOR_QUERY);

                statement.setString(1, this.name);
                statement.setString(2, this.address);
                statement.setInt(3, this.port);
                statement.setString(4, this.username);
                statement.setString(5, this.password);
                statement.setString(6, this.metadata);
                statement.setBoolean(7, this.isExternal);

                ResultSet resultSet = statement.executeQuery();

                connection.commit();

                if (resultSet.next()) {
                    this.id = resultSet.getInt("id");
                    this.exists = true;
                }
            } catch (SQLException e) {
                e.printStackTrace();
                connection.rollback();
                throw new SQLException("Failed to insert Connector object.");
            }
        } else {
            // The object exists, it should be updated.
            throw new UnsupportedOperationException("Operation not implemented.");
        }
    }

    public static Connector getConnectorInfoById(int id) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_CONNECTOR_BY_ID_QUERY);
            statement.setInt(1, id);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                Connector conn = new Connector(
                        resultSet.getString("name"),
                        resultSet.getString("address"),
                        resultSet.getInt("port"),
                        resultSet.getString("username"),
                        resultSet.getString("encrypted_password"),
                        resultSet.getString("metadata"),
                        resultSet.getBoolean("is_external")
                );

                conn.id = resultSet.getInt("id");
                conn.exists = true;

                return conn;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("Connector object not found.");
    }


    public static List<Connector> getConnectors() throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        List<Connector> connectors = new LinkedList<>();
        try {
            PreparedStatement statement = connection.prepareStatement(GET_CONNECTOR_QUERY);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                Connector conn = new Connector(
                        resultSet.getString("name"),
                        resultSet.getString("address"),
                        resultSet.getInt("port"),
                        resultSet.getString("username"),
                        resultSet.getString("encrypted_password"),
                        resultSet.getString("metadata"),
                        resultSet.getBoolean("is_external")
                );

                conn.id = resultSet.getInt("id");
                conn.exists = true;
                connectors.add(conn);
            }

            return connectors;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("Failed to retrieve Connectors info.");
    }
}
