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

package gr.ntua.ece.cslab.selis.bda.datastore.beans;

import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnectorException;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.PostgresqlConnector;

import java.io.Serializable;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.JsonParser;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "MessageType")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class MessageType implements Serializable {
    /**
     * This class implements the different message types which the BDA can receive. All incoming messages must be of a
     * defined type with specific structure and content to be accepted and processed.
     */
    private final static Logger LOGGER = Logger.getLogger(MessageType.class.getCanonicalName());
    private final static int DEFAULT_VECTOR_SIZE = 10;

    private transient Integer id;
    private String name;
    private String description;
    private boolean active;
    private String format;
    private Integer externalConnectorId;
    private String externalDatasource;

    private boolean exists = false;

    public MessageType() { }

    /**
     * Default constructor
     * @param name message type name
     * @param description message type description
     * @param active a boolean switch indicating whether a message type is active or not
     * @param format the actual message format, used for validation
     * @param externalConnectorId the ID of the connector responsible for pushing this message type into the BDA
     * @param externalDatasource the external datasource that sends this message
     */
    public MessageType(String name, String description, boolean active, String format, Integer externalConnectorId, String externalDatasource) {
        this.name = name;
        this.description = description;
        this.active = active;
        this.format = format;
        this.externalConnectorId = externalConnectorId;
        this.externalDatasource = externalDatasource;
    }

    public Integer getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public String getFormat() { return format; }

    public void setFormat(String format) {
        this.format = format;
    }

    public Integer getExternalConnectorId() { return externalConnectorId; }

    public void setExternalConnectorId(Integer externalConnectorId) { this.externalConnectorId = externalConnectorId; }

    public String getExternalDatasource() { return externalDatasource; }

    public void setExternalDatasource(String external_datasource) { this.externalDatasource = external_datasource; }

    public List<String> getMessageColumns() {
        List<String> columns = new ArrayList<>();
        columns.addAll(new JsonParser().parse(this.format).getAsJsonObject().keySet());
        return columns;
    }

    /**
     * @return String representation of MessageType objects
     */
    @Override
    public String toString() {
        return "MessageType{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", active=" + active +
                ", format='" + format + '\'' +
                ", external_connector_id='" + externalConnectorId + '\'' +
                ", external_datasource='" + externalDatasource + '\'' +
                '}';
    }

    private final static String CREATE_MESSAGE_TYPES_TABLE_QUERY =
        "CREATE TABLE metadata.message_type ( " +
        "id                    SERIAL PRIMARY KEY, " +
        "name                  VARCHAR(64) NOT NULL UNIQUE, " +
        "description           VARCHAR(256), " +
        "active                BOOLEAN DEFAULT(true), " +
        "format                VARCHAR, " +
        "external_connector_id INTEGER, " +
        "external_datasource   VARCHAR(256)" +
        ");";

    private final static String MESSAGE_TYPES_QUERY =
        "SELECT id, name, description, active, format, external_connector_id, external_datasource " +
        "FROM metadata.message_type;";

    private final static String ACTIVE_MESSAGE_TYPES_QUERY =
         "SELECT id, name, description, active, format, external_connector_id, external_datasource " +
         "FROM metadata.message_type " +
         "WHERE active = true and external_connector_id is null;";

    private final static String ACTIVE_EXTERNAL_MESSAGE_TYPES_QUERY =
         "SELECT id, name, description, active, format, external_connector_id, external_datasource " +
         "FROM metadata.message_type " +
         "WHERE active = true and external_connector_id is not null;";

    private final static String EXIST_ACTIVE_EXTERNAL_MESSAGE_TYPES_QUERY =
         "SELECT count(*) as num " +
         "FROM metadata.message_type " +
         "WHERE active = true and external_connector_id is not null;";

    private final static String ACTIVE_MESSAGE_NAMES_QUERY =
        "SELECT name " +
        "FROM metadata.message_type " +
        "WHERE active = true;";

    private final static String GET_MESSAGE_BY_NAME_QUERY =
        "SELECT id, name, description, active, format, external_connector_id, external_datasource " +
        "FROM metadata.message_type " +
        "WHERE name = ?;";

    private final static String GET_MESSAGE_BY_ID_QUERY =
        "SELECT * " +
        "FROM metadata.message_type " +
        "WHERE id = ?;";

    private final static String INSERT_MESSAGE_QUERY =
        "INSERT INTO metadata.message_type (name,description,active,format,external_connector_id,external_datasource) " +
        "VALUES (?, ?, ?, ?, ?, ?) " +
        "RETURNING id;";

    private final static String UPDATE_MESSAGE_QUERY =
        "UPDATE metadata.message_type " +
        "SET name = ?, description = ?, active = ?, format = ?, external_connector_id = ?, external_datasource = ? " +
        "WHERE id = ?";

    private final static String DELETE_MESSAGE_QUERY =
        "DELETE FROM metadata.message_type WHERE id = ?;";

    /**
     * @param slug The SCN slug as defined in the configuration process
     * @return List of all message types existing in the specified SCN
     * @throws SQLException
     * @throws SystemConnectorException
     */
    public static List<MessageType> getMessageTypes(String slug) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) 
            SystemConnector.getInstance().getDTconnector(slug);

        Connection connection = connector.getConnection();

        Vector<MessageType> messageTypes = new Vector<MessageType>(DEFAULT_VECTOR_SIZE);

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(MESSAGE_TYPES_QUERY);

            while (resultSet.next()) {
                MessageType messageType = new MessageType(
                    resultSet.getString("name"),
                    resultSet.getString("description"),
                    resultSet.getBoolean("active"),
                    resultSet.getString("format"),
                    resultSet.getInt("external_connector_id"),
                    resultSet.getString("external_datasource")
                );
                messageType.exists = true;
                messageType.id = resultSet.getInt("id");

                messageTypes.addElement(messageType);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }

        return messageTypes;
     }

    /**
     * @param slug The SCN slug as defined in the configuration process
     * @param external
     * @return List of all active message types existing in the specified SCN
     * @throws SQLException
     * @throws SystemConnectorException
     */
    public static List<MessageType> getActiveMessageTypes(String slug, boolean external) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector )
                SystemConnector.getInstance().getDTconnector(slug);

        Connection connection = connector.getConnection();

        Vector<MessageType> messageTypes = new Vector<MessageType>(DEFAULT_VECTOR_SIZE);

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(ACTIVE_MESSAGE_TYPES_QUERY);
            if (external)
                resultSet = statement.executeQuery(ACTIVE_EXTERNAL_MESSAGE_TYPES_QUERY);

            while (resultSet.next()) {
                MessageType messageType = new MessageType(
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getBoolean("active"),
                        resultSet.getString("format"),
                        resultSet.getInt("external_connector_id"),
                        resultSet.getString("external_datasource")
                );
                messageType.exists = true;
                messageType.id = resultSet.getInt("id");

                messageTypes.addElement(messageType);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }

        return messageTypes;
    }

    /**
     * @param slug The SCN slug as defined in the configuration process
     * @return List of all message types existing in the specified SCN
     * @throws SystemConnectorException
     */
    public static List<String> getActiveMessageTypeNames(String slug) throws SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector) SystemConnector.getInstance().getDTconnector(slug);
        Connection connection = connector.getConnection();

        Vector<String> messageTypeNames = new Vector<String>(10);

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(ACTIVE_MESSAGE_NAMES_QUERY);

            while (resultSet.next()) {
                messageTypeNames.addElement(resultSet.getString("name"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return messageTypeNames;
    }

    public static Boolean checkExternalMessageTypesExist(String slug) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector) SystemConnector.getInstance().getDTconnector(slug);
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(EXIST_ACTIVE_EXTERNAL_MESSAGE_TYPES_QUERY);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                Integer count = resultSet.getInt("num");
                return (count > 0);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("Malformed query to find messages with external connectors.");
    }

    /**
     * @param slug The SCN slug as defined in the configuration process
     * @param name message type name
     * @return The message type existing in the specified SCN which matches the provided name
     * @throws SQLException
     * @throws SystemConnectorException
     */
    public static MessageType getMessageByName(String slug, String name) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector) SystemConnector.getInstance().getDTconnector(slug);
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_MESSAGE_BY_NAME_QUERY);
            statement.setString(1, name);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                MessageType msg = new MessageType(
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getBoolean("active"),
                        resultSet.getString("format"),
                        resultSet.getInt("external_connector_id"),
                        resultSet.getString("external_datasource")
                );
                msg.exists = true;
                msg.id = resultSet.getInt("id");

                return msg;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("MessageType object not found.");
    }

    /**
     * @param slug The SCN slug as defined in the configuration process
     * @param id The message type ID
     * @return The message type existing in the specified SCN which matches the provided ID
     * @throws SQLException
     * @throws SystemConnectorException
     */
    public static MessageType getMessageById(String slug, int id) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector) SystemConnector.getInstance().getDTconnector(slug);
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_MESSAGE_BY_ID_QUERY);
            statement.setInt(1, id);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                MessageType msg = new MessageType(
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getBoolean("active"),
                        resultSet.getString("format"),
                        resultSet.getInt("external_connector_id"),
                        resultSet.getString("external_datasource")
                );
                msg.exists = true;
                msg.id = resultSet.getInt("id");

                return msg;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("MessageType object not found.");
    }

    public void save(String slug) throws SQLException, SystemConnectorException {
        if (!this.exists) {
            PostgresqlConnector connector = (PostgresqlConnector)
                    SystemConnector.getInstance().getDTconnector(slug);

            Connection connection = connector.getConnection();

            PreparedStatement statement = connection.prepareStatement(INSERT_MESSAGE_QUERY);

            statement.setString(1, this.name);
            statement.setString(2, this.description);
            statement.setBoolean(3, this.active);
            statement.setString(4, this.format);
            if (this.externalConnectorId != null)
                statement.setInt(5, this.externalConnectorId);
            else
                statement.setNull(5, Types.INTEGER);
            statement.setString(6, this.externalDatasource);

            try {
                ResultSet resultSet = statement.executeQuery();
                if (resultSet.next()) {
                    this.id = resultSet.getInt("id");

                }

                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        }
        else {
            PostgresqlConnector connector = (PostgresqlConnector)
                    SystemConnector.getInstance().getDTconnector(slug);

            Connection connection = connector.getConnection();

            PreparedStatement statement = connection.prepareStatement(UPDATE_MESSAGE_QUERY);

            statement.setString(1, this.name);
            statement.setString(2, this.description);
            statement.setBoolean(3, this.active);
            statement.setString(4, this.format);
            if (this.externalConnectorId != null)
                statement.setInt(5, this.externalConnectorId);
            else
                statement.setNull(5, Types.INTEGER);
            statement.setString(6, this.externalDatasource);
            statement.setInt(7, Integer.valueOf(this.id));

            try {
                statement.executeUpdate();

                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        }

        LOGGER.log(Level.INFO, "SUCCESS: Insert Into message_type. ID: "+this.id);
    }

    public static void destroy(String slug, int id) throws SQLException, UnsupportedOperationException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector )
                SystemConnector.getInstance().getDTconnector(slug);
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(DELETE_MESSAGE_QUERY);
            statement.setInt(1, id);

            statement.executeUpdate();
            connection.commit();
            return;
        } catch (SQLException e) {
            e.printStackTrace();
            connection.rollback();
        }

        throw new SQLException("MessageType object not found.");
    }

    public static void createTable(String slug) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector )
                SystemConnector.getInstance().getDTconnector(slug);

        Connection connection = connector.getConnection();

        PreparedStatement statement = connection.prepareStatement(CREATE_MESSAGE_TYPES_TABLE_QUERY);

        try {
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }

        LOGGER.log(Level.INFO, "SUCCESS: Create message_type table in metadata schema.");
    }
}
