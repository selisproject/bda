package gr.ntua.ece.cslab.selis.bda.datastore.beans;

import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.PostgresqlConnector;

import java.io.Serializable;
import java.sql.*;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "MessageType")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class MessageType implements Serializable {
    private final static Logger LOGGER = Logger.getLogger(MessageType.class.getCanonicalName());
    private final static int DEFAULT_VECTOR_SIZE = 10;

    private transient Integer id;
    private String name;
    private String description;
    private boolean active;
    private String format;

    public MessageType() { }

    public MessageType(String name, String description, boolean active, String format) {
        this.name = name;
        this.description = description;
        this.active = active;
        this.format = format;
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

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    @Override
    public String toString() {
        return "MessageType{" +
                "name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", active=" + active +
                ", format='" + format + '\'' +
                '}';
    }

    private final static String CREATE_MESSAGE_TYPES_TABLE_QUERY =
        "CREATE TABLE metadata.message_type ( " +
        "id          SERIAL PRIMARY KEY, " +
        "name        VARCHAR(64) NOT NULL UNIQUE, " +
        "description VARCHAR(256), " +
        "active      BOOLEAN DEFAULT(true), " +
        "format      VARCHAR " +
        ");";

    private final static String MESSAGE_TYPES_QUERY =
        "SELECT id, name, description, active, format " +
        "FROM metadata.message_type;";

    private final static String ACTIVE_MESSAGE_NAMES_QUERY =
        "SELECT name " +
        "FROM metadata.message_type " +
        "WHERE active = true;";

    private final static String GET_MESSAGE_BY_NAME_QUERY =
        "SELECT id, name, description, active, format " +
        "FROM metadata.message_type " +
        "WHERE name = ?;";

    private final static String GET_MESSAGE_BY_ID_QUERY =
        "SELECT * " +
        "FROM metadata.message_type " +
        "WHERE id = ?;";

    private final static String INSERT_MESSAGE_QUERY =
        "INSERT INTO metadata.message_type (name,description,active,format) " +
        "VALUES (?, ?, ?, ?) " +
        "RETURNING id;";

    public static List<MessageType> getMessageTypes(String slug) throws SQLException {
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
                    resultSet.getString("format")
                );

                messageType.id = resultSet.getInt("id");

                messageTypes.addElement(messageType);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }

        return messageTypes;
     }

    public static List<String> getActiveMessageTypeNames(String slug) {
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

    public static MessageType getMessageByName(String slug, String name) throws SQLException {
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
                        resultSet.getString("format")
                );

                msg.id = resultSet.getInt("id");

                return msg;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("JobDescription object not found.");
    }

    public static MessageType getMessageById(String slug, int id) throws SQLException {
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
                        resultSet.getString("format")
                );

                msg.id = resultSet.getInt("id");

                return msg;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("JobDescription object not found.");
    }

    public void save(String slug) throws SQLException {
        PostgresqlConnector connector = (PostgresqlConnector )
            SystemConnector.getInstance().getDTconnector(slug);

        Connection connection = connector.getConnection();

        PreparedStatement statement = connection.prepareStatement(INSERT_MESSAGE_QUERY);

        statement.setString(1, this.name);
        statement.setString(2, this.description);
        statement.setBoolean(3, this.active);
        statement.setString(4, this.format);

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

        LOGGER.log(Level.INFO, "SUCCESS: Insert Into message_type. ID: "+this.id);
    }

    public static void createTable(String slug) throws SQLException {
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
