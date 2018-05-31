package gr.ntua.ece.cslab.selis.bda.controller.beans;

import gr.ntua.ece.cslab.selis.bda.controller.connectors.BDAdbConnector;

import java.io.Serializable;
import java.sql.*;
import java.util.List;
import java.util.Vector;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "MessageType")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class MessageType implements Serializable {
    private String name;
    private String description;
    private boolean active;
    private String format;

    public MessageType(String name, String description, boolean active, String format) {
        this.name = name;
        this.description = description;
        this.active = active;
        this.format = format;
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

    private static String ACTIVE_MESSAGE_NAMES_QUERY =
            "SELECT name " +
                    "FROM message_type " +
                    "WHERE active = true";

    public static List<String> getActiveMessageTypeNames() {
        Connection connection = BDAdbConnector.getInstance().getBdaConnection();

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

    private static String INSERT_MESSAGE_QUERY =
            "INSERT INTO message_type (name,description,active,format) values (";

    public void save() {
        Connection connection = BDAdbConnector.getInstance().getBdaConnection();

        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate(INSERT_MESSAGE_QUERY+this.name+","+this.description+","+this.active+","+this.format+");");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
