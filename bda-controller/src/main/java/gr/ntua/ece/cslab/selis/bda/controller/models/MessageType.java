package gr.ntua.ece.cslab.selis.bda.controller.models;

import java.sql.*;
import java.util.List;
import java.util.Vector;

import gr.ntua.ece.cslab.selis.bda.datastore.connectors.PostgresqlPooledDataSource;


public class MessageType {

    private static String ACTIVE_MESSAGE_NAMES_QUERY = 
        "SELECT name " +
        "FROM message_type " +
        "WHERE active = true";

    public static List<String> getActiveMessageTypeNames() {
        Connection connection = PostgresqlPooledDataSource.getInstance().getBdaConnection();

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
}
