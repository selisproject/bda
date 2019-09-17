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

@XmlRootElement(name = "ExecutionLanguage")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class ExecutionLanguage implements Serializable {


    private int id;
    private String name;

    // Query to fetch all languages from db
    private static final String GET_LANGUAGES = "SELECT * FROM execution_languages;";

    // Query to fetch specific language from its id
    private static final String GET_LANGUAGE_BY_ID = "SELECT * FROM execution_languages WHERE id = ?;";


    // Empty constructor
    public ExecutionLanguage() {}

    public ExecutionLanguage(String name) {
        this.id = id;
        this.name = name;
    }

    // Getters and Setters
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "ExecutionLanguages{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }

    public static List<ExecutionLanguage> getLanguages() throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        List<ExecutionLanguage> executionLanguages = new LinkedList<>();
        try {
            PreparedStatement statement = connection.prepareStatement(GET_LANGUAGES);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                ExecutionLanguage language = new ExecutionLanguage(
                        resultSet.getString("name")
                );

                language.id = resultSet.getInt("id");
                executionLanguages.add(language);
            }

            return executionLanguages;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("Failed to retrieve ExecutionLanguage info.");
    }

    public static ExecutionLanguage getLanguageById(int id) throws SQLException, SystemConnectorException {

        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_LANGUAGE_BY_ID);
            statement.setInt(1, id);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                ExecutionLanguage language = new ExecutionLanguage(
                        resultSet.getString("name")
                );

                language.id = resultSet.getInt("id");

                return language;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("ExecutionLanguage object not found.");

    }
}
