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

import java.sql.*;
import java.util.List;
import java.util.Vector;
import java.io.Serializable;
import java.lang.UnsupportedOperationException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JobDescription implements Serializable {
    private final static Logger LOGGER = Logger.getLogger(JobDescription.class.getCanonicalName());
    private final static int DEFAULT_VECTOR_SIZE = 10;

    private transient int id;

    private String name;
    private String description;
    private boolean active;
    private Integer messageTypeId;
    private int recipeId;
    private transient String jobType;
    private String resultStorage;
    private String scheduleInfo;
    private transient Integer sessionId;
    private Integer dependJobId;

    private boolean exists = false;

    private final static String CREATE_JOBS_TABLE_QUERY =
        "CREATE TABLE metadata.jobs ( " +
        "id                 SERIAL PRIMARY KEY, " +
        "name               VARCHAR(64) NOT NULL UNIQUE, " +
        "description        VARCHAR(256), " +
        "message_type_id    INTEGER REFERENCES metadata.message_type(id), " +
        "recipe_id          INTEGER REFERENCES metadata.recipes(id), " +
        "session_id         INTEGER," +
        "schedule_info      VARCHAR(256), "+
        "result_storage     VARCHAR(256)," +
        "depend_job_id      INTEGER," +
        "active             BOOLEAN DEFAULT(true) " +
        ");";

    private final static String JOBS_QUERY =
        "SELECT id, name, description, active, message_type_id, recipe_id, session_id, schedule_info, result_storage, depend_job_id" +
        "FROM metadata.jobs";


    private final static String ACTIVE_JOBS_QUERY =
        "SELECT id, name, description, active, message_type_id, recipe_id, session_id, schedule_info, result_storage, depend_job_id " +
        "FROM metadata.jobs " +
        "WHERE active = true;";

    private final static String GET_JOB_BY_ID_QUERY =
        "SELECT id, name, description, active, message_type_id, recipe_id, session_id, schedule_info, result_storage, depend_job_id " +
        "FROM metadata.jobs " +
        "WHERE id = ?;";

    private final static String GET_JOB_BY_MESSAGE_ID_QUERY =
        "SELECT id, name, description, active, message_type_id, recipe_id, session_id, schedule_info, result_storage, depend_job_id " +
        "FROM metadata.jobs " +
        "WHERE message_type_id = ?;";

    private final static String INSERT_JOB_QUERY =
        "INSERT INTO metadata.jobs (name, description, active, message_type_id, recipe_id, schedule_info, result_storage, depend_job_id) " +
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
        "RETURNING id;";

    private final static String DELETE_JOB_BY_ID_QUERY =
        "DELETE FROM metadata.jobs where id = ?;";

    private final static String SET_LIVY_SESSION_FOR_JOB_ID_QUERY =
        "UPDATE metadata.jobs SET session_id=? where id=?;";

    public JobDescription() { }

    public JobDescription(String name, String description, boolean active, Integer messageTypeId,
                          int recipeId, String resultStorage, String scheduleInfo,
                          int dependJobId) {
        this.name = name;
        this.description = description;
        this.active = active;
        this.messageTypeId = messageTypeId;
        this.recipeId = recipeId;
        this.resultStorage = resultStorage;
        this.scheduleInfo = scheduleInfo;
        this.dependJobId = dependJobId;
        this.jobType = (this.messageTypeId == null) ? "batch" : "streaming";

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

    public int getId() {
        return id;
    }

    public Integer getMessageTypeId() {
        return messageTypeId;
    }

    public void setMessageTypeId(Integer messageTypeId) {
        this.messageTypeId = messageTypeId;
    }

    public int getRecipeId() {
        return recipeId;
    }

    public void setRecipeId(int recipeId) {
        this.recipeId = recipeId;
    }

    public String getJobType() {
        return jobType;
    }

    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    public String getResultStorage() { return resultStorage; }

    public void setResultStorage(String resultStorage) { this.resultStorage = resultStorage; }

    public String getScheduleInfo() { return scheduleInfo; }

    public void setScheduleInfo(String scheduleInfo) { this.scheduleInfo = scheduleInfo; }

    public Integer getSessionId() { return sessionId; }

    public void setSessionId(Integer sessionId) { this.sessionId = sessionId; }

    public int getDependJobId() { return dependJobId; }

    public void setDependJobId(int dependJobId) { this.dependJobId = dependJobId; }

    @Override
    public String toString() {
        return "JobDescription{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", active=" + active +
                ", messageTypeId=" + messageTypeId +
                ", recipeId=" + recipeId +
                ", jobType='" + jobType + '\'' +
                ", resultStorage='" + resultStorage + '\'' +
                ", scheduleInfo='" + scheduleInfo + '\'' +
                ", sessionId=" + sessionId +
                ", dependJobId=" + dependJobId +
                ", exists=" + exists +
                '}';
    }

    public static List<JobDescription> getJobs(String slug) throws SQLException, SystemConnectorException {

        PostgresqlConnector connector = (PostgresqlConnector ) 
            SystemConnector.getInstance().getDTconnector(slug);

        Connection connection = connector.getConnection();

        Vector<JobDescription> jobs = new Vector<JobDescription>(DEFAULT_VECTOR_SIZE);

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(JOBS_QUERY);


            while (resultSet.next()) {
                JobDescription job = new JobDescription(
                    resultSet.getString("name"),
                    resultSet.getString("description"),
                    resultSet.getBoolean("active"),
                    resultSet.getInt("message_type_id"),
                    resultSet.getInt("recipe_id"),
                    resultSet.getString("result_storage"),
                    resultSet.getString("schedule_info"),
                    resultSet.getInt("depend_job_id")
                );

                job.id = resultSet.getInt("id");
                job.exists = true;

                jobs.addElement(job);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }

        return jobs;
     }

    public static List<JobDescription> getActiveJobs(String slug) throws SQLException, SystemConnectorException {

        PostgresqlConnector connector = (PostgresqlConnector ) 
            SystemConnector.getInstance().getDTconnector(slug);

        Connection connection = connector.getConnection();

        Vector<JobDescription> jobs = new Vector<JobDescription>(DEFAULT_VECTOR_SIZE);

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(ACTIVE_JOBS_QUERY);

            while (resultSet.next()) {
                JobDescription job = new JobDescription(
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getBoolean("active"),
                        resultSet.getInt("message_type_id"),
                        resultSet.getInt("recipe_id"),
                        resultSet.getString("result_storage"),
                        resultSet.getString("schedule_info"),
                        resultSet.getInt("depend_job_id")
                );

                job.id = resultSet.getInt("id");
                job.exists = true;

                jobs.addElement(job);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }

        return jobs;
     }

    public static JobDescription getJobById(String slug, int id) throws SQLException, SystemConnectorException {

        PostgresqlConnector connector = (PostgresqlConnector ) 
            SystemConnector.getInstance().getDTconnector(slug);

        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_JOB_BY_ID_QUERY);
            statement.setInt(1, id);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                JobDescription job = new JobDescription(
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getBoolean("active"),
                        resultSet.getInt("message_type_id"),
                        resultSet.getInt("recipe_id"),
                        resultSet.getString("result_storage"),
                        resultSet.getString("schedule_info"),
                        resultSet.getInt("depend_job_id")
                );

                job.id = resultSet.getInt("id");
                job.sessionId = resultSet.getInt("session_id");
                if (resultSet.wasNull()) {
                    job.sessionId = null;
                }
                job.exists = true;

                return job;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("JobDescription object not found.");
    }

    public static JobDescription getJobByMessageId(String slug, int id) throws SQLException, SystemConnectorException {

        PostgresqlConnector connector = (PostgresqlConnector ) 
            SystemConnector.getInstance().getDTconnector(slug);

        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_JOB_BY_MESSAGE_ID_QUERY);
            statement.setInt(1, id);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                JobDescription job = new JobDescription(
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getBoolean("active"),
                        resultSet.getInt("message_type_id"),
                        resultSet.getInt("recipe_id"),
                        resultSet.getString("result_storage"),
                        resultSet.getString("schedule_info"),
                        resultSet.getInt("depend_job_id")
                );

                job.id = resultSet.getInt("id");
                job.sessionId = resultSet.getInt("session_id");
                if (resultSet.wasNull()) {
                    job.sessionId = null;
                }
                job.exists = true;

                return job;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("JobDescription object not found.");
    }

    public void save(String slug) throws SQLException, UnsupportedOperationException, SystemConnectorException {
        if (!this.exists) {
            // The object does not exist, it should be inserted.
            PostgresqlConnector connector = (PostgresqlConnector ) 
                SystemConnector.getInstance().getDTconnector(slug);

            Connection connection = connector.getConnection();

            PreparedStatement statement = connection.prepareStatement(INSERT_JOB_QUERY);

            statement.setString(1, this.name);
            statement.setString(2, this.description);
            statement.setBoolean(3, this.active);

            if (this.messageTypeId == null) {
                statement.setNull(4, Types.INTEGER);
            }
            else {
                statement.setInt(4, this.messageTypeId);
            }
            statement.setInt(5, this.recipeId);
            statement.setString(6, this.scheduleInfo);
            statement.setString(7, this.resultStorage);
            statement.setInt(8, this.dependJobId);


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
        } else {
            // The object exists, it should be updated.
            throw new UnsupportedOperationException("Operation not implemented.");
        }
    }

    public static void destroy(String slug, Integer jobId) throws SystemConnectorException, SQLException {
        PostgresqlConnector connector = (PostgresqlConnector )
                SystemConnector.getInstance().getDTconnector(slug);

        Connection connection = connector.getConnection();

        PreparedStatement statement = connection.prepareStatement(DELETE_JOB_BY_ID_QUERY);

        statement.setInt(1, jobId);

        try {
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }
    }

    public static void storeSession(String slug, Integer jobId, Integer livySessionId) throws SQLException, SystemConnectorException{
        PostgresqlConnector connector = (PostgresqlConnector )
                SystemConnector.getInstance().getDTconnector(slug);

        Connection connection = connector.getConnection();

        PreparedStatement statement = connection.prepareStatement(SET_LIVY_SESSION_FOR_JOB_ID_QUERY);

        statement.setInt(1, livySessionId);
        statement.setInt(2, jobId);

        try {
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }
    }

    public static void createTable(String slug) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector )
                SystemConnector.getInstance().getDTconnector(slug);

        Connection connection = connector.getConnection();

        PreparedStatement statement = connection.prepareStatement(CREATE_JOBS_TABLE_QUERY);

        try {
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }

        LOGGER.log(Level.INFO, "SUCCESS: Create jobs table in metadata schema.");
    }

    public static boolean hasChildren(String slug, int jobId) throws SQLException, SystemConnectorException {
        List<JobDescription> jobs = getJobs(slug);
        boolean hasChildren = false;
        for (JobDescription job : jobs)
            if (job.getDependJobId() == jobId) {
                hasChildren = true;
                break;
            }
        return hasChildren;
    }

    public static void setChildrenSessionId(String slug, int jobId,
                                               int sessionId) throws SQLException, SystemConnectorException {
        List<JobDescription> jobs = getJobs(slug);
        for (JobDescription job : jobs)
            if (job.getDependJobId() == jobId)
                storeSession(slug, job.getId(), sessionId);
    }
}
