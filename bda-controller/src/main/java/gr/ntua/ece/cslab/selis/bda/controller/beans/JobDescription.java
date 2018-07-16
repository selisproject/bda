package gr.ntua.ece.cslab.selis.bda.controller.beans;

import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.PostgresqlConnector;

import java.sql.*;
import java.util.List;
import java.util.Vector;
import java.io.Serializable;
import java.lang.UnsupportedOperationException;

public class JobDescription implements Serializable {
    private final static int DEFAULT_VECTOR_SIZE = 10;

    private transient int id;

    private String name;
    private String description;
    private boolean active;
    private int messageTypeId, recipeId;
    private String job_type;

    private boolean exists = false;

    private final static String JOBS_QUERY =
        "SELECT id, name, description, active, message_type_id, recipe_id, job_type " +
        "FROM metadata.jobs";

    private final static String ACTIVE_JOBS_QUERY =
        "SELECT id, name, description, active, message_type_id, recipe_id, job_type " +
        "FROM metadata.jobs " +
        "WHERE active = true;";

    private final static String GET_JOB_BY_ID_QUERY =
        "SELECT id, name, description, active, message_type_id, recipe_id, job_type " +
        "FROM metadata.jobs " +
        "WHERE id = ?;";

    private final static String GET_JOB_BY_MESSAGE_ID_QUERY =
        "SELECT id, name, description, active, message_type_id, recipe_id, job_type " +
        "FROM metadata.jobs " +
        "WHERE message_type_id = ?;";

    private final static String INSERT_JOB_QUERY =
        "INSERT INTO metadata.jobs (name, description, active, message_type_id, recipe_id, job_type) " +
        "VALUES (?, ?, ?, ?, ?, ?) " +
        "RETURNING id;";

    public JobDescription() { }

    public JobDescription(String name, String description, boolean active,
                          int messageTypeId, int recipeId, String job_type) {
        this.name = name;
        this.description = description;
        this.active = active;
        this.messageTypeId = messageTypeId;
        this.recipeId = recipeId;
        this.job_type = job_type;
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

    public int getMessageTypeId() {
        return messageTypeId;
    }

    public void setMessageTypeId(int messageTypeId) {
        this.messageTypeId = messageTypeId;
    }

    public int getRecipeId() {
        return recipeId;
    }

    public void setRecipeId(int recipeId) {
        this.recipeId = recipeId;
    }

    public String getJob_type() {
        return job_type;
    }

    public void setJob_type(String job_type) {
        this.job_type = job_type;
    }

    @Override
    public String toString() {
        return "JobDescription{" +
                "name='" + this.name + '\'' +
                ", description='" + this.description + '\'' +
                ", active=" + this.active +
                ", messageTypeId=" + this.messageTypeId +
                ", recipeId=" + this.recipeId +
                '}';
    }

    public static List<JobDescription> getJobs(String slug) throws SQLException {

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
                    resultSet.getString("job_type")
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

    public static List<JobDescription> getActiveJobs(String slug) throws SQLException {

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
                    resultSet.getString("job_type")
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

    public static JobDescription getJobById(String slug, int id) throws SQLException {

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
                    resultSet.getString("job_type")
                );

                job.id = resultSet.getInt("id");
                job.exists = true;

                return job;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("JobDescription object not found.");
    }

    public static JobDescription getJobByMessageId(String slug, int id) throws SQLException {

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
                        resultSet.getString("job_type")
                );

                job.id = resultSet.getInt("id");
                job.exists = true;

                return job;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("JobDescription object not found.");
    }

    public void save(String slug) throws SQLException, UnsupportedOperationException {
        if (!this.exists) {
            // The object does not exist, it should be inserted.
            PostgresqlConnector connector = (PostgresqlConnector ) 
                SystemConnector.getInstance().getDTconnector(slug);

            Connection connection = connector.getConnection();

            PreparedStatement statement = connection.prepareStatement(INSERT_JOB_QUERY);

            statement.setString(1, this.name);
            statement.setString(2, this.description);
            statement.setBoolean(3, this.active);
            statement.setInt(4, this.messageTypeId);
            statement.setInt(5, this.recipeId);
            statement.setString(6, this.job_type);

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
}
