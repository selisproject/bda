package gr.ntua.ece.cslab.selis.bda.controller.beans;

import gr.ntua.ece.cslab.selis.bda.controller.connectors.BDAdbConnector;

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

    private boolean exists = false;

    private final static String ACTIVE_JOBS_QUERY =
        "SELECT id, name, description, active, message_type_id, recipe_id " +
        "FROM jobs " +
        "WHERE active = true";

    private final static String GET_JOB_BY_ID_QUERY =
        "SELECT id, name, description, active, message_type_id, recipe_id " +
        "FROM jobs " +
        "WHERE id = ?";

    private final static String GET_JOB_BY_MESSAGE_ID_QUERY =
        "SELECT id, name, description, active, message_type_id, recipe_id " +
        "FROM jobs " +
        "WHERE message_type_id = ?";

    private final static String INSERT_JOB_QUERY =
        "INSERT INTO jobs (name, description, active, message_type_id, recipe_id) " +
        "VALUES (?, ?, ?, ?, ?) " +
        "RETURNING id";

    public JobDescription() { }

    public JobDescription(String name, String description, boolean active,
                          int messageTypeId, int recipeId) {
        this.name = name;
        this.description = description;
        this.active = active;
        this.messageTypeId = messageTypeId;
        this.recipeId = recipeId;
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

    public static List<JobDescription> getActiveJobs() {
        Connection connection = BDAdbConnector.getInstance().getBdaConnection();

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
                    resultSet.getInt("recipe_id")
                );

                job.id = resultSet.getInt("id");
                job.exists = true;

                jobs.addElement(job);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return jobs;
     }

    public static JobDescription getJobById(int id) throws SQLException {
        Connection connection = BDAdbConnector.getInstance().getBdaConnection();

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
                    resultSet.getInt("recipe_id")
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

    public static JobDescription getJobByMessageId(int id) throws SQLException {
        Connection connection = BDAdbConnector.getInstance().getBdaConnection();

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
                        resultSet.getInt("recipe_id")
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

    public void save() throws SQLException, UnsupportedOperationException {
        if (!this.exists) {
            // The object does not exist, it should be inserted.
            Connection connection = BDAdbConnector.getInstance().getBdaConnection();

            PreparedStatement statement = connection.prepareStatement(INSERT_JOB_QUERY);

            statement.setString(1, this.name);
            statement.setString(2, this.description);
            statement.setBoolean(3, this.active);
            statement.setInt(4, this.messageTypeId);
            statement.setInt(5, this.recipeId);

            ResultSet resultSet = statement.executeQuery();

            // TODO: Verify if we want autocommit. Set it explicitely.
            // connection.commit();

            if (resultSet.next()) {
                this.id = resultSet.getInt("id");
            }
        } else {
            // The object exists, it should be updated.
            throw new UnsupportedOperationException("Operation not implemented.");
        }
     }
}
