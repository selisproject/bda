package gr.ntua.ece.cslab.selis.bda.common.storage.beans;

import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.PostgresqlConnector;

import java.io.Serializable;
import java.sql.*;
import java.util.List;
import java.util.Vector;
import java.lang.UnsupportedOperationException;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Recipe implements Serializable {
    private final static Logger LOGGER = Logger.getLogger(Recipe.class.getCanonicalName());
    private final static int DEFAULT_VECTOR_SIZE = 10;

    private transient int id;
    private String name;
    private String description;
    private String executablePath;
    private int engineId;
    private String args;

    private boolean exists = false;

    private final static String CREATE_RECIPES_TABLE_QUERY =
        "CREATE TABLE metadata.recipes ( " +
        "id                  SERIAL PRIMARY KEY, " +
        "name                VARCHAR(64) NOT NULL UNIQUE, " +
        "description         VARCHAR(256), " +
        "executable_path     VARCHAR(512) NOT NULL UNIQUE, " +
        "engine_id           INTEGER NOT NULL, " +
        "args                VARCHAR(512)" +
        ");";

    private final static String ALL_RECIPES_QUERY = 
        "SELECT * " +
        "FROM metadata.recipes";

    private final static String INSERT_RECIPE_QUERY = 
        "INSERT INTO metadata.recipes (name, description, executable_path, engine_id, args) " +
        "VALUES (?, ?, ?, ? ,?) " +
        "RETURNING id";

    private final static String UPDATE_RECIPE_QUERY = 
        "UPDATE metadata.recipes " +
        "SET name = ?, description = ?, executable_path = ?, engine_id = ?, args = ? " +
        "WHERE id = ?";

    private final static String GET_RECIPE_BY_ID =
         "SELECT * FROM metadata.recipes WHERE id = ?;";

    private final static String GET_RECIPE_BY_NAME =
         "SELECT * FROM metadata.recipes WHERE name = ?;";

    private final static String SET_EXECUTABLE_PATH =
         "UPDATE metadata.recipes SET executable_path = ? WHERE id = ?;";

    public Recipe() {}

    public Recipe(String name, String description, String executablePath, int engineId, String args) {
        this.name = name;
        this.description = description;
        this.executablePath = executablePath;
        this.engineId = engineId;
        this.args = args;
    }

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

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getExecutablePath() {
        return executablePath;
    }

    public void setExecutablePath(String executablePath) {
        this.executablePath = executablePath;
    }

    public int getEngineId() {
        return engineId;
    }

    public void setEngineId(int engineId) {
        this.engineId = engineId;
    }

    public String getArgs() {
        return args;
    }

    public void setArgs(String args) {
        this.args = args;
    }

    public boolean isExists() {
        return exists;
    }

    public void setExists(boolean exists) {
        this.exists = exists;
    }

    @Override
    public String toString() {
        return "Recipe{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", executablePath='" + executablePath + '\'' +
                ", engineId=" + engineId +
                ", args=" + args +
                ", exists=" + exists +
                '}';
    }

    public static List<Recipe> getRecipes(String slug) throws SQLException {

        PostgresqlConnector connector = (PostgresqlConnector ) 
            SystemConnector.getInstance().getDTconnector(slug);

        Connection connection = connector.getConnection();

        Vector<Recipe> recipes = new Vector<Recipe>(DEFAULT_VECTOR_SIZE);

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(ALL_RECIPES_QUERY);

            while (resultSet.next()) {

                Recipe recipe;
                recipe = new Recipe(
                    resultSet.getString("name"),
                    resultSet.getString("description"),
                    resultSet.getString("executable_path"),
                    resultSet.getInt("engine_id"),
                    resultSet.getString("args")
                );

                recipe.id = resultSet.getInt("id");
                recipe.exists = true;

                recipes.addElement(recipe);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }

        return recipes;
     }

    public static Recipe getRecipeById(String slug, int id) {
        PostgresqlConnector connector = (PostgresqlConnector ) 
            SystemConnector.getInstance().getDTconnector(slug);

        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_RECIPE_BY_ID);
            statement.setInt(1, id);
            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {

                Recipe recipe;
                recipe = new Recipe(
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getString("executable_path"),
                        resultSet.getInt("engine_id"),
                        resultSet.getString("args")
                );

                recipe.id = resultSet.getInt("id");
                recipe.exists = true;
                return recipe;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }


    public static Recipe getRecipeByName(String slug, String name) {
        PostgresqlConnector connector = (PostgresqlConnector ) 
            SystemConnector.getInstance().getDTconnector(slug);

        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_RECIPE_BY_NAME);
            statement.setString(1, name);
            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {

                Recipe recipe;
                recipe = new Recipe(
                    resultSet.getString("name"),
                    resultSet.getString("description"),
                    resultSet.getString("executable_path"),
                    resultSet.getInt("engine_id"),
                    resultSet.getString("args")
                );

                recipe.id = resultSet.getInt("id");
                recipe.exists = true;

                return recipe;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    public void save(String slug) throws SQLException, UnsupportedOperationException {
        if (!this.exists) {
            // The object does not exist, it should be inserted.
            PostgresqlConnector connector = (PostgresqlConnector ) 
                SystemConnector.getInstance().getDTconnector(slug);

            Connection connection = connector.getConnection();

            PreparedStatement statement = connection.prepareStatement(INSERT_RECIPE_QUERY);

            statement.setString(1, this.name);
            statement.setString(2, this.description);
            statement.setString(3, this.executablePath);
            statement.setInt(4, Integer.valueOf(this.engineId));
            statement.setString(5, this.args);

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
            PostgresqlConnector connector = (PostgresqlConnector ) 
                SystemConnector.getInstance().getDTconnector(slug);

            Connection connection = connector.getConnection();

            PreparedStatement statement = connection.prepareStatement(UPDATE_RECIPE_QUERY);

            statement.setString(1, this.name);
            statement.setString(2, this.description);
            statement.setString(3, this.executablePath);
            statement.setInt(4, Integer.valueOf(this.engineId));
            statement.setString(5, this.args.toString());
            statement.setInt(6, Integer.valueOf(this.id));

            try {
                statement.executeUpdate();

                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            }
        }
        LOGGER.log(Level.INFO, "SUCCESS: Insert Into recipes. ID: "+this.id);
     }

    public static void createTable(String slug) throws SQLException {
        PostgresqlConnector connector = (PostgresqlConnector )
                SystemConnector.getInstance().getDTconnector(slug);

        Connection connection = connector.getConnection();

        PreparedStatement statement = connection.prepareStatement(CREATE_RECIPES_TABLE_QUERY);

        try {
            statement.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        }

        LOGGER.log(Level.INFO, "SUCCESS: Create recipes table in metadata schema.");
    }
}
