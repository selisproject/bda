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

@XmlRootElement(name = "ScnDbInfo")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class SharedRecipe implements Serializable {

    private transient int id;
    private String name;
    private String description;
    private String executable_path;
    private int engine_id;
    private String args;
    private int pair_recipe_id;

    // Fetch all shared recipes from db
    private static String GET_SHARED_RECIPES =
            "SELECT * FROM shared_recipes;";

    // Fetch a specific shared recipe info using its unique id
    private static String GET_SHARED_RECIPE_BY_ID =
            "SELECT * " +
            "FROM shared_recipes" +
            "WHERE id=?;";

    // Fetch a specific shared recipe info using its unique name
    private static String GET_SHARED_RECIPE_BY_NAME =
            "SELECT * " +
            "FROM shared_recipes" +
            "WHERE name=?;";


    public SharedRecipe() { }

    public SharedRecipe(String name, String description, String executable_path, int engine_id, String args, int pair_recipe_id) {
        this.name = name;
        this.description = description;
        this.executable_path = executable_path;
        this.engine_id = engine_id;
        this.args = args;
        this.pair_recipe_id = pair_recipe_id;
    }

    @Override
    public String toString() {
        return "SharedRecipe{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", description='" + description + '\'' +
                ", executable_path='" + executable_path + '\'' +
                ", engine_id='" + engine_id + '\'' +
                ", args='" + args + '\'' +
                ", pair_recipe_id=" + pair_recipe_id +
                '}';
    }

    public int getId() { return id; }

    public void setId(int id) { this.id = id; }

    public String getName() { return name; }

    public void setName(String name) { this.name = name; }

    public String getDescription() { return description; }

    public void setDescription(String description) { this.description = description; }

    public String getExecutable_path() { return executable_path; }

    public void setExecutable_path(String executable_path) { this.executable_path = executable_path; }

    public int getEngine_id() { return engine_id; }

    public void setEngine_id(int engine_id) { this.engine_id = engine_id; }

    public String getArgs() { return args; }

    public void setArgs(String args) { this.args = args; }

    public int getPair_recipe_id() { return pair_recipe_id; }

    public void setPair_recipe_id(int pair_recipe_id) { this.pair_recipe_id = pair_recipe_id; }

    // Method to return all available shared recipes
    public static List<SharedRecipe> getSharedRecipes() throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        List<SharedRecipe> shRecipes = new LinkedList<>();
        try {
            PreparedStatement statement = connection.prepareStatement(GET_SHARED_RECIPES);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                SharedRecipe shRecipe = new SharedRecipe(
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getString("executable_path"),
                        resultSet.getInt("engine_id"),
                        resultSet.getString("String args"),
                        resultSet.getInt("pair_recipe_id")
                );

                shRecipe.id = resultSet.getInt("id");
                shRecipes.add(shRecipe);
            }

            return shRecipes;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("Failed to retrieve SharedRecipe info.");
    }


    // Method to return a specific shared Recipe using its unique ID
    public static SharedRecipe getSharedRecipeById(int id) throws SQLException, SystemConnectorException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_SHARED_RECIPE_BY_ID);
            statement.setInt(1, id);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                SharedRecipe shRecipe = new SharedRecipe(
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getString("executable_path"),
                        resultSet.getInt("engine_id"),
                        resultSet.getString("String args"),
                        resultSet.getInt("pair_recipe_id")
                );

                shRecipe.id = resultSet.getInt("id");
                return shRecipe;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("SharedRecipe object not found.");
    }


    public static SharedRecipe getSharedRecipeByName(String name) throws SQLException, SystemConnectorException{
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_SHARED_RECIPE_BY_ID);
            statement.setString(1, name);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                SharedRecipe shRecipe = new SharedRecipe(
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getString("executable_path"),
                        resultSet.getInt("engine_id"),
                        resultSet.getString("String args"),
                        resultSet.getInt("pair_recipe_id")
                );

                shRecipe.id = resultSet.getInt("id");
                return shRecipe;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("SharedRecipe object not found.");
    }
}
