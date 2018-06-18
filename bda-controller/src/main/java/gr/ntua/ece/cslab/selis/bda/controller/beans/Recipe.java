package gr.ntua.ece.cslab.selis.bda.controller.beans;

import gr.ntua.ece.cslab.selis.bda.controller.connectors.BDAdbConnector;
import org.json.JSONObject;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;
import java.sql.*;
import java.util.List;
import java.util.Vector;
import java.lang.UnsupportedOperationException;

public class Recipe implements Serializable {
    private final static int DEFAULT_VECTOR_SIZE = 10;

    private int id;
    private String name;
    private String description;
    private String executable_path;
    private int engine_id;
    private String args;

    private boolean exists = false;

    private final static String ALL_RECIPES_QUERY = 
        "SELECT * " +
        "FROM recipes";

    private final static String INSERT_RECIPE_QUERY = 
        "INSERT INTO recipes (name, description, executable_path, engine_id, args) " +
        "VALUES (?, ?, ?, ? ,?::json) " +
        "RETURNING id";

    private final static String GET_RECIPE_BY_ID =
            "SELECT * FROM recipes WHERE id = ?;";

    private final static String GET_RECIPE_BY_NAME =
            "SELECT * FROM recipes WHERE name = ?;";

    private final static String SET_EXECUTABLE_PATH =
            "UPDATE recipes SET executable_path = ? WHERE id = ?;";

    public Recipe() {}

    public Recipe(String name, String description, String executable_path, int engine_id, String args) {
        this.name = name;
        this.description = description;
        this.executable_path = executable_path;
        this.engine_id = engine_id;
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

    public String getExecutable_path() {
        return executable_path;
    }

    public void setExecutable_path(String executable_path) {
        this.executable_path = executable_path;
    }

    public int getEngine_id() {
        return engine_id;
    }

    public void setEngine_id(int engine_id) {
        this.engine_id = engine_id;
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
                ", executable_path='" + executable_path + '\'' +
                ", engine_id=" + engine_id +
                ", args=" + args +
                ", exists=" + exists +
                '}';
    }

    public static List<Recipe> getRecipes() {
        Connection connection = BDAdbConnector.getInstance().getBdaConnection();

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
        }

        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return recipes;
     }

    public static Recipe getRecipeById(int id) {
        Connection connection = BDAdbConnector.getInstance().getBdaConnection();

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
                connection.close();
                return recipe;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }


    public static Recipe getRecipeByName(String name) {
        Connection connection = BDAdbConnector.getInstance().getBdaConnection();

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

                connection.close();
                return recipe;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }


    public void updateBinaryPath() throws SQLException, UnsupportedOperationException {
        // The object does not exist, it should be inserted.
        Connection connection = BDAdbConnector.getInstance().getBdaConnection();

        PreparedStatement statement = connection.prepareStatement(SET_EXECUTABLE_PATH);

        statement.setString(1, this.executable_path);
        statement.setInt(2, this.id);

        statement.executeUpdate();

        connection.close();
    }

    public void save() throws SQLException, UnsupportedOperationException {
        if (!this.exists) {
            // The object does not exist, it should be inserted.
            Connection connection = BDAdbConnector.getInstance().getBdaConnection();

            PreparedStatement statement = connection.prepareStatement(INSERT_RECIPE_QUERY);

            statement.setString(1, this.name);
            statement.setString(2, this.description);
            statement.setString(3, this.executable_path);
            statement.setInt(4, Integer.valueOf(this.engine_id));
            statement.setString(5, this.args.toString());


            ResultSet resultSet = statement.executeQuery();

            //connection.commit();

            if (resultSet.next()) {
                this.id = resultSet.getInt("id");
                System.out.println(this.id);
            }

            connection.close();
        } else {
            // The object exists, it should be updated.
            throw new UnsupportedOperationException("Operation not implemented.");
        }
     }

}
