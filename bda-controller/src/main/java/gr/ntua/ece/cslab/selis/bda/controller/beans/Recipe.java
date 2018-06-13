package gr.ntua.ece.cslab.selis.bda.controller.beans;

import gr.ntua.ece.cslab.selis.bda.controller.connectors.BDAdbConnector;

import java.sql.*;
import java.util.List;
import java.util.Vector;
import java.lang.UnsupportedOperationException;

public class Recipe {
    private final static int DEFAULT_VECTOR_SIZE = 10;

    private int id;
    private String name;
    private String description;

    private boolean exists = false;

    private final static String ALL_RECIPES_QUERY = 
        "SELECT id, name, description " +
        "FROM jobs";

    private final static String INSERT_RECIPE_QUERY = 
        "INSERT INTO recipes (name, description) " +
        "VALUES (?, ?) " +
        "RETURNING id";


    public Recipe(String name, String description) {
        this.name = name;
        this.description = description;
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

    public static List<Recipe> getRecipes() {
        Connection connection = BDAdbConnector.getInstance().getBdaConnection();

        Vector<Recipe> recipes = new Vector<Recipe>(DEFAULT_VECTOR_SIZE);

        try {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(ALL_RECIPES_QUERY);

            while (resultSet.next()) {
                Recipe recipe = new Recipe(
                    resultSet.getString("name"),
                    resultSet.getString("description")
                );

                recipe.id = resultSet.getInt("id");
                recipe.exists = true;

                recipes.addElement(recipe);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return recipes;
     }

    public void save() throws SQLException, UnsupportedOperationException {
        if (!this.exists) {
            // The object does not exist, it should be inserted.
            Connection connection = BDAdbConnector.getInstance().getBdaConnection();

            PreparedStatement statement = connection.prepareStatement(INSERT_RECIPE_QUERY);

            statement.setString(1, this.name);
            statement.setString(2, this.description);

            ResultSet resultSet = statement.executeQuery();

            connection.commit();

            if (resultSet.next()) {
                this.id = resultSet.getInt("id");
            }
        } else {
            // The object exists, it should be updated.
            throw new UnsupportedOperationException("Operation not implemented.");
        }
     }

}
