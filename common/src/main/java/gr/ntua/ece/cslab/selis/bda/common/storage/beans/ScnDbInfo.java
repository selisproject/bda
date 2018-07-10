package gr.ntua.ece.cslab.selis.bda.common.storage.beans;

import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnector;
import gr.ntua.ece.cslab.selis.bda.common.storage.connectors.PostgresqlConnector;

import java.sql.*;
import java.io.Serializable;
import java.lang.UnsupportedOperationException;
import java.util.LinkedList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "ScnDbInfo")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class ScnDbInfo implements Serializable {

    private transient int id;
    private String slug;
    private String name;
    private String description;
    private String dbname;

    private final static String GET_SCN_QUERY =
            "SELECT id, slug, name, description, dbname " +
                    "FROM scn_db_info;";

    private final static String GET_SCN_BY_ID_QUERY =
        "SELECT id, slug, name, description, dbname " +
        "FROM scn_db_info " +
        "WHERE id = ?;";

    private final static String GET_SCN_BY_SLUG_QUERY =
        "SELECT id, slug, name, description, dbname " +
        "FROM scn_db_info " +
        "WHERE slug = ?;";

    private final static String INSERT_SCN_QUERY =
        "INSERT INTO scn_db_info (slug, name, description, dbname) " +
        "VALUES (?, ?, ?, ?) " +
        "RETURNING id;";

    private boolean exists = false;

    public ScnDbInfo() { }

    public ScnDbInfo(String slug, String name, String description, String dbname) {
        this.slug = slug;
        this.name = name;
        this.description = description;
        this.dbname = dbname;
    }

    public int getId() {
        return id;
    }

    public String getSlug() {
        return this.slug;
    }

    public String getName() {
        return this.name;
    }

    public String getDescription() {
        return this.description;
    }

    public String getDbname() {
        return this.dbname;
    }

    public void setSlug(String slug) {
        this.slug = slug;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setDbname(String dbname) {
        this.dbname = dbname;
    }

    @Override
    public String toString() {
        return "ScnDbInfo {" +
                "slug='" + slug + "', " +
                "name='" + name + "', " +
                "description='" + description + "', " +
                "dbname='" + dbname + 
                "}";
    }

    public void save() throws SQLException, UnsupportedOperationException {
        if (!this.exists) {
            // The object does not exist, it should be inserted.
            PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
            Connection connection = connector.getConnection();

            PreparedStatement statement = connection.prepareStatement(INSERT_SCN_QUERY);

            statement.setString(1, this.slug);
            statement.setString(2, this.name);
            statement.setString(3, this.description);
            statement.setString(4, this.dbname);

            ResultSet resultSet = statement.executeQuery();

            connection.commit();

            if (resultSet.next()) {
                this.id     = resultSet.getInt("id");
                this.exists = true;
            }
        } else {
            // The object exists, it should be updated.
            throw new UnsupportedOperationException("Operation not implemented.");
        }
     }

    public static ScnDbInfo getScnDbInfoById(int id) throws SQLException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_SCN_BY_ID_QUERY);
            statement.setInt(1, id);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                ScnDbInfo scn = new ScnDbInfo(
                    resultSet.getString("slug"),
                    resultSet.getString("name"),
                    resultSet.getString("description"),
                    resultSet.getString("dbname")
                );

                scn.id = resultSet.getInt("id");
                scn.exists = true;

                return scn;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("ScnDbInfo object not found.");
    }

    public static ScnDbInfo getScnDbInfoBySlug(String slug) throws SQLException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_SCN_BY_SLUG_QUERY);
            statement.setString(1, slug);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                ScnDbInfo scn = new ScnDbInfo(
                    resultSet.getString("slug"),
                    resultSet.getString("name"),
                    resultSet.getString("description"),
                    resultSet.getString("dbname")
                );

                scn.id = resultSet.getInt("id");
                scn.exists = true;

                return scn;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("ScnDbInfo object not found.");
    }

    public static List<ScnDbInfo> getScnDbInfo() throws SQLException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        List<ScnDbInfo> scns = new LinkedList<>();
        try {
            PreparedStatement statement = connection.prepareStatement(GET_SCN_QUERY);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                ScnDbInfo scn = new ScnDbInfo(
                        resultSet.getString("slug"),
                        resultSet.getString("name"),
                        resultSet.getString("description"),
                        resultSet.getString("dbname")
                );

                scn.id = resultSet.getInt("id");
                scn.exists = true;
                scns.add(scn);
            }

            return scns;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("Failed to retrieve ScnDb info.");
    }
}
