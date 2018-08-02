package gr.ntua.ece.cslab.selis.bda.common.storage.beans;


import gr.ntua.ece.cslab.selis.bda.common.storage.SystemConnector;
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

@XmlRootElement(name = "ExecutionEngine")
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class ExecutionEngine implements Serializable {


    private int id;
    private String name;
    private String engine_path;
    private boolean local_engine;
    private String args;

    // Query to fetch all engines from db
    private static final String GET_ENGINES = "SELECT * FROM execution_engines;";

    // Query to fetch specific engine from its id
    private static final String GET_ENGINE_BY_ID = "SELECT * FROM execution_engines WHERE id = ?;";


    // Empty constructor
    public ExecutionEngine() {}

    public ExecutionEngine(String name, String engine_path, boolean local_engine, String args) {
        this.id = id;
        this.name = name;
        this.engine_path = engine_path;
        this.local_engine = local_engine;
        this.args = args;
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

    public String getEngine_path() {
        return engine_path;
    }

    public void setEngine_path(String engine_path) {
        this.engine_path = engine_path;
    }

    public boolean isLocal_engine() {
        return local_engine;
    }

    public void setLocal_engine(boolean local_engine) {
        this.local_engine = local_engine;
    }

    public String getArgs() {
        return args;
    }

    public void setArgs(String args) {
        this.args = args;
    }

    @Override
    public String toString() {
        return "ExecutionEngines{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", engine_path='" + engine_path + '\'' +
                ", local_engine=" + local_engine +
                ", args='" + args + '\'' +
                '}';
    }

    public static List<ExecutionEngine> getEngines() throws SQLException {
        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        List<ExecutionEngine> executionEngines = new LinkedList<>();
        try {
            PreparedStatement statement = connection.prepareStatement(GET_ENGINES);
            ResultSet resultSet = statement.executeQuery();

            while (resultSet.next()) {
                ExecutionEngine engine = new ExecutionEngine(
                        resultSet.getString("name"),
                        resultSet.getString("engine_path"),
                        resultSet.getBoolean("local_engine"),
                        resultSet.getString("args")
                );

                engine.id = resultSet.getInt("id");
                executionEngines.add(engine);
            }

            return executionEngines;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("Failed to retrieve ExecutionEngine info.");
    }

    public static ExecutionEngine getEngineById(int id) throws SQLException {

        PostgresqlConnector connector = (PostgresqlConnector ) SystemConnector.getInstance().getBDAconnector();
        Connection connection = connector.getConnection();

        try {
            PreparedStatement statement = connection.prepareStatement(GET_ENGINE_BY_ID);
            statement.setInt(1, id);

            ResultSet resultSet = statement.executeQuery();

            if (resultSet.next()) {
                ExecutionEngine engine = new ExecutionEngine(
                        resultSet.getString("name"),
                        resultSet.getString("engine_path"),
                        resultSet.getBoolean("local_engine"),
                        resultSet.getString("args")
                );

                engine.id = resultSet.getInt("id");

                return engine;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        throw new SQLException("ExecutionEngine object not found.");

    }
}
