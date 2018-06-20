package gr.ntua.ece.cslab.selis.bda.controller.connectors;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class BDAdbConnector {

    private static BDAdbConnector dataSource = null;

    private static HikariConfig bdaDataSourceConfig = null;
    private static HikariConfig labDataSourceConfig = null;

    private HikariDataSource bdaPooledDataSource = null;
    private HikariDataSource labPooledDataSource = null;

    private BDAdbConnector() {
        this.bdaPooledDataSource = new HikariDataSource(bdaDataSourceConfig);
        this.labPooledDataSource = new HikariDataSource(labDataSourceConfig);
    }

    public static void init(String bdaJdbcURL, String labJdbcURL, String username, String password) {
        bdaDataSourceConfig = new HikariConfig();

        bdaDataSourceConfig.setJdbcUrl(bdaJdbcURL);
        bdaDataSourceConfig.setUsername(username);
        bdaDataSourceConfig.setPassword(password);

        labDataSourceConfig = new HikariConfig();

        labDataSourceConfig.setJdbcUrl(labJdbcURL);
        labDataSourceConfig.setUsername(username);
        labDataSourceConfig.setPassword(password);
    }

    public static BDAdbConnector getInstance() {
        if (dataSource == null) {
            dataSource = new BDAdbConnector();
        }

        return dataSource;
    }

    public Connection getBdaConnection() {
        Connection connection = null;

        while (connection == null) {
            try {
                connection = this.bdaPooledDataSource.getConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public Connection getLabConnection() {
        Connection connection = null;

        try {
            connection = this.labPooledDataSource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return connection;
    }


}
