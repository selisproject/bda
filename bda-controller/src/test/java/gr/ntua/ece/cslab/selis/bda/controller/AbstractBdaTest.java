package gr.ntua.ece.cslab.selis.bda.controller;

import gr.ntua.ece.cslab.selis.bda.controller.Configuration;
import gr.ntua.ece.cslab.selis.bda.controller.connectors.BDAdbConnector;

import java.lang.StringBuffer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.SQLException;

import static org.junit.Assert.fail; 


public class AbstractBdaTest {

    private Configuration configuration = null;

    public String getTestDbUsername() {
        return this.configuration.testBackend.getDbUsername();
    }

    public String getTestDbPassword() {
        return this.configuration.testBackend.getDbPassword();
    }

    public String getTestDbBdaURL() {
        return this.configuration.testBackend.getDbUrl();
    }

    public String getTestDbLabURL() {
        return this.configuration.testBackend.getDbUrl();
    }

    AbstractBdaTest() {
        String configurationPath = System.getProperty("configuration.path");
        if (configurationPath == null || configurationPath == "") {
            fail("No `configuration.path` was provided.");
        }

        this.configuration = Configuration.parseConfiguration(
            configurationPath);

        String postgresSetupPath = System.getProperty("postgres.setup.path");
        if (postgresSetupPath == null || postgresSetupPath == "") {
            fail("No `postgres.setup.path` was provided.");
        }

        String postgresTeardownPath = System.getProperty("postgres.teardown.path");
        if (postgresTeardownPath == null || postgresTeardownPath == "") {
            fail("No `postgres.teardown.path` was provided.");
        }

        BDAdbConnector.init(
            this.getTestDbBdaURL(),
            this.getTestDbLabURL(),
            this.getTestDbUsername(),
            this.getTestDbPassword()
        );

        this.dropTablesIfExist(postgresTeardownPath);

        this.createTables(postgresSetupPath);
    }

    void dropTablesIfExist(String scriptPath) {
        String line = null;
        StringBuffer stringBuffer = new StringBuffer();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(scriptPath));

            while ((line = reader.readLine()) != null) {
                stringBuffer.append(line).append("\n");
            }

            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Connection connection = BDAdbConnector.getInstance().getBdaConnection();

        try {
            Statement statement = connection.createStatement();

            statement.execute(stringBuffer.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    void createTables(String scriptPath) {
        String line = null;
        StringBuffer stringBuffer = new StringBuffer();

        try {
            BufferedReader reader = new BufferedReader(new FileReader(scriptPath));

            while ((line = reader.readLine()) != null) {
                stringBuffer.append(line).append("\n");
            }

            if (reader != null) {
                reader.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Connection connection = BDAdbConnector.getInstance().getBdaConnection();

        try {
            Statement statement = connection.createStatement();

            statement.execute(stringBuffer.toString());

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
