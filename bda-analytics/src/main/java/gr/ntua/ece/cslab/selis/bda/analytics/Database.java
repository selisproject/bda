package gr.ntua.ece.cslab.selis.bda.analytics;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.io.IOException;

class Database {
	private String dblocation = null;
	private String database = null;
	private String dbuser = null;
	private String dbpassword = null;
	private String kpi_catalogue = null;
	private String kpi_db = null;

	private static String filename = "analytics.properties";

	public Database() {
		try {
			Properties prop = new Properties();

			// load a properties file from class path, inside static method
			prop.load(getClass().getClassLoader().getResourceAsStream(filename));

			// get the property value and print it out
			dblocation = new String(prop.getProperty("dblocation"));
			database = new String(prop.getProperty("database"));
			dbuser = new String(prop.getProperty("dbuser"));
			dbpassword = new String(prop.getProperty("dbpassword"));
			kpi_catalogue = new String(prop.getProperty("kpitable"));
			kpi_db = new String(prop.getProperty("kpi_db"));

		} catch (IOException ex) {
			ex.printStackTrace();
			System.out.println(
					"Unable to read properties file. Please make sure " + filename + " is configured correctly.");

		}
	}

	public void fetchCatalogue() {
		try {
			Class.forName("org.postgresql.Driver");

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return;
		}
		System.out.println("PostgreSQL JDBC Driver Registered!");
		Connection connection = null;
		try {
			connection = DriverManager.getConnection("jdbc:postgresql://" + dblocation + ":5432/" + database, dbuser,
					dbpassword);
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}
		if (connection == null) {
			System.out.println("Failed to make connection!");
		}

		// make sure autocommit is off
		try {
			connection.setAutoCommit(false);

			Statement st = connection.createStatement();

			// Turn use of the cursor on.
			st.setFetchSize(50);
			ResultSet rs = st.executeQuery("SELECT * FROM "+kpi_catalogue);
			while (rs.next()) {
				System.out.println("a row was returned.");
			    System.out.print(rs.getString(1)+" || ");
			    System.out.print(rs.getString(2)+" || ");
			    System.out.print(rs.getString(4)+" || ");
			    System.out.println(rs.getString(3));


			}
			rs.close();
			connection.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		
	}

	public void storeKPI(int kpi_id, double value, Data data) {}


}
