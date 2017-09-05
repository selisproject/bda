package gr.ntua.ece.cslab.selis.bda.analytics;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class CreateKPI {

	public static void main(String[] args) {
		// load properties file from classpath
		Properties prop = new Properties();
		InputStream input = null;
		String dblocation = null;
		String database = null;
		String dbuser = null;
		String dbpassword = null;
		String kpitable=null;
		try {

			String filename = "analytics.properties";
			input = CreateKPI.class.getClassLoader().getResourceAsStream(filename);
			if (input == null) {
				System.out.println("Sorry, unable to find " + filename);
				return;
			}

			// load a properties file from class path, inside static method
			prop.load(input);

			// get the property value and print it out
			dblocation = new String(prop.getProperty("dblocation"));
			database = new String(prop.getProperty("database"));
			dbuser = new String(prop.getProperty("dbuser"));
			dbpassword = new String(prop.getProperty("dbpassword"));
			kpitable = new String(prop.getProperty("kpitable"));

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
		System.out.println("-------- PostgreSQL " + "JDBC Connection Testing ------------");
		try {
			Class.forName("org.postgresql.Driver");

		} catch (ClassNotFoundException e) {
			System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
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
		if (connection != null) {
			System.out.println("You made it, take control your database now!");
		} else {
			System.out.println("Failed to make connection!");
		}

		// make sure autocommit is off
		try {
			connection.setAutoCommit(false);

			Statement st = connection.createStatement();

			// Turn use of the cursor on.
			st.setFetchSize(50);
			ResultSet rs = st.executeQuery("SELECT * FROM "+kpitable);
			while (rs.next()) {
				System.out.println("a row was returned.");
			    System.out.print(rs.getString(1)+" || ");
			    System.out.print(rs.getString(2)+" || ");
			    System.out.println(rs.getString(3));


			}
			rs.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
