package gr.ntua.ece.cslab.selis.bda.analytics;

import java.sql.ResultSet;

public class AnalyticsSystem {

	private static AnalyticsInstance system;

	public static AnalyticsInstance getInstance(String kpidbURL, String username,
												String password) {
		if (system == null) {
			system = new AnalyticsInstance(kpidbURL, username, password);
		}
		return system;
	}

	public static AnalyticsInstance getInstance() {
		return system;
	}



}
