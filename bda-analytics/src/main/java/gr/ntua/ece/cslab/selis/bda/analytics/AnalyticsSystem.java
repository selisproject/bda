package gr.ntua.ece.cslab.selis.bda.analytics;

import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.ExecutableCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.KpiCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.kpis.KpiFactory;

public class AnalyticsSystem {

	private static AnalyticsInternal system;

	private AnalyticsSystem() {

	}

	public static AnalyticsInternal getInstance() {
		if (system == null)
			system = new AnalyticsInternal();
		return system;
	}



}
