package gr.ntua.ece.cslab.selis.bda.analytics;

import java.util.Arrays;
import java.util.List;

import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.Executable;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.KpiCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.ExecutableCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.kpis.Kpi;
import gr.ntua.ece.cslab.selis.bda.analytics.kpis.KpiFactory;
import gr.ntua.ece.cslab.selis.bda.kpidb.KPIBackend;

public class AnalyticsSystem implements AnalyticsInterface {

	private static AnalyticsSystem system;
	private static KPIBackend kpidb;

	private AnalyticsSystem() {
	}

	public static AnalyticsSystem getInstance() {
		if (system == null)
			system = new AnalyticsSystem();
		return system;
	}

	public ExecutableCatalog getExecutableCatalog() {
		// TODO Auto-generated method stub
		return ExecutableCatalog.getInstance();
	}

	public KpiCatalog getKpiCatalog() {
		// TODO Auto-generated method stub
		return KpiCatalog.getInstance();
	}

	public KpiFactory getKpiFactory() {
		// TODO Auto-generated method stub
		return KpiFactory.getInstance();
	}

}
