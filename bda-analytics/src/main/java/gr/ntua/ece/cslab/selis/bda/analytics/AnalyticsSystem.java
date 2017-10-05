package gr.ntua.ece.cslab.selis.bda.analytics;

import java.util.Arrays;
import java.util.List;

import gr.ntua.ece.cslab.selis.bda.analytics.basicObjects.KpiPrimitiveDescriptor;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.KpiCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.catalogs.KpiPrimitiveCatalog;
import gr.ntua.ece.cslab.selis.bda.analytics.kpis.Kpi;
import gr.ntua.ece.cslab.selis.bda.analytics.kpis.KpiFactory;

public class AnalyticsSystem implements AnalyticsInterface {

	private static AnalyticsSystem system;

	private AnalyticsSystem() {
	}

	public static AnalyticsSystem getInstance() {
		if (system == null)
			system = new AnalyticsSystem();
		return system;
	}

	public KpiPrimitiveCatalog getKpiPrimitiveCatalog() {
		// TODO Auto-generated method stub
		return KpiPrimitiveCatalog.getInstance();
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
